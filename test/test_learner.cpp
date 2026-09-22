// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Kai FAN(fankai@baidu.com)

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <butil/logging.h>
#include <butil/files/file_path.h>
#include <butil/file_util.h>
#include <brpc/closure_guard.h>
#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include "braft/node.h"
#include "braft/learner.h"
#include "braft/enum.pb.h"
#include "braft/errno.pb.h"
#include "braft/snapshot_throttle.h" 
#include "braft/snapshot_executor.h" 
#include "braft/snapshot_fetcher.h"

namespace braft {
extern bvar::Adder<int64_t> g_num_nodes;
DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
DECLARE_bool(raft_enable_append_entries_cache);
DECLARE_int32(raft_max_append_entries_cache_size);
}

using braft::raft_mutex_t;

bool g_dont_print_apply_log = false;

class MockFSM : public braft::StateMachine {
public:
    MockFSM(const butil::EndPoint& address_)
        : address(address_)
        , applied_index(0)
        , snapshot_index(0)
        , _on_start_following_times(0)
        , _on_stop_following_times(0)
        , _leader_term(-1)
        , enable_fetch_snapshot(false)
    {
        pthread_mutex_init(&mutex, NULL);
    }
    virtual ~MockFSM() {
        pthread_mutex_destroy(&mutex);
    }

    butil::EndPoint address;
    std::vector<butil::IOBuf> logs;
    pthread_mutex_t mutex;
    int64_t applied_index;
    int64_t snapshot_index;
    int64_t _on_start_following_times;
    int64_t _on_stop_following_times;
    volatile int64_t _leader_term;
    bool enable_fetch_snapshot;
    butil::EndPoint snapshot_service_addr;

    void lock() {
        pthread_mutex_lock(&mutex);
    }

    void unlock() {
        pthread_mutex_unlock(&mutex);
    }

    void on_leader_start(int64_t term) {
        _leader_term = term;
        LOG(INFO) << "on_leader_start, addr:" << address << ", term:" << term;
    }
    void on_leader_stop(const braft::LeaderChangeContext&) {
        _leader_term = -1;
    }

    bool is_leader() { return _leader_term > 0; }

    virtual void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            LOG_IF(TRACE, !g_dont_print_apply_log) << "addr " << address 
                                                   << " apply " << iter.index()
                                                   << " data " << iter.data();
            ::brpc::ClosureGuard guard(iter.done());
            lock();
            logs.push_back(iter.data());
            unlock();
            applied_index = iter.index();
        }
    }

    virtual void on_shutdown() {
        LOG(INFO) << "addr " << address << " shutdowned";
    }

    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        if (enable_fetch_snapshot) {
            return fetch_snapshot(writer, done);
        }
        std::string file_path = writer->get_path();
        file_path.append("/data");
        brpc::ClosureGuard done_guard(done);

        LOG(INFO) << "on_snapshot_save to " << file_path;

        int fd = ::creat(file_path.c_str(), 0644);
        if (fd < 0) {
            LOG(ERROR) << "create file failed, path: " << file_path << " err: " << berror();
            done->status().set_error(EIO, "Fail to create file");
            return;
        }
        lock();
        // write snapshot and log to file
        for (size_t i = 0; i < logs.size(); i++) {
            butil::IOBuf data = logs[i];
            int len = data.size();
            int ret = write(fd, &len, sizeof(int));
            CHECK_EQ(ret, 4);
            data.cut_into_file_descriptor(fd, len);
        }
        ::close(fd);
        snapshot_index = applied_index;
        unlock();
        writer->add_file("data");
    }

    void fetch_snapshot(braft::SnapshotWriter* writer, braft::Closure* done) {
        braft::SnapshotFetcher snapshot_fetcher(braft::NodeId("unittest", braft::PeerId(address, 0)));
        braft::SynchronizedClosure fetch_snapshot_done;
        snapshot_fetcher.fetch_snapshot(braft::PeerId(snapshot_service_addr, 0), writer, 
                &fetch_snapshot_done);
        fetch_snapshot_done.wait();
        done->status().swap(fetch_snapshot_done.status());
        done->Run();
    }

    virtual int on_snapshot_load(braft::SnapshotReader* reader) {
        std::string file_path = reader->get_path();
        file_path.append("/data");

        LOG(INFO) << "on_snapshot_load from " << file_path;

        int fd = ::open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            LOG(ERROR) << "creat file failed, path: " << file_path << " err: " << berror();
            return EIO;
        }

        lock();
        logs.clear();
        while (true) {
            int len = 0;
            int ret = read(fd, &len, sizeof(int));
            if (ret <= 0) {
                break;
            }

            butil::IOPortal data;
            data.append_from_file_descriptor(fd, len);
            logs.push_back(data);
        }

        ::close(fd);
        unlock();
        return 0;
    }

    virtual int on_snapshot_fetched(braft::SnapshotReader* reader) {
        std::string file_path = reader->get_path();
        file_path.append("/data");

        LOG(INFO) << "on_snapshot_fetched from " << file_path;
        return 0;
    }

    virtual void on_start_following(const braft::LeaderChangeContext& start_following_context) {
        LOG(INFO) << "address " << address << " start following new leader: " 
                   <<  start_following_context;
        ++_on_start_following_times;
    }

    virtual void on_stop_following(const braft::LeaderChangeContext& stop_following_context) {
        LOG(INFO) << "address " << address << " stop following old leader: " 
                   <<  stop_following_context;
        ++_on_stop_following_times;
    }

    virtual void on_configuration_committed(const ::braft::Configuration& conf, int64_t index) {
        LOG(INFO) << "address " << address << " commit conf: " << conf << " at index " << index;
    }

    virtual void on_reset() {
        lock();
        logs.clear();
        unlock();
    }
};

class ExpectClosure : public braft::Closure {
public:
    void Run() {
        if (_expect_err_code >= 0) {
            ASSERT_EQ(status().error_code(), _expect_err_code) 
                << _pos << " : " << status();
        }
        if (_cond) {
            _cond->signal();
        }
        delete this;
    }
private:
    ExpectClosure(bthread::CountdownEvent* cond, int expect_err_code, const char* pos)
        : _cond(cond), _expect_err_code(expect_err_code), _pos(pos) {}

    ExpectClosure(bthread::CountdownEvent* cond, const char* pos)
        : _cond(cond), _expect_err_code(-1), _pos(pos) {}

    bthread::CountdownEvent* _cond;
    int _expect_err_code;
    const char* _pos;
};

typedef ExpectClosure ShutdownClosure;
typedef ExpectClosure ApplyClosure;
typedef ExpectClosure AddPeerClosure;
typedef ExpectClosure RemovePeerClosure;
typedef ExpectClosure SnapshotClosure;

#define NEW_SHUTDOWNCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_APPLYCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_ADDPEERCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_REMOVEPEERCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_CHANGEPEERCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_SNAPSHOTCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))

class Cluster {
public:
    Cluster(const std::string& name, const std::vector<braft::PeerId>& peers,
            const std::set<braft::PeerId>& learners,
            int32_t election_timeout_ms = 3000)
        : _name(name), _peers(peers), _learner_ids(learners)
        , _election_timeout_ms(election_timeout_ms) {

        int64_t throttle_throughput_bytes = 10 * 1024 * 1024;
        int64_t check_cycle = 10;
        _throttle = new braft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
    }
    ~Cluster() {
        stop_all();
    }

    int start(const butil::EndPoint& listen_addr, bool learner_auto_load_applied_logs = true, bool empty_peers = false,
              int snapshot_interval_s = 0) {
        if (_server_map[listen_addr] == NULL) {
            brpc::Server* server = new brpc::Server();
            if (braft::add_service(server, listen_addr) != 0 
                    || server->Start(listen_addr, NULL) != 0) {
                LOG(ERROR) << "Fail to start raft service";
                delete server;
                return -1;
            }
            _server_map[listen_addr] = server;
        }

        braft::NodeOptions options;
        options.election_timeout_ms = _election_timeout_ms;
        options.snapshot_interval_s = snapshot_interval_s;
        options.fetch_log_timeout_s = 5;
        if (!empty_peers) {
            options.initial_conf = braft::Configuration(_peers);
        }
        MockFSM* fsm = new MockFSM(listen_addr);
        options.fsm = fsm;
        options.node_owns_fsm = true;
        butil::string_printf(&options.log_uri, "local://./data/%s/log",
                            butil::endpoint2str(listen_addr).c_str());
        butil::string_printf(&options.raft_meta_uri, "local://./data/%s/stable",
                            butil::endpoint2str(listen_addr).c_str());
        butil::string_printf(&options.snapshot_uri, "local://./data/%s/snapshot",
                            butil::endpoint2str(listen_addr).c_str());
        
        scoped_refptr<braft::SnapshotThrottle> tst(_throttle);
        options.snapshot_throttle = &tst;

        options.catchup_margin = 2;
        if (_learner_ids.count(braft::PeerId(listen_addr, 0)) > 0) {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            braft::Learner* node = new braft::Learner(_name, braft::PeerId(listen_addr, 0));
            options.learner_auto_load_applied_logs = learner_auto_load_applied_logs;
            int ret = node->init(options);
            if (ret != 0) {
                LOG(WARNING) << "init_node failed, server: " << listen_addr;
                delete node;
                return ret;
            } else {
                _learners.push_back(node);
                _learner_fsms.push_back(fsm);
                LOG(INFO) << "init node " << listen_addr;
            }
        } else {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            braft::Node* node = new braft::Node(_name, braft::PeerId(listen_addr, 0));
            int ret = node->init(options);
            if (ret != 0) {
                LOG(WARNING) << "init_node failed, server: " << listen_addr;
                delete node;
                return ret;
            } else {
                _nodes.push_back(node);
                _node_fsms.push_back(fsm);
                LOG(INFO) << "init node " << listen_addr;
            }
        }
        return 0;
    }

    int stop_learner(const butil::EndPoint& listen_addr) {
        
        bthread::CountdownEvent cond;
        braft::Learner* node = remove_learner(listen_addr);
        if (node) {
            node->shutdown(NEW_SHUTDOWNCLOSURE(&cond));
            cond.wait();
            node->join();
        }

        if (_server_map[listen_addr] != NULL) {
            delete _server_map[listen_addr];
            _server_map.erase(listen_addr);
        }
        _server_map.erase(listen_addr);
        delete node;
        return node ? 0 : -1;
    }

    int stop_node(const butil::EndPoint& listen_addr) {
        
        bthread::CountdownEvent cond;
        braft::Node* node = remove_node(listen_addr);
        if (node) {
            node->shutdown(NEW_SHUTDOWNCLOSURE(&cond));
            cond.wait();
            node->join();
        }

        if (_server_map[listen_addr] != NULL) {
            delete _server_map[listen_addr];
            _server_map.erase(listen_addr);
        }
        _server_map.erase(listen_addr);
        delete node;
        return node ? 0 : -1;
    }

    void stop_all() {
        std::vector<butil::EndPoint> addrs;
        all_nodes(&addrs);
        for (size_t i = 0; i < addrs.size(); i++) {
            stop_node(addrs[i]);
        }
        all_learners(&addrs);
        for (size_t i = 0; i < addrs.size(); i++) {
            stop_learner(addrs[i]);
        }
    }

    void clean(const butil::EndPoint& listen_addr) {
        std::string data_path;
        butil::string_printf(&data_path, "./data/%s",
                            butil::endpoint2str(listen_addr).c_str());

        if (!butil::DeleteFile(butil::FilePath(data_path), true)) {
            LOG(ERROR) << "delete path failed, path: " << data_path;
        }
    }

    braft::Node* leader() {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        braft::Node* node = NULL;
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (_nodes[i]->is_leader() &&
                    _node_fsms[i]->_leader_term == _nodes[i]->_impl->_current_term) {
                node = _nodes[i];
                break;
            }
        }
        return node;
    }

    void followers(std::vector<braft::Node*>* nodes) {
        nodes->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            braft::NodeStatus status;
            _nodes[i]->get_status(&status);
            if (status.state == braft::STATE_FOLLOWER) {
                nodes->push_back(_nodes[i]);
            }
        }
    }

    MockFSM* get_fsm(const butil::EndPoint& addr) {
        for (size_t i = 0; i < _node_fsms.size(); i++) {
            if (_node_fsms[i]->address == addr) {
                return _node_fsms[i];
            }
        }
        return NULL;
    }

    void learners(std::vector<braft::Learner*>* nodes) {
        nodes->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        *nodes = _learners;
    }

    bool learner_fsm_clear() {
        return _learner_fsms[0]->logs.size() == 0;
    }
    
    void wait_leader() {
        while (true) {
            braft::Node* node = leader();
            if (node) {
                return;
            } else {
                usleep(100 * 1000);
            }
        }
    }

    void check_node_status() {
        std::vector<braft::Node*> nodes;
        {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            for (size_t i = 0; i < _nodes.size(); i++) {
                nodes.push_back(_nodes[i]);
            }
        }
        for (size_t i = 0; i < _nodes.size(); ++i) {
            braft::NodeStatus status;
            nodes[i]->get_status(&status);
            if (nodes[i]->is_leader()) {
                ASSERT_EQ(status.state, braft::STATE_LEADER);
            } else {
                ASSERT_NE(status.state, braft::STATE_LEADER);
                ASSERT_EQ(status.stable_followers.size(), 0);
            }
        }
    }

    void ensure_leader(const butil::EndPoint& expect_addr) {
CHECK:
        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            braft::PeerId leader_id = _nodes[i]->leader_id();
            if (leader_id.addr != expect_addr) {
                goto WAIT;
            }
        }

        return;
WAIT:
        usleep(100 * 1000);
        goto CHECK;
    }

    bool ensure_same(int wait_time_s = -1) {
        std::unique_lock<raft_mutex_t> guard(_mutex);
        std::vector<MockFSM*> fsms;
        for (size_t i = 0; i < _node_fsms.size(); ++i) {
            fsms.push_back(_node_fsms[i]);
        }
        for (size_t i = 0; i < _learner_fsms.size(); ++i) {
            fsms.push_back(_learner_fsms[i]);
        }
        if (fsms.size() <= 1) {
            return true;
        }
        LOG(INFO) << "fsms.size()=" << fsms.size();

        int nround = 0;
        MockFSM* first = fsms[0];
CHECK:
        first->lock();
        for (size_t i = 1; i < fsms.size(); i++) {
            MockFSM* fsm = fsms[i];
            fsm->lock();

            if (first->logs.size() != fsm->logs.size()) {
                LOG(INFO) << "logs size not match, "
                          << " addr: " << first->address << " vs "
                          << fsm->address << ", log num "
                          << first->logs.size() << " vs " << fsm->logs.size();
                fsm->unlock();
                goto WAIT;
            }

            for (size_t j = 0; j < first->logs.size(); j++) {
                butil::IOBuf& first_data = first->logs[j];
                butil::IOBuf& fsm_data = fsm->logs[j];
                if (first_data.to_string() != fsm_data.to_string()) {
                    LOG(INFO) << "log data of index=" << j << " not match, "
                              << " addr: " << first->address << " vs "
                              << fsm->address << ", data ("
                              << first_data.to_string() << ") vs " 
                              << fsm_data.to_string() << ")";
                    fsm->unlock();
                    goto WAIT;
                }
            }

            fsm->unlock();
        }
        first->unlock();
        guard.unlock();
        check_node_status();

        return true;
WAIT:
        first->unlock();
        sleep(1);
        ++nround;
        if (wait_time_s > 0 && nround > wait_time_s) {
            return false;
        }
        goto CHECK;
    }

    void ensure_snapshot(const butil::EndPoint& addr, const int64_t expect_index) {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        braft::SnapshotExecutor* snapshot_executor = nullptr;
        for (size_t i = 0; i < _nodes.size(); i++) {
            braft::NodeId node_id= _nodes[i]->node_id();
            if (node_id.peer_id.addr == addr) {
                snapshot_executor = _nodes[i]->_impl->_snapshot_executor;
                break;
            }
        }
        for (size_t i = 0; i < _learners.size(); i++) {
            braft::NodeId node_id= _learners[i]->node_id();
            if (node_id.peer_id.addr == addr) {
                snapshot_executor = _learners[i]->_impl->_snapshot_executor;
                break;
            }
        }
        ASSERT_TRUE(snapshot_executor != NULL);
        while (snapshot_executor->last_snapshot_index() != expect_index) {
            usleep(100 * 1000);
        }

        return;
    }

private:
    void all_learners(std::vector<butil::EndPoint>* addrs) {
        addrs->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _learners.size(); i++) {
            addrs->push_back(_learners[i]->node_id().peer_id.addr);
        }
    }

    void all_nodes(std::vector<butil::EndPoint>* addrs) {
        addrs->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            addrs->push_back(_nodes[i]->node_id().peer_id.addr);
        }
    }

    braft::Learner* remove_learner(const butil::EndPoint& addr) {
        std::lock_guard<raft_mutex_t> guard(_mutex);

        // remove node
        braft::Learner* node = NULL;
        std::vector<braft::Learner*> new_nodes;
        for (size_t i = 0; i < _learners.size(); i++) {
            if (addr.port == _learners[i]->node_id().peer_id.addr.port) {
                node = _learners[i];
            } else {
                new_nodes.push_back(_learners[i]);
            }
        }
        _learners.swap(new_nodes);

        // remove fsm
        std::vector<MockFSM*> new_fsms;
        for (size_t i = 0; i < _learner_fsms.size(); i++) {
            if (_learner_fsms[i]->address != addr) {
                new_fsms.push_back(_learner_fsms[i]);
            }
        }
        _learner_fsms.swap(new_fsms);

        return node;
    }

    braft::Node* remove_node(const butil::EndPoint& addr) {
        std::lock_guard<raft_mutex_t> guard(_mutex);

        // remove node
        braft::Node* node = NULL;
        std::vector<braft::Node*> new_nodes;
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (addr.port == _nodes[i]->node_id().peer_id.addr.port) {
                node = _nodes[i];
            } else {
                new_nodes.push_back(_nodes[i]);
            }
        }
        _nodes.swap(new_nodes);

        // remove fsm
        std::vector<MockFSM*> new_fsms;
        for (size_t i = 0; i < _node_fsms.size(); i++) {
            if (_node_fsms[i]->address != addr) {
                new_fsms.push_back(_node_fsms[i]);
            }
        }
        _node_fsms.swap(new_fsms);

        return node;
    }

    std::string _name;
    std::vector<braft::PeerId> _peers;
    std::set<braft::PeerId> _learner_ids;
    std::vector<braft::Node*> _nodes;
    std::vector<braft::Learner*> _learners;
    std::vector<MockFSM*> _node_fsms;
    std::vector<MockFSM*> _learner_fsms;
    std::map<butil::EndPoint, brpc::Server*> _server_map;
    int32_t _election_timeout_ms;
    raft_mutex_t _mutex;
    braft::SnapshotThrottle* _throttle;
};

class LearnerTest : public testing::Test {
protected:
    void SetUp() {
        g_dont_print_apply_log = false;
        logging::FLAGS_v = 90;
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        ::system("rm -rf data");
        ASSERT_EQ(0, braft::g_num_nodes.get_value());
    }
    void TearDown() {
        ::system("rm -rf data");
        // Sleep for a while to wait all timer has stopped
        if (braft::g_num_nodes.get_value() != 0) {
            usleep(1000 * 1000);
            ASSERT_EQ(0, braft::g_num_nodes.get_value());
        }
    }
};

TEST_F(LearnerTest, init_shutdown) {
    brpc::Server server;
    int ret = braft::add_service(&server, "0.0.0.0:4006");
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, server.Start("0.0.0.0:4006", NULL));

    braft::PeerId peer;
    peer.addr.ip = butil::my_ip();
    peer.addr.port = 4006;
    peer.idx = 1;
    std::vector<braft::PeerId> peers;
    peers.push_back(peer);

    braft::NodeOptions options;
    options.fsm = new MockFSM(butil::EndPoint());
    options.log_uri = "local://./data/log";
    options.raft_meta_uri = "local://./data/stable";
    options.snapshot_uri = "local://./data/snapshot";
    options.initial_conf = braft::Configuration(peers);

    braft::Learner node("unittest", braft::PeerId(butil::EndPoint(butil::my_ip(), 4006), 0));
    ASSERT_EQ(0, node.init(options));

    node.shutdown(NULL);
    node.join();
}

TEST_F(LearnerTest, single_learner) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    ASSERT_EQ(0, cluster.start(learners.begin()->addr));

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    cluster.ensure_same();

    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, double_learner) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 5; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    for (auto& learner : learners) {
        ASSERT_EQ(0, cluster.start(learner.addr));
    }

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    cluster.ensure_same();

    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

// test explore
TEST_F(LearnerTest, no_nodes) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    ASSERT_EQ(0, cluster.start(learners.begin()->addr));

    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader == NULL);

    sleep(5);

    // stop cluster
    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    ASSERT_EQ(1, nodes.size());

    braft::NodeStatus status;
    nodes[0]->_impl->get_status(&status);
    ASSERT_EQ(0, status.committed_index);

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, fetch_snapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    cond.reset(1);
    leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
    cond.wait();
    cluster.ensure_snapshot(leader->node_id().peer_id.addr, 11);

    ASSERT_EQ(0, cluster.start(learners.begin()->addr));
    cluster.ensure_same();

    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    ASSERT_EQ(11, nodes[0]->_impl->_snapshot_executor->last_snapshot_index());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, tail_snapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    ASSERT_EQ(0, cluster.start(learners.begin()->addr));

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    cond.reset(1);
    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    nodes[0]->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }
    cluster.ensure_same();

    cond.reset(1);
    MockFSM* fsm = cluster.get_fsm(leader->node_id().peer_id.addr);
    fsm->enable_fetch_snapshot = true;
    fsm->snapshot_service_addr = learners.begin()->addr;
    leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
    cond.wait();
    cluster.ensure_snapshot(leader->node_id().peer_id.addr, 11);
    ASSERT_EQ(11, nodes[0]->_impl->_snapshot_executor->last_snapshot_index());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, change_configration) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    braft::PeerId peer0;
    peer0.addr.ip = butil::my_ip();
    peer0.addr.port = 4006;
    peer0.idx = 0;

    braft::PeerId learner_id;
    learner_id.addr.ip = butil::my_ip();
    learner_id.addr.port = 4016;
    learner_id.idx = 0;
    learners.insert(learner_id);

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers, learners);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(INFO) << "start single cluster " << peer0;

    cluster.wait_leader();

    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(leader->node_id().peer_id, peer0);
    LOG(WARNING) << "leader is " << leader->node_id();

    bthread::CountdownEvent cond(10);
    // apply something
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    // start learner 
    ASSERT_EQ(0, cluster.start(learner_id.addr));
    LOG(INFO) << "start learner " << learner_id;
    cluster.ensure_same();

    // start peer1
    braft::PeerId peer1;
    peer1.addr.ip = butil::my_ip();
    peer1.addr.port = 4006 + 1;
    peer1.idx = 0;
    ASSERT_EQ(0, cluster.start(peer1.addr, false, true));
    LOG(INFO) << "start peer " << peer1;
    // wait until started successfully
    usleep(1000* 1000);

    // add peer1
    cond.reset(1);
    leader->add_peer(peer1, NEW_ADDPEERCLOSURE(&cond, 0));
    cond.wait();
    LOG(INFO) << "add peer " << peer1;
    cond.reset(1);
    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "with closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    // remove peer0
    cond.reset(1);
    leader->remove_peer(peer0, NEW_REMOVEPEERCLOSURE(&cond, 0));
    cond.wait();
    LOG(INFO) << "remove peer " << peer0;

    cluster.wait_leader();
    leader = cluster.leader();
    cluster.stop_node(peer0.addr);

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    braft::NodeStatus status;
    nodes[0]->_impl->get_status(&status);
    ASSERT_EQ(25, status.committed_index);

    cluster.stop_all();
}

TEST_F(LearnerTest, load_snapshot_dynamically) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    cond.reset(1);
    leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
    cond.wait();
    cluster.ensure_snapshot(leader->node_id().peer_id.addr, 11);

    ASSERT_EQ(0, cluster.start(learners.begin()->addr));
    cluster.ensure_same();
    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    ASSERT_EQ(11, nodes[0]->_impl->_snapshot_executor->last_snapshot_index());

    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    sleep(1);

    cond.reset(1);
    leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
    cond.wait();
    cluster.ensure_snapshot(leader->node_id().peer_id.addr, 21);
    cluster.ensure_same();
    ASSERT_EQ(21, nodes[0]->_impl->_snapshot_executor->last_snapshot_index());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, auto_snapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    ASSERT_EQ(0, cluster.start(learners.begin()->addr, true, false, 10));

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    sleep(10);

    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    ASSERT_EQ(11, nodes[0]->_impl->_snapshot_executor->last_snapshot_index());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, read_barrier) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();

    ASSERT_EQ(0, cluster.start(learners.begin()->addr));
    std::vector<braft::Learner*> nodes;
    cluster.learners(&nodes);
    ASSERT_EQ(1, nodes.size());
    braft::Learner* learner = nodes[0];
    ASSERT_TRUE(learner->set_read_barrier(3).ok());

    sleep(5);
    braft::NodeStatus status;
    learner->_impl->get_status(&status);
    ASSERT_LE(status.committed_index, 3);

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

class MockFSM1 : public MockFSM {
protected:
    MockFSM1() : MockFSM(butil::EndPoint()) {}
    virtual int on_snapshot_load(braft::SnapshotReader* reader) {
        (void)reader;
        return -1;
    }
};

TEST_F(LearnerTest, shutdown_and_join_work_after_init_fails) {
    brpc::Server server;
    int ret = braft::add_service(&server, 4006);
    server.Start(4006, NULL);
    ASSERT_EQ(0, ret);

    braft::PeerId peer;
    peer.addr.ip = butil::my_ip();
    peer.addr.port = 4006;
    peer.idx = 0;
    std::vector<braft::PeerId> peers;
    peers.push_back(peer);

    {
        braft::NodeOptions options;
        options.election_timeout_ms = 300;
        options.initial_conf = braft::Configuration(peers);
        options.fsm = new MockFSM1();
        options.log_uri = "local://./data/log";
        options.raft_meta_uri = "local://./data/stable";
        options.snapshot_uri = "local://./data/snapshot";
        braft::Node node("unittest", peer);
        ASSERT_EQ(0, node.init(options));
        sleep(1);
        bthread::CountdownEvent cond(10);
        for (int i = 0; i < 10; i++) {
            butil::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            braft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            node.apply(task);
        }
        cond.wait();
        LOG(INFO) << "begin to save snapshot";
        node.snapshot(NULL);
        LOG(INFO) << "begin to shutdown";
        node.shutdown(NULL);
        node.join();
    }
    
    braft::PeerId peer1;
    peer1.addr.ip = butil::my_ip();
    peer1.addr.port = 4006;
    peer1.idx = 1;
    {
        braft::NodeOptions options;
        options.election_timeout_ms = 300;
        options.initial_conf = braft::Configuration(peers);
        options.learner_auto_load_applied_logs = true;
        options.fsm = new MockFSM1();
        options.log_uri = "local://./data/log";
        options.raft_meta_uri = "local://./data/stable";
        options.snapshot_uri = "local://./data/snapshot";
        braft::Learner node("unittest", peer1);
        LOG(INFO) << "node init again";
        ASSERT_NE(0, node.init(options));
        node.shutdown(NULL);
        node.join();
    }

    server.Stop(200);
    server.Join();
}

// TODO test on_shutdown/out_of_order
//TEST_F(LearnerTest, step_down) {
//    std::vector<braft::PeerId> peers;
//    for (int i = 0; i < 3; i++) {
//        braft::PeerId peer;
//        peer.addr.ip = butil::my_ip();
//        peer.addr.port = 4006 + i;
//        peer.idx = 0;
//
//        peers.push_back(peer);
//    }
//
//    // start cluster
//    Cluster cluster("unittest", peers);
//    for (size_t i = 0; i < peers.size(); i++) {
//        ASSERT_EQ(0, cluster.start(peers[i].addr));
//    }
//
//    // elect leader
//    cluster.wait_leader();
//    braft::Node* leader = cluster.leader();
//    ASSERT_TRUE(leader != NULL);
//    LOG(WARNING) << "leader is " << leader->node_id();
//
//    // apply something
//    bthread::CountdownEvent cond(10);
//    for (int i = 0; i < 10; i++) {
//        butil::IOBuf data;
//        std::string data_buf;
//        data_buf.resize(256 * 1024, 'a');
//        data.append(data_buf);
//
//        braft::Task task;
//        task.data = &data;
//        task.done = NEW_APPLYCLOSURE(&cond, 0);
//        leader->apply(task);
//    }
//    cond.wait();
//
//    cluster.ensure_same();
//
//    std::vector<braft::Node*> nodes;
//    cluster.followers(&nodes);
//    ASSERT_EQ(2, nodes.size());
//
//    // stop follower
//    LOG(WARNING) << "stop follower";
//    butil::EndPoint follower_addr = nodes[0]->node_id().peer_id.addr;
//    cluster.stop(follower_addr);
//
//    // apply something
//    cond.reset(10);
//    for (int i = 10; i < 20; i++) {
//        butil::IOBuf data;
//        std::string data_buf;
//        data_buf.resize(256 * 1024, 'b');
//        data.append(data_buf);
//
//        braft::Task task;
//        task.data = &data;
//        task.done = NEW_APPLYCLOSURE(&cond, 0);
//        leader->apply(task);
//    }
//    cond.wait();
//
//    // trigger leader snapshot
//    LOG(WARNING) << "trigger leader snapshot ";
//    cond.reset(1);
//    leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
//    cond.wait();
//
//    // apply something
//    cond.reset(10);
//    for (int i = 20; i < 30; i++) {
//        butil::IOBuf data;
//        std::string data_buf;
//        data_buf.resize(256 * 1024, 'c');
//        data.append(data_buf);
//
//        braft::Task task;
//        task.data = &data;
//        task.done = NEW_APPLYCLOSURE(&cond, 0);
//        leader->apply(task);
//    }
//    cond.wait();
//
//    // trigger leader snapshot again to compact logs
//    LOG(WARNING) << "trigger leader snapshot again";
//    cond.reset(1);
//    leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
//    cond.wait();
//
//    LOG(WARNING) << "restart follower";
//    ASSERT_EQ(0, cluster.start(follower_addr));
//    usleep(1*1000*1000);
//    
//    // trigger newly-started follower report_error when install_snapshot
//    cluster._nodes.back()->_impl->_snapshot_executor->report_error(EIO, "%s", 
//                                                    "Fail to close writer");
//    
//    sleep(2);
//    LOG(WARNING) << "cluster stop";
//    cluster.stop_all();
//}

TEST_F(LearnerTest, dynamic_unloadsnapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    ASSERT_EQ(0, cluster.start(learners.begin()->addr));

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    std::vector<braft::Learner*> node;
    cluster.learners(&node);
    ASSERT_EQ(1, node.size());
    braft::Learner* learner = node[0];

    bthread::CountdownEvent cond2(1);
    learner->reset(NEW_SNAPSHOTCLOSURE(&cond2, 0));
    cond2.wait();
    ASSERT_EQ(true, cluster.learner_fsm_clear());

    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, dynamic_loadsnapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    ASSERT_EQ(0, cluster.start(learners.begin()->addr, true)); //启动了

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    std::vector<braft::Learner*> node;
    cluster.learners(&node);
    ASSERT_EQ(1, node.size());
    braft::Learner* learner = node[0];

    bthread::CountdownEvent cond1(1);
    learner->resume(NEW_SNAPSHOTCLOSURE(&cond1, 0));
    cond1.wait();
    cluster.ensure_same();

    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, dynamic_savesnapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, false, 1200));
    }

    ASSERT_EQ(0, cluster.start(learners.begin()->addr, true));

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    std::vector<braft::Learner*> node;
    cluster.learners(&node);
    ASSERT_EQ(1, node.size());
    braft::Learner* learner = node[0];

    bthread::CountdownEvent cond1(1);
    learner->resume(NEW_SNAPSHOTCLOSURE(&cond1, 0));
    cond1.wait();
    cluster.ensure_same();

    bthread::CountdownEvent cond3(1);
    learner->snapshot(NEW_SNAPSHOTCLOSURE(&cond3, 0));
    cond3.wait();
    cluster.ensure_snapshot(learner->node_id().peer_id.addr, 12);

    bthread::CountdownEvent cond2(1);
    learner->reset(NEW_SNAPSHOTCLOSURE(&cond2, 0));
    cond2.wait();
    ASSERT_EQ(true, cluster.learner_fsm_clear());

    bthread::CountdownEvent cond4(1);
    learner->snapshot(NEW_SNAPSHOTCLOSURE(&cond4, 0)); //skip snapshot and return success due to the gap is less than the min index.
    cond4.wait();
    //cluster.ensure_snapshot(learner->node_id().peer_id.addr, 0);
   
    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(LearnerTest, dynamic_resume_not_fetch_snapshot) {
    std::vector<braft::PeerId> peers;
    std::set<braft::PeerId> learners;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 4006 + i;
        peer.idx = 0;

        if (i < 3) {
            peers.push_back(peer);
        } else {
            learners.insert(peer);
        }
    }

    // start cluster
    Cluster cluster("unittest", peers, learners);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, false, 1200));
    }

    ASSERT_EQ(0, cluster.start(learners.begin()->addr, false));

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    std::vector<braft::Learner*> node;
    cluster.learners(&node);
    ASSERT_EQ(1, node.size());
    braft::Learner* learner = node[0];

    bthread::CountdownEvent cond2(1);
    learner->reset(NEW_SNAPSHOTCLOSURE(&cond2, 0));
    cond2.wait();
    ASSERT_EQ(true, cluster.learner_fsm_clear());

    bthread::CountdownEvent cond1(1);
    learner->resume(NEW_SNAPSHOTCLOSURE(&cond1, 0));
    cond1.wait();
    cluster.ensure_same();
   
    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}
