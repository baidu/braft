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

// Authors: Pengfei Zheng (zhengpengfei@baidu.com)

#ifndef PUBLIC_RAFT_TEST_UTIL_H
#define PUBLIC_RAFT_TEST_UTIL_H

#include "braft/node.h"
#include "braft/enum.pb.h"
#include "braft/errno.pb.h"
#include "braft/snapshot_throttle.h" 
#include "braft/snapshot_executor.h"

using namespace braft;
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
        , _on_leader_start_closure(NULL)
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
    braft::Closure* _on_leader_start_closure;

    void lock() {
        pthread_mutex_lock(&mutex);
    }

    void unlock() {
        pthread_mutex_unlock(&mutex);
    }

    void set_on_leader_start_closure(braft::Closure* closure) {
        _on_leader_start_closure = closure;
    }

    void on_leader_start(int64_t term) {
        _leader_term = term;
        if (_on_leader_start_closure) {
            LOG(INFO) << "addr " << address << " before leader start closure";
            _on_leader_start_closure->Run();
            LOG(INFO) << "addr " << address << " after leader start closure";
            _on_leader_start_closure = NULL;
        }
    }
    void on_leader_stop(const braft::LeaderChangeContext&) {
        _leader_term = -1;
    }

    bool is_leader() { return _leader_term > 0; }

    virtual void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            LOG_IF(INFO, !g_dont_print_apply_log) << "addr " << address 
                                                   << " apply " << iter.index()
                                                   << " data_size " << iter.data().size();
            BRAFT_VLOG << "data " << iter.data();
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
#define NEW_SNAPSHOTCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))

class Cluster {
public:
    Cluster(const std::string& name, const std::vector<braft::PeerId>& peers,
            int32_t election_timeout_ms = 3000, int max_clock_drift_ms = 1000)
        : _name(name), _peers(peers) 
        , _election_timeout_ms(election_timeout_ms)
        , _max_clock_drift_ms(max_clock_drift_ms) {

        int64_t throttle_throughput_bytes = 10 * 1024 * 1024;
        int64_t check_cycle = 10;
        _throttle = new braft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
    }
    ~Cluster() {
        stop_all();
    }

    int start(const butil::EndPoint& listen_addr, bool empty_peers = false,
              int snapshot_interval_s = 30,
              braft::Closure* leader_start_closure = NULL) {
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
        options.max_clock_drift_ms = _max_clock_drift_ms;
        options.snapshot_interval_s = snapshot_interval_s;
        if (!empty_peers) {
            options.initial_conf = braft::Configuration(_peers);
        }
        MockFSM* fsm = new MockFSM(listen_addr);
        if (leader_start_closure) {
            fsm->set_on_leader_start_closure(leader_start_closure);
        }
        options.fsm = fsm;
        options.node_owns_fsm = true;
        butil::string_printf(&options.log_uri, "local://./data/%s/log",
                            butil::endpoint2str(listen_addr).c_str());
        butil::string_printf(&options.raft_meta_uri, "local://./data/%s/raft_meta",
                            butil::endpoint2str(listen_addr).c_str());
        butil::string_printf(&options.snapshot_uri, "local://./data/%s/snapshot",
                            butil::endpoint2str(listen_addr).c_str());
        
        scoped_refptr<braft::SnapshotThrottle> tst(_throttle);
        options.snapshot_throttle = &tst;

        options.catchup_margin = 2;
        
        braft::Node* node = new braft::Node(_name, braft::PeerId(listen_addr, 0));
        int ret = node->init(options);
        if (ret != 0) {
            LOG(WARNING) << "init_node failed, server: " << listen_addr;
            delete node;
            return ret;
        } else {
            LOG(INFO) << "init node " << listen_addr;
        }

        {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            _nodes.push_back(node);
            _fsms.push_back(fsm);
        }
        return 0;
    }

    int stop(const butil::EndPoint& listen_addr) {
        
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
            stop(addrs[i]);
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
                    _fsms[i]->_leader_term == _nodes[i]->_impl->_current_term) {
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
            if (!_nodes[i]->is_leader()) {
                nodes->push_back(_nodes[i]);
            }
        }
    }

    void all_nodes(std::vector<braft::Node*>* nodes) {
        nodes->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            nodes->push_back(_nodes[i]);
        }
    }

    braft::Node* find_node(const braft::PeerId& peer_id) {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (peer_id == _nodes[i]->node_id().peer_id) {
                return _nodes[i];
            }
        }
        return NULL;
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
        for (size_t i = 0; i < nodes.size(); ++i) {
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
        if (_fsms.size() <= 1) {
            return true;
        }
        LOG(INFO) << "_fsms.size()=" << _fsms.size();

        int nround = 0;
        MockFSM* first = _fsms[0];
CHECK:
        first->lock();
        for (size_t i = 1; i < _fsms.size(); i++) {
            MockFSM* fsm = _fsms[i];
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

private:
    void all_nodes(std::vector<butil::EndPoint>* addrs) {
        addrs->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            addrs->push_back(_nodes[i]->node_id().peer_id.addr);
        }
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
        for (size_t i = 0; i < _fsms.size(); i++) {
            if (_fsms[i]->address != addr) {
                new_fsms.push_back(_fsms[i]);
            }
        }
        _fsms.swap(new_fsms);

        return node;
    }

    std::string _name;
    std::vector<braft::PeerId> _peers;
    std::vector<braft::Node*> _nodes;
    std::vector<MockFSM*> _fsms;
    std::map<butil::EndPoint, brpc::Server*> _server_map;
    int32_t _election_timeout_ms;
    int32_t _max_clock_drift_ms;
    raft_mutex_t _mutex;
    braft::SnapshotThrottle* _throttle;
};

#endif // ~PUBLIC_RAFT_TEST_UTIL_H
