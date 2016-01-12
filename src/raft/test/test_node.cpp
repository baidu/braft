/*
 * =====================================================================================
 *
 *       Filename:  test_node.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/12/07 11:27:46
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <base/files/file_path.h>
#include <base/file_util.h>
#include <baidu/rpc/closure_guard.h>
#include <bthread.h>
#include "raft/node.h"
#include "bthread_sync.h"

class MockFSM : public raft::StateMachine {
public:
    MockFSM(const base::EndPoint& address_)
        : address(address_), applied_index(0), snapshot_index(0) {
        raft_mutex_init(&mutex, NULL);
    }
    virtual ~MockFSM() {
        raft_mutex_destroy(&mutex);
    }

    base::EndPoint address;
    std::vector<base::IOBuf> logs;
    raft_mutex_t mutex;
    int64_t applied_index;
    int64_t snapshot_index;

    void lock() {
        raft_mutex_lock(&mutex);
    }

    void unlock() {
        raft_mutex_unlock(&mutex);
    }

    virtual void on_apply(const base::IOBuf& buf, const int64_t index, raft::Closure* done) {
        LOG(TRACE) << "addr " << address << " apply " << index;
        ::baidu::rpc::ClosureGuard guard(done);

        lock();
        logs.push_back(buf);
        unlock();

        applied_index = index;
    }

    virtual void on_shutdown() {
        LOG(TRACE) << "addr " << address << " shutdowned";
        delete this;
    }

    virtual int on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
        std::string file_path = raft::fileuri2path(writer->get_uri(base::EndPoint()));
        file_path.append("/data");

        LOG(NOTICE) << "on_snapshot_save to " << file_path;

        int fd = ::creat(file_path.c_str(), 0644);
        if (fd < 0) {
            LOG(ERROR) << "creat file failed, path: " << file_path << " err: " << berror();
            return EIO;
        }

        // write snapshot and log to file
        for (size_t i = 0; i < logs.size(); i++) {
            base::IOBuf& data = logs[i];
            int len = data.size();
            int ret = write(fd, &len, sizeof(int));
            CHECK_EQ(ret, 4);
            data.cut_into_file_descriptor(fd, len);
        }
        ::close(fd);
        done->Run();

        snapshot_index = applied_index;
        return 0;
    }

    int on_snapshot_load(raft::SnapshotReader* reader) {
        std::string file_path = raft::fileuri2path(reader->get_uri(base::EndPoint()));
        file_path.append("/data");

        LOG(INFO) << "on_snapshot_load from " << file_path;

        logs.clear();
        int fd = ::open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            LOG(ERROR) << "creat file failed, path: " << file_path << " err: " << berror();
            return EIO;
        }

        while (true) {
            int len = 0;
            int ret = read(fd, &len, sizeof(int));
            if (ret <= 0) {
                break;
            }

            base::IOPortal data;
            data.append_from_file_descriptor(fd, len);
            logs.push_back(data);
        }

        ::close(fd);
        return 0;
    }
};

class ExpectClosure : public raft::Closure {
public:
    ExpectClosure(BthreadCond* cond, int expect_err_code = -1)
        : _cond(cond), _expect_err_code(expect_err_code) {}

    void Run() {
        if (_cond) {
            _cond->Signal();
        }
        if (_expect_err_code >= 0) {
            ASSERT_EQ(_err_code, _expect_err_code);
        }
        delete this;
    }
private:
    BthreadCond* _cond;
    int _expect_err_code;
};

typedef ExpectClosure ShutdownClosure;
typedef ExpectClosure ApplyClosure;
typedef ExpectClosure AddPeerClosure;
typedef ExpectClosure RemovePeerClosure;
typedef ExpectClosure SnapshotClosure;

class Cluster {
public:
    Cluster(const std::string& name, const std::vector<raft::PeerId>& peers)
        : _name(name), _peers(peers) {
        raft_mutex_init(&_mutex, NULL);
    }
    ~Cluster() {
        stop_all();
        raft_mutex_destroy(&_mutex);
    }

    int start(const base::EndPoint& listen_addr, bool empty_peers = false) {
        int ret = raft::start_raft(listen_addr, NULL, NULL);
        if (ret != 0) {
            LOG(WARNING) << "start_raft failed, server: " << listen_addr;
            return ret;
        }

        raft::NodeOptions options;
        options.election_timeout = 300;
        if (!empty_peers) {
            options.conf = raft::Configuration(_peers);
        }
        options.fsm = new MockFSM(listen_addr);
        base::string_printf(&options.log_uri, "./data/%s/log",
                            base::endpoint2str(listen_addr).c_str());
        base::string_printf(&options.stable_uri, "./data/%s/stable",
                            base::endpoint2str(listen_addr).c_str());
        base::string_printf(&options.snapshot_uri, "./data/%s/snapshot",
                            base::endpoint2str(listen_addr).c_str());

        raft::Node* node = new raft::Node(_name, raft::PeerId(listen_addr, 0));
        ret = node->init(options);
        if (ret != 0) {
            LOG(WARNING) << "init_node failed, server: " << listen_addr;
            raft::stop_raft(listen_addr, NULL);
            return ret;
        } else {
            LOG(NOTICE) << "init node " << listen_addr;
        }

        {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            _nodes.push_back(node);
        }
        return 0;
    }

    int stop(const base::EndPoint& listen_addr) {
        raft::stop_raft(listen_addr, NULL);

        BthreadCond cond;
        raft::Node* node = remove_node(listen_addr);
        cond.Init(1);
        node->shutdown(new ShutdownClosure(&cond));
        cond.Wait();

        delete node;
        return 0;
    }

    void stop_all() {
        std::vector<base::EndPoint> addrs;
        all_nodes(&addrs);

        for (size_t i = 0; i < addrs.size(); i++) {
            stop(addrs[i]);
        }
    }

    void clean(const base::EndPoint& listen_addr) {
        std::string data_path;
        base::string_printf(&data_path, "./data/%s", base::endpoint2str(listen_addr).c_str());

        if (!base::DeleteFile(base::FilePath(data_path), true)) {
            LOG(ERROR) << "delete path failed, path: " << data_path;
        }
    }

    raft::Node* leader() {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        raft::Node* node = NULL;
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (_nodes[i]->is_leader()) {
                node = _nodes[i];
                break;
            }
        }
        return node;
    }

    void followers(std::vector<raft::Node*>* nodes) {
        nodes->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (!_nodes[i]->is_leader()) {
                nodes->push_back(_nodes[i]);
            }
        }
    }
    
    void wait_leader() {
        while (true) {
            raft::Node* node = leader();
            if (node) {
                return;
            } else {
                sleep(1);
            }
        }
    }

    void ensure_leader(const base::EndPoint& expect_addr) {
CHECK:
        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            raft::PeerId leader_id = _nodes[i]->leader_id();
            if (leader_id.addr != expect_addr) {
                goto WAIT;
            }
        }

        return;
WAIT:
        sleep(1);
        goto CHECK;
    }

    void ensure_same() {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        if (_fsms.size() <= 1) {
            return;
        }

        MockFSM* first = _fsms[0];
CHECK:
        first->lock();
        for (size_t i = 1; i < _fsms.size(); i++) {
            MockFSM* fsm = _fsms[i];
            fsm->lock();

            if (first->logs.size() != _fsms[i]->logs.size()) {
                fsm->unlock();
                goto WAIT;
            }

            for (size_t i = 0; i < first->logs.size(); i++) {
                base::IOBuf& first_data = first->logs[i];
                base::IOBuf& fsm_data = _fsms[i]->logs[i];
                if (first_data.size() != fsm_data.size() ||
                    raft::murmurhash32(first_data) != raft::murmurhash32(fsm_data)) {
                    fsm->unlock();
                    goto WAIT;
                }
            }

            fsm->unlock();
        }
        first->unlock();

        return;
WAIT:
        first->unlock();
        sleep(1);
        goto CHECK;
    }

private:
    void all_nodes(std::vector<base::EndPoint>* addrs) {
        addrs->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            addrs->push_back(_nodes[i]->node_id().peer_id.addr);
        }
    }

    raft::Node* remove_node(const base::EndPoint& addr) {
        std::lock_guard<raft_mutex_t> guard(_mutex);

        // remove node
        raft::Node* node = NULL;
        std::vector<raft::Node*> new_nodes;
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
    std::vector<raft::PeerId> _peers;
    std::vector<raft::Node*> _nodes;
    std::vector<MockFSM*> _fsms;
    raft_mutex_t _mutex;
};

class RaftTestSuits : public testing::Test {
protected:
    void SetUp() {
        //logging::FLAGS_verbose = 90;
        ::system("rm -rf data");
    }
    void TearDown() {
        ::system("rm -rf data");
    }
};

TEST_F(RaftTestSuits, InitShutdown) {
    int ret = raft::start_raft("0.0.0.0:60006", NULL, NULL);
    ASSERT_EQ(0, ret);

    raft::NodeOptions options;
    options.fsm = new MockFSM(base::EndPoint());
    options.log_uri = "./data/log";
    options.stable_uri = "./data/stable";
    options.snapshot_uri = "./data/snapshot";

    raft::Node node("unittest", raft::PeerId(base::EndPoint(base::IP_ANY, 60006), 0));
    ASSERT_EQ(0, node.init(options));

    node.shutdown(NULL);

    node.shutdown(new ShutdownClosure(NULL));

    //FIXME:
    BthreadCond cond;
    cond.Init(1);
    base::IOBuf data;
    data.append("hello");
    node.apply(data, new ApplyClosure(&cond));
    cond.Wait();

    raft::stop_raft("0.0.0.0:60006", NULL);
}

TEST_F(RaftTestSuits, Server) {
    ASSERT_EQ(0, raft::start_raft("0.0.0.0:60006", NULL, NULL));
    ASSERT_NE(0, raft::start_raft("0.0.0.0:60006", NULL, NULL));
    baidu::rpc::Server server;
    baidu::rpc::ServerOptions server_options;
    ASSERT_EQ(0, raft::start_raft("0.0.0.0:60007", &server, &server_options));

    baidu::rpc::Server* server_ptr = NULL;
    raft::stop_raft("0.0.0.0:60006", &server_ptr);
    ASSERT_TRUE(server_ptr == NULL);
    raft::stop_raft("0.0.0.0:60007", &server_ptr);
    ASSERT_TRUE(server_ptr != NULL);
    ASSERT_TRUE(server_ptr == &server);
    raft::stop_raft("0.0.0.0:60007", &server_ptr);
    ASSERT_TRUE(server_ptr == NULL);
    raft::stop_raft("127.0.0.1:60007", &server_ptr);
    ASSERT_TRUE(server_ptr == NULL);

    server.Stop(200);
    server.Join();
}

TEST_F(RaftTestSuits, SingleNode) {
    baidu::rpc::Server server;
    baidu::rpc::ServerOptions server_options;
    int ret = raft::start_raft("0.0.0.0:60006", &server, &server_options);
    ASSERT_EQ(0, ret);

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 60006;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);

    raft::NodeOptions options;
    options.election_timeout = 300;
    options.conf = raft::Configuration(peers);
    options.fsm = new MockFSM(base::EndPoint());
    options.log_uri = "./data/log";
    options.stable_uri = "./data/stable";
    options.snapshot_uri = "./data/snapshot";

    raft::Node node("unittest", peer);
    ASSERT_EQ(0, node.init(options));

    sleep(2);

    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        node.apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    cond.Init(1);
    node.shutdown(new ShutdownClosure(&cond, 0));
    cond.Wait();

    raft::stop_raft("0.0.0.0:60006", NULL);
    server.Stop(200);
    server.Join();
}

TEST_F(RaftTestSuits, NoLeader) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    cluster.start(peers[0].addr);

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());

    raft::Node* follower = nodes[0];

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        follower->apply(data, new ApplyClosure(&cond, EPERM));
    }
    cond.Wait();

    // add peer1
    raft::PeerId peer3;
    peer3.addr.ip = base::get_host_ip();
    peer3.addr.port = 60006 + 3;
    peer3.idx = 0;

    cond.Init(1);
    follower->add_peer(peers, peer3, new AddPeerClosure(&cond, EPERM));
    cond.Wait();
    LOG(NOTICE) << "add peer " << peer3;

    // remove peer1
    raft::PeerId peer0;
    peer0.addr.ip = base::get_host_ip();
    peer0.addr.port = 60006 + 0;
    peer0.idx = 0;

    cond.Init(1);
    follower->add_peer(peers, peer0, new RemovePeerClosure(&cond, EPERM));
    cond.Wait();
    LOG(NOTICE) << "remove peer " << peer0;
}

TEST_F(RaftTestSuits, TripleNode) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        leader->apply(data, NULL);
    }

    cluster.ensure_same();

    {
        baidu::rpc::Channel channel;
        baidu::rpc::ChannelOptions options;
        options.protocol = baidu::rpc::PROTOCOL_HTTP;

        if (channel.Init(leader->node_id().peer_id.addr, &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }

        {
            baidu::rpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat";
            cntl.http_request().set_method(baidu::rpc::HTTP_METHOD_GET);

            channel.CallMethod(NULL, &cntl, NULL, NULL, NULL/*done*/);

            LOG(NOTICE) << "http return: \n" << cntl.response_attachment();
        }

        {
            baidu::rpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat/unittest";
            cntl.http_request().set_method(baidu::rpc::HTTP_METHOD_GET);

            channel.CallMethod(NULL, &cntl, NULL, NULL, NULL/*done*/);

            LOG(NOTICE) << "http return: \n" << cntl.response_attachment();
        }
    }

    // stop cluster
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(RaftTestSuits, LeaderFail) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    // stop leader
    base::EndPoint old_leader = leader->node_id().peer_id.addr;
    LOG(WARNING) << "stop leader " << leader->node_id();
    cluster.stop(leader->node_id().peer_id.addr);

    // apply something when follower
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(nodes.size(), 2);
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "follower apply: %d", i + 1);
        data.append(data_buf);

        nodes[0]->apply(data, new ApplyClosure(&cond, -1));
    }
    cond.Wait();

    // elect new leader
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "elect new leader " << leader->node_id();

    // apply something
    cond.Init(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    // old leader restart
    ASSERT_EQ(0, cluster.start(old_leader));
    LOG(WARNING) << "restart old leader " << old_leader;

    // apply something
    cond.Init(10);
    for (int i = 20; i < 30; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    // stop and clean old leader
    LOG(WARNING) << "stop old leader " << old_leader;
    cluster.stop(old_leader);
    LOG(WARNING) << "clean old leader data " << old_leader;
    cluster.clean(old_leader);

    sleep(2);
    // restart old leader
    ASSERT_EQ(0, cluster.start(old_leader));
    LOG(WARNING) << "restart old leader " << old_leader;

    cluster.ensure_same();

    cluster.stop_all();
}

TEST_F(RaftTestSuits, JoinNode) {
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::get_host_ip();
    peer0.addr.port = 60006;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;

    cluster.ensure_leader(peer0.addr);
    LOG(NOTICE) << "peer become leader " << peer0;

    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(leader->node_id().peer_id, peer0);
    LOG(WARNING) << "leader is " << leader->node_id();

    BthreadCond cond;
    // apply something
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    // start peer1
    raft::PeerId peer1;
    peer1.addr.ip = base::get_host_ip();
    peer1.addr.port = 60006 + 1;
    peer1.idx = 0;
    ASSERT_EQ(0, cluster.start(peer1.addr, true));
    LOG(NOTICE) << "start peer " << peer1;

    // add peer1
    cond.Init(1);
    leader->add_peer(peers, peer1, new AddPeerClosure(&cond, 0));
    cond.Wait();
    LOG(NOTICE) << "add peer " << peer1;

    cluster.ensure_same();

    // add peer2 when peer not start
    raft::PeerId peer2;
    peer2.addr.ip = base::get_host_ip();
    peer2.addr.port = 60006 + 2;
    peer2.idx = 0;

    cond.Init(1);
    peers.push_back(peer1);
    leader->add_peer(peers, peer2, new AddPeerClosure(&cond, ETIMEDOUT));
    cond.Wait();

    // start peer2 after some seconds wait 
    sleep(2);
    ASSERT_EQ(0, cluster.start(peer2.addr, true));
    LOG(NOTICE) << "start peer " << peer2;

    // re add peer2
    cond.Init(3);
    // {peer0,peer1} add peer2
    leader->add_peer(peers, peer2, new AddPeerClosure(&cond, 0));
    // concurrent configration change
    leader->add_peer(peers, peer2, new AddPeerClosure(&cond, EINVAL));
    // new peer equal old configuration
    leader->add_peer(peers, peer1, new AddPeerClosure(&cond, 0));
    cond.Wait();

    cond.Init(2);
    // retry add_peer direct ok
    leader->add_peer(peers, peer2, new AddPeerClosure(&cond, 0));
    // {peer0, peer1, peer2} can't accept peers{peer0, peer1}, must skip same check
    leader->add_peer(peers, peer1, new AddPeerClosure(&cond, EINVAL));
    cond.Wait();

    cluster.ensure_same();

    cluster.stop_all();
}

TEST_F(RaftTestSuits, RemoveFollower) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    BthreadCond cond;
    // apply something
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    const raft::PeerId follower_id = nodes[0]->node_id().peer_id;
    const base::EndPoint follower_addr = follower_id.addr;
    // stop follower
    LOG(WARNING) << "stop and clean follower " << follower_addr;
    cluster.stop(follower_addr);
    cluster.clean(follower_addr);

    // remove follower
    LOG(WARNING) << "remove follower " << follower_addr;
    cond.Init(1);
    leader->remove_peer(peers, follower_id, new RemovePeerClosure(&cond, 0));
    cond.Wait();

    // stop and clean one follower
    //LOG(WARNING) << "stop follower " << follower_addr;
    //cluster.stop(follower_addr);
    //LOG(WARNING) << "clean follower data " << follower_addr;
    //cluster.clean(follower_addr);

    // apply something
    cond.Init(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());

    peers.clear();
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        if (peer.addr != follower_addr) {
            peers.push_back(peer);
        }
    }

    // start follower
    LOG(WARNING) << "start follower " << follower_addr;
    ASSERT_EQ(0, cluster.start(follower_addr));

    // re add follower fail when leader step down
    LOG(WARNING) << "add follower " << follower_addr;
    cond.Init(1);
    leader->add_peer(peers, follower_id, new AddPeerClosure(&cond, 0));
    cond.Wait();

    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.ensure_same();
}

TEST_F(RaftTestSuits, RemoveLeader) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    BthreadCond cond;
    // apply something
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    base::EndPoint old_leader_addr = leader->node_id().peer_id.addr;
    LOG(WARNING) << "remove leader " << old_leader_addr;
    cond.Init(1);
    leader->remove_peer(peers, leader->node_id().peer_id, new RemovePeerClosure(&cond, 0));
    cond.Wait();

    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    cond.Init(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    LOG(WARNING) << "stop and clear leader " << old_leader_addr;
    cluster.stop(old_leader_addr);
    cluster.clean(old_leader_addr);

    LOG(WARNING) << "start old leader " << old_leader_addr;
    cluster.start(old_leader_addr);

    LOG(WARNING) << "add old leader " << old_leader_addr;
    cond.Init(1);
    peers.clear();
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        if (peer.addr != old_leader_addr) {
            peers.push_back(peer);
        }
    }
    leader->add_peer(peers, raft::PeerId(old_leader_addr, 0), new AddPeerClosure(&cond, 0));
    cond.Wait();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.ensure_same();
}

TEST_F(RaftTestSuits, PreVote) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    BthreadCond cond;
    // apply something
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());
    const raft::PeerId follower_id = nodes[0]->node_id().peer_id;
    const base::EndPoint follower_addr = follower_id.addr;

    const int64_t saved_term = leader->_impl->_current_term;
    //remove follower
    LOG(WARNING) << "remove follower " << follower_addr;
    cond.Init(1);
    leader->remove_peer(peers, follower_id, new RemovePeerClosure(&cond, 0));
    cond.Wait();

    // apply something
    cond.Init(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    sleep(2);

    //add follower
    LOG(WARNING) << "add follower " << follower_addr;
    peers.clear();
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        if (peer.addr != follower_addr) {
            peers.push_back(peer);
        }
    }
    cond.Init(1);
    leader->add_peer(peers, follower_id, new RemovePeerClosure(&cond, 0));
    cond.Wait();

    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);

    ASSERT_EQ(saved_term, leader->_impl->_current_term);
}

TEST_F(RaftTestSuits, SetPeer1) {
    // bootstrap from null
    Cluster cluster("unittest", std::vector<raft::PeerId>());
    raft::PeerId boot_peer;
    boot_peer.addr.ip = base::get_host_ip();
    boot_peer.addr.port = 60006;
    boot_peer.idx = 0;

    ASSERT_EQ(0, cluster.start(boot_peer.addr));
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());

    std::vector<raft::PeerId> peers;
    peers.push_back(boot_peer);
    ASSERT_EQ(EINVAL, nodes[0]->set_peer(peers, peers));
    ASSERT_EQ(0, nodes[0]->set_peer(std::vector<raft::PeerId>(), peers));

    cluster.wait_leader();
}

TEST_F(RaftTestSuits, SetPeer2) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    std::cout << "Here" << std::endl;
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    base::EndPoint leader_addr = leader->node_id().peer_id.addr;
    LOG(WARNING) << "leader is " << leader->node_id();
    std::cout << "Here" << std::endl;

    BthreadCond cond;
    // apply something
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();
    std::cout << "Here" << std::endl;

    // check follower
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());
    raft::PeerId follower_peer1 = nodes[0]->node_id().peer_id;
    raft::PeerId follower_peer2 = nodes[1]->node_id().peer_id;

    LOG(WARNING) << "stop and clean follower " << follower_peer1;
    cluster.stop(follower_peer1.addr);
    cluster.clean(follower_peer1.addr);

    std::cout << "Here" << std::endl;
    // apply something
    cond.Init(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();
    
    std::cout << "Here" << std::endl;
    //set peer when no quorum die
    std::vector<raft::PeerId> new_peers;
    LOG(WARNING) << "set peer to " << leader_addr;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        if (peer != follower_peer1) {
            new_peers.push_back(peer);
        }
    }
    std::cout << "Here" << std::endl;
    ASSERT_EQ(EINVAL, leader->set_peer(peers, new_peers));
    std::cout << "Here 7" << std::endl;

    LOG(WARNING) << "stop and clean follower " << follower_peer2;
    cluster.stop(follower_peer2.addr);
    cluster.clean(follower_peer2.addr);

    std::cout << "Here 8" << std::endl;
    // leader will stepdown, become follower
    sleep(2);

    new_peers.clear();
    new_peers.push_back(raft::PeerId(leader_addr, 0));

    // new peers equal current conf
    ASSERT_EQ(0, leader->set_peer(new_peers, peers));
    // old peers not match current conf
    ASSERT_EQ(EINVAL, leader->set_peer(new_peers, new_peers));
    // new_peers not include in current conf
    new_peers.push_back(raft::PeerId(leader_addr, 1));
    ASSERT_EQ(EINVAL, leader->set_peer(new_peers, new_peers));

    // set peer when quorum die
    LOG(WARNING) << "set peer to " << leader_addr;
    new_peers.clear();
    new_peers.push_back(raft::PeerId(leader_addr, 0));
    ASSERT_EQ(0, leader->set_peer(peers, new_peers));
    std::cout << "Here 9" << std::endl;

    cluster.wait_leader();
    std::cout << "Here 10" << std::endl;
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(leader->node_id().peer_id.addr, leader_addr);

    LOG(WARNING) << "start old follower " << follower_peer1;
    ASSERT_EQ(0, cluster.start(follower_peer1.addr));
    LOG(WARNING) << "start old follower " << follower_peer2;
    ASSERT_EQ(0, cluster.start(follower_peer2.addr));

    LOG(WARNING) << "add old follower " << follower_peer1;
    cond.Init(1);
    leader->add_peer(new_peers, follower_peer1, new AddPeerClosure(&cond, 0));
    cond.Wait();

    LOG(WARNING) << "add old follower " << follower_peer2;
    cond.Init(1);
    new_peers.push_back(follower_peer1);
    leader->add_peer(new_peers, follower_peer2, new AddPeerClosure(&cond, 0));
    cond.Wait();

    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.ensure_same();
}

TEST_F(RaftTestSuits, RestoreSnapshot) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    base::EndPoint leader_addr = leader->node_id().peer_id.addr;

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    cluster.ensure_same();

    // trigger leader snapshot
    LOG(WARNING) << "trigger leader snapshot ";
    cond.Init(1);
    leader->snapshot(new SnapshotClosure(&cond, 0));
    cond.Wait();

    // stop leader
    LOG(WARNING) << "stop leader";
    cluster.stop(leader->node_id().peer_id.addr);

    sleep(2);

    LOG(WARNING) << "restart leader";
    ASSERT_EQ(0, cluster.start(leader_addr));

    cluster.ensure_same();

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(RaftTestSuits, InstallSnapshot) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 60006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    // stop follower
    LOG(WARNING) << "stop follower";
    base::EndPoint follower_addr = nodes[0]->node_id().peer_id.addr;
    cluster.stop(follower_addr);

    // apply something
    cond.Init(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    // trigger leader snapshot
    LOG(WARNING) << "trigger leader snapshot ";
    cond.Init(1);
    leader->snapshot(new SnapshotClosure(&cond, 0));
    cond.Wait();

    // apply something
    cond.Init(10);
    for (int i = 20; i < 30; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        leader->apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    LOG(WARNING) << "restart follower";
    ASSERT_EQ(0, cluster.start(follower_addr));

    sleep(2);

    cluster.ensure_same();

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_F(RaftTestSuits, NoSnapshot) {
    baidu::rpc::Server server;
    baidu::rpc::ServerOptions server_options;
    int ret = raft::start_raft("0.0.0.0:60006", &server, &server_options);
    ASSERT_EQ(0, ret);

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 60006;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);

    raft::NodeOptions options;
    options.election_timeout = 300;
    options.conf = raft::Configuration(peers);
    options.fsm = new MockFSM(base::EndPoint());
    options.log_uri = "./data/log";
    options.stable_uri = "./data/stable";

    raft::Node node("unittest", peer);
    ASSERT_EQ(0, node.init(options));

    // wait node elect to leader
    sleep(2);

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        node.apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    // trigger snapshot, not expect ret
    cond.Init(1);
    node.snapshot(new SnapshotClosure(&cond, -1));
    cond.Wait();

    // shutdown
    cond.Init(1);
    node.shutdown(new ShutdownClosure(&cond, 0));
    cond.Wait();

    // stop
    raft::stop_raft("0.0.0.0:60006", NULL);
    server.Stop(200);
    server.Join();
}

TEST_F(RaftTestSuits, AutoSnapshot) {
    baidu::rpc::Server server;
    baidu::rpc::ServerOptions server_options;
    int ret = raft::start_raft("0.0.0.0:60006", &server, &server_options);
    ASSERT_EQ(0, ret);

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 60006;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);

    raft::NodeOptions options;
    options.election_timeout = 300;
    options.conf = raft::Configuration(peers);
    options.fsm = new MockFSM(base::EndPoint());
    options.log_uri = "./data/log";
    options.stable_uri = "./data/stable";
    options.snapshot_uri = "./data/snapshot";
    options.snapshot_interval = 10;

    raft::Node node("unittest", peer);
    ASSERT_EQ(0, node.init(options));

    // wait node elect to leader
    sleep(2);

    // apply something
    BthreadCond cond;
    cond.Init(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        node.apply(data, new ApplyClosure(&cond, 0));
    }
    cond.Wait();

    sleep(10);
    ASSERT_GT(static_cast<MockFSM*>(options.fsm)->snapshot_index, 0);

    // shutdown
    cond.Init(1);
    node.shutdown(new ShutdownClosure(&cond, 0));
    cond.Wait();

    // stop
    raft::stop_raft("0.0.0.0:60006", NULL);
    server.Stop(200);
    server.Join();
}
