// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2018/01/12 12:56:17

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <baidu/rpc/server.h>
#include "raft/raft.h"
#include "raft/cli.h"
#include "raft/node.h"

class CliTest : public testing::Test {
public:
    void SetUp() {
        ::system("rm -rf data");
    }
    void TearDown() {
        ::system("rm -rf data");
    }
};

class MockFSM : public raft::StateMachine {
public:
    virtual void on_apply(raft::Iterator& /*iter*/) {
        google::SetCommandLineOption("raft_sync", "false");
        ASSERT_FALSE(true) << "Can't reach here";
    }
};

class RaftNode {
public:
    RaftNode() : _node(NULL) {}
    ~RaftNode() {
        stop();
        delete _node;
    }
    int start(int port, bool is_leader) {
        if (raft::add_service(&_server, port) != 0) {
            return -1;
        }
        if (_server.Start(port, NULL) != 0) {
            return -1;
        }
        raft::NodeOptions options;
        std::string prefix;
        base::string_printf(&prefix, "local://./data/%d", port);
        options.log_uri = prefix + "/log";
        options.stable_uri = prefix + "/stable";
        options.snapshot_uri = prefix + "/snapshot";
        options.fsm = &_fsm;
        options.node_owns_fsm = false;
        options.disable_cli = false;
        base::ip_t my_ip;
        EXPECT_EQ(0, base::str2ip("127.0.0.1", &my_ip));
        base::EndPoint addr(my_ip, port);
        raft::PeerId my_id(addr, 0);
        if (is_leader) {
            options.initial_conf.add_peer(my_id);
        }
        _node = new raft::Node("test", my_id);
        return _node->init(options);
    }
    void stop() {
        if (_node) {
            _node->shutdown(NULL);
            _node->join();
        }
        _server.Stop(0);
        _server.Join();
    }
    raft::PeerId peer_id() const { return _node->node_id().peer_id; }
protected:
    baidu::rpc::Server _server;
    raft::Node* _node;
    MockFSM _fsm;
};

TEST_F(CliTest, add_and_remove_peer) {
    RaftNode node1;
    ASSERT_EQ(0, node1.start(9500, true));
    // Add a non-exists peer should return ECATCHUP
    base::Status st;
    raft::Configuration old_conf;
    raft::PeerId peer1 = node1.peer_id();
    old_conf.add_peer(peer1);
    raft::PeerId peer2("127.0.0.1:9501");
    st = raft::cli::add_peer("test", old_conf, peer2,
                             raft::cli::CliOptions()); 
    ASSERT_FALSE(st.ok());
    LOG(INFO) << "st=" << st;
    RaftNode node2;
    ASSERT_EQ(0, node2.start(peer2.addr.port, false));
    st = raft::cli::add_peer("test", old_conf, peer2,
                             raft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    st = raft::cli::add_peer("test", old_conf, peer2,
                             raft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    raft::PeerId peer3("127.0.0.1:9502");
    RaftNode node3;
    ASSERT_EQ(0, node3.start(peer3.addr.port, false));
    st = raft::cli::add_peer("test", old_conf, peer3,
                             raft::cli::CliOptions()); 
    ASSERT_FALSE(st.ok()) << st;
    LOG(INFO) << "st=" << st;
    old_conf.add_peer(peer2);
    st = raft::cli::add_peer("test", old_conf, peer3,
                             raft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    st = raft::cli::remove_peer("test", old_conf, peer1,
                                raft::cli::CliOptions()); 
    ASSERT_FALSE(st.ok()) << st;
    old_conf.add_peer(peer3);
    st = raft::cli::remove_peer("test", old_conf, peer1,
                                raft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    usleep(100 * 1000);
    // Retried remove_peer
    st = raft::cli::remove_peer("test", old_conf, peer1,
                                raft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;

    st = raft::cli::remove_peer("test", old_conf, peer2,
                                raft::cli::CliOptions()); 
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(CliTest, set_peer) {
    RaftNode node1;
    ASSERT_EQ(0, node1.start(9500, false));
    raft::Configuration conf1;
    for (int i = 0; i < 3; ++i) {
        raft::PeerId peer_id = node1.peer_id();
        peer_id.addr.port += i;
        conf1.add_peer(peer_id);
    }
    base::Status st;
    st = raft::cli::set_peer("test", node1.peer_id(), conf1,
                             raft::cli::CliOptions());
    ASSERT_TRUE(st.ok());
    raft::Configuration conf2;
    conf2.add_peer(node1.peer_id());
    st = raft::cli::set_peer("test", node1.peer_id(), conf2,
                             raft::cli::CliOptions());
    ASSERT_TRUE(st.ok());
    usleep(2 * 1000 * 1000);
    ASSERT_TRUE(node1._node->is_leader());
}
