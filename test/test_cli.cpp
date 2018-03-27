// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2018/01/12 12:56:17

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <butil/unique_ptr.h>
#include <brpc/server.h>
#include "braft/raft.h"
#include "braft/cli.h"
#include "braft/node.h"

class CliTest : public testing::Test {
public:
    void SetUp() {
        GFLAGS_NS::SetCommandLineOption("raft_sync", "false");
        ::system("rm -rf data");
    }
    void TearDown() {
        ::system("rm -rf data");
    }
};

class MockFSM : public braft::StateMachine {
public:
    virtual void on_apply(braft::Iterator& /*iter*/) {
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
        if (braft::add_service(&_server, port) != 0) {
            return -1;
        }
        if (_server.Start(port, NULL) != 0) {
            return -1;
        }
        braft::NodeOptions options;
        std::string prefix;
        butil::string_printf(&prefix, "local://./data/%d", port);
        options.log_uri = prefix + "/log";
        options.raft_meta_uri = prefix + "/raft_meta";
        options.snapshot_uri = prefix + "/snapshot";
        options.fsm = &_fsm;
        options.node_owns_fsm = false;
        options.disable_cli = false;
        butil::ip_t my_ip;
        EXPECT_EQ(0, butil::str2ip("127.0.0.1", &my_ip));
        butil::EndPoint addr(my_ip, port);
        braft::PeerId my_id(addr, 0);
        if (is_leader) {
            options.initial_conf.add_peer(my_id);
        }
        _node = new braft::Node("test", my_id);
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
    braft::PeerId peer_id() const { return _node->node_id().peer_id; }
protected:
    brpc::Server _server;
    braft::Node* _node;
    MockFSM _fsm;
};

TEST_F(CliTest, add_and_remove_peer) {
    RaftNode node1;
    ASSERT_EQ(0, node1.start(9500, true));
    // Add a non-exists peer should return ECATCHUP
    butil::Status st;
    braft::Configuration old_conf;
    braft::PeerId peer1 = node1.peer_id();
    old_conf.add_peer(peer1);
    braft::PeerId peer2("127.0.0.1:9501");
    st = braft::cli::add_peer("test", old_conf, peer2,
                             braft::cli::CliOptions()); 
    ASSERT_FALSE(st.ok());
    LOG(INFO) << "st=" << st;
    RaftNode node2;
    ASSERT_EQ(0, node2.start(peer2.addr.port, false));
    st = braft::cli::add_peer("test", old_conf, peer2,
                             braft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    st = braft::cli::add_peer("test", old_conf, peer2,
                             braft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    braft::PeerId peer3("127.0.0.1:9502");
    RaftNode node3;
    ASSERT_EQ(0, node3.start(peer3.addr.port, false));
    old_conf.add_peer(peer2);
    st = braft::cli::add_peer("test", old_conf, peer3,
                             braft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    old_conf.add_peer(peer3);
    st = braft::cli::remove_peer("test", old_conf, peer1,
                                braft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
    usleep(1000 * 1000);
    // Retried remove_peer
    st = braft::cli::remove_peer("test", old_conf, peer1,
                                braft::cli::CliOptions()); 
    ASSERT_TRUE(st.ok()) << st;
}

TEST_F(CliTest, set_peer) {
    RaftNode node1;
    ASSERT_EQ(0, node1.start(9500, false));
    braft::Configuration conf1;
    for (int i = 0; i < 3; ++i) {
        braft::PeerId peer_id = node1.peer_id();
        peer_id.addr.port += i;
        conf1.add_peer(peer_id);
    }
    butil::Status st;
    st = braft::cli::reset_peer("test", node1.peer_id(), conf1,
                                braft::cli::CliOptions());
    ASSERT_TRUE(st.ok());
    braft::Configuration conf2;
    conf2.add_peer(node1.peer_id());
    st = braft::cli::reset_peer("test", node1.peer_id(), conf2,
                             braft::cli::CliOptions());
    ASSERT_TRUE(st.ok());
    usleep(2 * 1000 * 1000);
    ASSERT_TRUE(node1._node->is_leader());
}

TEST_F(CliTest, change_peers) {
    RaftNode node1;
    ASSERT_EQ(0, node1.start(9500, false));
    braft::Configuration conf1;
    for (int i = 0; i < 3; ++i) {
        braft::PeerId peer_id = node1.peer_id();
        peer_id.addr.port += i;
        conf1.add_peer(peer_id);
    }
    butil::Status st;
    st = braft::cli::reset_peer("test", node1.peer_id(), conf1,
                                braft::cli::CliOptions());
    ASSERT_TRUE(st.ok());
    braft::Configuration conf2;
    conf2.add_peer(node1.peer_id());
    st = braft::cli::reset_peer("test", node1.peer_id(), conf2,
                             braft::cli::CliOptions());
    ASSERT_TRUE(st.ok());
    usleep(2 * 1000 * 1000);
    ASSERT_TRUE(node1._node->is_leader());
}

TEST_F(CliTest, change_peer) {
    size_t N = 10;
    std::unique_ptr<RaftNode[]> nodes(new RaftNode[N]);
    nodes[0].start(9500, true);
    for (size_t i = 1; i < N; ++i) {
        ASSERT_EQ(0, nodes[i].start(9500 + i, false));
    }
    braft::Configuration conf;
    for (size_t i = 0; i < N; ++i) {
        conf.add_peer("127.0.0.1:" + std::to_string(9500 + i));
    }
    butil::Status st;
    for (size_t i = 0; i < N; ++i) {
        usleep(1000 * 1000);
        braft::Configuration new_conf;
        new_conf.add_peer("127.0.0.1:" + std::to_string(9500 + i));
        st = braft::cli::change_peers("test", conf, new_conf, braft::cli::CliOptions());
        ASSERT_TRUE(st.ok()) << st;
    }
    usleep(1000 * 1000);
    st = braft::cli::change_peers("test", conf, conf, braft::cli::CliOptions());
    ASSERT_TRUE(st.ok()) << st;
    for (size_t i = 0; i < N; ++i) {
        usleep(10 * 1000);
        braft::Configuration new_conf;
        new_conf.add_peer("127.0.0.1:" + std::to_string(9500 + i));
        LOG(WARNING) << "change " << conf << " to " << new_conf;
        st = braft::cli::change_peers("test", conf, new_conf, braft::cli::CliOptions());
        ASSERT_TRUE(st.ok()) << st;
        usleep(1000 * 1000);
        LOG(WARNING) << "change " << new_conf << " to " << conf;
        st = braft::cli::change_peers("test", new_conf, conf, braft::cli::CliOptions());
        ASSERT_TRUE(st.ok()) << st << " i=" << i;
    }
}

