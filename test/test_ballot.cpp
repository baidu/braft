// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include <gtest/gtest.h>
#include "braft/ballot.h"

class BallotTest : public testing::Test {};

TEST(BallotTest, sanity) {
    braft::PeerId peer1("127.0.0.1:1");
    braft::PeerId peer2("127.0.0.1:2");
    braft::PeerId peer3("127.0.0.1:3");
    braft::PeerId peer4("127.0.0.1:4");
    braft::Configuration conf;
    conf.add_peer(peer1);
    conf.add_peer(peer2);
    conf.add_peer(peer3);
    braft::Ballot bl;
    ASSERT_EQ(0, bl.init(conf, NULL));
    ASSERT_EQ(2, bl._quorum);
    ASSERT_EQ(0, bl._old_quorum);
    bl.grant(peer1);
    ASSERT_EQ(1, bl._quorum);
    braft::Ballot::PosHint hint = bl.grant(peer1, braft::Ballot::PosHint());
    ASSERT_EQ(1, bl._quorum);
    hint = bl.grant(peer1, hint);
    ASSERT_EQ(1, bl._quorum);
    hint = bl.grant(peer4, hint);
    ASSERT_EQ(1, bl._quorum);
    hint = bl.grant(peer2, hint);
    ASSERT_TRUE(bl.granted());
}

TEST(BallotTest, joint_consensus_same_conf) {
    braft::PeerId peer1("127.0.0.1:1");
    braft::PeerId peer2("127.0.0.1:2");
    braft::PeerId peer3("127.0.0.1:3");
    braft::PeerId peer4("127.0.0.1:4");
    braft::Configuration conf;
    conf.add_peer(peer1);
    conf.add_peer(peer2);
    conf.add_peer(peer3);
    braft::Ballot bl;
    ASSERT_EQ(0, bl.init(conf, &conf));
    ASSERT_EQ(2, bl._quorum);
    ASSERT_EQ(2, bl._old_quorum);
    bl.grant(peer1);
    ASSERT_EQ(1, bl._quorum);
    ASSERT_EQ(1, bl._old_quorum);
    braft::Ballot::PosHint hint = bl.grant(peer1, braft::Ballot::PosHint());
    ASSERT_EQ(1, bl._quorum);
    ASSERT_EQ(1, bl._old_quorum);
    hint = bl.grant(peer1, hint);
    ASSERT_EQ(1, bl._quorum);
    ASSERT_EQ(1, bl._old_quorum);
    hint = bl.grant(peer4, hint);
    ASSERT_EQ(1, bl._quorum);
    ASSERT_EQ(1, bl._old_quorum);
    ASSERT_FALSE(bl.granted());
    hint = bl.grant(peer2, hint);
    ASSERT_TRUE(bl.granted());
    hint = bl.grant(peer3, hint);
    ASSERT_EQ(-1, bl._quorum);
    ASSERT_EQ(-1, bl._old_quorum);
}

TEST(BallotTest, joint_consensus_different_conf) {
    braft::PeerId peer1("127.0.0.1:1");
    braft::PeerId peer2("127.0.0.1:2");
    braft::PeerId peer3("127.0.0.1:3");
    braft::PeerId peer4("127.0.0.1:4");
    braft::Configuration conf;
    conf.add_peer(peer1);
    conf.add_peer(peer2);
    conf.add_peer(peer3);
    braft::Configuration conf2;
    conf2.add_peer(peer1);
    conf2.add_peer(peer2);
    conf2.add_peer(peer3);
    conf2.add_peer(peer4);
    braft::Ballot bl;
    ASSERT_EQ(0, bl.init(conf, &conf2));
    bl.grant(peer1);
    bl.grant(peer2);
    ASSERT_FALSE(bl.granted());
    ASSERT_EQ(0, bl._quorum);
    ASSERT_EQ(1, bl._old_quorum);
    bl.grant(peer4);
    ASSERT_TRUE(bl.granted());
}
