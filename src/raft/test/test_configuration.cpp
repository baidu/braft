/*
 * =====================================================================================
 *
 *       Filename:  test_configuration.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月22日 15时16分31秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include <base/logging.h>
#include "raft/raft.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, PeerId) {
    raft::PeerId id1;
    ASSERT_TRUE(id1.is_empty());

    ASSERT_NE(0, id1.parse("1.1.1.1:1000:"));
    ASSERT_TRUE(id1.is_empty());

    ASSERT_EQ(0, id1.parse("1.1.1.1:1000:0"));
    LOG(NOTICE) << "id:" << id1.to_string();
    LOG(NOTICE) << "id:" << id1;

    raft::PeerId id2(id1);
    LOG(NOTICE) << "id:" << id2;

    raft::PeerId id3("1.2.3.4:1000:0");
    LOG(NOTICE) << "id:" << id3;
}

TEST_F(TestUsageSuits, Configuration) {
    raft::Configuration conf1;
    ASSERT_TRUE(conf1.empty());
    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.1.1.1:1000:0"));
    peers.push_back(raft::PeerId("1.1.1.1:1000:1"));
    peers.push_back(raft::PeerId("1.1.1.1:1000:2"));
    conf1.set_peer(peers);
    LOG(NOTICE) << conf1;

    ASSERT_TRUE(conf1.contain(raft::PeerId("1.1.1.1:1000:0")));
    ASSERT_FALSE(conf1.contain(raft::PeerId("1.1.1.1:2000:0")));

    std::vector<raft::PeerId> peers2;
    peers2.push_back(raft::PeerId("1.1.1.1:1000:0"));
    peers2.push_back(raft::PeerId("1.1.1.1:1000:1"));
    ASSERT_TRUE(conf1.contain(peers2));
    peers2.push_back(raft::PeerId("1.1.1.1:2000:1"));
    ASSERT_FALSE(conf1.contain(peers2));

    ASSERT_FALSE(conf1.equal(peers2));
    ASSERT_TRUE(conf1.equal(peers));

    raft::Configuration conf2(peers);
    conf2.remove_peer(raft::PeerId("1.1.1.1:1000:1"));
    conf2.add_peer(raft::PeerId("1.1.1.1:1000:3"));
    ASSERT_FALSE(conf2.contain(raft::PeerId("1.1.1.1:1000:1")));
    ASSERT_TRUE(conf2.contain(raft::PeerId("1.1.1.1:1000:3")));

    std::set<raft::PeerId> peer_set;
    conf2.peer_set(&peer_set);
    ASSERT_EQ(peer_set.size(), 3);
    std::vector<raft::PeerId> peer_vector;
    conf2.peer_vector(&peer_vector);
    ASSERT_EQ(peer_vector.size(), 3);
}

TEST_F(TestUsageSuits, ConfigurationManager) {
    scoped_refptr<raft::ConfigurationManager> conf_manager =
        make_scoped_refptr(new raft::ConfigurationManager());

    raft::ConfigurationPair it1 = conf_manager->get_configuration(10);
    ASSERT_EQ(it1.first, 0);
    ASSERT_TRUE(it1.second.empty());
    ASSERT_EQ(0, conf_manager->last_configuration_index());

    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.1.1.1:1000:0"));
    peers.push_back(raft::PeerId("1.1.1.1:1000:1"));
    peers.push_back(raft::PeerId("1.1.1.1:1000:2"));
    conf_manager->add(8, raft::Configuration(peers));
    ASSERT_EQ(8, conf_manager->last_configuration_index());

    it1 = conf_manager->get_configuration(10);
    ASSERT_EQ(it1.first, 8);

    conf_manager->truncate_suffix(7);
    ASSERT_EQ(0, conf_manager->last_configuration_index());

    conf_manager->add(10, raft::Configuration(peers));
    peers.push_back(raft::PeerId("1.1.1.1:1000:3"));
    conf_manager->add(20, raft::Configuration(peers));
    ASSERT_EQ(20, conf_manager->last_configuration_index());

    conf_manager->truncate_prefix(15);
    ASSERT_EQ(20, conf_manager->last_configuration_index());

    conf_manager->truncate_prefix(25);
    ASSERT_EQ(20, conf_manager->last_configuration_index());

    raft::ConfigurationPair pair = conf_manager->last_configuration();
    ASSERT_EQ(pair.first, 20);
}
