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
#include "raft/configuration_manager.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, PeerId) {
    raft::PeerId id1;
    ASSERT_TRUE(id1.is_empty());

    ASSERT_NE(0, id1.parse("1.1.1.1::"));
    ASSERT_TRUE(id1.is_empty());

    ASSERT_EQ(0, id1.parse("1.1.1.1:1000:"));
    LOG(NOTICE) << "id:" << id1.to_string();
    LOG(NOTICE) << "id:" << id1;

    ASSERT_EQ(0, id1.parse("1.1.1.1:1000:0"));
    LOG(NOTICE) << "id:" << id1.to_string();
    LOG(NOTICE) << "id:" << id1;

    ASSERT_EQ(0, id1.parse("1.1.1.1:1000"));
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
    conf1 = peers;
    LOG(NOTICE) << conf1;

    ASSERT_TRUE(conf1.contains(raft::PeerId("1.1.1.1:1000:0")));
    ASSERT_FALSE(conf1.contains(raft::PeerId("1.1.1.1:2000:0")));

    std::vector<raft::PeerId> peers2;
    peers2.push_back(raft::PeerId("1.1.1.1:1000:0"));
    peers2.push_back(raft::PeerId("1.1.1.1:1000:1"));
    ASSERT_TRUE(conf1.contains(peers2));
    peers2.push_back(raft::PeerId("1.1.1.1:2000:1"));
    ASSERT_FALSE(conf1.contains(peers2));

    ASSERT_FALSE(conf1.equals(peers2));
    ASSERT_TRUE(conf1.equals(peers));

    raft::Configuration conf2(peers);
    conf2.remove_peer(raft::PeerId("1.1.1.1:1000:1"));
    conf2.add_peer(raft::PeerId("1.1.1.1:1000:3"));
    ASSERT_FALSE(conf2.contains(raft::PeerId("1.1.1.1:1000:1")));
    ASSERT_TRUE(conf2.contains(raft::PeerId("1.1.1.1:1000:3")));

    std::set<raft::PeerId> peer_set;
    conf2.list_peers(&peer_set);
    ASSERT_EQ(peer_set.size(), 3);
    std::vector<raft::PeerId> peer_vector;
    conf2.list_peers(&peer_vector);
    ASSERT_EQ(peer_vector.size(), 3);
}

TEST_F(TestUsageSuits, ConfigurationManager) {
    raft::ConfigurationManager* conf_manager = new raft::ConfigurationManager();

    raft::ConfigurationPair it1;
    conf_manager->get_configuration(10, &it1);
    ASSERT_EQ(it1.first, raft::LogId(0, 0));
    ASSERT_TRUE(it1.second.empty());
    ASSERT_EQ(raft::LogId(0, 0), conf_manager->last_configuration().first);

    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.1.1.1:1000:0"));
    peers.push_back(raft::PeerId("1.1.1.1:1000:1"));
    peers.push_back(raft::PeerId("1.1.1.1:1000:2"));
    conf_manager->add(raft::LogId(8, 1), raft::Configuration(peers));
    ASSERT_EQ(raft::LogId(8, 1), conf_manager->last_configuration().first);

    conf_manager->get_configuration(10, &it1);
    ASSERT_EQ(it1.first, raft::LogId(8, 1));

    conf_manager->truncate_suffix(7);
    ASSERT_EQ(raft::LogId(0, 0), conf_manager->last_configuration().first);

    conf_manager->add(raft::LogId(10, 1), raft::Configuration(peers));
    peers.push_back(raft::PeerId("1.1.1.1:1000:3"));
    conf_manager->add(raft::LogId(20, 1), raft::Configuration(peers));
    ASSERT_EQ(raft::LogId(20, 1), conf_manager->last_configuration().first);

    conf_manager->truncate_prefix(15);
    ASSERT_EQ(raft::LogId(20, 1), conf_manager->last_configuration().first);

    conf_manager->truncate_prefix(25);
    ASSERT_EQ(raft::LogId(0, 0), conf_manager->last_configuration().first);

    raft::ConfigurationPair pair = conf_manager->last_configuration();
    ASSERT_EQ(pair.first, raft::LogId(0, 0));

    delete conf_manager;
}
