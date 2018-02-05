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
#include <butil/logging.h>
#include "braft/raft.h"
#include "braft/configuration_manager.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, PeerId) {
    braft::PeerId id1;
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

    braft::PeerId id2(id1);
    LOG(NOTICE) << "id:" << id2;

    braft::PeerId id3("1.2.3.4:1000:0");
    LOG(NOTICE) << "id:" << id3;
}

TEST_F(TestUsageSuits, Configuration) {
    braft::Configuration conf1;
    ASSERT_TRUE(conf1.empty());
    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.1.1.1:1000:0"));
    peers.push_back(braft::PeerId("1.1.1.1:1000:1"));
    peers.push_back(braft::PeerId("1.1.1.1:1000:2"));
    conf1 = peers;
    LOG(NOTICE) << conf1;

    ASSERT_TRUE(conf1.contains(braft::PeerId("1.1.1.1:1000:0")));
    ASSERT_FALSE(conf1.contains(braft::PeerId("1.1.1.1:2000:0")));

    std::vector<braft::PeerId> peers2;
    peers2.push_back(braft::PeerId("1.1.1.1:1000:0"));
    peers2.push_back(braft::PeerId("1.1.1.1:1000:1"));
    ASSERT_TRUE(conf1.contains(peers2));
    peers2.push_back(braft::PeerId("1.1.1.1:2000:1"));
    ASSERT_FALSE(conf1.contains(peers2));

    ASSERT_FALSE(conf1.equals(peers2));
    ASSERT_TRUE(conf1.equals(peers));

    braft::Configuration conf2(peers);
    conf2.remove_peer(braft::PeerId("1.1.1.1:1000:1"));
    conf2.add_peer(braft::PeerId("1.1.1.1:1000:3"));
    ASSERT_FALSE(conf2.contains(braft::PeerId("1.1.1.1:1000:1")));
    ASSERT_TRUE(conf2.contains(braft::PeerId("1.1.1.1:1000:3")));

    std::set<braft::PeerId> peer_set;
    conf2.list_peers(&peer_set);
    ASSERT_EQ(peer_set.size(), 3);
    std::vector<braft::PeerId> peer_vector;
    conf2.list_peers(&peer_vector);
    ASSERT_EQ(peer_vector.size(), 3);
}

TEST_F(TestUsageSuits, ConfigurationManager) {
    braft::ConfigurationManager conf_manager;

    braft::ConfigurationEntry it1;
    conf_manager.get(10, &it1);
    ASSERT_EQ(it1.id, braft::LogId(0, 0));
    ASSERT_TRUE(it1.conf.empty());
    ASSERT_EQ(braft::LogId(0, 0), conf_manager.last_configuration().id);
    braft::ConfigurationEntry entry;
    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.1.1.1:1000:0"));
    peers.push_back(braft::PeerId("1.1.1.1:1000:1"));
    peers.push_back(braft::PeerId("1.1.1.1:1000:2"));
    entry.conf = peers;
    entry.id = braft::LogId(8, 1);
    conf_manager.add(entry);
    ASSERT_EQ(braft::LogId(8, 1), conf_manager.last_configuration().id);

    conf_manager.get(10, &it1);
    ASSERT_EQ(it1.id, entry.id);

    conf_manager.truncate_suffix(7);
    ASSERT_EQ(braft::LogId(0, 0), conf_manager.last_configuration().id);

    entry.id = braft::LogId(10, 1);
    entry.conf = peers;
    conf_manager.add(entry);
    peers.push_back(braft::PeerId("1.1.1.1:1000:3"));
    entry.id = braft::LogId(20, 1);
    entry.conf = peers;
    conf_manager.add(entry);
    ASSERT_EQ(braft::LogId(20, 1), conf_manager.last_configuration().id);

    conf_manager.truncate_prefix(15);
    ASSERT_EQ(braft::LogId(20, 1), conf_manager.last_configuration().id);

    conf_manager.truncate_prefix(25);
    ASSERT_EQ(braft::LogId(0, 0), conf_manager.last_configuration().id);

}
