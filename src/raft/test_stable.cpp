/*
 * =====================================================================================
 *
 *       Filename:  test_stable.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年09月24日 20时58分35秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "raft/stable.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, stable) {
    raft::LocalStableStorage* storage = new raft::LocalStableStorage("./data");

    // no init
    {
        ASSERT_EQ(-1, storage->set_term(10));
        int64_t term = storage->get_term();
        ASSERT_EQ(term, -1L);
        raft::PeerId candidate;
        ASSERT_EQ(0, candidate.parse("1.1.1.1:1000:0"));
        ASSERT_NE(0, candidate.parse("1.1.1.1,1000,0"));
        ASSERT_NE(0, candidate.parse("1.1.1.1:1000,0"));
        ASSERT_EQ(-1, storage->set_votedfor(candidate));
        raft::PeerId candidate2;
        ASSERT_EQ(-1, storage->get_votedfor(&candidate2));
    }

    ASSERT_EQ(0, storage->init());
    {
        ASSERT_EQ(0, storage->set_term(10));
        int64_t term = storage->get_term();
        ASSERT_EQ(term, 10);
        raft::PeerId candidate;
        ASSERT_EQ(0, candidate.parse("1.1.1.1:1000:0"));
        ASSERT_EQ(0, storage->set_votedfor(candidate));
        raft::PeerId candidate2;
        ASSERT_EQ(0, storage->get_votedfor(&candidate2));
        ASSERT_EQ(candidate2.addr, candidate.addr);
        ASSERT_EQ(candidate2.idx, candidate.idx);
    }

    delete storage;

    storage = new raft::LocalStableStorage("./data");
    ASSERT_EQ(0, storage->init());
    {
        int64_t term = storage->get_term();
        ASSERT_EQ(term, 10);
        raft::PeerId candidate2;
        ASSERT_EQ(0, storage->get_votedfor(&candidate2));
        base::ip_t ip;
        base::str2ip("1.1.1.1", &ip);
        ASSERT_EQ(candidate2.addr.ip, ip);
        ASSERT_EQ(candidate2.addr.port, 1000);
        ASSERT_EQ(candidate2.idx, 0);
    }
    delete storage;
}
