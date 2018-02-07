#include <gtest/gtest.h>
#include "braft/raft_meta.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, sanity) {
    braft::LocalRaftMetaStorage* storage = new braft::LocalRaftMetaStorage("./data");

    // no init
    {
        ASSERT_EQ(-1, storage->set_term(10));
        int64_t term = storage->get_term();
        ASSERT_EQ(term, -1L);
        braft::PeerId candidate;
        ASSERT_EQ(0, candidate.parse("1.1.1.1:1000:0"));
        ASSERT_NE(0, candidate.parse("1.1.1.1,1000,0"));
        ASSERT_EQ(-1, storage->set_votedfor(candidate));
        braft::PeerId candidate2;
        ASSERT_EQ(-1, storage->get_votedfor(&candidate2));
        ASSERT_NE(0, storage->set_term_and_votedfor(10, candidate));
    }

    ASSERT_EQ(0, storage->init());
    ASSERT_EQ(0, storage->init());
    {
        ASSERT_EQ(0, storage->set_term(10));
        int64_t term = storage->get_term();
        ASSERT_EQ(term, 10);
        braft::PeerId candidate;
        ASSERT_EQ(0, candidate.parse("1.1.1.1:1000:0"));
        ASSERT_EQ(0, storage->set_votedfor(candidate));
        braft::PeerId candidate2;
        ASSERT_EQ(0, storage->get_votedfor(&candidate2));
        ASSERT_EQ(candidate2.addr, candidate.addr);
        ASSERT_EQ(candidate2.idx, candidate.idx);

        braft::PeerId candidate3;
        ASSERT_EQ(0, candidate3.parse("2.2.2.2:2000:0"));
        ASSERT_EQ(0, storage->set_term_and_votedfor(11, candidate3));
    }

    delete storage;

    storage = new braft::LocalRaftMetaStorage("./data");
    ASSERT_EQ(0, storage->init());
    {
        int64_t term = storage->get_term();
        ASSERT_EQ(term, 11);
        braft::PeerId candidate2;
        ASSERT_EQ(0, storage->get_votedfor(&candidate2));
        butil::ip_t ip;
        butil::str2ip("2.2.2.2", &ip);
        ASSERT_EQ(candidate2.addr.ip, ip);
        ASSERT_EQ(candidate2.addr.port, 2000);
        ASSERT_EQ(candidate2.idx, 0);
    }
    delete storage;
}
