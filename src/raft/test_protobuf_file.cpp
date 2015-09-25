/*
 * =====================================================================================
 *
 *       Filename:  test_protobuf_file.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/22 19:48:31
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "raft/local_storage.pb.h"
#include "raft/protobuf_file.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, protobuf_file) {
    int ret = 0;

    raft::ProtoBufFile pb_file("./log.meta");
    raft::local_storage::EntryMeta meta;
    meta.set_term(1234);
    meta.set_type(raft::protocol::DATA);
    meta.add_peers("1.1.1.1");
    meta.add_peers("2.2.2.2");

    ret = pb_file.save(static_cast<google::protobuf::Message*>(&meta), false);
    ASSERT_EQ(ret, 0);

    {
        raft::local_storage::EntryMeta new_meta;
        ret = pb_file.load(&new_meta);
        ASSERT_EQ(ret, 0);

        ASSERT_EQ(new_meta.term(), 1234);
        ASSERT_EQ(new_meta.type(), raft::protocol::DATA);
        ASSERT_EQ(new_meta.peers_size(), 2);
        ASSERT_EQ(0, strcmp(new_meta.peers(0).c_str(), "1.1.1.1"));
        ASSERT_EQ(0, strcmp(new_meta.peers(1).c_str(), "2.2.2.2"));
    }

    ret = pb_file.save(&meta, true);
    ASSERT_EQ(ret, 0);

    {
        raft::local_storage::EntryMeta new_meta;
        ret = pb_file.load(&new_meta);
        ASSERT_EQ(ret, 0);

        ASSERT_EQ(new_meta.term(), 1234);
        ASSERT_EQ(new_meta.type(), raft::protocol::DATA);
        ASSERT_EQ(new_meta.peers_size(), 2);
        ASSERT_EQ(0, strcmp(new_meta.peers(0).c_str(), "1.1.1.1"));
        ASSERT_EQ(0, strcmp(new_meta.peers(1).c_str(), "2.2.2.2"));

        new_meta.PrintDebugString();
    }
}
