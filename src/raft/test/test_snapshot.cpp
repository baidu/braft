/*
 * =====================================================================================
 *
 *       Filename:  test_snapshot.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年11月26日 16时58分05秒
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
#include <errno.h>
#include <baidu/rpc/server.h>
#include "raft/snapshot.h"
#include "raft/raft.h"
#include "raft/util.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, create) {
    raft::SnapshotStorage* storage = raft::create_local_snapshot_storage("");
    ASSERT_TRUE(storage == NULL);
    storage = raft::create_local_snapshot_storage("file://./data");
    ASSERT_TRUE(storage != NULL);
    delete storage;
}

TEST_F(TestUsageSuits, writer_and_reader) {
    ::system("rm -rf data");
    raft::SnapshotStorage* storage = raft::create_local_snapshot_storage("file://./data");

    // empty snapshot
    raft::SnapshotReader* reader = storage->open();
    ASSERT_TRUE(reader == NULL);

    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.2.3.4:1000"));
    peers.push_back(raft::PeerId("1.2.3.4:2000"));
    peers.push_back(raft::PeerId("1.2.3.4:3000"));

    raft::SnapshotMeta meta;
    meta.last_included_index = 1000;
    meta.last_included_term = 2;
    meta.last_configuration = raft::Configuration(peers);

    // normal create writer
    raft::SnapshotWriter* writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal create writer again
    meta.last_included_index = 2000;
    meta.last_included_term = 2;
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);

    // double create will fail, because flock
    // lockf only work in multi processes
    //raft::SnapshotWriter* writer2 = storage->create(meta);
    //ASSERT_TRUE(writer2 == NULL);

    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal open reader
    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    raft::SnapshotMeta new_meta;
    ASSERT_EQ(0, reader->load_meta(&new_meta));
    ASSERT_EQ(meta.last_included_index, new_meta.last_included_index);
    ASSERT_EQ(meta.last_included_term, new_meta.last_included_term);
    reader->set_error(EIO, "read failed");
    storage->close(reader);

    delete storage;

    // reinit
    storage = raft::create_local_snapshot_storage("file://./data");

    // normal create writer after reinit
    meta.last_included_index = 3000;
    meta.last_included_term = 3;
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ("file://./data/temp", writer->get_uri(base::EndPoint()));
    ASSERT_EQ(0, storage->close(writer));

    // normal open reader after reinit
    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    raft::SnapshotMeta new_meta2;
    ASSERT_EQ(0, reader->load_meta(&new_meta2));
    ASSERT_EQ(meta.last_included_index, new_meta2.last_included_index);
    ASSERT_EQ(meta.last_included_term, new_meta2.last_included_term);
    storage->close(reader);

    delete storage;
}

TEST_F(TestUsageSuits, copy) {
    ::system("rm -rf data");

    baidu::rpc::Server server;
    ASSERT_EQ(0, raft::start_raft("0.0.0.0:60006", &server, NULL));

    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.2.3.4:1000"));
    peers.push_back(raft::PeerId("1.2.3.4:2000"));
    peers.push_back(raft::PeerId("1.2.3.4:3000"));

    raft::SnapshotMeta meta;
    meta.last_included_index = 1000;
    meta.last_included_term = 2;
    meta.last_configuration = raft::Configuration(peers);

    // storage1
    raft::SnapshotStorage* storage1 = raft::create_local_snapshot_storage("file://./data");

    // normal create writer
    raft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    raft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->get_uri(base::EndPoint(base::get_host_ip(), 60006));
    ASSERT_EQ(0, storage1->close(reader1));

    // storage2
    ::system("rm -rf data2");
    raft::SnapshotStorage* storage2 = raft::create_local_snapshot_storage("file://./data2");
    raft::SnapshotWriter* writer2 = storage2->create();
    ASSERT_TRUE(writer2 != NULL);
    ASSERT_EQ(0, writer2->save_meta(meta));
    ASSERT_NE(0, writer2->copy(""));
    ASSERT_EQ(0, writer2->copy(uri));
    uri.assign("file://127.0.0.1:60006/./data_noexist");
    ASSERT_NE(0, writer2->copy(uri));
    writer2->set_error(EIO, "writer failed");
    ASSERT_NE(0, storage2->close(writer2));

    ASSERT_EQ(0, ((raft::LocalSnapshotStorage*)storage2)->_last_snapshot_index);
}
