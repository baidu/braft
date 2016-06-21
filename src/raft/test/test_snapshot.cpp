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

TEST_F(TestUsageSuits, writer_and_reader) {
    ::system("rm -rf data");
    raft::SnapshotStorage* storage = new raft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage);
    ASSERT_EQ(0, storage->init());

    // empty snapshot
    raft::SnapshotReader* reader = storage->open();
    ASSERT_TRUE(reader == NULL);

    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.2.3.4:1000"));
    peers.push_back(raft::PeerId("1.2.3.4:2000"));
    peers.push_back(raft::PeerId("1.2.3.4:3000"));

    raft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    //meta.last_configuration = raft::Configuration(peers);

    // normal create writer
    raft::SnapshotWriter* writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal create writer again
    meta.set_last_included_index(2000);
    meta.set_last_included_term(2);
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
    ASSERT_EQ(meta.last_included_index(), new_meta.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta.last_included_term());
    reader->set_error(EIO, "read failed");
    storage->close(reader);

    delete storage;

    // reinit
    storage = new raft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage);
    ASSERT_EQ(0, storage->init());

    // normal create writer after reinit
    meta.set_last_included_index(3000);
    meta.set_last_included_term(3);
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ("./data/temp", writer->get_path());
    ASSERT_EQ(0, storage->close(writer));

    // normal open reader after reinit
    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    raft::SnapshotMeta new_meta2;
    ASSERT_EQ(0, reader->load_meta(&new_meta2));
    ASSERT_EQ(meta.last_included_index(), new_meta2.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta2.last_included_term());
    storage->close(reader);

    delete storage;
}

TEST_F(TestUsageSuits, copy) {
    ::system("rm -rf data");

    baidu::rpc::Server server;
    ASSERT_EQ(0, raft::add_service(&server, "0.0.0.0:60006"));
    ASSERT_EQ(0, server.Start(60006, NULL));

    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.2.3.4:1000"));
    peers.push_back(raft::PeerId("1.2.3.4:2000"));
    peers.push_back(raft::PeerId("1.2.3.4:3000"));

    raft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    raft::LocalSnapshotStorage* storage1 = new raft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(base::EndPoint(base::get_host_ip(), 60006));
    // normal create writer
    raft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    raft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    ::system("rm -rf data2");
    raft::SnapshotStorage* storage2 = new raft::LocalSnapshotStorage("./data2");
    raft::SnapshotReader* reader2 = storage2->copy_from(uri);
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
}

struct Arg {
    raft::SnapshotStorage* storage;
    volatile bool stopped;
};

void *read_thread(void* arg) {
    Arg *a = (Arg*)arg;
    while (!a->stopped) {
        raft::SnapshotMeta meta;
        raft::SnapshotReader* reader = a->storage->open();
        if (reader == NULL) {
            EXPECT_TRUE(false);
            break;
        }
        if (reader->load_meta(&meta) != 0) {
            abort();
            break;
        }
        if (a->storage->close(reader) != 0) {
            EXPECT_TRUE(false);
            break;
        }
    }
    return NULL;
}

void *write_thread(void* arg) {
    Arg *a = (Arg*)arg;
    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.2.3.4:1000"));
    peers.push_back(raft::PeerId("1.2.3.4:2000"));
    peers.push_back(raft::PeerId("1.2.3.4:3000"));

    raft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);

    while (!a->stopped) {
        // normal create writer
        raft::SnapshotWriter* writer = a->storage->create();
        if (writer == NULL) {
            EXPECT_TRUE(false);
            break;
        }
        if (writer->save_meta(meta) ) {
            EXPECT_TRUE(false);
            break;
        }
        if (a->storage->close(writer) != 0) {
            EXPECT_TRUE(false);
            break;
        }
    }
    return NULL;
}

TEST_F(TestUsageSuits, thread_safety) {
    raft::SnapshotStorage* storage = new raft::LocalSnapshotStorage("./data");
    ASSERT_EQ(0, storage->init());
    Arg arg;
    arg.storage = storage;
    arg.stopped = false;
    pthread_t writer;
    pthread_t reader;
    ASSERT_EQ(0, pthread_create(&writer, NULL, write_thread, &arg));
    usleep(10 * 1000);
    ASSERT_EQ(0, pthread_create(&reader, NULL, read_thread, &arg));
    usleep(1L * 1000 * 1000);
    arg.stopped = true;
    pthread_join(writer, NULL);
    pthread_join(reader, NULL);
    delete storage;
}
