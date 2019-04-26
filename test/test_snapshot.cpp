// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/08 17:00:05

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/file_util.h>
#include <errno.h>
#include <brpc/server.h>
#include "braft/snapshot.h"
#include "braft/raft.h"
#include "braft/util.h"
#include "braft/local_file_meta.pb.h"
#include "braft/snapshot_throttle.h"
#include "memory_file_system_adaptor.h"

namespace logging {
DECLARE_int32(minloglevel);
};

class SnapshotTest : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

#define FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs)                                   \
    braft::FileSystemAdaptor* file_system_adaptors[] = {                          \
        NULL, new braft::PosixFileSystemAdaptor, new MemoryFileSystemAdaptor \
    };                                                                           \
    for (size_t fs_index = 0; fs_index != sizeof(file_system_adaptors)           \
         / sizeof(file_system_adaptors[0]); ++fs_index) {                        \
        fs = file_system_adaptors[fs_index];

#define FOR_EACH_FILE_SYSTEM_ADAPTOR_END }

TEST_F(SnapshotTest, writer_and_reader) {
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);
    
    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }
    braft::SnapshotStorage* storage = new braft::LocalSnapshotStorage("./data");
    if (fs) {
        ASSERT_EQ(storage->set_file_system_adaptor(fs), 0);
    }
    ASSERT_TRUE(storage);
    ASSERT_EQ(0, storage->init());

    // empty snapshot
    braft::SnapshotReader* reader = storage->open();
    ASSERT_TRUE(reader == NULL);

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);

    // normal create writer
    braft::SnapshotWriter* writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal create writer again
    meta.set_last_included_index(2000);
    meta.set_last_included_term(2);
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);

    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal open reader
    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    braft::SnapshotMeta new_meta;
    ASSERT_EQ(0, reader->load_meta(&new_meta));
    ASSERT_EQ(meta.last_included_index(), new_meta.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta.last_included_term());
    reader->set_error(EIO, "read failed");
    storage->close(reader);

    delete storage;

    // reinit
    storage = new braft::LocalSnapshotStorage("./data");
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
    braft::SnapshotMeta new_meta2;
    ASSERT_EQ(0, reader->load_meta(&new_meta2));
    ASSERT_EQ(meta.last_included_index(), new_meta2.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta2.last_included_term());
    storage->close(reader);

    // normal create writer after reinit
    meta.Clear();
    meta.set_last_included_index(5000);
    meta.set_last_included_term(4);
    for (int i = 1; i <= 3; ++i) {
        meta.add_peers("127.0.0.1:" + std::to_string(i));
    }
    for (int i = 4; i <= 6; ++i) {
        meta.add_old_peers("127.0.0.1:" + std::to_string(i));
    }
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ("./data/temp", writer->get_path());
    ASSERT_EQ(0, storage->close(writer));

    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    braft::SnapshotMeta new_meta3;
    ASSERT_EQ(0, reader->load_meta(&new_meta3));
    ASSERT_EQ(meta.last_included_index(), new_meta3.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta3.last_included_term());
    storage->close(reader);

    ASSERT_EQ(new_meta3.peers_size(), meta.peers_size());
    ASSERT_EQ(new_meta3.old_peers_size(), meta.old_peers_size());

    for (int i = 0; i < new_meta3.peers_size(); ++i) {
        ASSERT_EQ(new_meta3.peers(i), meta.peers(i));
    }

    for (int i = 0; i < new_meta3.old_peers_size(); ++i) {
        ASSERT_EQ(new_meta3.old_peers(i), meta.old_peers(i));
    }

    delete storage;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
}

TEST_F(SnapshotTest, copy) {
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
}

TEST_F(SnapshotTest, file_escapes_directory) {
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    braft::LocalSnapshotStorage* storage1
            = new braft::LocalSnapshotStorage("./data/snapshot1/data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    if (!fs) {
        ASSERT_EQ(0, system("mkdir -p ./data/snapshot1/dir1/ && touch ./data/snapshot1/dir1/file"));
    } else {
        ASSERT_TRUE(fs->create_directory("./data/snapshot1/dir1/", NULL, true));
        braft::FileAdaptor* file = fs->open("./data/snapshot1/dir1/file", 
                O_CREAT | O_TRUNC | O_RDWR, NULL, NULL);
        CHECK(file != NULL);
        delete file;
    }
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    ASSERT_EQ(0, writer1->add_file("../../dir1/file"));
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    braft::LocalSnapshotStorage* storage2
            = new braft::LocalSnapshotStorage("./data/snapshot2/data");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    if (!fs) {
        ASSERT_TRUE(butil::PathExists(butil::FilePath("./data/snapshot2/dir1/file")));
    } else {
        ASSERT_TRUE(fs->path_exists("./data/snapshot2/dir1/file"));
    }
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
}

struct Arg {
    braft::SnapshotStorage* storage;
    volatile bool stopped;
};

void *read_thread(void* arg) {
    Arg *a = (Arg*)arg;
    while (!a->stopped) {
        braft::SnapshotMeta meta;
        braft::SnapshotReader* reader = a->storage->open();
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
    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);

    while (!a->stopped) {
        // normal create writer
        braft::SnapshotWriter* writer = a->storage->create();
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

TEST_F(SnapshotTest, thread_safety) {
    // writer thread will make much log when sleep
    logging::FLAGS_minloglevel = 1;

    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    braft::SnapshotStorage* storage = new braft::LocalSnapshotStorage("./data");
    if (fs) {
        ASSERT_EQ(storage->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage->init());
    Arg arg;
    arg.storage = storage;
    arg.stopped = false;
    pthread_t writer;
    pthread_t reader;
    ASSERT_EQ(0, pthread_create(&writer, NULL, write_thread, &arg));
    usleep(100 * 1000);
    ASSERT_EQ(0, pthread_create(&reader, NULL, read_thread, &arg));
    usleep(1L * 1000 * 1000);
    arg.stopped = true;
    pthread_join(writer, NULL);
    pthread_join(reader, NULL);
    delete storage;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;

    logging::FLAGS_minloglevel = 0;
}

void write_file(braft::FileSystemAdaptor* fs, const std::string& path, const std::string& data) {
    if (!fs) {
        fs = braft::default_file_system();
    }
    braft::FileAdaptor* file = fs->open(path, O_CREAT | O_TRUNC | O_RDWR, NULL, NULL);
    CHECK(file != NULL);
    butil::IOBuf io_buf;
    io_buf.append(data);
    CHECK_EQ(data.size(), file->write(io_buf, 0));
    delete file;
}

void add_file_meta(braft::FileSystemAdaptor* fs, braft::SnapshotWriter* writer, int index, 
                   const std::string* checksum, const std::string& data) {
    std::stringstream path;
    path << "file" << index;
    braft::LocalFileMeta file_meta;
    if (checksum) {
        file_meta.set_checksum(*checksum);
    }
    write_file(fs, writer->get_path() + "/" + path.str(), path.str() + ": " + data);
    ASSERT_EQ(0, writer->add_file(path.str(), &file_meta));
}

void add_file_without_meta(braft::FileSystemAdaptor* fs, braft::SnapshotWriter* writer, int index, 
                   const std::string& data) {
    std::stringstream path;
    path << "file" << index;
    write_file(fs, writer->get_path() + "/" + path.str(), path.str() + ": " + data);
}

bool check_file_exist(braft::FileSystemAdaptor* fs, const std::string& path, int index) {
    if (fs == NULL) {
        fs = braft::default_file_system();
    }
    std::stringstream ss;
    ss << path << "/file" << index;
    return fs->path_exists(ss.str());
}

std::string read_from_file(braft::FileSystemAdaptor* fs, const std::string& path, int index) {
    if (fs == NULL) {
        fs = braft::default_file_system();
    }
    std::stringstream ss;
    ss << path << "/file" << index;
    braft::FileAdaptor* file = fs->open(ss.str(), O_RDONLY, NULL, NULL);
    ssize_t size = file->size();
    butil::IOPortal buf;
    file->read(&buf, 0, size_t(size));
    delete file;
    return buf.to_string();
}

TEST_F(SnapshotTest, filter_before_copy) {
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);

    const std::string data1("aaa");
    const std::string checksum1("1");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    add_file_meta(fs, writer1, 2, NULL, data1);
    add_file_meta(fs, writer1, 3, &checksum1, data1);
    add_file_meta(fs, writer1, 4, &checksum1, data1);
    add_file_meta(fs, writer1, 5, &checksum1, data1);
    add_file_meta(fs, writer1, 6, &checksum1, data1);
    add_file_meta(fs, writer1, 7, NULL, data1);
    add_file_meta(fs, writer1, 8, &checksum1, data1);
    add_file_meta(fs, writer1, 9, &checksum1, data1);
   
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
        ::system("rm -rf snapshot_temp");
    } else {
        fs->delete_file("data2", true);
        fs->delete_file("snapshot_temp", true);
    }

    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    storage2->set_filter_before_copy_remote();
    ASSERT_EQ(0, storage2->init());

    braft::SnapshotWriter* writer2 = storage2->create();
    ASSERT_TRUE(writer2 != NULL);

    meta.set_last_included_index(900);
    meta.set_last_included_term(1);
    const std::string& data2("bbb");
    const std::string& checksum2("2");
    // same checksum, will not copy
    add_file_meta(fs, writer2, 1, &checksum1, data2);
    // remote checksum not set, local set, will copy
    add_file_meta(fs, writer2, 2, &checksum1, data2);
    // remote checksum set, local not set, will copy
    add_file_meta(fs, writer2, 3, NULL, data2);
    // different checksum, will copy
    add_file_meta(fs, writer2, 4, &checksum2, data2);
    // file not exist in remote, will delete
    add_file_meta(fs, writer2, 100, &checksum2, data2);
    // file exit but meta not exit, will delete
    add_file_without_meta(fs, writer2, 102, data2);

    ASSERT_EQ(0, writer2->save_meta(meta));
    ASSERT_EQ(0, storage2->close(writer2));
    if (fs == NULL) {
        ::system("mv data2/snapshot_00000000000000000900 snapshot_temp");
    } else {
        fs->rename("data2/snapshot_00000000000000000900", "snapshot_temp");
    }

    writer2 = storage2->create();
    ASSERT_TRUE(writer2 != NULL);

    meta.set_last_included_index(901);
    const std::string data3("ccc");
    const std::string checksum3("3");
    // same checksum, will copy from last_snapshot with index=901
    add_file_meta(fs, writer2, 6, &checksum1, data3);
    // remote checksum not set, local last_snapshot set, will copy
    add_file_meta(fs, writer2, 7, &checksum1, data3);
    // remote checksum set, local last_snapshot not set, will copy
    add_file_meta(fs, writer2, 8, NULL, data3);
    // remote and local last_snapshot different checksum, will copy
    add_file_meta(fs, writer2, 9, &checksum3, data3);
    // file not exist in remote, will not copy
    add_file_meta(fs, writer2, 101, &checksum3, data3);
    ASSERT_EQ(0, writer2->save_meta(meta));
    ASSERT_EQ(0, storage2->close(writer2));

    if (fs == NULL) {
        ::system("mv snapshot_temp data2/temp");
    } else {
        fs->rename("snapshot_temp", "data2/temp");
    }

    ASSERT_EQ(0, storage2->init());
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));

    const std::string snapshot_path("data2/snapshot_00000000000000001000");
    for (int i = 1; i <= 9; ++i) {
        ASSERT_TRUE(check_file_exist(fs, snapshot_path, i));
        std::stringstream content;
        content << "file" << i << ": ";
        if (i == 1) {
            content << data2;
        } else if (i == 6) {
            content << data3;
        } else {
            content << data1;
        }
        ASSERT_EQ(content.str(), read_from_file(fs, snapshot_path, i));
    }
    ASSERT_TRUE(!check_file_exist(fs, snapshot_path, 100));
    ASSERT_TRUE(!check_file_exist(fs, snapshot_path, 101));
    ASSERT_TRUE(!check_file_exist(fs, snapshot_path, 102));

    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
}

TEST_F(SnapshotTest, snapshot_throttle_for_reading) {
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage1
    braft::ThroughputSnapshotThrottle* throttle = new 
        braft::ThroughputSnapshotThrottle(60, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage1->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add file meta for storage1
    const std::string data1("aaa");
    const std::string checksum1("1");
    add_file_meta(fs, writer1, 1, &checksum1, data1);

    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    // copy
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    LOG(INFO) << "Copy finish.";
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
}

TEST_F(SnapshotTest, snapshot_throttle_for_writing) {
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // create writer1
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add nomal file for storage1
    LOG(INFO) << "add nomal file";
    const std::string data1("aaa");
    const std::string checksum1("1000");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    // save snapshot meta for storage1 
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));
    // get uri of storage1
    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    // create and set snapshot throttle for storage2
    braft::ThroughputSnapshotThrottle* throttle2 = new 
        braft::ThroughputSnapshotThrottle(3 * 1000 * 1000, 10);
    ASSERT_TRUE(throttle2);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle2), 0);
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage2
    braft::SnapshotThrottle* throttle = 
        new braft::ThroughputSnapshotThrottle(20, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage2->init());

    // copy from storage1 to storage2
    LOG(INFO) << "Copy start.";
    braft::SnapshotCopier* copier = storage2->start_to_copy_from(uri);
    ASSERT_TRUE(copier != NULL);
    copier->join();
    LOG(INFO) << "Copy finish.";
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(copier));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
}

TEST_F(SnapshotTest, snapshot_throttle_for_reading_without_enable_throttle) {
    GFLAGS_NS::SetCommandLineOption("raft_enable_throttle_when_install_snapshot", "false");
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage1
    braft::ThroughputSnapshotThrottle* throttle = new
        braft::ThroughputSnapshotThrottle(30, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage1->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add file meta for storage1
    const std::string data1("aaa");
    const std::string checksum1("1");
    add_file_meta(fs, writer1, 1, &checksum1, data1);

    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    // create and set snapshot throttle for storage2
    braft::ThroughputSnapshotThrottle* throttle2 = new
        braft::ThroughputSnapshotThrottle(3 * 1000 * 1000, 10);
    ASSERT_TRUE(throttle2);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle2), 0);
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    // copy
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    LOG(INFO) << "Copy finish.";
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
    GFLAGS_NS::SetCommandLineOption("raft_enable_throttle_when_install_snapshot", "true");
}

TEST_F(SnapshotTest, snapshot_throttle_for_writing_without_enable_throttle) {
    GFLAGS_NS::SetCommandLineOption("raft_enable_throttle_when_install_snapshot", "false");
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // create writer1
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add nomal file for storage1
    LOG(INFO) << "add nomal file";
    const std::string data1("aaa");
    const std::string checksum1("1000");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    // save snapshot meta for storage1
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));
    // get uri of storage1
    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage2
    braft::SnapshotThrottle* throttle =
        new braft::ThroughputSnapshotThrottle(20, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage2->init());

    // copy from storage1 to storage2
    LOG(INFO) << "Copy start.";
    braft::SnapshotCopier* copier = storage2->start_to_copy_from(uri);
    ASSERT_TRUE(copier != NULL);
    copier->join();
    LOG(INFO) << "Copy finish.";
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(copier));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
    GFLAGS_NS::SetCommandLineOption("raft_enable_throttle_when_install_snapshot", "true");
}

TEST_F(SnapshotTest, dynamically_change_throttle_threshold) {
    GFLAGS_NS::SetCommandLineOption("raft_minimal_throttle_threshold_mb", "1");
    braft::FileSystemAdaptor* fs;
    FOR_EACH_FILE_SYSTEM_ADAPTOR_BEGIN(fs);

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, braft::add_service(&server, "0.0.0.0:6006"));
    ASSERT_EQ(0, server.Start(6006, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    braft::LocalSnapshotStorage* storage1 = new braft::LocalSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    storage1->set_server_addr(butil::EndPoint(butil::my_ip(), 6006));
    // create writer1
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add nomal file for storage1
    LOG(INFO) << "add nomal file";
    const std::string data1("aaa");
    const std::string checksum1("1000");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    // save snapshot meta for storage1
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));
    // get uri of storage1
    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    braft::SnapshotStorage* storage2 = new braft::LocalSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage2
    braft::SnapshotThrottle* throttle =
        new braft::ThroughputSnapshotThrottle(10, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage2->init());

    // copy from storage1 to storage2
    LOG(INFO) << "Copy start.";
    braft::SnapshotCopier* copier = storage2->start_to_copy_from(uri);
    ASSERT_TRUE(copier != NULL);
    copier->join();
    LOG(INFO) << "Copy finish.";
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(copier));
    delete storage2;
    delete storage1;

    FOR_EACH_FILE_SYSTEM_ADAPTOR_END;
    GFLAGS_NS::SetCommandLineOption("raft_minimal_throttle_threshold_mb", "0");
}
