// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/03/29 10:35:06

#include <gtest/gtest.h>
#include "braft/storage.h"

namespace braft {
extern void global_init_once_or_die();
};

class StorageTest : public testing::Test {
protected:
    void SetUp() {
        system("rm -rf data");
        braft::global_init_once_or_die();
    }
    void TearDown() {}
};

TEST_F(StorageTest, sanity) {
    // LogStorage
    braft::LogStorage* log_storage = braft::LogStorage::create("local://data/log");
    ASSERT_TRUE(log_storage);
    braft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    ASSERT_FALSE(braft::LogStorage::create("hdfs://data/log"));
    ASSERT_FALSE(braft::LogStorage::create("://data/log"));
    ASSERT_FALSE(braft::LogStorage::create("data/log"));
    ASSERT_FALSE(braft::LogStorage::create("  ://data/log"));

    // RaftMetaStorage
    braft::RaftMetaStorage* meta_storage 
            = braft::RaftMetaStorage::create("local://data/raft_meta");
    ASSERT_TRUE(meta_storage);
    ASSERT_EQ(0, meta_storage->init());
    ASSERT_FALSE(braft::RaftMetaStorage::create("hdfs://data/raft_meta"));
    ASSERT_FALSE(braft::RaftMetaStorage::create("://data/raft_meta"));
    ASSERT_FALSE(braft::RaftMetaStorage::create("data/raft_meta"));
    ASSERT_FALSE(braft::RaftMetaStorage::create("  ://data/raft_meta"));

    // SnapshotStorage
    braft::SnapshotStorage* snapshot_storage 
            = braft::SnapshotStorage::create("local://data/snapshot");
    ASSERT_TRUE(snapshot_storage);
    ASSERT_EQ(0, snapshot_storage->init());
    ASSERT_FALSE(braft::SnapshotStorage::create("hdfs://data/snapshot"));
    ASSERT_FALSE(braft::SnapshotStorage::create("://data/snapshot"));
    ASSERT_FALSE(braft::SnapshotStorage::create("data/snapshot"));
    ASSERT_FALSE(braft::SnapshotStorage::create("  ://data/snapshot"));

}

TEST_F(StorageTest, extra_space_should_be_trimmed) {
    // LogStorage
    braft::LogStorage* log_storage = braft::LogStorage::create("local://data/log");
    ASSERT_TRUE(log_storage);
    braft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    braft::LogEntry* entry = new braft::LogEntry();
    entry->data.append("hello world");
    entry->id = braft::LogId(1, 1);
    entry->type = braft::ENTRY_TYPE_DATA;
    std::vector<braft::LogEntry*> entries;
    entries.push_back(entry);
    ASSERT_EQ(1u, log_storage->append_entries(entries));
    entry->Release();
    delete log_storage;

    // reopen
    log_storage = braft::LogStorage::create(" local://./  data// // log ////");
    ASSERT_EQ(0, log_storage->init(&cm));
    
    ASSERT_EQ(1, log_storage->first_log_index());
    ASSERT_EQ(1, log_storage->last_log_index());
    entry = log_storage->get_entry(1);
    ASSERT_TRUE(entry);
    ASSERT_EQ("hello world", entry->data.to_string());
    ASSERT_EQ(braft::LogId(1, 1), entry->id);
    entry->Release();
    entry = NULL;
}
