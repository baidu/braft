// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/03/29 10:35:06

#include <gtest/gtest.h>
#include "raft/storage.h"

namespace raft {
extern void global_init_once_or_die();
};

class StorageTest : public testing::Test {
protected:
    void SetUp() {
        system("rm -rf data");
        raft::global_init_once_or_die();
    }
    void TearDown() {}
};

TEST_F(StorageTest, sanity) {
    // LogStorage
    raft::LogStorage* log_storage = raft::LogStorage::create("local://data/log");
    ASSERT_TRUE(log_storage);
    raft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    ASSERT_FALSE(raft::LogStorage::create("hdfs://data/log"));
    ASSERT_FALSE(raft::LogStorage::create("://data/log"));
    ASSERT_FALSE(raft::LogStorage::create("data/log"));
    ASSERT_FALSE(raft::LogStorage::create("  ://data/log"));

    // StableStorage
    raft::StableStorage* stable_storage 
            = raft::StableStorage::create("local://data/stable");
    ASSERT_TRUE(stable_storage);
    ASSERT_EQ(0, stable_storage->init());
    ASSERT_FALSE(raft::StableStorage::create("hdfs://data/stable"));
    ASSERT_FALSE(raft::StableStorage::create("://data/stable"));
    ASSERT_FALSE(raft::StableStorage::create("data/stable"));
    ASSERT_FALSE(raft::StableStorage::create("  ://data/stable"));

    // SnapshotStorage
    raft::SnapshotStorage* snapshot_storage 
            = raft::SnapshotStorage::create("local://data/snapshot");
    ASSERT_TRUE(snapshot_storage);
    ASSERT_EQ(0, snapshot_storage->init());
    ASSERT_FALSE(raft::SnapshotStorage::create("hdfs://data/snapshot"));
    ASSERT_FALSE(raft::SnapshotStorage::create("://data/snapshot"));
    ASSERT_FALSE(raft::SnapshotStorage::create("data/snapshot"));
    ASSERT_FALSE(raft::SnapshotStorage::create("  ://data/snapshot"));

}

TEST_F(StorageTest, extra_space_should_be_trimmed) {
    // LogStorage
    raft::LogStorage* log_storage = raft::LogStorage::create("local://data/log");
    ASSERT_TRUE(log_storage);
    raft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    raft::LogEntry* entry = new raft::LogEntry();
    entry->data.append("hello world");
    entry->id = raft::LogId(1, 1);
    entry->type = raft::ENTRY_TYPE_DATA;
    std::vector<raft::LogEntry*> entries;
    entries.push_back(entry);
    ASSERT_EQ(1u, log_storage->append_entries(entries));
    entry->Release();
    delete log_storage;

    // reopen
    log_storage = raft::LogStorage::create(" local://./  data// // log ////");
    ASSERT_EQ(0, log_storage->init(&cm));
    
    ASSERT_EQ(1, log_storage->first_log_index());
    ASSERT_EQ(1, log_storage->last_log_index());
    entry = log_storage->get_entry(1);
    ASSERT_TRUE(entry);
    ASSERT_EQ("hello world", entry->data.to_string());
    ASSERT_EQ(raft::LogId(1, 1), entry->id);
    entry->Release();
    entry = NULL;
}
