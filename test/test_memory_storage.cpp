// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: qinduohao@baidu.com
// Date: 2017/05/23

#include <gtest/gtest.h>
#include "braft/memory_log.h"

namespace braft {
extern void global_init_once_or_die();
};

class MemStorageTest : public testing::Test {
protected:
    void SetUp() {
        system("rm -rf data");
        braft::global_init_once_or_die();
    }
    void TearDown() {}
};

TEST_F(MemStorageTest, init) {
    braft::LogStorage* log_storage = braft::LogStorage::create("memory://data/log");
    ASSERT_TRUE(log_storage);
    braft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    ASSERT_FALSE(braft::LogStorage::create("hdfs://data/log"));
    ASSERT_FALSE(braft::LogStorage::create("://data/log"));
    ASSERT_FALSE(braft::LogStorage::create("data/log"));
    ASSERT_FALSE(braft::LogStorage::create("  ://data/log"));
}

TEST_F(MemStorageTest, entry_operation) {
    braft::LogStorage* log_storage = braft::LogStorage::create("memory://data/log");
    ASSERT_TRUE(log_storage);
    braft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    braft::LogEntry* entry = new braft::LogEntry();
    entry->data.append("hello world");
    entry->id = braft::LogId(1, 1);
    entry->type = braft::ENTRY_TYPE_DATA;
    std::vector<braft::LogEntry*> entries;
    entries.push_back(entry);
    ASSERT_EQ(1u, log_storage->append_entries(entries, NULL));

    ASSERT_EQ(1, log_storage->first_log_index());
    ASSERT_EQ(1, log_storage->last_log_index());
    entry = log_storage->get_entry(1);
    ASSERT_TRUE(entry);
    ASSERT_EQ("hello world", entry->data.to_string());
    ASSERT_EQ(braft::LogId(1, 1), entry->id);
    int64_t term = log_storage->get_term(1);
    ASSERT_EQ(1, term);
    entry->Release();
    entry = NULL;

    ASSERT_EQ(0, log_storage->reset(10));
    ASSERT_EQ(10, log_storage->first_log_index());
    ASSERT_EQ(9, log_storage->last_log_index());
    delete log_storage;
}

TEST_F(MemStorageTest, trunk_operation) {
    braft::LogStorage* log_storage = braft::LogStorage::create("memory://data/log");
    ASSERT_TRUE(log_storage);
    braft::ConfigurationManager cm;
    ASSERT_EQ(0, log_storage->init(&cm));
    std::vector<braft::LogEntry*> entries;
    braft::LogEntry* entry = new braft::LogEntry();
    entry->data.append("hello world");
    entry->id = braft::LogId(1, 1);
    entry->type = braft::ENTRY_TYPE_DATA;
    entries.push_back(entry);
    braft::LogEntry* entry1 = new braft::LogEntry();
    entry1->data.append("hello world");
    entry1->id = braft::LogId(2, 1);
    entry1->type = braft::ENTRY_TYPE_DATA;
    entries.push_back(entry1);
    braft::LogEntry* entry2 = new braft::LogEntry();
    entry2->data.append("hello world");
    entry2->id = braft::LogId(3, 1);
    entry2->type = braft::ENTRY_TYPE_DATA;
    entries.push_back(entry2);
    ASSERT_EQ(3u, log_storage->append_entries(entries, NULL));

    size_t ret = log_storage->truncate_suffix(2);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, log_storage->first_log_index());
    ASSERT_EQ(2, log_storage->last_log_index());

    ret = log_storage->truncate_prefix(2);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, log_storage->first_log_index());
    ASSERT_EQ(2, log_storage->last_log_index());
    entry1->Release();
    delete log_storage;
}
