/*
 * =====================================================================================
 *
 *       Filename:  test_log.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/23 11:14:18
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include <boost/atomic.hpp>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include <base/string_printf.h>
#include <base/logging.h>
#include "raft/log.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, open_segment) {
    // open segment operation
    raft::Segment* seg1 = new raft::Segment("./data", 1L);

    // not open
    raft::LogEntry* entry = seg1->get(1);
    ASSERT_TRUE(entry == NULL);

    // create and open
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());

    // append entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->term = 1;
        entry->index = i + 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        entry->data.append(data_buf);

        ASSERT_EQ(0, seg1->append(entry));

        entry->Release();
    }

    // read entry
    for (int i = 0; i < 10; i++) {
        int64_t term = seg1->get_term(i+1);
        ASSERT_EQ(term, 1);

        raft::LogEntry* entry = seg1->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        raft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    // load open segment
    raft::Segment* seg2 = new raft::Segment("./data", 1);
    ASSERT_EQ(0, seg2->load(configuration_manager));

    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg2->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        raft::LogEntry* entry = seg2->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg2->get(11);
        ASSERT_TRUE(entry == NULL);
    }
    delete seg2;

    // truncate and read
    ASSERT_EQ(0, seg1->truncate(5));
    for (int i = 0; i < 5; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->term = 1;
        entry->index = i + 6;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 6);
        entry->data.append(data_buf); 

        ASSERT_EQ(0, seg1->append(entry));

        entry->Release();
    }
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg1->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        if (i < 5) {
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        } else {
            snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 1);
        }
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(0, seg1->close());
    ASSERT_FALSE(seg1->is_open());
    ASSERT_EQ(0, seg1->unlink());

    delete configuration_manager;
}

TEST_F(TestUsageSuits, closed_segment) {
    // open segment operation
    raft::Segment* seg1 = new raft::Segment("./data", 1L);
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());
    // append entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->term = 1;
        entry->index = i + 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        entry->data.append(data_buf);

        ASSERT_EQ(0, seg1->append(entry));

        entry->Release();
    }
    seg1->close();

    // read entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg1->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        raft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    // load open segment
    raft::Segment* seg2 = new raft::Segment("./data", 1, 10);
    ASSERT_EQ(0, seg2->load(configuration_manager));

    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg2->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        raft::LogEntry* entry = seg2->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg2->get(11);
        ASSERT_TRUE(entry == NULL);
    }
    delete seg2;

    // truncate and read
    ASSERT_EQ(0, seg1->truncate(5));
    for (int i = 0; i < 5; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->term = 1;
        entry->index = i + 6;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 6);
        entry->data.append(data_buf);

        ASSERT_NE(0, seg1->append(entry));

        entry->Release();
    }
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg1->get(i+1);
        char data_buf[128];
        if (i < 5) {
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        } else {
            snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 1);
            ASSERT_TRUE(entry == NULL);
            continue;
        }
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(0, seg1->unlink());

    delete configuration_manager;
}

TEST_F(TestUsageSuits, multi_segment_and_segment_logstorage) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");

    // init
    ASSERT_EQ(0, storage->init(new raft::ConfigurationManager()));
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_entries(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            entries[j]->Release();
        }
    }

    // read entry
    for (int i = 0; i < 500000; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = storage->get_entry(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 500000);
    // truncate prefix
    ASSERT_EQ(0, storage->truncate_prefix(10001));
    ASSERT_EQ(storage->first_log_index(), 10001);
    ASSERT_EQ(storage->last_log_index(), 500000);

    // boundary truncate prefix
    {
        raft::SegmentLogStorage::SegmentMap& segments1 = storage->segments();
        size_t old_segment_num = segments1.size();
        raft::Segment* first_seg = segments1.begin()->second.get();

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->last_index()));
        raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
        ASSERT_EQ(old_segment_num, segments2.size());

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->last_index() + 1));
        raft::SegmentLogStorage::SegmentMap& segments3 = storage->segments();
        ASSERT_EQ(old_segment_num - 1, segments3.size());
    }

    ASSERT_EQ(0, storage->truncate_prefix(250001));
    ASSERT_EQ(storage->first_log_index(), 250001);
    ASSERT_EQ(storage->last_log_index(), 500000);
    for (int i = 250001; i <= 500000; i++) {
        int64_t index = i;
        raft::LogEntry* entry = storage->get_entry(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    // append
    for (int i = 100000; i < 200000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_entries(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // truncate suffix
    ASSERT_EQ(250001, storage->first_log_index());
    ASSERT_EQ(1000000, storage->last_log_index());
    ASSERT_EQ(0, storage->truncate_suffix(750000));
    ASSERT_EQ(250001, storage->first_log_index());
    ASSERT_EQ(750000, storage->last_log_index());

    // boundary truncate suffix
    {
        raft::SegmentLogStorage::SegmentMap& segments1 = storage->segments();
        raft::Segment* first_seg = segments1.begin()->second.get();
        if (segments1.size() > 1) {
            storage->truncate_suffix(first_seg->last_index() + 1);
        }
        raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
        ASSERT_EQ(2ul, segments2.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index() + 1);
        storage->truncate_suffix(first_seg->last_index());
        raft::SegmentLogStorage::SegmentMap& segments3 = storage->segments();
        ASSERT_EQ(1ul, segments3.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index());
    }

    // read
    for (int i = 250001; i <= storage->last_log_index(); i++) {
        int64_t index = i;
        raft::LogEntry* entry = storage->get_entry(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;

    // re load
    ::system("rm -rf data/log_meta");
    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage2->init(new raft::ConfigurationManager()));
    ASSERT_EQ(1, storage2->first_log_index());
    ASSERT_EQ(0, storage2->last_log_index());
    delete storage2;
}

TEST_F(TestUsageSuits, append_close_load_append) {
    ::system("rm -rf data");
    raft::LogStorage* storage = raft::create_local_log_storage("file://./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_entries(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    delete storage;
    delete configuration_manager;

    // reinit 
    storage = raft::create_local_log_storage("file://./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 100000; i < 200000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->term = 2;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_entries(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 200000*5);

    for (int i = 0; i < 200000*5; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = storage->get_entry(index);
        if (i < 100000*5) {
            ASSERT_EQ(entry->term, 1);
        } else {
            ASSERT_EQ(entry->term, 2);
        }
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;
    delete configuration_manager;
}

TEST_F(TestUsageSuits, append_read_badcase) {
    ::system("rm -rf data");
    raft::LogStorage* storage = raft::create_local_log_storage("file://./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_entries(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 100000*5);

    delete storage;
    delete configuration_manager;

    // make file unwrite
    base::FileEnumerator dir1(base::FilePath("./data"), false, 
                              base::FileEnumerator::FILES 
                              | base::FileEnumerator::DIRECTORIES);
    for (base::FilePath sub_path = dir1.Next(); !sub_path.empty(); sub_path = dir1.Next()) {
        base::File::Info info;
        base::GetFileInfo(sub_path, &info);
        if (!info.is_directory) {
            chmod(sub_path.value().c_str(), 0444);
        }
    }

    // reinit failed, because load open no permission
    storage = raft::create_local_log_storage("file://./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_NE(0, storage->init(configuration_manager));
    delete storage;
    delete configuration_manager;

    base::FileEnumerator dir2(base::FilePath("./data"), false, 
                              base::FileEnumerator::FILES 
                              | base::FileEnumerator::DIRECTORIES);
    for (base::FilePath sub_path = dir2.Next(); !sub_path.empty(); sub_path = dir2.Next()) {
        base::File::Info info;
        base::GetFileInfo(sub_path, &info);
        if (!info.is_directory) {
            chmod(sub_path.value().c_str(), 0644);
        }
    }

    // reinit success
    storage = raft::create_local_log_storage("file://./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // make file chaos
    base::FileEnumerator dir3(base::FilePath("./data"), false, 
                              base::FileEnumerator::FILES 
                              | base::FileEnumerator::DIRECTORIES);
    for (base::FilePath sub_path = dir3.Next(); !sub_path.empty(); sub_path = dir3.Next()) {
        base::File::Info info;
        base::GetFileInfo(sub_path, &info);
        if (!info.is_directory) {
            chmod(sub_path.value().c_str(), 0644);

            int fd = ::open(sub_path.value().c_str(), O_RDWR, 0644);
            int64_t off = rand() % info.size;
            int64_t len = rand() % (info.size - off);
            if (len > 4096) {
                len = 4096;
            }
            char data[4096] = {0};
            pwrite(fd, data, len, off);
            close(fd);
        }
    }

    // read will fail
    for (int i = 0; i < 100000*5; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = storage->get_entry(index);
        if (entry) {
            entry->Release();
        }
    }

    delete storage;
    delete configuration_manager;
}

TEST_F(TestUsageSuits, configuration) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_NO_OP;
        entry.term = 1;
        entry.index = 1;

        ASSERT_EQ(0, storage->append_entry(&entry));
    }

    // add peer
    {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_CONFIGURATION;
        entry.term = 1;
        entry.index = 2;
        entry.peers = new std::vector<raft::PeerId>;
        entry.peers->push_back(raft::PeerId("1.1.1.1:1000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:2000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:3000:0"));
        storage->append_entry(&entry);
    }

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 3 + i*5+j;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }
        ASSERT_EQ(5, storage->append_entries(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // remove peer
    {
        int64_t index = 2 + 100000*5 + 1;
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_CONFIGURATION;
        entry.term = 1;
        entry.index = index;
        entry.peers = new std::vector<raft::PeerId>;
        entry.peers->push_back(raft::PeerId("1.1.1.1:1000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:2000:0"));
        storage->append_entry(&entry);
    }

    delete storage;

    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage2->init(configuration_manager));

    std::pair<int64_t, raft::Configuration> pair =
        configuration_manager->get_configuration(2 + 100000*5);
    ASSERT_EQ(2, pair.first);
    LOG(NOTICE) << pair.second;

    pair = configuration_manager->get_configuration(2 + 100000*5 + 1);
    ASSERT_EQ(2+100000*5+1, pair.first);
    LOG(NOTICE) << pair.second;

    storage2->truncate_suffix(400000);
    pair = configuration_manager->get_configuration(400000);
    ASSERT_EQ(2, pair.first);

    storage2->truncate_prefix(2);
    pair = configuration_manager->get_configuration(400000);
    ASSERT_EQ(2, pair.first);

    delete storage2;
}

boost::atomic<int> g_first_read_index(0); 
boost::atomic<int> g_last_read_index(0);
bool g_stop = false;

void* read_thread_routine(void* arg) {
    raft::SegmentLogStorage* storage = (raft::SegmentLogStorage*)arg;
    while (!g_stop) {
        int a = g_first_read_index.load(boost::memory_order_relaxed);
        int b = g_last_read_index.load(boost::memory_order_relaxed);
        EXPECT_LE(a, b);
        int index = base::fast_rand_in(a, b);
        raft::LogEntry* entry = storage->get_entry(index);
        if (entry != NULL) {
            std::string expect;
            base::string_printf(&expect, "hello_%d", index);
            EXPECT_EQ(expect, entry->data.to_string());
            entry->Release();
        } else {
            EXPECT_LT(index, storage->first_log_index()) 
                    << "first_read_index=" << g_first_read_index.load()
                    << " last_read_index=" << g_last_read_index.load()
                    << " a=" << a << " b=" << b;
            g_stop = true;
            return NULL;
        }
    }
    return NULL;
}

void* write_thread_routine(void* arg) {
    raft::SegmentLogStorage* storage = (raft::SegmentLogStorage*)arg;
    // Write operation distrubution: 
    //  - 10% truncate_prefix
    //  - 10% truncate_suffix,
    //  - 30% increase last_read_index (which stands for committment in the real
    // world), 
    //  - 50% append new entry
    int next_log_index = storage->last_log_index() + 1;
    while (!g_stop) {
        const int r = base::fast_rand_in(0, 9);
        if (r < 1) {  // truncate_prefix
            int truncate_index = base::fast_rand_in(
                    g_first_read_index.load(boost::memory_order_relaxed), 
                    g_last_read_index.load(boost::memory_order_relaxed));
            EXPECT_EQ(0, storage->truncate_prefix(truncate_index));
            g_first_read_index.store(truncate_index, boost::memory_order_relaxed);
        } else if (r < 2) {  // truncate suffix
            int truncate_index = base::fast_rand_in(
                    g_last_read_index.load(boost::memory_order_relaxed),
                    next_log_index - 1);
            EXPECT_EQ(0, storage->truncate_suffix(truncate_index));
            next_log_index = truncate_index + 1;
        } else if (r < 5) { // increase last_read_index which cannot be truncate
            int next_read_index = base::fast_rand_in(
                    g_last_read_index.load(boost::memory_order_relaxed),
                    next_log_index - 1);
            g_last_read_index.store(next_read_index, boost::memory_order_relaxed);
        } else  {  // Append entry
            raft::LogEntry* entry = new raft::LogEntry;
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->index = next_log_index;
            std::string data;
            base::string_printf(&data, "hello_%d", next_log_index);
            entry->data.append(data);
            ++next_log_index;
            EXPECT_EQ(0, storage->append_entry(entry));
            EXPECT_EQ(0, entry->Release());
        }
    }
    return NULL;
}

namespace raft {
DECLARE_int32(raft_max_segment_size);
}

TEST_F(TestUsageSuits, multi_read_single_modify_thread_safe) {
    int32_t saved_max_segment_size = raft::FLAGS_raft_max_segment_size;
    raft::FLAGS_raft_max_segment_size = 1024;
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    const int N = 10000;
    for (int i = 1; i <= N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->index = i;
        std::string data;
        base::string_printf(&data, "hello_%d", i);
        entry->data.append(data);
        ASSERT_EQ(0, storage->append_entry(entry));
        ASSERT_EQ(0, entry->Release());
    }
    ASSERT_EQ(N, storage->last_log_index());
    g_stop = false;
    g_first_read_index.store(1);
    g_last_read_index.store(N);
    bthread_t read_thread[8];
    for (size_t i = 0; i < ARRAY_SIZE(read_thread); ++i) {
        ASSERT_EQ(0, bthread_start_urgent(&read_thread[i], NULL, 
                                   read_thread_routine, storage));
    }
    bthread_t write_thread;
    ASSERT_EQ(0, bthread_start_urgent(&write_thread, NULL,
                                      write_thread_routine, storage));
    ::usleep(5 * 1000 * 1000);
    g_stop = true;
    for (size_t i = 0; i < ARRAY_SIZE(read_thread); ++i) {
        bthread_join(read_thread[i], NULL);
    }
    bthread_join(write_thread, NULL);

    delete configuration_manager;
    delete storage;
    raft::FLAGS_raft_max_segment_size = saved_max_segment_size;
}

