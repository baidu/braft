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
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
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

TEST_F(TestUsageSuits, no_inited) {
    ::system("rm -rf data");

    raft::LogStorage* storage = raft::create_local_log_storage("");
    ASSERT_TRUE(storage == NULL);

    storage = raft::create_local_log_storage("file://./data");
    ASSERT_TRUE(storage != NULL);

    // no inited
    {
        ASSERT_TRUE(NULL == storage->get_entry(10));
        ASSERT_EQ(0, storage->get_term(10));

        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->term = 1;
        entry->index = 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", entry->index);
        entry->data.append(data_buf);

        ASSERT_NE(0, storage->append_entry(entry));
        std::vector<raft::LogEntry*> entries;
        entries.push_back(entry);
        ASSERT_NE(1, storage->append_entries(entries));

        ASSERT_NE(0, storage->truncate_prefix(10));
        ASSERT_NE(0, storage->truncate_suffix(10));
    }

    // double init
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    ASSERT_EQ(0, storage->init(configuration_manager));

    delete storage;
    delete configuration_manager;
}

TEST_F(TestUsageSuits, multi_segment_and_segment_logstorage) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");

    // no init append
    {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_NO_OP;
        entry.term = 1;
        entry.index = 1;

        ASSERT_NE(0, storage->append_entry(&entry));
    }

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
            delete entries[j];
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
        raft::Segment* first_seg = segments1.begin()->second;

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->end_index()));
        raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
        ASSERT_EQ(old_segment_num, segments2.size());

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->end_index() + 1));
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
        raft::Segment* first_seg = segments1.begin()->second;
        if (segments1.size() > 1) {
            storage->truncate_suffix(first_seg->end_index() + 1);
        }
        raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
        ASSERT_EQ(2ul, segments2.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->end_index() + 1);
        storage->truncate_suffix(first_seg->end_index());
        raft::SegmentLogStorage::SegmentMap& segments3 = storage->segments();
        ASSERT_EQ(1ul, segments3.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->end_index());
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
        entry.type = raft::ENTRY_TYPE_ADD_PEER;
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
        entry.type = raft::ENTRY_TYPE_REMOVE_PEER;
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
