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
#include <base/bind.h>
#include <base/callback.h>
#include <base/memory/ref_counted.h>
#include <base/logging.h>
#include "raft/log.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

static void config_add(const int64_t index, const raft::Configuration& config) {
    LOG(NOTICE) << "load index: " << index << " index: " << config;
}

TEST_F(TestUsageSuits, open_segment) {
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
        entry->data = strndup(data_buf, strlen(data_buf));
        entry->len = strlen(data_buf);

        ASSERT_EQ(0, seg1->append(entry));

        delete entry;
    }

    // read entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg1->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }
    {
        raft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    base::Callback<void(const int64_t, const raft::Configuration&)> configuration_cb =
        base::Bind(&config_add);
    // load open segment
    raft::Segment* seg2 = new raft::Segment("./data", 1);
    ASSERT_EQ(0, seg2->load(configuration_cb));

    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg2->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
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
        entry->data = strndup(data_buf, strlen(data_buf));
        entry->len = strlen(data_buf);

        ASSERT_EQ(0, seg1->append(entry));

        delete entry;
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
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }

    ASSERT_EQ(0, seg1->close());
    ASSERT_FALSE(seg1->is_open());
    ASSERT_EQ(0, seg1->unlink());
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
        entry->data = strndup(data_buf, strlen(data_buf));
        entry->len = strlen(data_buf);

        ASSERT_EQ(0, seg1->append(entry));

        delete entry;
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
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }
    {
        raft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    base::Callback<void(const int64_t, const raft::Configuration&)> configuration_cb =
        base::Bind(&config_add);
    // load open segment
    raft::Segment* seg2 = new raft::Segment("./data", 1, 10);
    ASSERT_EQ(0, seg2->load(configuration_cb));

    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg2->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
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
        entry->data = strndup(data_buf, strlen(data_buf));
        entry->len = strlen(data_buf);

        ASSERT_NE(0, seg1->append(entry));

        delete entry;
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
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }

    ASSERT_EQ(0, seg1->unlink());
}

TEST_F(TestUsageSuits, multi_segment_and_segment_logstorage) {
    ::system("rm -rf data");
    scoped_refptr<raft::SegmentLogStorage> storage(new raft::SegmentLogStorage("./data"));

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
            entry->data = strndup(data_buf, strlen(data_buf));
            entry->len = strlen(data_buf);
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
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }

    // truncate prefix
    ASSERT_EQ(0, storage->truncate_prefix(10001));
    ASSERT_EQ(storage->first_log_index(), 10001);

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
    for (int i = 250001; i <= 500000; i++) {
        int64_t index = i;
        raft::LogEntry* entry = storage->get_entry(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
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
            entry->data = strndup(data_buf, strlen(data_buf));
            entry->len = strlen(data_buf);
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
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }

    //delete storage;

    // re load
    ::system("rm -rf data/log_meta");
    scoped_refptr<raft::SegmentLogStorage> storage2(new raft::SegmentLogStorage("./data"));
    ASSERT_EQ(0, storage2->init(new raft::ConfigurationManager()));
    ASSERT_EQ(1, storage2->first_log_index());
    ASSERT_EQ(0, storage2->last_log_index());
    //delete storage;
}

TEST_F(TestUsageSuits, configuration) {
    ::system("rm -rf data");
    scoped_refptr<raft::SegmentLogStorage> storage(new raft::SegmentLogStorage("./data"));
    ASSERT_EQ(0, storage->init(new raft::ConfigurationManager()));
    scoped_refptr<raft::ConfigurationManager> config_manager = storage->configuration_manager();

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

        std::vector<raft::PeerId> peers;
        peers.push_back(raft::PeerId("1.1.1.1:1000:0"));
        peers.push_back(raft::PeerId("1.1.1.1:2000:0"));
        peers.push_back(raft::PeerId("1.1.1.1:3000:0"));
        config_manager->add(2, raft::Configuration(peers));
    }

    std::pair<int64_t, raft::Configuration> pair = config_manager->get_configuration(1);
    ASSERT_EQ(0, pair.first);

    pair = config_manager->get_configuration(2);
    ASSERT_EQ(2, pair.first);
    LOG(NOTICE) << pair.second;

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
            entry->data = strndup(data_buf, strlen(data_buf));
            entry->len = strlen(data_buf);
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

        std::vector<raft::PeerId> peers;
        peers.push_back(raft::PeerId("1.1.1.1:1000:0"));
        peers.push_back(raft::PeerId("1.1.1.1:2000:0"));
        config_manager->add(index, raft::Configuration(peers));
    }

    scoped_refptr<raft::SegmentLogStorage> storage2(new raft::SegmentLogStorage("./data"));
    ASSERT_EQ(0, storage2->init(new raft::ConfigurationManager()));
    scoped_refptr<raft::ConfigurationManager> config_manager2 = storage2->configuration_manager();

    pair = config_manager2->get_configuration(2 + 100000*5);
    ASSERT_EQ(2, pair.first);
    LOG(NOTICE) << pair.second;

    pair = config_manager2->get_configuration(2 + 100000*5 + 1);
    ASSERT_EQ(2+100000*5+1, pair.first);
    LOG(NOTICE) << pair.second;

    storage->truncate_suffix(400000);
    pair = config_manager->get_configuration(400000);
    ASSERT_EQ(2, pair.first);

    storage->truncate_prefix(2);
    pair = config_manager->get_configuration(400000);
    ASSERT_EQ(2, pair.first);
}

