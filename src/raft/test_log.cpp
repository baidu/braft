/*
 * =====================================================================================
 *
 *       Filename:  test_log.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年09月23日 11时14分18秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "raft/log.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, open_segment) {
    // open segment operation
    raft::Segment* seg1 = new raft::Segment("./data", 1L);
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());
    // append entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::DATA;
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
        ASSERT_EQ(entry->type, raft::DATA);
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

    // load open segment
    raft::Segment* seg2 = new raft::Segment("./data", 1);
    ASSERT_EQ(0, seg2->load());

    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg2->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::DATA);
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
        entry->type = raft::DATA;
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
        ASSERT_EQ(entry->type, raft::DATA);
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
        entry->type = raft::DATA;
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
        ASSERT_EQ(entry->type, raft::DATA);
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

    // load open segment
    raft::Segment* seg2 = new raft::Segment("./data", 1, 10);
    ASSERT_EQ(0, seg2->load());

    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = seg2->get(i+1);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::DATA);
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
        entry->type = raft::DATA;
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
        ASSERT_EQ(entry->type, raft::DATA);
        ASSERT_EQ(entry->index, i+1);
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }

    ASSERT_EQ(0, seg1->unlink());
}

TEST_F(TestUsageSuits, multi_segment_and_segment_logstorage) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");

    // no init append
    {
        raft::LogEntry entry;
        entry.type = raft::NO_OP;
        entry.term = 1;
        entry.index = 1;

        ASSERT_NE(0, storage->append_log(&entry));
    }

    // init
    ASSERT_EQ(0, storage->init());
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data = strndup(data_buf, strlen(data_buf));
            entry->len = strlen(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_logs(entries));

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // read entry
    for (int i = 0; i < 500000; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = storage->get_log(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::DATA);
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
        raft::LogEntry* entry = storage->get_log(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::DATA);
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
            entry->type = raft::DATA;
            entry->term = 1;
            entry->index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data = strndup(data_buf, strlen(data_buf));
            entry->len = strlen(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_logs(entries));

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
        raft::LogEntry* entry = storage->get_log(index);
        ASSERT_EQ(entry->term, 1);
        ASSERT_EQ(entry->type, raft::DATA);
        ASSERT_EQ(entry->index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(0, memcmp(entry->data, data_buf, entry->len));
        delete entry;
    }

    delete storage;

    // re load
    ::system("rm -rf data/log_meta");
    storage = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage->init());
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());
    delete storage;
}

