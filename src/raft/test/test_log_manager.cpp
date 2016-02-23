// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/24 16:30:49

#include <gtest/gtest.h>

#include <base/memory/scoped_ptr.h>
#include <base/string_printf.h>
#include <base/macros.h>

#include "raft/log_manager.h"
#include "raft/configuration.h"
#include "raft/log.h"

class LogManagerTest : public testing::Test {
protected:
    LogManagerTest() {}
    void SetUp() { }
    void TearDown() { }
};

TEST_F(LogManagerTest, without_disk_thread) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 10000;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    for (size_t i = 0; i < N; ++i) {
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->index = i + 1;
        entries.push_back(entry);
        ASSERT_EQ(0, lm->append_entries(entries));
        entry->AddRef();
        saved_entries[i] = entry;
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry *entry = lm->get_entry(i + 1);
        ASSERT_TRUE(entry != NULL) << "i=" << i;
        std::string exptected;
        base::string_printf(&exptected, "hello_%lu", i);
        ASSERT_EQ(exptected, entry->data.to_string());
        entry->Release();
    }

    lm->clear_memory_logs(N);
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref);
        saved_entries[i]->Release();
    }
}

class StuckClosure : public raft::LogManager::StableClosure {
public:
    StuckClosure()
        : _stuck(NULL)
        , _expected_next_log_index(NULL)
    {}
    void Run() {
        while (_stuck && *_stuck) {
            bthread_usleep(100);
        }
        ASSERT_EQ(0, _err_code) << _err_text;
        if (_expected_next_log_index) {
            ASSERT_EQ((*_expected_next_log_index)++, _first_log_index);
        }
    }
private:
    bool* _stuck;
    int64_t* _expected_next_log_index;
};

TEST_F(LogManagerTest, get_should_be_ok_when_disk_thread_stucks) {
    bool stuck = true;
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(opt));
    ASSERT_EQ(0, lm->start_disk_thread());
    const size_t N = 10000;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    int64_t expected_next_log_index = 1;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        StuckClosure* c = new StuckClosure;
        c->_stuck = &stuck;
        c->_expected_next_log_index = &expected_next_log_index;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->AddRef();
        saved_entries[i] = entry;
        lm->append_entry(entry, c);
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry *entry = lm->get_entry(i + 1);
        ASSERT_TRUE(entry != NULL) << "i=" << i;
        std::string exptected;
        base::string_printf(&exptected, "hello_%lu", i);
        ASSERT_EQ(exptected, entry->data.to_string());
        entry->Release();
    }

    stuck = false;
    LOG(INFO) << "Stop and join disk thraad";
    ASSERT_EQ(0, lm->stop_disk_thread());
    lm->clear_memory_logs(N);
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref);
        saved_entries[i]->Release();
    }
}

TEST_F(LogManagerTest, configuration_changes) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 5;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    raft::ConfigurationPair conf;
    conf.first = 0;
    for (size_t i = 0; i < N; ++i) {
        std::vector<raft::PeerId> peers;
        for (size_t j = 0; j <= i; ++j) {
            peers.push_back(raft::PeerId(base::EndPoint(), j));
        }
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->add_peer(peers);
        entry->AddRef();
        entry->index = i + 1;
        saved_entries[i] = entry;
        entries.push_back(entry);
        ASSERT_EQ(0, lm->append_entries(entries));
        ASSERT_TRUE(lm->check_and_set_configuration(&conf));
        ASSERT_EQ(i + 1, conf.second._peers.size());
    }
    raft::ConfigurationPair new_conf;
    new_conf.first = 0;
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(N, new_conf.second._peers.size());

    lm->clear_memory_logs(N);
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref) << "i=" << i;
        saved_entries[i]->Release();
    }
}

TEST_F(LogManagerTest, truncate_suffix_also_revert_configuration) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 5;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    raft::ConfigurationPair conf;
    conf.first = 0;
    for (size_t i = 0; i < N; ++i) {
        std::vector<raft::PeerId> peers;
        for (size_t j = 0; j <= i; ++j) {
            peers.push_back(raft::PeerId(base::EndPoint(), j));
        }
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->add_peer(peers);
        entry->AddRef();
        entry->index = i + 1;
        saved_entries[i] = entry;
        entries.push_back(entry);
        ASSERT_EQ(0, lm->append_entries(entries));
        ASSERT_TRUE(lm->check_and_set_configuration(&conf));
        ASSERT_EQ(i + 1, conf.second._peers.size());
    }
    raft::ConfigurationPair new_conf;
    new_conf.first = 0;
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(N, new_conf.second._peers.size());

    lm->truncate_suffix(2);
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(2u, new_conf.second._peers.size());
    

    lm->clear_memory_logs(N);
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref) << "i=" << i;
        saved_entries[i]->Release();
    }
}

