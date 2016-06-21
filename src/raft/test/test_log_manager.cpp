// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/24 16:30:49

#include <gtest/gtest.h>

#include <base/memory/scoped_ptr.h>
#include <base/string_printf.h>
#include <base/macros.h>

#include <bthread/butex.h>
#include "raft/log_manager.h"
#include "raft/configuration.h"
#include "raft/log.h"

class LogManagerTest : public testing::Test {
protected:
    LogManagerTest() {}
    void SetUp() { }
    void TearDown() { }
};

class StuckClosure : public raft::LogManager::StableClosure {
public:
    StuckClosure()
        : _stuck(NULL)
        , _expected_next_log_index(NULL)
    {}
    ~StuckClosure() {}
    void Run() {
        while (_stuck && *_stuck) {
            bthread_usleep(100);
        }
        ASSERT_TRUE(status().ok()) << status();
        if (_expected_next_log_index) {
            ASSERT_EQ((*_expected_next_log_index)++, _first_log_index);
        }
        delete this;
    }
private:
    bool* _stuck;
    int64_t* _expected_next_log_index;
};

class SyncClosure : public raft::LogManager::StableClosure {
public:
    SyncClosure() {
        _butex = (boost::atomic<int>*)bthread::butex_construct(_butex_memory);
        *_butex = 0;
    }
    ~SyncClosure() {
        bthread::butex_destruct(_butex_memory);
    }
    void Run() {
        _butex->store(1);
        bthread::butex_wake(_butex);
    }
    void reset() {
        status().reset();
        *_butex = 0;
    }
    void join() {
        while (*_butex != 1) {
            bthread::butex_wait(_butex, 0, NULL);
        }
    }
    bool has_run() {
        return *_butex == 1;
    }
private:
    char _butex_memory[BUTEX_MEMORY_SIZE];
    boost::atomic<int> *_butex;
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
    const size_t N = 10000;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    int64_t expected_next_log_index = 1;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(i + 1, 1);
        StuckClosure* c = new StuckClosure;
        c->_stuck = &stuck;
        c->_expected_next_log_index = &expected_next_log_index;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->AddRef();
        saved_entries[i] = entry;
        std::vector<raft::LogEntry*> entries;
        entries.push_back(entry);
        lm->append_entries(&entries, c);
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
    lm->clear_memory_logs(raft::LogId(N, 1));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref_count_);
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
    SyncClosure sc;
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
        entry->id = raft::LogId(i + 1, 1);
        saved_entries[i] = entry;
        entries.push_back(entry);
        sc.reset();
        lm->append_entries(&entries, &sc);
        ASSERT_TRUE(lm->check_and_set_configuration(&conf));
        ASSERT_EQ(i + 1, conf.second._peers.size());
        sc.join();
        ASSERT_TRUE(sc.status().ok()) << sc.status();
    }
    raft::ConfigurationPair new_conf;
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(N, new_conf.second._peers.size());

    lm->clear_memory_logs(raft::LogId(N, 1));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref_count_) << "i=" << i;
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
    SyncClosure sc;
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
        entry->id = raft::LogId(i + 1, 1);
        saved_entries[i] = entry;
        entries.push_back(entry);
        sc.reset();
        lm->append_entries(&entries, &sc);
        ASSERT_TRUE(lm->check_and_set_configuration(&conf));
        ASSERT_EQ(i + 1, conf.second._peers.size());
        sc.join();
        ASSERT_TRUE(sc.status().ok()) << sc.status();
    }
    raft::ConfigurationPair new_conf;
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(N, new_conf.second._peers.size());

    lm->unsafe_truncate_suffix(2);
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(2u, new_conf.second._peers.size());
    

    lm->clear_memory_logs(raft::LogId(N, 1));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->ref_count_) << "i=" << i;
        saved_entries[i]->Release();
    }
}

TEST_F(LogManagerTest, append_with_the_same_index) {
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
    const size_t N = 1000;
    std::vector<raft::LogEntry*> entries0;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 1);
        entries0.push_back(entry);
        entry->AddRef();
    }
    std::vector<raft::LogEntry*> saved_entries0(entries0);
    SyncClosure sc;
    lm->append_entries(&entries0, &sc);
    sc.join();
    ASSERT_TRUE(sc.status().ok()) << sc.status();
    ASSERT_EQ(N, lm->last_log_index());

    // Append the same logs, should be ok
    std::vector<raft::LogEntry*> entries1;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 1);
        entries1.push_back(entry);
        entry->AddRef();
    }

    std::vector<raft::LogEntry*> saved_entries1(entries1);
    sc.reset();
    lm->append_entries(&entries1, &sc);
    sc.join();
    ASSERT_TRUE(sc.status().ok()) << sc.status();
    ASSERT_EQ(N, lm->last_log_index());
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(3u, saved_entries0[i]->ref_count_ + saved_entries1[i]->ref_count_);
    }

    // new term should overwrite the old ones
    std::vector<raft::LogEntry*> entries2;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", (i + 1) * 10);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 2);
        entries2.push_back(entry);
        entry->AddRef();
    }
    std::vector<raft::LogEntry*> saved_entries2(entries2);
    sc.reset();
    lm->append_entries(&entries2, &sc);
    sc.join();
    ASSERT_TRUE(sc.status().ok()) << sc.status();
    ASSERT_EQ(N, lm->last_log_index());

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries0[i]->ref_count_);
        ASSERT_EQ(1u, saved_entries1[i]->ref_count_);
        ASSERT_EQ(2u, saved_entries2[i]->ref_count_);
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = lm->get_entry(i + 1);
        ASSERT_TRUE(entry != NULL);
        std::string buf;
        base::string_printf(&buf, "hello_%lu", (i + 1) * 10);
        ASSERT_EQ(buf, entry->data.to_string());
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }
    lm->set_applied_id(raft::LogId(N, 2));

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries0[i]->ref_count_);
        ASSERT_EQ(1u, saved_entries1[i]->ref_count_);
        ASSERT_EQ(1u, saved_entries2[i]->ref_count_);
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = lm->get_entry(i + 1);
        ASSERT_TRUE(entry != NULL);
        std::string buf;
        base::string_printf(&buf, "hello_%lu", (i + 1) * 10);
        ASSERT_EQ(buf, entry->data.to_string());
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }

    for (size_t i = 0; i < N; ++i) {
        saved_entries0[i]->Release();
        saved_entries1[i]->Release();
        saved_entries2[i]->Release();
    }
}

TEST_F(LogManagerTest, pipelined_append) {
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
    const size_t N = 1000;
    raft::ConfigurationPair conf;
    std::vector<raft::LogEntry*> entries0;
    for (size_t i = 0; i < N - 1; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", 0lu);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 1);
        entries0.push_back(entry);
        entry->AddRef();
    }
    {
        std::vector<raft::PeerId> peers;
        peers.push_back(raft::PeerId("127.0.0.1:1234"));
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(N, 1);
        entry->add_peer(peers);
        entries0.push_back(entry);
    }
    SyncClosure sc0;
    lm->append_entries(&entries0, &sc0);
    ASSERT_TRUE(lm->check_and_set_configuration(&conf));
    ASSERT_EQ(raft::LogId(N, 1), conf.first);
    ASSERT_EQ(1u, conf.second.size());
    ASSERT_EQ(N, lm->last_log_index());

    // entries1 overwrites entries0
    std::vector<raft::LogEntry*> entries1;
    for (size_t i = 0; i < N - 1; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i + 1);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 2);
        entries1.push_back(entry);
        entry->AddRef();
    }
    {
        std::vector<raft::PeerId> peers;
        peers.push_back(raft::PeerId("127.0.0.2:1234"));
        peers.push_back(raft::PeerId("127.0.0.2:2345"));
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(N, 2);
        entry->add_peer(peers);
        entries1.push_back(entry);
    }
    SyncClosure sc1;
    lm->append_entries(&entries1, &sc1);
    ASSERT_TRUE(lm->check_and_set_configuration(&conf));
    ASSERT_EQ(raft::LogId(N, 2), conf.first);
    ASSERT_EQ(2u, conf.second.size());
    ASSERT_EQ(N, lm->last_log_index());

    // entries2 is next to entries1
    ASSERT_EQ(2, lm->get_term(N));
    std::vector<raft::LogEntry*> entries2;
    for (size_t i = N; i < 2 * N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i + 1);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 2);
        entries2.push_back(entry);
        entry->AddRef();
    }

    SyncClosure sc2;
    lm->append_entries(&entries2, &sc2);
    ASSERT_FALSE(lm->check_and_set_configuration(&conf));
    ASSERT_EQ(raft::LogId(N, 2), conf.first);
    ASSERT_EQ(2u, conf.second.size());
    ASSERT_EQ(2 * N, lm->last_log_index());
    LOG(INFO) << conf.second;

    // It's safe to get entry when disk thread is still running
    for (size_t i = 0; i < 2 * N; ++i) {
        raft::LogEntry* entry = lm->get_entry(i + 1);
        ASSERT_TRUE(entry != NULL);
        if (entry->type == raft::ENTRY_TYPE_DATA) {
            std::string buf;
            base::string_printf(&buf, "hello_%lu", i + 1);
            ASSERT_EQ(buf, entry->data.to_string());
        }
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }

    sc0.join();
    ASSERT_TRUE(sc0.status().ok()) << sc0.status();
    sc1.join();
    ASSERT_TRUE(sc1.status().ok()) << sc1.status();
    sc2.join();
    ASSERT_TRUE(sc2.status().ok()) << sc2.status();

    // Wrong applied id doen't change _logs_in_memory
    lm->set_applied_id(raft::LogId(N * 2, 1));
    ASSERT_EQ(N * 2, lm->_logs_in_memory.size());

    lm->set_applied_id(raft::LogId(N * 2, 2));
    ASSERT_TRUE(lm->_logs_in_memory.empty());

    // We can still get the right data from storage
    for (size_t i = 0; i < 2 * N; ++i) {
        raft::LogEntry* entry = lm->get_entry(i + 1);
        ASSERT_TRUE(entry != NULL);
        if (entry->type == raft::ENTRY_TYPE_DATA) {
            std::string buf;
            base::string_printf(&buf, "hello_%lu", i + 1);
            ASSERT_EQ(buf, entry->data.to_string());
        }
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }
}

TEST_F(LogManagerTest, set_snapshot) {
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
    raft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    lm->set_snapshot(&meta);
    ASSERT_EQ(raft::LogId(1000, 2), lm->last_log_id(false));
}

int on_new_log(void* arg, int /*error_code*/) {
    SyncClosure* sc = (SyncClosure*)arg;
    sc->Run();
    return 0;
}

int append_entry(raft::LogManager* lm, base::StringPiece data, int64_t index) {
    raft::LogEntry* entry = new raft::LogEntry;
    entry->type = raft::ENTRY_TYPE_DATA;
    entry->data.append(data.data(), data.size());
    entry->id = raft::LogId(index, 1);
    SyncClosure sc;
    std::vector<raft::LogEntry*> entries;
    entries.push_back(entry);
    lm->append_entries(&entries, &sc);
    sc.join();
    return sc.status().error_code();
}

TEST_F(LogManagerTest, wait) {
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
    SyncClosure sc;   
    raft::LogManager::WaitId wait_id = 
            lm->wait(lm->last_log_index(), on_new_log, &sc);
    ASSERT_NE(0, wait_id);
    ASSERT_EQ(0, lm->remove_waiter(wait_id));
    ASSERT_FALSE(sc.has_run());
    ASSERT_EQ(0, append_entry(lm.get(), "hello", 1));
    wait_id = lm->wait(0, on_new_log, &sc);
    ASSERT_EQ(0, wait_id);
    sc.join();
    sc.reset();
    wait_id = lm->wait(lm->last_log_index(), on_new_log, &sc);
    ASSERT_NE(0, wait_id);
    ASSERT_EQ(0, append_entry(lm.get(), "hello", 2));
    sc.join();
    ASSERT_NE(0, lm->remove_waiter(wait_id));
}
