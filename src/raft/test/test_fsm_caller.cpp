// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/01 17:03:46

#include <gtest/gtest.h>
#include <base/string_printf.h>
#include <base/memory/scoped_ptr.h>
#include "raft/fsm_caller.h"
#include "raft/raft.h"
#include "raft/log.h"
#include "raft/configuration.h"
#include "raft/log_manager.h"

class FSMCallerTest : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

class OrderedStateMachine : public raft::StateMachine {
public:
    OrderedStateMachine() 
        : _expected_next(0)
        , _stopped(false)
        , _on_leader_start_times(0)
        , _on_leader_stop_times(0)
        , _on_snapshot_save_times(0)
        , _on_snapshot_load_times(0)
    {}
    void on_apply(const int64_t index, const raft::Task& task) {
        std::string expected;
        base::string_printf(&expected, "hello_%lu", _expected_next++);
        ASSERT_EQ(expected, task.data->to_string());
        if (task.done) {
            ASSERT_TRUE(task.done->status().ok()) << "index=" << index;
            task.done->Run();
        }
    }
    void on_shutdown() {
        _stopped = true;
    }
    void on_snapshot_save(raft::SnapshotWriter* /*writer*/, raft::Closure* done) {
        done->Run();
        ++_on_snapshot_save_times;
    }
    int on_snapshot_load(raft::SnapshotReader* /*reader*/) {
        ++_on_snapshot_load_times;
        return 0;
    }
    void on_leader_start() {
        _on_leader_start_times++;
    }
    void on_leader_stop() {
        _on_leader_stop_times++;
    }
    void join() {
        while (!_stopped) {
            bthread_usleep(100);
        }
    }
private:
    uint64_t _expected_next;
    bool _stopped;
    int _on_leader_start_times;
    int _on_leader_stop_times;
    int _on_snapshot_save_times;
    int _on_snapshot_load_times;
};

TEST_F(FSMCallerTest, sanity) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    raft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(log_opt));

    raft::ClosureQueue cq;

    OrderedStateMachine fsm;
    fsm._expected_next = 0;

    raft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;

    raft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));

    const size_t N = 1000;

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
    }
    ASSERT_EQ(0, caller.on_committed(N));
    ASSERT_EQ(0, caller.shutdown());
    fsm.join();
    ASSERT_EQ(fsm._expected_next, N);
}

TEST_F(FSMCallerTest, on_leader_start_and_stop) {
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    raft::ClosureQueue cq;
    raft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;
    raft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));
    caller.on_leader_stop();
    caller.shutdown();
    fsm.join();
    ASSERT_EQ(0, fsm._on_leader_start_times);
    ASSERT_EQ(1, fsm._on_leader_stop_times);
}

class DummySnapshotReader : public raft::SnapshotReader {
public:
    DummySnapshotReader(raft::SnapshotMeta* meta) 
        : _meta(meta)
    {
    };
    ~DummySnapshotReader() {}
    std::string get_uri(const base::EndPoint& /*hint_addr*/) { return std::string(); }
    int load_meta(raft::SnapshotMeta* meta) {
        *meta = *_meta;
        return 0;
    }
private:
    raft::SnapshotMeta* _meta;
};

class DummySnapshoWriter : public raft::SnapshotWriter {
public:
    DummySnapshoWriter() {}
    ~DummySnapshoWriter() {}
    int save_meta(const raft::SnapshotMeta&) {
        EXPECT_TRUE(false) << "Should never be called";
        return 0;
    }
    int copy(const std::string&) {
        EXPECT_TRUE(false) << "Should never be called";
        return 0;
    }
    std::string get_uri(const base::EndPoint& /*hint_addr*/) { return std::string(); }
private:
};

class MockSaveSnapshotClosure : public raft::SaveSnapshotClosure {
public:
    MockSaveSnapshotClosure(raft::SnapshotWriter* writer, 
                            raft::SnapshotMeta *expected_meta) 
        : _start_times(0)
        , _writer(writer)
        , _expected_meta(expected_meta)
    {
    }
    ~MockSaveSnapshotClosure() {}
    void Run() {
        ASSERT_TRUE(status().ok()) << status();
    }
    raft::SnapshotWriter* start(const raft::SnapshotMeta& meta) {
        EXPECT_EQ(meta.last_included_index, 
                    _expected_meta->last_included_index);
        EXPECT_EQ(meta.last_included_term, 
                    _expected_meta->last_included_term);
        ++_start_times;
        return _writer;
    }
private:
    int _start_times;
    raft::SnapshotWriter* _writer;
    raft::SnapshotMeta* _expected_meta;
};

class MockLoadSnapshotClosure : public raft::LoadSnapshotClosure {
public:
    MockLoadSnapshotClosure(raft::SnapshotReader* reader)
        : _start_times(0)
        , _reader(reader)
    {}
    ~MockLoadSnapshotClosure() {}
    void Run() {
        ASSERT_TRUE(status().ok()) << status();
    }
    raft::SnapshotReader* start() {
        ++_start_times;
        return _reader;
    }
private:
    int _start_times;
    raft::SnapshotReader* _reader;
};

TEST_F(FSMCallerTest, snapshot) {
    raft::SnapshotMeta snapshot_meta;
    snapshot_meta.last_included_index = 0;
    snapshot_meta.last_included_term = 0;
    snapshot_meta.last_configuration._peers.clear();
    DummySnapshotReader dummy_reader(&snapshot_meta);
    DummySnapshoWriter dummy_writer;
    MockSaveSnapshotClosure save_snapshot_done(&dummy_writer, &snapshot_meta);
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_ptr<raft::LogManager> lm(new raft::LogManager());
    raft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(log_opt));

    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    raft::ClosureQueue cq;
    raft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;
    raft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));
    ASSERT_EQ(0, caller.on_snapshot_save(&save_snapshot_done));
    MockLoadSnapshotClosure load_snapshot_done(&dummy_reader);
    ASSERT_EQ(0, caller.on_snapshot_load(&load_snapshot_done));
    ASSERT_EQ(0, caller.shutdown());
    fsm.join();
    ASSERT_EQ(1, fsm._on_snapshot_save_times);
    ASSERT_EQ(1, fsm._on_snapshot_load_times);
    ASSERT_EQ(1, save_snapshot_done._start_times);
    ASSERT_EQ(1, load_snapshot_done._start_times);
}

