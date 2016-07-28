// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/07/27 20:13:42

#include <gtest/gtest.h>
#include <boost/atomic.hpp>
#include <baidu/rpc/server.h>
#include "raft/snapshot_executor.h"
#include "raft/fsm_caller.h"
#include "raft/util.h"
#include "raft/raft.h"

namespace raft {

static const char* SERVER_ADDR = "127.0.0.1:54321";

class SnapshotExecutorTest : public testing::Test {
protected:
    void SetUp() {
        system("rm -rf .data");
        ASSERT_EQ(0, raft::add_service(&_server, SERVER_ADDR));
        ASSERT_EQ(0, _server.Start(SERVER_ADDR, NULL));
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    baidu::rpc::Server _server;
};

class MockFSMCaller : public raft::FSMCaller {
protected:
    RAFT_MOCK int on_committed(int64_t /*committed_index*/) { return 0; }
    RAFT_MOCK int on_snapshot_load(LoadSnapshotClosure* done) {
        _snapshot_load_times.fetch_add(1);
        raft::run_closure_in_bthread(done);
        return 0;
    }
    RAFT_MOCK int on_snapshot_save(SaveSnapshotClosure* done) {
        _snapshot_save_times.fetch_add(1);
        raft::run_closure_in_bthread(done);
        return 0;
    }
    RAFT_MOCK int on_error(const Error& /*e*/) {
        _on_error_times.fetch_add(1);
        return 0;
    }
    boost::atomic<int> _on_error_times;
    boost::atomic<int> _snapshot_load_times;
    boost::atomic<int> _snapshot_save_times;
};

class MockLogManager : public raft::LogManager {
protected:
    // Notify the log manager about the latest snapshot, which indicates the
    // logs which can be safely truncated.
    RAFT_MOCK void set_snapshot(const SnapshotMeta* /*meta*/) {
        _set_times.fetch_add(1);
    }

    // We don't delete all the logs before last snapshot to avoid installing
    // snapshot on slow replica. Call this method to drop all the logs before
    // last snapshot immediately.
    RAFT_MOCK void clear_bufferred_logs() {
        _clear_timers.fetch_add(1);
    }

    boost::atomic<int> _set_times;
    boost::atomic<int> _clear_timers;
};

void write_file(const std::string& file, const std::string& content) {
    base::ScopedFILE fp(fopen(file.c_str(), "w"));
    ASSERT_TRUE(fp) << berror();
    fprintf(fp.get(), "%s", content.c_str());
}

std::string read_file(const std::string& file) {
    base::ScopedFILE fp(fopen(file.c_str(), "r"));
    char buf[1024];
    fscanf(fp.get(), "%s", buf);
    return buf;
}

class SyncClosure : public google::protobuf::Closure {
protected:
    SyncClosure() {
        _event.init();
    }
    void Run() {
        _event.signal();
    }
    void wait() {
        _event.wait();
    }
    bthread::CountdownEvent _event;
};

struct InstallArg {
    SnapshotExecutor* e;
    InstallSnapshotRequest request;
    InstallSnapshotResponse response;
    baidu::rpc::Controller cntl;
    SyncClosure done;
};

void* install_thread(void* arg) {
    InstallArg* ia = (InstallArg*)arg;
    ia->e->install_snapshot(
            &ia->cntl, &ia->request, &ia->response, &ia->done);
    ia->done.wait();
    return NULL;
}

TEST_F(SnapshotExecutorTest, retry_request) {
    MockFSMCaller fsm_caller;
    MockLogManager log_manager;
    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = &log_manager;
    options.uri = "local://.data/snapshot0";
    SnapshotExecutor executor;
    ASSERT_EQ(0, executor.init(options));
    LocalSnapshotStorage storage1(".data/snapshot1");
    storage1.set_server_addr(_server.listen_address());
    ASSERT_EQ(0, storage1.init());
    SnapshotWriter* writer = storage1.create();
    ASSERT_TRUE(writer);
    std::string file_path = writer->get_path() + "/data";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1M count=128",
             file_path.c_str());
    ASSERT_EQ(0, system(cmd));
    writer->add_file("data");
    SnapshotMeta meta;
    meta.set_last_included_index(1);
    meta.set_last_included_term(1);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage1.close(writer));
    SnapshotReader* reader= storage1.open();
    std::string uri = reader->generate_uri_for_copy();
    const size_t N = 10;
    bthread_t tids[N];
    InstallArg args[N];
    for (size_t i = 0; i < N; ++i) {
        args[i].e = &executor;
        args[i].request.set_group_id("test");
        args[i].request.set_term(1);
        args[i].request.mutable_meta()->CopyFrom(meta);
        args[i].request.set_uri(uri);
    }
    for (size_t i = 0; i < N; ++i) {
        bthread_start_background(&tids[i], NULL, install_thread, &args[i]);
    }
    for (size_t i = 0; i < N; ++i) {
        bthread_join(tids[i], NULL);
    }
    size_t suc = 0;
    for (size_t i = 0; i < N; ++i) {
        suc += !args[i].cntl.Failed();
        if (args[i].cntl.Failed()) {
            ASSERT_EQ(EINTR, args[i].cntl.ErrorCode());
        }
    }
    ASSERT_EQ(0, storage1.close(reader));
    ASSERT_EQ(1u, suc);
    reader = executor.snapshot_storage()->open();
    ASSERT_EQ(0, reader->get_file_meta("data", NULL));
    ASSERT_EQ(0, executor.snapshot_storage()->close(reader));
}

TEST_F(SnapshotExecutorTest, interrupt_installing) {
    MockFSMCaller fsm_caller;
    MockLogManager log_manager;
    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = &log_manager;
    options.uri = "local://.data/snapshot0";
    SnapshotExecutor executor;
    ASSERT_EQ(0, executor.init(options));
    LocalSnapshotStorage storage1(".data/snapshot1");
    storage1.set_server_addr(_server.listen_address());
    ASSERT_EQ(0, storage1.init());
    SnapshotWriter* writer = storage1.create();
    ASSERT_TRUE(writer);
    std::string file_path = writer->get_path() + "/data";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1M count=128",
             file_path.c_str());
    ASSERT_EQ(0, system(cmd));
    writer->add_file("data");
    SnapshotMeta meta;
    meta.set_last_included_index(1);
    meta.set_last_included_term(1);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage1.close(writer));
    SnapshotReader* reader= storage1.open();
    std::string uri = reader->generate_uri_for_copy();
    InstallArg arg;
    arg.e = &executor;
    arg.request.set_group_id("test");
    arg.request.set_term(1);
    arg.request.mutable_meta()->CopyFrom(meta);
    arg.request.set_uri(uri);
    bthread_t tid;
    bthread_start_background(&tid, NULL, install_thread, &arg);
    usleep(5000);
    executor.interrupt_downloading_snapshot(2);
    bthread_join(tid, NULL);
    ASSERT_TRUE(arg.cntl.Failed());
    ASSERT_EQ(ECANCELED, arg.cntl.ErrorCode());
    ASSERT_EQ(0, storage1.close(reader));
}

}  // namespace raft
