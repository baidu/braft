// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/07/27 20:13:42

#include <gtest/gtest.h>
#include <butil/atomicops.h>
#include <brpc/server.h>
#include "braft/snapshot_executor.h"
#include "braft/fsm_caller.h"
#include "braft/util.h"
#include "braft/raft.h"

namespace braft {

class SnapshotExecutorTest : public testing::Test {
protected:
    void SetUp() {
        system("rm -rf .data");
        bool server_started = false;
        for (int i = 0; i < 10; ++i) {
            std::stringstream addr_ss;
            addr_ss << "127.0.0.1:" << (6500 + i);
            if (0 != braft::add_service(&_server, addr_ss.str().c_str())) {
                continue;
            }
            if (0 != _server.Start(addr_ss.str().c_str(), NULL)) {
                continue;
            }
            server_started = true;
            break;
        }
        ASSERT_TRUE(server_started);
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    brpc::Server _server;
};

class MockFSMCaller : public braft::FSMCaller {
protected:
    BRAFT_MOCK int on_committed(int64_t /*committed_index*/) { return 0; }
    BRAFT_MOCK int on_snapshot_load(LoadSnapshotClosure* done) {
        _snapshot_load_times.fetch_add(1);
        braft::run_closure_in_bthread(done);
        return 0;
    }
    BRAFT_MOCK int on_snapshot_save(SaveSnapshotClosure* done) {
        _snapshot_save_times.fetch_add(1);
        braft::run_closure_in_bthread(done);
        return 0;
    }
    BRAFT_MOCK int on_error(const Error& /*e*/) {
        _on_error_times.fetch_add(1);
        return 0;
    }
    butil::atomic<int> _on_error_times;
    butil::atomic<int> _snapshot_load_times;
    butil::atomic<int> _snapshot_save_times;
};

class MockLogManager : public braft::LogManager {
protected:
    // Notify the log manager about the latest snapshot, which indicates the
    // logs which can be safely truncated.
    BRAFT_MOCK void set_snapshot(const SnapshotMeta* /*meta*/) {
        _set_times.fetch_add(1);
    }

    // We don't delete all the logs before last snapshot to avoid installing
    // snapshot on slow replica. Call this method to drop all the logs before
    // last snapshot immediately.
    BRAFT_MOCK void clear_bufferred_logs() {
        _clear_timers.fetch_add(1);
    }

    butil::atomic<int> _set_times;
    butil::atomic<int> _clear_timers;
};

class MockSnapshotReader : public braft::SnapshotReader {
public:
    MockSnapshotReader(const std::string& path)
        : _path(path)
    {}
    virtual ~MockSnapshotReader() {}

    // Load meta from 
    virtual int load_meta(SnapshotMeta* meta) {
        return 0;
    }

    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy() {
        return "remote://ip:port/reader_id";    
    }

    void list_files(std::vector<std::string> *files) {
        return;
    }

    virtual std::string get_path() { return _path; }

private:
    std::string _path;
};

class MockSnapshotStorage;

class MockSnapshotCopier : public braft::SnapshotCopier {
friend class MockSnapshotStorage;
public:
    MockSnapshotCopier();
    virtual ~MockSnapshotCopier() {}
    // Cancel the copy job
    virtual void cancel();
    // Block the thread until this copy job finishes, or some error occurs.
    virtual void join();
    // Get the the SnapshotReader which represents the copied Snapshot
    virtual SnapshotReader* get_reader();

    void start();
    
    static void* start_copy(void* arg);
    
private:
    bthread_t _tid;
    MockSnapshotStorage* _storage;
    SnapshotReader* _reader;
};

class MockSnapshotStorage : public braft::SnapshotStorage {
friend class MockSnapshotCopier;
public:
    MockSnapshotStorage(const std::string& path)
        : _path(path)
        , _last_snapshot_index(0)
    {}

    virtual ~MockSnapshotStorage() {}

    // Initialize
    virtual int init() { return 0; }

    // create new snapshot writer
    virtual SnapshotWriter* create() {
        return NULL; 
    }

    // close snapshot writer
    virtual int close(SnapshotWriter* writer) {
        return 0;
    }

    // get lastest snapshot reader
    virtual SnapshotReader* open() { 
        MockSnapshotReader* reader = new MockSnapshotReader(_path);
        return reader; 
    }

    // close snapshot reader
    virtual int close(SnapshotReader* reader) { 
        delete reader;
        return 0; 
    }

    // Copy snapshot from uri and open it as a SnapshotReader
    virtual SnapshotReader* copy_from(const std::string& uri) {
        return NULL;
    }

    virtual SnapshotCopier* start_to_copy_from(const std::string& uri) {
        MockSnapshotCopier* copier = new MockSnapshotCopier();
        copier->_storage = this;
        copier->start();
        return copier;
    }

    virtual int close(SnapshotCopier* copier) {
        delete copier;
        return 0;
    }

    virtual SnapshotStorage* new_instance(const std::string& uri) const {
        return NULL;
    }
    
    virtual butil::Status gc_instance(const std::string& uri) const {
        return butil::Status::OK();
    }
    
private:
    std::string _path;
    int64_t _last_snapshot_index;
};

MockSnapshotCopier::MockSnapshotCopier() 
    : _tid(INVALID_BTHREAD)
    , _storage(NULL)
    , _reader(NULL)
{}

void MockSnapshotCopier::cancel() {}

void MockSnapshotCopier::join() {
    bthread_join(_tid, NULL);
}

SnapshotReader* MockSnapshotCopier::get_reader() { return _reader; }

void MockSnapshotCopier::start() {
    LOG(INFO) << "In MockSnapshotCopier::start()"; 
    _reader = _storage->open();
    if (bthread_start_background(
                &_tid, NULL, start_copy, this) != 0) {
        LOG(INFO) << "Fail to start bthread."; 
    } 
}

void* MockSnapshotCopier::start_copy(void* arg) {
    usleep(5 * 1000 * 1000);
    return NULL;
}

void write_file(const std::string& file, const std::string& content) {
    butil::ScopedFILE fp(fopen(file.c_str(), "w"));
    ASSERT_TRUE(fp) << berror();
    fprintf(fp.get(), "%s", content.c_str());
}

std::string read_file(const std::string& file) {
    butil::ScopedFILE fp(fopen(file.c_str(), "r"));
    char buf[1024];
    fscanf(fp.get(), "%s", buf);
    return buf;
}

class SyncClosure : public google::protobuf::Closure {
protected:
    SyncClosure() {
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
    brpc::Controller cntl;
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
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1048576 count=128",
             file_path.c_str());
    system(cmd);
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
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1048576 count=128",
             file_path.c_str());
    system(cmd);
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
    if (arg.cntl.Failed()) {
        LOG(ERROR) << "error: " << arg.cntl.ErrorText();
    } else {
        LOG(INFO) << "success.";
    }
    ASSERT_EQ(ECANCELED, arg.cntl.ErrorCode());
    ASSERT_EQ(0, storage1.close(reader));
}

TEST_F(SnapshotExecutorTest, retry_install_snapshot) {
    MockFSMCaller fsm_caller;
    MockLogManager log_manager;

    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = &log_manager;
    
    SnapshotExecutor executor;
    executor._log_manager = options.log_manager;
    executor._fsm_caller = options.fsm_caller;
    executor._node = options.node;
    executor._term = options.init_term;
    executor._usercode_in_pthread = options.usercode_in_pthread;
    MockSnapshotStorage* storage0 = new MockSnapshotStorage(".data/snapshot0");
    executor._snapshot_storage = storage0;

    // target snapshot_storage
    MockSnapshotStorage storage1(".data/snapshot1");
    
    SnapshotMeta meta;
    meta.set_last_included_index(1);
    meta.set_last_included_term(1);
    SnapshotReader* reader= storage1.open();
    std::string uri = reader->generate_uri_for_copy();    
    // using bthreads to simulate install_snapshot requests
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
        LOG(INFO) << "Try number: " << i << "------------------------"; 
        if (args[i].cntl.Failed()) {
            LOG(ERROR) << "Result, Error: " << args[i].cntl.ErrorText();
        } else {
            suc += 1;
            LOG(INFO) << "Result, Success.";
        }
    }
    ASSERT_EQ(1, suc);
    ASSERT_EQ(0, storage1.close(reader));
}

TEST_F(SnapshotExecutorTest, retry_request_with_throttle) {
    MockFSMCaller fsm_caller;
    MockLogManager log_manager;
    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = &log_manager;
    options.uri = "local://.data/snapshot0";

    int64_t throttle_throughput_bytes = 100 * 1024 * 1024;
    int64_t check_cycle = 10;
    braft::ThroughputSnapshotThrottle* throttle = 
        new braft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
    scoped_refptr<braft::SnapshotThrottle> tst(throttle);
    options.snapshot_throttle = tst;

    SnapshotExecutor executor;
    ASSERT_EQ(0, executor.init(options));
    LocalSnapshotStorage storage1(".data/snapshot1");
    storage1.set_server_addr(_server.listen_address());
    ASSERT_EQ(0, storage1.init());
    SnapshotWriter* writer = storage1.create();
    ASSERT_TRUE(writer);
    std::string file_path = writer->get_path() + "/data";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1048576 count=128",
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
        LOG(INFO) << "Try number: " << i << "------------------------"; 
        if (args[i].cntl.Failed()) {
            LOG(ERROR) << "Result, Error: " << args[i].cntl.ErrorText();
        } else {
            suc += 1;
            LOG(INFO) << "Result, Success.";
        }
    }
    ASSERT_EQ(0, storage1.close(reader));
    ASSERT_EQ(1u, suc);
    reader = executor.snapshot_storage()->open();
    ASSERT_EQ(0, reader->get_file_meta("data", NULL));
    ASSERT_EQ(0, executor.snapshot_storage()->close(reader));
}

}  // namespace raft
