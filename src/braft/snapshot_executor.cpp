// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include "braft/snapshot_executor.h"
#include "braft/util.h"
#include "braft/node.h"
#include "braft/storage.h"
#include "braft/snapshot.h"
#include "braft/snapshot_service.pb.h"

namespace braft {

DEFINE_int32(raft_do_snapshot_min_index_gap, 1,
             "Will do snapshot only when actual gap between applied_index and"
             " last_snapshot_index is equal to or larger than this value");
BRPC_VALIDATE_GFLAG(raft_do_snapshot_min_index_gap, brpc::PositiveInteger);
DEFINE_int32(raft_tail_snapshot_req_timeout_ms, 500,
             "Timeout of tail snapshot rpc requests");
DEFINE_int32(raft_fetch_snapshot_reader_expire_time_s, 900,
             "Survival time of fetch snapshot reader");
BRPC_VALIDATE_GFLAG(raft_fetch_snapshot_reader_expire_time_s,
                    ::brpc::PositiveInteger);

class SaveSnapshotDone : public SaveSnapshotClosure {
public:
    SaveSnapshotDone(SnapshotExecutor* node, SnapshotWriter* writer, Closure* done);
    virtual ~SaveSnapshotDone();

    SnapshotWriter* start(const SnapshotMeta& meta);
    virtual void Run();
 
private:
    static void* continue_run(void* arg);

    SnapshotExecutor* _se;
    SnapshotWriter* _writer;
    Closure* _done; // user done
    SnapshotMeta _meta;
};

class InstallSnapshotDone : public LoadSnapshotClosure {
public:
    InstallSnapshotDone(SnapshotExecutor* se,
                        SnapshotReader* reader);
    virtual ~InstallSnapshotDone();

    SnapshotReader* start();
    virtual void Run();
private:
    SnapshotExecutor* _se;
    SnapshotReader* _reader;
};

class FetchSnapshotDone : public FetchSnapshotClosure {
public:
    FetchSnapshotDone(SnapshotExecutor* se,
                      SnapshotReader* reader,
                      bool load_snapshot)
        : FetchSnapshotClosure(load_snapshot)
        , _se(se)
        , _reader(reader) {}

    virtual ~FetchSnapshotDone() {
        if (_reader) {
            _se->snapshot_storage()->close(_reader);
        }
    }

    SnapshotReader* start() { return _reader; }

    virtual void Run() {
        _se->on_load_fetched_snapshot_done(status());
        _event.signal();
    }

    void wait_for_run() {
        _event.wait();
    }
private:
    SnapshotExecutor* _se;
    SnapshotReader* _reader;
    bthread::CountdownEvent _event;
};

class SnapshotLoadDone : public LoadSnapshotClosure {
public:
    SnapshotLoadDone(SnapshotExecutor* se,
                     SnapshotReader* reader, FSMCaller* fsm_caller,
                     int64_t commited_index, Closure* done)
        : _se(se)
        , _reader(reader)
        , _fsm_caller(fsm_caller)
        , _commited_index(commited_index)
        , _done(done) {
    }
    ~SnapshotLoadDone() {
    }

    SnapshotReader* start() { return _reader; }
    void Run() {
        _se->_snapshot_storage->close(_reader);
        _se->on_snapshot_load_done(status());
        if (status().ok() && _fsm_caller) {
            _fsm_caller->set_state_machine_running(_commited_index);
        }
        if (_done) {
            _done->status() = status();
            _done->Run();
        }
        delete this;
    }
private:
    SnapshotExecutor* _se;
    SnapshotReader* _reader;
    FSMCaller* _fsm_caller;
    int64_t _commited_index;
    Closure* _done;
};

class FirstSnapshotLoadDone : public LoadSnapshotClosure {
public:
    FirstSnapshotLoadDone(SnapshotExecutor* se,
                          SnapshotReader* reader)
        : _se(se)
        , _reader(reader) {
    }
    ~FirstSnapshotLoadDone() {
    }

    SnapshotReader* start() { return _reader; }
    void Run() {
        _se->on_snapshot_load_done(status());
        _event.signal();
    }
    void wait_for_run() {
        _event.wait();
    }
private:
    SnapshotExecutor* _se;
    SnapshotReader* _reader;
    bthread::CountdownEvent _event;
};

SnapshotExecutor::SnapshotExecutor()
    : _last_snapshot_term(0)
    , _last_snapshot_index(0)
    , _term(0)
    , _saving_snapshot(false)
    , _loading_snapshot(false)
    , _stopped(false)
    , _snapshot_storage(NULL)
    , _cur_copier(NULL)
    , _fsm_caller(NULL)
    , _node(NULL)
    , _log_manager(NULL)
    , _downloading_snapshot(NULL)
    , _running_jobs(0)
    , _snapshot_throttle(NULL)
    , _fetch_snapshot_reader(NULL)
    , _reader_expire_timer(bthread_timer_t())
    , _snapshot_fetcher_id(INVALID_BTHREAD_ID)
{
}

SnapshotExecutor::~SnapshotExecutor() {
    shutdown();
    join();
    CHECK(!_saving_snapshot);
    CHECK(!_cur_copier);
    CHECK(!_loading_snapshot);
    CHECK(!_downloading_snapshot.load(butil::memory_order_relaxed));
    if (_snapshot_storage) {
        delete _snapshot_storage;
    }
}

void SnapshotExecutor::do_snapshot(Closure* done) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    int64_t saved_last_snapshot_index = _last_snapshot_index;
    int64_t saved_last_snapshot_term = _last_snapshot_term;
    if (_stopped) {
        lck.unlock();
        if (done) {
            done->status().set_error(EPERM, "Is stopped");
            run_closure_in_bthread(done, _usercode_in_pthread);
        }
        return;
    }
    // check snapshot install/load
    if (_downloading_snapshot.load(butil::memory_order_relaxed)) {
        lck.unlock();
        if (done) {
            done->status().set_error(EBUSY, "Is loading another snapshot");
            run_closure_in_bthread(done, _usercode_in_pthread);
        }
        return;
    }

    // check snapshot saving?
    if (_saving_snapshot) {
        lck.unlock();
        if (done) {
            done->status().set_error(EBUSY, "Is saving another snapshot");
            run_closure_in_bthread(done, _usercode_in_pthread);
        }
        return;
    }
    int64_t saved_fsm_applied_index = _fsm_caller->last_applied_index();
    if (saved_fsm_applied_index - _last_snapshot_index < 
                                        FLAGS_raft_do_snapshot_min_index_gap) {
        // There might be false positive as the last_applied_index() is being
        // updated. But it's fine since we will do next snapshot saving in a
        // predictable time.
        lck.unlock();

        _log_manager->clear_bufferred_logs();
        LOG_IF(INFO, _node != NULL) << "node " << _node->node_id()
            << " the gap between fsm applied index " << saved_fsm_applied_index
            << " and last_snapshot_index " << saved_last_snapshot_index
            << " is less than " << FLAGS_raft_do_snapshot_min_index_gap
            << ", will clear bufferred logs and return success";

        if (done) {
            run_closure_in_bthread(done, _usercode_in_pthread);
        }
        return;
    }
    
    SnapshotWriter* writer = _snapshot_storage->create();
    if (!writer) {
        lck.unlock();
        if (done) {
            done->status().set_error(EIO, "Fail to create writer");
            run_closure_in_bthread(done, _usercode_in_pthread);
        }
        report_error(EIO, "Fail to create SnapshotWriter");
        return;
    }
    _saving_snapshot = true;
    SaveSnapshotDone* snapshot_save_done = new SaveSnapshotDone(this, writer, done);
    if (_fsm_caller->on_snapshot_save(snapshot_save_done) != 0) {
        lck.unlock();
        if (done) {
            snapshot_save_done->status().set_error(EHOSTDOWN, "The raft node is down");
            run_closure_in_bthread(snapshot_save_done, _usercode_in_pthread);
        }
        return;
    }
    _running_jobs.add_count(1);
}

int SnapshotExecutor::on_snapshot_save_done(
    const butil::Status& st, const SnapshotMeta& meta, SnapshotWriter* writer) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    int ret = st.error_code();
    // InstallSnapshot can break SaveSnapshot, check InstallSnapshot when SaveSnapshot
    // because upstream Snapshot maybe newer than local Snapshot.
    if (st.ok()) {
        if (meta.last_included_index() <= _last_snapshot_index) {
            ret = ESTALE;
            LOG_IF(WARNING, _node != NULL) << "node " << _node->node_id()
                << " discards an stale snapshot "
                << " last_included_index " << meta.last_included_index()
                << " last_snapshot_index " << _last_snapshot_index;
            writer->set_error(ESTALE, "Installing snapshot is older than local snapshot");
        }
    }
    lck.unlock();
    
    if (ret == 0) {
        if (writer->save_meta(meta)) {
            LOG(WARNING) << "node " << _node->node_id() << " fail to save snapshot";    
            ret = EIO;
        }
    } else {
        if (writer->ok()) {
            writer->set_error(ret, "Fail to do snapshot");
        }
    }

    if (_snapshot_storage->close(writer) != 0) {
        ret = EIO;
        LOG(WARNING) << "node " << _node->node_id() << " fail to close writer";
    }

    std::stringstream ss;
    if (_node) {
        ss << "node " << _node->node_id() << ' ';
    }
    lck.lock();
    if (ret == 0) {
        _last_snapshot_index = meta.last_included_index();
        _last_snapshot_term = meta.last_included_term();
        // A new snapshot has been saved, the cached fetch snapshot reader (if
        // any) points to a stale snapshot. Destroy it so that subsequent
        // tail_snapshot requests from learners serve the latest snapshot.
        unsafe_destroy_fetch_snapshot_reader();
        lck.unlock();
        ss << "snapshot_save_done, last_included_index=" << meta.last_included_index()
           << " last_included_term=" << meta.last_included_term();
        LOG(INFO) << ss.str();
        _log_manager->set_snapshot(&meta);
        lck.lock();
    }
    if (ret == EIO) {
        report_error(EIO, "Fail to save snapshot");
    }
    _saving_snapshot = false;
    lck.unlock();
    _running_jobs.signal();
    return ret;
}

void SnapshotExecutor::on_snapshot_load_done(const butil::Status& st) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    CHECK(_loading_snapshot);
    DownloadingSnapshot* m = _downloading_snapshot.load(butil::memory_order_relaxed);

    if (st.ok()) {
        _last_snapshot_index = _loading_snapshot_meta.last_included_index();
        _last_snapshot_term = _loading_snapshot_meta.last_included_term();
        _log_manager->set_snapshot(&_loading_snapshot_meta);
    }
    std::stringstream ss;
    if (_node) {
        ss << "node " << _node->node_id() << ' ';
    }
    ss << "snapshot_load_done, "
              << _loading_snapshot_meta.ShortDebugString();
    LOG(INFO) << ss.str();
    lck.unlock();
    if (_node) {
        // FIXME: race with set_peer, not sure if this is fine
        _node->update_configuration_after_installing_snapshot();
    }
    lck.lock();
    _loading_snapshot = false;
    _downloading_snapshot.store(NULL, butil::memory_order_release);
    lck.unlock();
    if (m) {
        // Respond RPC
        if (!st.ok()) {
            m->cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        } else {
            m->response->set_success(true);
        }
        m->done->Run();
        delete m;
    }
    _running_jobs.signal();
}

SaveSnapshotDone::SaveSnapshotDone(SnapshotExecutor* se, 
                                   SnapshotWriter* writer, 
                                   Closure* done)
    : _se(se), _writer(writer), _done(done) {
    // here AddRef, SaveSnapshot maybe async
    if (se->node()) {
        se->node()->AddRef();
    }
}

SaveSnapshotDone::~SaveSnapshotDone() {
    if (_se->node()) {
        _se->node()->Release();
    }
}

SnapshotWriter* SaveSnapshotDone::start(const SnapshotMeta& meta) {
    _meta = meta;
    return _writer;
}

void* SaveSnapshotDone::continue_run(void* arg) {
    SaveSnapshotDone* self = (SaveSnapshotDone*)arg;
    std::unique_ptr<SaveSnapshotDone> self_guard(self);
    // Must call on_snapshot_save_done to clear _saving_snapshot
    int ret = self->_se->on_snapshot_save_done(
        self->status(), self->_meta, self->_writer);
    if (ret != 0 && self->status().ok()) {
        self->status().set_error(ret, "node call on_snapshot_save_done failed");
    }
    //user done, need set error
    if (self->_done) {
        self->_done->status() = self->status();
    }
    if (self->_done) {
        run_closure_in_bthread(self->_done, true);
    }
    return NULL;
}

void SaveSnapshotDone::Run() {
    // Avoid blocking FSMCaller
    // This continuation of snapshot saving is likely running inplace where the
    // on_snapshot_save is called (in the FSMCaller thread) and blocks all the
    // following on_apply. As blocking is not necessary and the continuation is
    // not important, so we start a bthread to do this.
    bthread_t tid;
    if (bthread_start_urgent(&tid, NULL, continue_run, this) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        continue_run(this);
    }
}

int SnapshotExecutor::init(const SnapshotExecutorOptions& options) {
    if (options.uri.empty()) {
        LOG(ERROR) << "node " << _node->node_id() << " uri is empty()";
        return -1;
    }
    _log_manager = options.log_manager;
    _fsm_caller = options.fsm_caller;
    _node = options.node;
    _term = options.init_term;
    _usercode_in_pthread = options.usercode_in_pthread;

    _snapshot_storage = SnapshotStorage::create(options.uri);
    if (!_snapshot_storage) {
        LOG(ERROR)  << "node " << _node->node_id() 
                    << " fail to find snapshot storage, uri " << options.uri;
        return -1;
    }
    if (options.filter_before_copy_remote) {
        _snapshot_storage->set_filter_before_copy_remote();
    }
    if (options.file_system_adaptor) {
        _snapshot_storage->set_file_system_adaptor(options.file_system_adaptor);
    }
    if (options.snapshot_throttle) {
        _snapshot_throttle = options.snapshot_throttle;
        _snapshot_storage->set_snapshot_throttle(options.snapshot_throttle);
    }
    if (_snapshot_storage->init() != 0) {
        LOG(ERROR) << "node " << _node->node_id() 
                   << " fail to init snapshot storage, uri " << options.uri;
        return -1;
    }
    LocalSnapshotStorage* tmp = dynamic_cast<LocalSnapshotStorage*>(_snapshot_storage);
    if (tmp != NULL && !tmp->has_server_addr()) {
        tmp->set_server_addr(options.addr);
    }
    if (!options.copy_file) {
        tmp->set_copy_file(false);
    }
    SnapshotReader* reader = _snapshot_storage->open();
    if (reader == NULL) {
        return 0;
    }
    if (reader->load_meta(&_loading_snapshot_meta) != 0) {
        LOG(ERROR) << "Fail to load meta from `" << options.uri << "'";
        _snapshot_storage->close(reader);
        return -1;
    }
    if (!options.learner_auto_load_applied_logs) {
        _last_snapshot_index = _loading_snapshot_meta.last_included_index();
        _last_snapshot_term = _loading_snapshot_meta.last_included_term();
        _log_manager->set_snapshot(&_loading_snapshot_meta);
        _snapshot_storage->close(reader);
        return 0;
    }
    _loading_snapshot = true;
    _running_jobs.add_count(1);
    // Load snapshot ater startup
    FirstSnapshotLoadDone done(this, reader);
    CHECK_EQ(0, _fsm_caller->on_snapshot_load(&done));
    done.wait_for_run();
    _snapshot_storage->close(reader);
    if (!done.status().ok()) {
        LOG(ERROR) << "Fail to load snapshot from " << options.uri;
        return -1;
    }
    return 0;
}

int SnapshotExecutor::load_snapshot(int64_t commited_index, Closure* done) {
    // check _snapshot_storage is not 0 before call
    SnapshotReader* reader = _snapshot_storage->open();
    if (reader == NULL) {
        _fsm_caller->set_state_machine_running(commited_index);
        if (done != NULL) {
            done->Run();
        }
        return 0;
    }
    if (reader->load_meta(&_loading_snapshot_meta) != 0) {
        LOG(ERROR) << "Fail to load meta";
        _snapshot_storage->close(reader);
        return -1;
    }
    _loading_snapshot = true;
    _running_jobs.add_count(1);

    SnapshotLoadDone* load_done = new SnapshotLoadDone(
            this, reader, _fsm_caller, commited_index, done);
    CHECK_EQ(0, _fsm_caller->on_snapshot_load(load_done));
    return 0;
}

void SnapshotExecutor::check_consistency() {
    butil::Status st = _log_manager->check_consistency();
    CHECK_EQ(true, st.ok());
}

butil::Status SnapshotExecutor::check_snapshot_consistency() {
    SnapshotReader* reader = _snapshot_storage->open();
    if (reader == NULL) {
        return butil::Status::OK();
    }
    SnapshotMeta meta;
    if (reader->load_meta(&meta) != 0) {
        _snapshot_storage->close(reader);
        return butil::Status(-1, "fail to load meta");
    }
    butil::Status st = _log_manager->check_snapshot_consistency(
            LogId(meta.last_included_index(), meta.last_included_term()));
    _snapshot_storage->close(reader);
    return st;
}

void SnapshotExecutor::install_snapshot(brpc::Controller* cntl,
                                        const InstallSnapshotRequest* request,
                                        InstallSnapshotResponse* response,
                                        google::protobuf::Closure* done) {
    int ret = 0;
    brpc::ClosureGuard done_guard(done);
    SnapshotMeta meta = request->meta();

    // check if install_snapshot tasks num exceeds threshold 
    if (_snapshot_throttle && !_snapshot_throttle->add_one_more_task(false)) {
        LOG(WARNING) << "Fail to install snapshot";
        cntl->SetFailed(EBUSY, "Fail to add install_snapshot tasks now");
        return;
    }

    std::unique_ptr<DownloadingSnapshot> ds(new DownloadingSnapshot);
    ds->cntl = cntl;
    ds->done = done;
    ds->response = response;
    ds->request = request;
    ret = register_downloading_snapshot(ds.get());
    //    ^^^ DON'T access request, response, done and cntl after this point
    //        as the retry snapshot will replace this one.
    if (ret != 0) {
        if (_node) {
            LOG(WARNING) << "node " << _node->node_id()
                         << " fail to register_downloading_snapshot";
        } else {
            LOG(WARNING) << "Fail to register_downloading_snapshot";
        }
        if (ret > 0) {
            // This RPC will be responded by the previous session
            done_guard.release();
        }
        if (_snapshot_throttle) {
            _snapshot_throttle->finish_one_task(false);
        }
        return;
    }
    // Release done first as this RPC might be replaced by the retry one
    done_guard.release();
    CHECK(_cur_copier);
    _cur_copier->join();
    // when copying finished or canceled, more install_snapshot tasks are allowed
    if (_snapshot_throttle) {
        _snapshot_throttle->finish_one_task(false);
    }
    return load_downloading_snapshot(ds.release(), meta);
}

void SnapshotExecutor::load_downloading_snapshot(DownloadingSnapshot* ds,
                                                 const SnapshotMeta& meta) {
    std::unique_ptr<DownloadingSnapshot> ds_guard(ds);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    CHECK_EQ(ds, _downloading_snapshot.load(butil::memory_order_relaxed));
    brpc::ClosureGuard done_guard(ds->done);
    CHECK(_cur_copier);
    SnapshotReader* reader = _cur_copier->get_reader();
    if (!_cur_copier->ok()) {
        if (_cur_copier->error_code() == EIO) {
            report_error(_cur_copier->error_code(), 
                         "%s", _cur_copier->error_cstr());
        }
        if (reader) {
            _snapshot_storage->close(reader);
        }
        ds->cntl->SetFailed(_cur_copier->error_code(), "%s",
                            _cur_copier->error_cstr());
        _snapshot_storage->close(_cur_copier);
        _cur_copier = NULL;
        _downloading_snapshot.store(NULL, butil::memory_order_relaxed);
        // Release the lock before responding the RPC
        lck.unlock();
        _running_jobs.signal();
        return;
    }
    _snapshot_storage->close(_cur_copier);
    _cur_copier = NULL;
    if (reader == NULL || !reader->ok()) {
        if (reader) {
            _snapshot_storage->close(reader);
        }
        _downloading_snapshot.store(NULL, butil::memory_order_release);
        lck.unlock();
        ds->cntl->SetFailed(brpc::EINTERNAL, 
                           "Fail to copy snapshot from %s",
                            ds->request->uri().c_str());
        _running_jobs.signal();
        return;
    }
    // The owner of ds is on_snapshot_load_done
    ds_guard.release();
    done_guard.release();
    _loading_snapshot = true;
    //                ^ After this point, this installing cannot be interrupted
    _loading_snapshot_meta = meta;
    lck.unlock();
    InstallSnapshotDone* install_snapshot_done =
            new InstallSnapshotDone(this, reader);
    int ret = _fsm_caller->on_snapshot_load(install_snapshot_done);
    if (ret != 0) {
        LOG(WARNING) << "node " << _node->node_id() << " fail to call on_snapshot_load";
        install_snapshot_done->status().set_error(EHOSTDOWN, "This raft node is down");
        return install_snapshot_done->Run();
    }
}
void SnapshotExecutor::unlock_snapshot_fetcher_id(bthread_id_t context_id) {
    if (context_id != INVALID_BTHREAD_ID) {
        bthread_id_unlock(context_id);
    }
}

void* SnapshotExecutor::run_snapshot_fetcher(void* arg) {
    SnapshotFetcherCtx* ctx = NULL;
    bthread_id_t context_id = { (uint64_t)arg };
    if (bthread_id_lock(context_id, (void**)&ctx) != 0) {
        LOG(INFO) << "run_snapshot_fetcher error, thread id invalid";
        return NULL;
    }
    GroupId group_id = ctx->group_id;
    PeerId peer_id = ctx->peer_id;
    Closure* done = ctx->done;
    SnapshotExecutor* se = ctx->se;
    unlock_snapshot_fetcher_id(context_id);

    int ret = se->fetch_snapshot(group_id, peer_id, 0, context_id, false);
    if (ret != ECANCELED && destroy_snapshot_fetcher_id(context_id)) {
        done->status().set_error(ret, "Fail to fetch snapshot");
        done->Run();
    }
    return NULL;
}

bool SnapshotExecutor::destroy_snapshot_fetcher_id(const bthread_id_t& id) {
    SnapshotFetcherCtx* snapshot_fetcher_ctx = NULL;
    if (bthread_id_lock(id, (void**)&snapshot_fetcher_ctx) != 0) {
        return false;
    }
    SnapshotExecutor* se = snapshot_fetcher_ctx->se;
    if (!se->reset_snapshot_fetcher_id(id)) {
        bthread_id_unlock(id);
        return false;
    }
    bthread_id_unlock_and_destroy(id);
    delete snapshot_fetcher_ctx;
    return true;
}

bool SnapshotExecutor::reset_snapshot_fetcher_id(const bthread_id_t& id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_snapshot_fetcher_id != id) {
        return false;
    }
    _snapshot_fetcher_id = INVALID_BTHREAD_ID;
    return true;
}
void SnapshotExecutor::fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                                      const int64_t max_snapshot_index,
                                      SnapshotWriter* writer, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (writer == NULL) {
        if (done) {
            done->status().set_error(EINVAL, "Argument invalid.");
        }
        return;
    }

    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_stopped) {
        if (done) {
            done->status().set_error(EHOSTDOWN, "Node is stopped");
        }
        return;
    }
    if (_snapshot_fetcher_id != INVALID_BTHREAD_ID) {
        if (done) {
            done->status().set_error(EINVAL, "Fetching snapshot now");
        }
        return;
    }

    SnapshotFetcherCtx* snapshot_fetcher_ctx = new SnapshotFetcherCtx();
    CHECK(snapshot_fetcher_ctx);
    snapshot_fetcher_ctx->max_index = max_snapshot_index;
    snapshot_fetcher_ctx->group_id = group_id;
    snapshot_fetcher_ctx->peer_id = peer_id;
    snapshot_fetcher_ctx->se = this;
    snapshot_fetcher_ctx->writer = writer;
    snapshot_fetcher_ctx->done = done;
    if (bthread_id_create(&snapshot_fetcher_ctx->resource_id,
                          snapshot_fetcher_ctx, NULL) != 0) {
        delete snapshot_fetcher_ctx;
        if (done) {
            done->status().set_error(EINVAL, "Fail to create bthread_id");
        }
        return;
    }
    _snapshot_fetcher_id = snapshot_fetcher_ctx->resource_id;

    if (bthread_start_background(&snapshot_fetcher_ctx->tid, NULL,
                run_snapshot_fetcher,
                reinterpret_cast<void*>(_snapshot_fetcher_id.value)) != 0) {
        bthread_id_cancel(_snapshot_fetcher_id);
        _snapshot_fetcher_id = INVALID_BTHREAD_ID;
        delete snapshot_fetcher_ctx;
        PLOG(ERROR) << "Fail to start bthread";
        if (done) {
            done->status().set_error(EINVAL, "start bthread failed");
        }
        return;
    }
    done_guard.release();
}
void SnapshotExecutor::cancel_fetch_snapshot(const bool is_wait) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_snapshot_fetcher_id == INVALID_BTHREAD_ID) {
        LOG(INFO) << "Snapshot fetcher has been canceled";
        return;
    }
    if (_downloading_snapshot.load(butil::memory_order_relaxed) == NULL && _cur_copier) {
        _cur_copier->cancel();
    }
    bthread_id_t context_id = _snapshot_fetcher_id;
    _snapshot_fetcher_id = INVALID_BTHREAD_ID;
    lck.unlock();

    SnapshotFetcherCtx* snapshot_fetcher_ctx = NULL;
    if (bthread_id_lock(context_id, (void**)&snapshot_fetcher_ctx) != 0) {
        LOG(INFO) << "Snapshot fetcher has been canceled";
        return;
    }
    bthread_t tid = snapshot_fetcher_ctx->tid;
    Closure* done = snapshot_fetcher_ctx->done;
    bthread_id_unlock_and_destroy(context_id);
    delete snapshot_fetcher_ctx;

    bthread_stop(tid);
    if (done) {
        done->status().set_error(ECANCELED, "Snapshot fetcher canceled");
        done->Run();
    }

    if (is_wait) {
        bthread_join(tid, NULL);
    }
}

int SnapshotExecutor::fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                                     const int64_t max_snapshot_index) {
    return fetch_snapshot(group_id, peer_id, max_snapshot_index, INVALID_BTHREAD_ID, true);
}

int SnapshotExecutor::fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                                     const int64_t snapshot_index,
                                     const bthread_id_t& context_id,
                                     bool load_snapshot) {
    if (group_id.empty() || peer_id.is_empty()) {
        LOG(INFO) << "Fail to fetch snapshot with empty group_id or peer_id";
        return -1;
    }

    // get remote uri
    TailSnapshotRequest request;
    TailSnapshotResponse response;
    brpc::Controller cntl;

    brpc::Channel channel;
    brpc::ChannelOptions channel_opt;
    channel_opt.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    if (channel.Init(peer_id.addr, &channel_opt) != 0) {
        LOG(INFO) << "Fail to init fetch_snapshot channel"
                  << ", group_id:" << group_id << ", peer_id:" << peer_id;
        return -1;
    }

    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    if (snapshot_index != 0) {
        request.set_last_snapshot_index(snapshot_index);
    }
    SnapshotService_Stub stub(&channel);
    cntl.set_timeout_ms(FLAGS_raft_tail_snapshot_req_timeout_ms);
    stub.tail_snapshot(&cntl, &request, &response, NULL);
    if (cntl.Failed() || !response.success()) {
        LOG(INFO) << "Fail to request tail_snapshot, group_id:" << group_id
                  << ", peer_id:" << peer_id
                  << ", last_snapshot_index:" << snapshot_index
                  << ", errmsg:" << cntl.ErrorText().c_str();
        return -1;
    }

    int64_t last_included_index = response.meta().last_included_index();
    const std::string& uri = response.uri();
    SnapshotFetcherCtx* snapshot_fetcher_ctx = NULL;
    if (context_id != INVALID_BTHREAD_ID &&
            bthread_id_lock(context_id, (void**)&snapshot_fetcher_ctx) != 0) {
        LOG(INFO) << "Fail to fetch snapshot, is canceled, group_id:" << group_id;
        return ECANCELED;
    }

    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (context_id == INVALID_BTHREAD_ID && _saving_snapshot) {
        LOG(INFO) << "Fail to fetch snapshot, is saving snapshot, group_id:" << group_id;
        return -1;
    }
    // avoid generated a fresh snapshot when tailing snapshot
    if (last_included_index <= _last_snapshot_index) {
        LOG(INFO) << "Fail to fetch snapshot, remote snapshot index "
            << last_included_index << " is less than local index " << _last_snapshot_index
            << ", group_id:" << group_id;
        lck.unlock();
        unlock_snapshot_fetcher_id(context_id);
        return -1;
    }
    SnapshotWriter* writer = NULL;
    // learner may fetch snapshot to load,
    // snapshot can be fetched only when index is less than last applied index
    if (snapshot_fetcher_ctx) {
        writer = snapshot_fetcher_ctx->writer;
        CHECK(writer);
        if (last_included_index > snapshot_fetcher_ctx->max_index) {
            LOG(INFO) << "Fail to fetch snapshot, remote snapshot index "
                << last_included_index << " is greater than max snapshot index "
                << snapshot_fetcher_ctx->max_index << ", group_id:" << group_id;
            lck.unlock();
            unlock_snapshot_fetcher_id(context_id);
            return -1;
        }
    }
    LOG(INFO) << "Tailed snapshot, snapshot_index:" << last_included_index << ", uri:" << uri
              << ", group_id:" << group_id;
    if (_downloading_snapshot.load(butil::memory_order_relaxed) != NULL) {
        LOG(INFO) << "Fail to fetch snapshot, installing now, snapshot_index:"
                  << last_included_index << ", group_id:" << group_id;
        lck.unlock();
        unlock_snapshot_fetcher_id(context_id);
        return -1;
    }
    if (_cur_copier) {
        LOG(INFO) << "Fail to fetch snapshot, fetching now, snapshot_index:"
                  << last_included_index << ", group_id:" << group_id;
        lck.unlock();
        unlock_snapshot_fetcher_id(context_id);
        return -1;
    }

    if (_snapshot_throttle && !_snapshot_throttle->add_one_more_task(false)) {
        LOG(WARNING) << "Fail to fetch snapshot, controlled by flow, group_id:" << group_id;
        lck.unlock();
        unlock_snapshot_fetcher_id(context_id);
        return -1;
    }
    _cur_copier = _snapshot_storage->start_to_copy_from(uri, writer);
    if (_cur_copier == NULL) {
        if (_snapshot_throttle) {
            _snapshot_throttle->finish_one_task(false);
        }
        LOG(WARNING) << "Fail to fetch snapshot, copy error, group_id:" << group_id;
        lck.unlock();
        unlock_snapshot_fetcher_id(context_id);
        return -1;
    }
    _running_jobs.add_count(1);
    lck.unlock();
    unlock_snapshot_fetcher_id(context_id);

    _cur_copier->join();
    // when copying finished or canceled, more fetch_snapshot tasks are allowed
    if (_snapshot_throttle) {
        _snapshot_throttle->finish_one_task(false);
    }

    if (context_id != INVALID_BTHREAD_ID) {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        int error_code = _cur_copier->error_code();
        _snapshot_storage->close(_cur_copier);
        _cur_copier = NULL;
        lck.unlock();

        SnapshotFetcherCtx* snapshot_fetcher_ctx = NULL;
        if (bthread_id_lock(context_id, (void**)&snapshot_fetcher_ctx) != 0) {
            LOG(INFO) << "Fail to fetch snapshot, is canceled, group_id:" << group_id;
            _running_jobs.signal();
            return ECANCELED;
        }

        error_code = writer->error_code() != 0 ? writer->error_code() : error_code;
        LOG(INFO) << "Finish fetch snapshot, group_id:" << group_id
            << ", index:" << last_included_index << ", errno:" << error_code;
        unlock_snapshot_fetcher_id(context_id);
        _running_jobs.signal();
        return error_code;
    }
    return load_fetched_snapshot(uri, load_snapshot);
}

int SnapshotExecutor::load_fetched_snapshot(const std::string& uri, bool load_snapshot) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    SnapshotReader* reader = _cur_copier->get_reader();
    if (!_cur_copier->ok()) {
        if (reader) {
            _snapshot_storage->close(reader);
        }
        _snapshot_storage->close(_cur_copier);
        _cur_copier = NULL;
        _running_jobs.signal();
        LOG(WARNING) << "Fail to copy snapshot from " << uri;
        return -1;
    }
    _snapshot_storage->close(_cur_copier);
    _cur_copier = NULL;
    if (reader == NULL || !reader->ok()) {
        if (reader) {
            _snapshot_storage->close(reader);
        }
        LOG(WARNING) << "Fail to copy snapshot from " << uri;
        _running_jobs.signal();
        return -1;
    }
    SnapshotMeta meta;
    if (reader->load_meta(&meta) != 0) {
        _snapshot_storage->close(reader);
        _running_jobs.signal();
        return -1;
    }

    if (_last_snapshot_index >= meta.last_included_index()) {
        LOG(INFO) << "Fail to fetch snapshot, remote snapshot index "
            << meta.last_included_index() << " is less than local index "
            << _last_snapshot_index;
        _snapshot_storage->close(reader);
        _running_jobs.signal();
        return -1;
    }
    _loading_snapshot = true;
    _loading_snapshot_meta = meta;
    lck.unlock();

    int ret = 0;
    FetchSnapshotDone fetch_snapshot_done(this, reader, load_snapshot);
    ret = _fsm_caller->on_snapshot_fetched(&fetch_snapshot_done);
    if (ret != 0) {
        LOG(FATAL) << "Fail to call fsm_caller when snapshot fetched, uri:" << uri;
        fetch_snapshot_done.status().set_error(EHOSTDOWN, "This raft node is down");
        fetch_snapshot_done.Run();
        return -1;
    }
    fetch_snapshot_done.wait_for_run();
    return 0;
}

void SnapshotExecutor::on_load_fetched_snapshot_done(const butil::Status& st) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    CHECK(_loading_snapshot);
    if (st.ok()) {
        _last_snapshot_index = _loading_snapshot_meta.last_included_index();
        _last_snapshot_term = _loading_snapshot_meta.last_included_term();
        _log_manager->set_snapshot(&_loading_snapshot_meta);
    }
    if (_node) {
        LOG(INFO) << "node " << _node->node_id()
                  << " fetch_snapshot_done, "
                  << _loading_snapshot_meta.ShortDebugString();
    } else {
        LOG(INFO) << "fetch_snapshot_done, "
                  << _loading_snapshot_meta.ShortDebugString();
    }
    lck.unlock();
    if (_node) {
        // FIXME: race with set_peer, not sure if this is fine
        _node->update_configuration_after_installing_snapshot();
    }
    lck.lock();
    _loading_snapshot = false;
    lck.unlock();

    _running_jobs.signal();
}

void SnapshotExecutor::handle_tail_snapshot(brpc::Controller* cntl,
                                            const TailSnapshotRequest* request,
                                            TailSnapshotResponse* response) {
    bool openned_before = true;
    std::unique_lock<raft_mutex_t> lck(_mutex);

    PeerId server_id;
    server_id.parse(request->peer_id());
    int64_t last_snapshot_index = _snapshot_storage->last_snapshot_index();
    if (request->has_last_snapshot_index()
            && request->last_snapshot_index() != last_snapshot_index) {
        LOG(WARNING) << "node " << request->group_id() << ":" << server_id
                     << " refuse to handle TailSnapshotRequest, required snapshot "
                     << request->last_snapshot_index() << " is not the last one:"
                     << last_snapshot_index;
        cntl->SetFailed(EINVAL, "Required snapshot is not in service");
        return;
    }

    if (_fetch_snapshot_reader == NULL) {
        _fetch_snapshot_reader = _snapshot_storage->open();
        openned_before = false;
    }

    if (!_fetch_snapshot_reader) {
        LOG(WARNING) << "node " << request->group_id() << ":" << server_id
                     << " refuse to handle TailSnapshotRequest"
                     << " because open reader failed";
        cntl->SetFailed(EIO, "Fail to open snapshot reader");
        return;
    }
    std::string uri = _fetch_snapshot_reader->generate_uri_for_copy();
    if (uri.empty()) {
        LOG(WARNING) << "node " << request->group_id() << ":" << server_id
                     << " refuse to handle TailSnapshotRequest"
                     << " because snapshot uri is empty";
        unsafe_destroy_fetch_snapshot_reader();
        cntl->SetFailed(EIO, "Fail to generate uri for copy");
        return;
    }
    SnapshotMeta meta;
    // unnecessary to report error on failure,
    // because node works well even if failed to fetch snapshot
    if (_fetch_snapshot_reader->load_meta(&meta) != 0) {
        std::string snapshot_path = _fetch_snapshot_reader->get_path();
        LOG(ERROR) << "Fail to load meta from " << snapshot_path;
        unsafe_destroy_fetch_snapshot_reader();
        cntl->SetFailed(EIO, "Fail to generate uri for copy");
        return;
    }
    int rc = 0;
    if (openned_before) {
        rc = reset_reader_expire_timer();
    } else {
        rc = start_reader_expire_timer();
    }
    if (rc != 0) {
        unsafe_destroy_fetch_snapshot_reader();
        LOG(ERROR) << "Fail to start gc timer for snapshot " << meta.last_included_index();
        cntl->SetFailed(EINVAL, "Fail to start gc timer for reader");
        return;
    }
    LOG(INFO) << "node " << request->group_id() << ":" << server_id
              << " send TailSnapshotResponse,"
              << " last_included_term " << meta.last_included_term()
              << " last_included_index " << meta.last_included_index() << " uri " << uri;

    response->set_success(true);
    response->mutable_meta()->CopyFrom(meta);
    response->set_uri(uri);
}

int SnapshotExecutor::reset_reader_expire_timer() {
    CHECK_NE(_reader_expire_timer, bthread_timer_t());
    if (bthread_timer_del(_reader_expire_timer) != 0) {
        _reader_expire_timer = bthread_timer_t();
        return -1;
    }

    if (bthread_timer_add(&_reader_expire_timer,
                          butil::seconds_from_now(FLAGS_raft_fetch_snapshot_reader_expire_time_s),
                          on_snapshot_reader_expired,
                          (void *)this) != 0) {
        _reader_expire_timer = bthread_timer_t();
        _running_jobs.signal();
        return -1;
    }
    return 0;
}

int SnapshotExecutor::start_reader_expire_timer() {
    if (_reader_expire_timer != bthread_timer_t()) {
        return -1;
    }

    if (bthread_timer_add(&_reader_expire_timer,
                          butil::seconds_from_now(FLAGS_raft_fetch_snapshot_reader_expire_time_s),
                          on_snapshot_reader_expired,
                          (void *)this) == 0) {
        _running_jobs.add_count(1);
        return 0;
    }
    return -1;
}

void SnapshotExecutor::on_snapshot_reader_expired(void* arg) {
    SnapshotExecutor* se = static_cast<SnapshotExecutor*>(arg);
    se->destroy_fetch_snapshot_reader();
    se->_running_jobs.signal();
}

void SnapshotExecutor::destroy_fetch_snapshot_reader() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    unsafe_destroy_fetch_snapshot_reader();
}

void SnapshotExecutor::unsafe_destroy_fetch_snapshot_reader() {
    if (_reader_expire_timer != bthread_timer_t() &&
            bthread_timer_del(_reader_expire_timer) == 0) {
        _running_jobs.signal();
    }
    _reader_expire_timer = bthread_timer_t();

    if (_fetch_snapshot_reader != NULL) {
        _snapshot_storage->close(_fetch_snapshot_reader);
        _fetch_snapshot_reader = NULL;
    }
}

int SnapshotExecutor::register_downloading_snapshot(DownloadingSnapshot* ds) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_stopped) {
        LOG(WARNING) << "Register failed: node is stopped.";
        ds->cntl->SetFailed(EHOSTDOWN, "Node is stopped");
        return -1;
    }
    if (ds->request->term() != _term) {
        LOG(WARNING) << "Register failed: term unmatch.";
        ds->response->set_success(false);
        ds->response->set_term(_term);
        return -1;
    }
    if (ds->request->meta().last_included_index() <= _last_snapshot_index) {
        LOG(WARNING) << "Register failed: snapshot is not newer.";
        ds->response->set_term(_term);
        ds->response->set_success(true);
        return -1;
    }
    ds->response->set_term(_term);
    if (_saving_snapshot) {
        LOG(WARNING) << "Register failed: is saving snapshot.";
        ds->cntl->SetFailed(EBUSY, "Is saving snapshot");
        return -1;
    }
    DownloadingSnapshot* m = _downloading_snapshot.load(
            butil::memory_order_relaxed);
    if (!m) {
        _downloading_snapshot.store(ds, butil::memory_order_relaxed);
        // Now this session has the right to download the snapshot.
        CHECK(!_cur_copier);
        _cur_copier = _snapshot_storage->start_to_copy_from(ds->request->uri());
        if (_cur_copier == NULL) {
            _downloading_snapshot.store(NULL, butil::memory_order_relaxed);
            lck.unlock();
            LOG(WARNING) << "Register failed: fail to copy file.";
            ds->cntl->SetFailed(EINVAL, "Fail to copy from , %s",
                                ds->request->uri().c_str());
            return -1;
        }
        _running_jobs.add_count(1);
        return 0;
    }
    int rc = 0;
    DownloadingSnapshot saved;
    bool has_saved = false;
    // A previouse snapshot is under installing, check if this is the same
    // snapshot and resume it, otherwise drop previous snapshot as this one is
    // newer
    if (m->request->meta().last_included_index() 
            == ds->request->meta().last_included_index()) {
        // m is a retry
        has_saved = true;
        // Copy |*ds| to |*m| so that the former session would respond
        // this RPC.
        saved = *m;
        *m = *ds;
        rc = 1;
    } else if (m->request->meta().last_included_index() 
            > ds->request->meta().last_included_index()) {
        // |ds| is older
        LOG(WARNING) << "Register failed: is installing a newer one.";
        ds->cntl->SetFailed(EINVAL, "A newer snapshot is under installing");
        return -1;
    } else {
        // |ds| is newer
        if (_loading_snapshot) {
            // We can't interrupt the loading one
            LOG(WARNING) << "Register failed: is loading an older snapshot.";
            ds->cntl->SetFailed(EBUSY, "A former snapshot is under loading");
            return -1;
        }
        CHECK(_cur_copier);
        _cur_copier->cancel();
        LOG(WARNING) << "Register failed: an older snapshot is under installing,"
            " cancle downloading.";
        ds->cntl->SetFailed(EBUSY, "A former snapshot is under installing, "
                                   " trying to cancel");
        return -1;
    }
    lck.unlock();
    if (has_saved) {
        // Respond replaced session
        LOG(WARNING) << "Register failed: interrupted by retry installing request.";
        saved.cntl->SetFailed(
                EINTR, "Interrupted by the retry InstallSnapshotRequest");
        saved.done->Run();
    }
    return rc;
}

void SnapshotExecutor::interrupt_downloading_snapshot(int64_t new_term) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    CHECK_GE(new_term, _term);
    _term = new_term;
    if (!_downloading_snapshot.load(butil::memory_order_relaxed)) {
        return;
    }
    if (_loading_snapshot) {
        // We can't interrupt loading
        return;
    }
    CHECK(_cur_copier);
    _cur_copier->cancel();
    std::stringstream ss;
    if (_node) {
        ss << "node " << _node->node_id() << ' ';
    }
    ss << "Trying to cancel downloading snapshot : " 
       << _downloading_snapshot.load(butil::memory_order_relaxed)
          ->request->ShortDebugString();
    LOG(INFO) << ss.str();
}

void SnapshotExecutor::report_error(int error_code, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    Error e;
    e.set_type(ERROR_TYPE_SNAPSHOT);
    e.status().set_errorv(error_code, fmt, ap);
    va_end(ap);
    _fsm_caller->on_error(e);
}

void SnapshotExecutor::describe(std::ostream&os, bool use_html) {
    SnapshotMeta meta;
    InstallSnapshotRequest request;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    const int64_t last_snapshot_index = _last_snapshot_index;
    const int64_t last_snapshot_term = _last_snapshot_term;
    const bool is_loading_snapshot = _loading_snapshot;
    if (is_loading_snapshot) {
        meta = _loading_snapshot_meta;
        //   ^
        //   Cloning Configuration is expansive, since snapshot is not the hot spot,
        //   we think it's fine
    }
    const DownloadingSnapshot* m = 
            _downloading_snapshot.load(butil::memory_order_acquire);
    if (m) {
        request.CopyFrom(*m->request);
             // ^ It's also a little expansive, but fine
    }
    const bool is_saving_snapshot = _saving_snapshot;
    // TODO: add timestamp of snapshot
    lck.unlock();
    const char *newline = use_html ? "<br>" : "\r\n";
    os << "last_snapshot_index: " << last_snapshot_index << newline;
    os << "last_snapshot_term: " << last_snapshot_term << newline;
    if (m && is_loading_snapshot) {
        CHECK(!is_saving_snapshot);
        os << "snapshot_status: LOADING" << newline;
        os << "snapshot_from: " << request.uri() << newline;
        os << "snapshot_meta: " << meta.ShortDebugString();
    } else if (m) {
        CHECK(!is_saving_snapshot);
        os << "snapshot_status: DOWNLOADING" << newline;
        os << "downloading_snapshot_from: " << request.uri() << newline;
        os << "downloading_snapshot_meta: " << request.meta().ShortDebugString();
    } else if (is_saving_snapshot) {
        os << "snapshot_status: SAVING" << newline;
    } else {
        os << "snapshot_status: IDLE" << newline;
    }
}

void SnapshotExecutor::shutdown() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    const int64_t saved_term = _term;
    _stopped = true;
    lck.unlock();
    cancel_fetch_snapshot(true);
    interrupt_downloading_snapshot(saved_term);
    unsafe_destroy_fetch_snapshot_reader();
}

void SnapshotExecutor::join() {
    // Wait until all the running jobs finishes
    _running_jobs.wait();
}

InstallSnapshotDone::InstallSnapshotDone(SnapshotExecutor* se, 
                                         SnapshotReader* reader)
    : _se(se) , _reader(reader) {
    // node not need AddRef, FSMCaller::shutdown will flush running InstallSnapshot task
}

InstallSnapshotDone::~InstallSnapshotDone() {
    if (_reader) {
        _se->snapshot_storage()->close(_reader);
    }
}

SnapshotReader* InstallSnapshotDone::start() {
    return _reader;
}

void InstallSnapshotDone::Run() {
    _se->on_snapshot_load_done(status());
    delete this;
}

}  //  namespace braft
