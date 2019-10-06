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

namespace braft {

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
    if (_fsm_caller->last_applied_index() == _last_snapshot_index) {
        // There might be false positive as the last_applied_index() is being
        // updated. But it's fine since we will do next snapshot saving in a
        // predictable time.
        lck.unlock();
        LOG_IF(INFO, _node != NULL) << "node " << _node->node_id()
            << " has no applied logs since last snapshot, "
            << " last_snapshot_index " << saved_last_snapshot_index
            << " last_snapshot_term " << saved_last_snapshot_term
            << ", will clear bufferred log and return success";
        _log_manager->clear_bufferred_logs();
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
    SnapshotReader* reader = _snapshot_storage->open();
    if (reader == NULL) {
        return 0;
    }
    if (reader->load_meta(&_loading_snapshot_meta) != 0) {
        LOG(ERROR) << "Fail to load meta from `" << options.uri << "'";
        _snapshot_storage->close(reader);
        return -1;
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
    interrupt_downloading_snapshot(saved_term);
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
