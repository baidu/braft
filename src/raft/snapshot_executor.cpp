// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/25 12:19:19

#include "raft/snapshot_executor.h"
#include "raft/util.h"
#include "raft/node.h"
#include "raft/storage.h"

namespace raft {

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
        , _reader(reader)
        , _has_run(false) {
        CHECK_EQ(0, raft_cond_init(&_cond, NULL));
    }
    ~FirstSnapshotLoadDone() {
        CHECK_EQ(0, raft_cond_destroy(&_cond));
    }

    SnapshotReader* start() { return _reader; }
    void Run() {
        _se->on_snapshot_load_done(_err_code, _err_text);
        BAIDU_SCOPED_LOCK(_mutex);
        _has_run = true;
        raft_cond_signal(&_cond);
    }
    void wait_for_run() {
        BAIDU_SCOPED_LOCK(_mutex);
        while (!_has_run) {
            raft_cond_wait(&_cond, &_mutex.mutex());
        }
    }
    bool failed() const {
        return _err_code != 0;
    }
private:
    SnapshotExecutor* _se;
    SnapshotReader* _reader;
    raft_mutex_t _mutex;
    raft_cond_t _cond;
    bool _has_run;
};

SnapshotExecutor::SnapshotExecutor()
    : _last_snapshot_term(0)
    , _last_snapshot_index(0)
    , _term(0)
    , _version(0)
    , _saving_snapshot(false)
    , _loading_snapshot(false)
    , _snapshot_storage(NULL)
    , _fsm_caller(NULL)
    , _node(NULL)
    , _log_manager(NULL)
    , _downloading_snapshot(NULL)
{
}

SnapshotExecutor::~SnapshotExecutor() {
}

void SnapshotExecutor::do_snapshot(Closure* done) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    // check snapshot install/load
    if (_downloading_snapshot.load(boost::memory_order_relaxed)) {
        lck.unlock();
        if (done) {
            done->set_error(EBUSY, "Is loading another snapshot");
            run_closure_in_bthread(done);
        }
        return;
    }

    // check snapshot saving?
    if (_saving_snapshot) {
        lck.unlock();
        if (done) {
            done->set_error(EBUSY, "Is saving anther snapshot");
            run_closure_in_bthread(done);
        }
        return;
    }
    SnapshotWriter* writer = _snapshot_storage->create();
    if (!writer) {
        lck.unlock();
        if (done) {
            done->set_error(EIO, "Fail to create writer");
            run_closure_in_bthread(done);
        }
        return;
    }
    _saving_snapshot = true;
    SaveSnapshotDone* snapshot_save_done = new SaveSnapshotDone(this, writer, done);
    if (_fsm_caller->on_snapshot_save(snapshot_save_done) != 0) {
        lck.unlock();
        if (done) {
            snapshot_save_done->set_error(EHOSTDOWN, "The raft node is down");
            run_closure_in_bthread(snapshot_save_done);
        }
        return;
    }
}

int SnapshotExecutor::on_snapshot_save_done(
            int error_code, const SnapshotMeta& meta, SnapshotWriter* writer) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    int ret = error_code;
    // InstallSnapshot can break SaveSnapshot, check InstallSnapshot when SaveSnapshot
    // because upstream Snapshot maybe newer than local Snapshot.
    if (ret == 0) {
        if (meta.last_included_index <= _last_snapshot_index) {
            ret = ESTALE;
            LOG_IF(WARNING, _node != NULL) << "node " << _node->node_id()
                << " discard saved snapshot, because has a newer snapshot."
                << " last_included_index " << meta.last_included_index
                << " last_snapshot_index " << _last_snapshot_index;
            writer->set_error(ESTALE, "snapshot is staled, maybe InstallSnapshot when snapshot");
        }
    }

    if (ret == 0) {
        if (writer->save_meta(meta)) {
            LOG(WARNING) << "Fail to save snapshot";    
            ret = EIO;
        }
    }
    if (_snapshot_storage->close(writer) != 0) {
        ret = EIO;
        LOG(WARNING) << "Fail to close writer";
    }
    if (ret == 0) {
        _last_snapshot_index = meta.last_included_index;
        _last_snapshot_term = meta.last_included_term;
        _log_manager->set_snapshot(&meta);
    }

    _saving_snapshot = false;
    return ret;
}

void SnapshotExecutor::on_snapshot_load_done(int error_code, 
                                             const std::string& error_text) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    CHECK(_loading_snapshot);
    DownloadingSnapshot* m = _downloading_snapshot.load(boost::memory_order_relaxed);

    if (error_code == 0) {
        _last_snapshot_index = _loading_snapshot_meta.last_included_index;
        _last_snapshot_term = _loading_snapshot_meta.last_included_term;
        _log_manager->set_snapshot(&_loading_snapshot_meta);
    }
    LOG(INFO) << "node " << _node->node_id() << " snapshot_load_done,"
        << " last_included_index " << _loading_snapshot_meta.last_included_index
        << " last_included_term " << _loading_snapshot_meta.last_included_term
        << " last_configuration " << _loading_snapshot_meta.last_configuration;
    lck.unlock();
    if (_node) {
        // FIXME: race with set_peer, maybe it's fine
        _node->update_configuration_after_installing_snapshot();
    }
    lck.lock();
    _loading_snapshot = false;
    _downloading_snapshot.store(NULL, boost::memory_order_release);
    lck.unlock();
    if (m) {
        // Respond RPC
        if (error_code) {
            m->cntl->SetFailed(error_code, "%s", error_text.c_str());
        } else {
            m->response->set_success(true);
        }
        m->done->Run();
        delete m;
    }
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
    // Must call on_snapshot_load_done to clear _saving_snapshot
    int ret = self->_se->on_snapshot_save_done(
                self->_err_code, self->_meta, self->_writer);
    if (ret != 0 && self->_err_code == 0) {
        self->set_error(ret, "node call on_snapshot_save_done failed");
    }
    //user done, need set error
    if (self->_err_code != 0 && self->_done) {
        self->_done->set_error(self->_err_code, self->_err_text);
    }
    if (self->_done) {
        self->_done->Run();
    }
    return NULL;
}

void SaveSnapshotDone::Run() {
    // Avoid blocking FSMCaller
    // This continuation of snapshot saving is likely running inplace where the
    // on_snapshot_save is called (in the FSMCaller thread) and blocks all the
    // following on_apply. As blocking is not neccessary and the continuation is
    // not important, so we start a bthread to do this.
    bthread_t tid;
    if (bthread_start_urgent(&tid, NULL, continue_run, this) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        continue_run(this);
    }
}

int SnapshotExecutor::init(const SnapshotExecutorOptions& options) {
    if (options.uri.empty()) {
        LOG(ERROR) << "uri is empty()";
        return EINVAL;
    }
    _log_manager = options.log_manager;
    _fsm_caller = options.fsm_caller;
    _node = options.node;

    Storage* storage = find_storage(options.uri);
    if (storage) {
        _snapshot_storage = storage->create_snapshot_storage(
                options.uri);
        if (_snapshot_storage == NULL) {
            LOG(ERROR)  << "Fail to find snapshot storage, uri " << options.uri;
            return EINVAL;
        }
    } else {
        LOG(ERROR)  << "Fail to find snapshot storage, uri " << options.uri;
        return EINVAL;
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
    // Load snapshot ater startup
    FirstSnapshotLoadDone done(this, reader);
    CHECK_EQ(0, _fsm_caller->on_snapshot_load(&done));
    done.wait_for_run();
    _snapshot_storage->close(reader);
    if (done.failed()) {
        LOG(ERROR) << "Fail to load snapshot from " << options.uri;
        return -1;
    }
    return 0;
}

int SnapshotExecutor::parse_install_snapshot_request(
            const InstallSnapshotRequest* request,
            SnapshotMeta* meta) {
    meta->last_included_index = request->last_included_log_index();
    meta->last_included_term = request->last_included_log_term();
    for (int i = 0; i < request->peers_size(); i++) {
        PeerId peer;
        if (0 != peer.parse(request->peers(i))) {
            LOG(WARNING) << "Fail to parse " << request->peers(i);
            return -1;
        }
        meta->last_configuration.add_peer(peer);
    }
    return 0;
}

void SnapshotExecutor::install_snapshot(baidu::rpc::Controller* cntl,
                                        const InstallSnapshotRequest* request,
                                        InstallSnapshotResponse* response,
                                        google::protobuf::Closure* done) {
    int ret = 0;
    baidu::rpc::ClosureGuard done_guard(done);
    SnapshotMeta meta;
    ret = parse_install_snapshot_request(request, &meta);
    if (ret != 0) {
        cntl->SetFailed(baidu::rpc::EREQUEST,
                        "Fail to parse request");
        return;
    }
    std::unique_ptr<DownloadingSnapshot> ds(new DownloadingSnapshot);
    ds->cntl = cntl;
    ds->done = done;
    ds->response = response;
    ds->request = request;
    const std::string saved_uri = request->uri();
    int64_t saved_version = 0;
    ret = register_downloading_snapshot(ds.get(), &saved_version);
    //    ^^^ DON'T access request, response, done and cntl after this point
    //        as the retry snapshot will replace this one.
    if (ret != 0) {
        // This RPC will be responded by the previous session
        if (ret > 0) {
            done_guard.release();
        }
        return;
    }
    // Release done first as this RPC might be replaced by the retry one
    done_guard.release();

    // Copy snapshot from remote
    SnapshotWriter* writer = NULL;
    writer = _snapshot_storage->create();
    if (NULL == writer) {
        ret = EINVAL;
    } else {
        ret = writer->copy(request->uri());
    }
    return load_downloading_snapshot(ds.release(), meta, saved_version, writer);
}

void SnapshotExecutor::load_downloading_snapshot(DownloadingSnapshot* ds,
                                                const SnapshotMeta& meta,
                                                int64_t expected_version,
                                                SnapshotWriter* writer) {
    std::unique_ptr<DownloadingSnapshot> ds_guard(ds);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (expected_version != _version) {
        // The RPC was responded where the version changed
        if (writer) {
            writer->set_error(EINVAL, "Out of date");
            _snapshot_storage->close(writer);
        }
        return;
    }
    CHECK_EQ(ds, _downloading_snapshot.load(boost::memory_order_relaxed));
    baidu::rpc::ClosureGuard done_guard(ds->done);
    if (writer == NULL) {
        _downloading_snapshot.store(NULL, boost::memory_order_release);
        lck.unlock();
        ds->cntl->SetFailed(baidu::rpc::EINTERNAL, 
                           "Fail to create snapshot writer");
        return;
    }
    int ret = writer->save_meta(meta);
    if (ret == 0) {
        ret = _snapshot_storage->close(writer);
    } else {
        // Don't care close if failing to save meta
        _snapshot_storage->close(writer);
    }
    if (ret) {
        _downloading_snapshot.store(NULL, boost::memory_order_release);
        lck.unlock();
        ds->cntl->SetFailed(baidu::rpc::EINTERNAL, 
                           "Fail to close _snapshot_storage");
        return;
    }
    // Open the recently closed writer
    SnapshotReader* reader = _snapshot_storage->open();
    if (reader == NULL) {
        _downloading_snapshot.store(NULL, boost::memory_order_release);
        lck.unlock();
        LOG(ERROR) << "Fail to open recently closed snapshot";
        ds->cntl->SetFailed(baidu::rpc::EINTERNAL, 
                            "Fail to open snapshot to read");
        return;
    }
    // TODO: check if reader is the same as writer.
 
    // The owner of ds is on_snapshot_load_done
    ds_guard.release();
    done_guard.release();
    _loading_snapshot = true;
    //                ^ After this point, this installing cannot be interrupted
    _loading_snapshot_meta = meta;
    InstallSnapshotDone* install_snapshot_done =
            new InstallSnapshotDone(this, reader);
    ret = _fsm_caller->on_snapshot_load(install_snapshot_done);
    lck.unlock();
    if (ret != 0) {
        LOG(WARNING) << "Fail to call on_snapshot_load";
        install_snapshot_done->set_error(EHOSTDOWN, "This raft node is down");
        return install_snapshot_done->Run();
    }
}

int SnapshotExecutor::register_downloading_snapshot(DownloadingSnapshot* ds,
                                                   int64_t* saved_version) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (ds->request->term() != _term) {
        ds->response->set_success(false);
        ds->response->set_term(_term);
        return -1;
    }
    if (ds->request->last_included_log_index() <= _last_snapshot_index) {
        ds->response->set_term(_term);
        ds->response->set_success(true);
        return -1;
    }
    ds->response->set_term(_term);
    if (_saving_snapshot) {
        ds->cntl->SetFailed(EBUSY, "Is saving snapshot");
        return -1;
    }
    DownloadingSnapshot* m = _downloading_snapshot.load(
            boost::memory_order_relaxed);
    if (!m) {
        *saved_version = _version;
        _downloading_snapshot.store(ds, boost::memory_order_relaxed);
        return 0;
    }
    int rc = 0;
    DownloadingSnapshot saved;
    bool has_saved = false;
    // A previouse snapshot is under installing, check if this is the same
    // snapshot and resume it, otherwise drop previous snapshot as this is a
    // new one
    if (m->request->last_included_log_index() 
            == ds->request->last_included_log_index()) {
        // m is a retry
        has_saved = true;
        // Copy |*ds| to |*m| so that the former session would respond
        // this RPC.
        saved = *m;
        *m = *ds;
        rc = 1;
    } else if (m->request->last_included_log_index() 
            > ds->request->last_included_log_index()) {
        // |is| is older
        ds->cntl->SetFailed(EINVAL, "A newer snapshot is under installing");
        return -1;
    } else {
        // |is| is newer
        if (_loading_snapshot) {
            // We can't interrupt the loading one
            ds->cntl->SetFailed(EBUSY, "A former snapshot is under loading");
            return -1;
        }
        // Copy |*ds| to |*m| so that the fomer downloading thread would respond
        // this RPC.
        has_saved = true;
        saved = *m;
        *m = *ds;
        *saved_version = ++_version;
    }
    lck.unlock();
    if (has_saved) {
        // Respond replaced session
        saved.cntl->SetFailed(EINTR, "Interrupted by another snapshot");
        saved.done->Run();
    }
    return rc;
}

void SnapshotExecutor::interrupt_downloading_snapshot(int64_t new_term) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    CHECK_GE(new_term, _term);
    _term = new_term;
    ++_version;
    if (!_downloading_snapshot.load(boost::memory_order_relaxed)) {
        return;
    }
    if (_loading_snapshot) {
        // We can't interrupt loading
        return;
    }
    DownloadingSnapshot saved = 
            *_downloading_snapshot.load(boost::memory_order_relaxed);
    _downloading_snapshot.store(NULL, boost::memory_order_release);
    lck.unlock();
    saved.response->set_term(new_term);
    saved.response->set_success(false);
    saved.done->Run();
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
            _downloading_snapshot.load(boost::memory_order_acquire);
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
        os << "snapshot_from: " << request.uri();
        os << "loading_snapshot_index: " << meta.last_included_index << newline;
        os << "loading_snapshot_term: " << meta.last_included_term << newline;
        os << "loading_snapshot_configuration: " << meta.last_configuration << newline;
    } else if (m) {
        CHECK(!is_saving_snapshot);
        parse_install_snapshot_request(&request, &meta);
        os << "snapshot_status: DOWNLOADING" << newline;
        os << "downloading_snapshot_index: " << meta.last_included_index << newline;
        os << "downloading_snapshot_term: " << meta.last_included_term << newline;
        os << "downloading_snapshot_configuration: " << meta.last_configuration << newline;
        os << "downloading_snapshot_from: " << request.uri() << newline;
    } else if (is_saving_snapshot) {
        os << "snapshot_status: SAVING" << newline;
    } else {
        os << "snapshot_status: IDLE" << newline;
    }
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
    _se->on_snapshot_load_done(_err_code, _err_text);
    delete this;
}

}  // namespace raft
