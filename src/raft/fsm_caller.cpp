// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/20 23:45:17

#include <base/logging.h>
#include "raft/raft.h"
#include "raft/log_manager.h"
#include "raft/node.h"
#include "raft/util.h"
#include "raft/raft.pb.h"
#include "raft/log_entry.h"
#include "raft/errno.pb.h"
#include "raft/node.h"

#include "raft/fsm_caller.h"
#include <bthread_unstable.h>

namespace raft {

FSMCaller::FSMCaller()
    : _log_manager(NULL)
    , _fsm(NULL)
    , _closure_queue(NULL)
    , _last_applied_index(0)
    , _last_applied_term(0)
    , _after_shutdown(NULL)
    , _node(NULL)
    , _cur_task(IDLE)
    , _applying_index(0)
{
}

FSMCaller::~FSMCaller() {
    CHECK(_after_shutdown == NULL);
}

int FSMCaller::run(void* meta, bthread::TaskIterator<ApplyTask>& iter) {
    FSMCaller* caller = (FSMCaller*)meta;
    if (iter.is_queue_stopped()) {
        caller->do_shutdown();
        return 0;
    }
    int64_t max_committed_index = -1;
    for (; iter; ++iter) {
        if (iter->type == COMMITTED) {
            if (iter->committed_index > max_committed_index) {
                max_committed_index = iter->committed_index;
            }
        } else {
            if (max_committed_index >= 0) {
                caller->_cur_task = COMMITTED;
                caller->do_committed(max_committed_index);
                max_committed_index = -1;
            }
            switch (iter->type) {
            case COMMITTED:
                CHECK(false) << "Impossible";
                break;
            case SNAPSHOT_SAVE:
                caller->_cur_task = SNAPSHOT_SAVE;
                if (caller->pass_by_status(iter->done)) {
                    caller->do_snapshot_save((SaveSnapshotClosure*)iter->done);
                }
                break;
            case SNAPSHOT_LOAD:
                caller->_cur_task = SNAPSHOT_LOAD;
                // TODO: do we need to allow the snapshot loading to recover the
                // StateMachine if possible?
                if (caller->pass_by_status(iter->done)) {
                    caller->do_snapshot_load((LoadSnapshotClosure*)iter->done);
                }
                break;
            case LEADER_STOP:
                caller->_cur_task = LEADER_STOP;
                CHECK(!iter->done);
                caller->do_leader_stop();
                break;
            case ERROR:
                caller->_cur_task = ERROR;
                caller->do_on_error((OnErrorClousre*)iter->done);
                break;
            case IDLE:
                CHECK(false) << "Can't reach here";
                break;
            };
        }
    }
    if (max_committed_index >= 0) {
        caller->_cur_task = COMMITTED;
        caller->do_committed(max_committed_index);
    }
    caller->_cur_task = IDLE;
    return 0;
}

bool FSMCaller::pass_by_status(Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    if (!_error.status().ok()) {
        if (done) {
            done->status().set_error(
                        EINVAL, "FSMCaller is in bad status=`%s'",
                                _error.status().error_cstr());
        }
        return false;
    }
    done_guard.release();
    return true;
}

int FSMCaller::init(const FSMCallerOptions &options) {
    if (options.log_manager == NULL || options.fsm == NULL 
            || options.closure_queue == NULL) {
        return EINVAL;
    }
    _log_manager = options.log_manager;
    _fsm = options.fsm;
    _closure_queue = options.closure_queue;
    _after_shutdown = options.after_shutdown;
    _node = options.node;
    if (_node) {
        _node->AddRef();
    }
    
    bthread::ExecutionQueueOptions execq_opt;
    execq_opt.bthread_attr = options.usercode_in_pthread 
                             ? BTHREAD_ATTR_PTHREAD
                             : BTHREAD_ATTR_NORMAL;
    bthread::execution_queue_start(&_queue_id,
                                   &execq_opt,
                                   FSMCaller::run,
                                   this);
    return 0;
}

int FSMCaller::shutdown() {
    return bthread::execution_queue_stop(_queue_id);
}

void FSMCaller::do_shutdown() {
    if (_node) {
        _node->Release();
        _node = NULL;
    }
    _fsm->on_shutdown();
    if (_after_shutdown) {
        google::protobuf::Closure* saved_done = _after_shutdown;
        _after_shutdown = NULL;
        // after this point, |this| is likely to be destroyed, don't touch
        // anything
        saved_done->Run();
    }
}

int FSMCaller::on_committed(int64_t committed_index) {
    ApplyTask t;
    t.type = COMMITTED;
    t.committed_index = committed_index;
    return bthread::execution_queue_execute(_queue_id, t);
}

class OnErrorClousre : public Closure {
public:
    OnErrorClousre(const Error& e) : _e(e) {
    }
    const Error& error() { return _e; }
    void Run() {
        delete this;
    }
private:
    ~OnErrorClousre() {}
    Error _e;
};

int FSMCaller::on_error(const Error& e) {
    OnErrorClousre* c = new OnErrorClousre(e);
    ApplyTask t;
    t.type = ERROR;
    t.done = c;
    if (bthread::execution_queue_execute(_queue_id, t, 
                                         &bthread::TASK_OPTIONS_URGENT) != 0) {
        c->Run();
        return -1;
    }
    return 0;
}

void FSMCaller::do_on_error(OnErrorClousre* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    set_error(done->error());
}

void FSMCaller::set_error(const Error& e) {
    if (_error.type() != ERROR_TYPE_NONE) {
        return;
    }
    _error = e;
    _fsm->on_error(_error);
    if (_node) {
        _node->on_error(_error);
    }
}

void FSMCaller::do_committed(int64_t committed_index) {
    if (!_error.status().ok()) {
        return;
    }
    int64_t last_applied_index = _last_applied_index.load(boost::memory_order_relaxed);

    // We can tolerate the disorder of committed_index
    if (last_applied_index >= committed_index) {
        return;
    }
    std::vector<Closure*> closure;
    int64_t first_closure_index = 0;
    CHECK_EQ(0, _closure_queue->pop_closure_until(committed_index, &closure,
                                                  &first_closure_index));

    IteratorImpl iter_impl(_fsm, _log_manager, &closure, first_closure_index,
                 last_applied_index, committed_index, &_applying_index);
    for (; iter_impl.is_good();) {
        if (iter_impl.entry()->type != ENTRY_TYPE_DATA) {
            // For other entries, we have nothing to do besides flush the
            // pending tasks and run this closure to notify the caller that the
            // entries before this one were successfully committed and applied.
            if (iter_impl.done()) {
                iter_impl.done()->Run();
            }
            iter_impl.next();
            continue;
        }
        Iterator iter(&iter_impl);
        _fsm->on_apply(iter);
        LOG_IF(ERROR, iter.valid()) 
                << "Iterator is still valid, did you return before iterator "
                   " reached the end?";
        // Try move to next in case that we pass the same log twice.
        iter.next();
    }
    if (iter_impl.has_error()) {
        set_error(iter_impl.error());
        iter_impl.run_the_rest_closure_with_error();
    }
    const int64_t last_index = iter_impl.index() - 1;
    const int64_t last_term = _log_manager->get_term(last_index);
    LogId last_applied_id(last_index, last_term);
    _last_applied_index.store(committed_index, boost::memory_order_release);
    _last_applied_term = last_term;
    _log_manager->set_applied_id(last_applied_id);
}

int FSMCaller::on_snapshot_save(SaveSnapshotClosure* done) {
    ApplyTask task;
    task.type = SNAPSHOT_SAVE;
    task.done = done;
    return bthread::execution_queue_execute(_queue_id, task);
}

void FSMCaller::do_snapshot_save(SaveSnapshotClosure* done) {
    CHECK(done);

    int64_t last_applied_index = _last_applied_index.load(boost::memory_order_relaxed);

    SnapshotMeta meta;
    meta.set_last_included_index(last_applied_index);
    meta.set_last_included_term(_last_applied_term);
    ConfigurationPair conf_pair;
    _log_manager->get_configuration(last_applied_index, &conf_pair);
    std::vector<PeerId> peers;
    conf_pair.second.list_peers(&peers);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    SnapshotWriter* writer = done->start(meta);
    if (!writer) {
        done->status().set_error(EINVAL, "snapshot_storage create SnapshotWriter failed");
        done->Run();
        return;
    }

    _fsm->on_snapshot_save(writer, done);
    return;
}

int FSMCaller::on_snapshot_load(LoadSnapshotClosure* done) {
    ApplyTask task;
    task.type = SNAPSHOT_LOAD;
    task.done = done;
    return bthread::execution_queue_execute(_queue_id, task);
}

void FSMCaller::do_snapshot_load(LoadSnapshotClosure* done) {
    //TODO done_guard
    SnapshotReader* reader = done->start();
    if (!reader) {
        done->status().set_error(EINVAL, "open SnapshotReader failed");
        done->Run();
        return;
    }

    SnapshotMeta meta;
    int ret = reader->load_meta(&meta);
    if (0 != ret) {
        done->status().set_error(ret, "SnapshotReader load_meta failed.");
        done->Run();
        if (ret == EIO) {
            Error e;
            e.set_type(ERROR_TYPE_SNAPSHOT);
            e.status().set_error(ret, "Fail to load snapshot meta");
            set_error(e);
        }
        return;
    }

    ret = _fsm->on_snapshot_load(reader);
    if (ret != 0) {
        done->status().set_error(ret, "StateMachine on_snapshot_load failed");
        done->Run();
        Error e;
        e.set_type(ERROR_TYPE_STATE_MACHINE);
        e.status().set_error(ret, "StateMachine on_snapshot_load failed");
        set_error(e);
        return;
    }

    _last_applied_index.store(meta.last_included_index(),
                              boost::memory_order_release);
    _last_applied_term = meta.last_included_term();
    done->Run();
}

int FSMCaller::on_leader_stop() {
    ApplyTask task;
    task.type = LEADER_STOP;
    task.done = NULL;
    return bthread::execution_queue_execute(_queue_id, task);
}

void FSMCaller::do_leader_stop() {
    _fsm->on_leader_stop();
}

void FSMCaller::describe(std::ostream &os, bool use_html) {
    const char* newline = (use_html) ? "<br>" : "\n";
    TaskType cur_task = _cur_task;
    const int64_t applying_index = _applying_index.load(
                                    boost::memory_order_relaxed);
    os << "state_machine: ";
    switch (cur_task) {
    case IDLE:
        os << "Idle";
        break;
    case COMMITTED:
        os << "Applying log_index=" << applying_index;
        break;
    case SNAPSHOT_SAVE:
        os << "Saving snapshot";
        break;
    case SNAPSHOT_LOAD:
        os << "Loading snapshot";
        break;
    case ERROR:
        os << "Notifying error";
        break;
    case LEADER_STOP:
        os << "Notifying leader stop";
        break;
    }
    os << newline;
}

void FSMCaller::join() {
    bthread::execution_queue_join(_queue_id);
}

IteratorImpl::IteratorImpl(StateMachine* sm, LogManager* lm,
                          std::vector<Closure*> *closure, 
                          int64_t first_closure_index,
                          int64_t last_applied_index, 
                          int64_t committed_index,
                          boost::atomic<int64_t>* applying_index)
        : _sm(sm)
        , _lm(lm)
        , _closure(closure)
        , _first_closure_index(first_closure_index)
        , _cur_index(last_applied_index)
        , _committed_index(committed_index)
        , _cur_entry(NULL)
        , _applying_index(applying_index)
{ next(); }

void IteratorImpl::next() {
    if (_cur_entry) {
        _cur_entry->Release();
        _cur_entry = NULL;
    }
    if (_cur_index <= _committed_index) {
        ++_cur_index;
        if (_cur_index <= _committed_index) {
            _cur_entry = _lm->get_entry(_cur_index);
            if (_cur_entry == NULL) {
                _error.set_type(ERROR_TYPE_LOG);
                _error.status().set_error(-1,
                        "Fail to get entry at index=%ld "
                        "while committed_index=%ld",
                        _cur_index, _committed_index);
            }
            _applying_index->store(_cur_index, boost::memory_order_relaxed);
        }
    }
}

Closure* IteratorImpl::done() const {
    if (_cur_index < _first_closure_index) {
        return NULL;
    }
    return (*_closure)[_cur_index - _first_closure_index];
}

void IteratorImpl::set_error_and_rollback(
            size_t ntail, const base::Status* st) {
    if (ntail == 0) {
        CHECK(false) << "Invalid ntail=" << ntail;
        return;
    }
    if (_cur_entry == NULL || _cur_entry->type != ENTRY_TYPE_DATA) {
        _cur_index -= ntail;
    } else {
        _cur_index -= (ntail - 1);
    }
    if (_cur_entry) {
        _cur_entry->Release();
        _cur_entry = NULL;
    }
    _error.set_type(ERROR_TYPE_STATE_MACHINE);
    _error.status().set_error(ESTATEMACHINE, 
            "StateMachine meet critical error when applying one "
            " or more tasks since index=%ld, %s", _cur_index,
            (st ? st->error_cstr() : "none"));
}

void IteratorImpl::run_the_rest_closure_with_error() {
    for (int64_t i = std::max(_cur_index, _first_closure_index);
            i <= _committed_index; ++i) {
        Closure* done = (*_closure)[i - _first_closure_index];
        if (done) {
            done->status() = _error.status();
            run_closure_in_bthread(done);
        }
    }
}

}  // namespace raft
