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
                caller->do_committed(max_committed_index);
                max_committed_index = -1;
            }
            switch (iter->type) {
            case COMMITTED:
                CHECK(false) << "Impossible";
                break;
            case SNAPSHOT_SAVE:
                caller->do_snapshot_save((SaveSnapshotClosure*)iter->done);
                break;
            case SNAPSHOT_LOAD:
                caller->do_snapshot_load((LoadSnapshotClosure*)iter->done);
                break;
            case LEADER_STOP:
                CHECK(!iter->done);
                caller->do_leader_stop();
                break;
            };
        }
    }
    if (max_committed_index >= 0) {
        caller->do_committed(max_committed_index);
    }
    return 0;
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
    bthread::ExecutionQueueOptions execq_opt;
    // TODO: It should be a options 
    execq_opt.max_tasks_size = 256;

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


// Hole a batch of tasks to apply in temporary storage and apply them when
// the storage is full
class TaskBatcher {
public:
    TaskBatcher(StateMachine* sm, Task* data, LogEntry** entry_store,
                       size_t max_size, int64_t first_index)
        : _sm(sm)
        , _data(data)
        , _entry_store(entry_store)
        , _max_size(max_size)
        , _first_index(first_index)
        , _num_task(0) {}

    ~TaskBatcher() {
        apply_task();
    }

    void append(LogEntry* entry, Closure* done) {
        if (entry->type == ENTRY_TYPE_DATA) {  // fast path
            if (full()) {
                apply_task();
            }
            _entry_store[_num_task] = entry;
            _data[_num_task].data = &entry->data;
            _data[_num_task].done = done;
            ++_num_task;
        } else {
            apply_task();
            entry->Release();
            if (done) {
                done->Run();
            }
            _first_index = entry->id.index + 1;
        }
    }

    void apply_task() {
        if (_num_task > 0) {
            _sm->on_apply_in_batch(_first_index, _data, _num_task);
            for (size_t i = 0; i < _num_task; ++i) {
                _entry_store[i]->Release();
            }
            _first_index += _num_task;
            _num_task = 0;
        }
    }

private:
    bool full() const { return _num_task == _max_size; }

    StateMachine* _sm;
    Task* _data;
    LogEntry** _entry_store;
    size_t _max_size;
    int64_t _first_index;
    size_t _num_task;
};

void FSMCaller::do_committed(int64_t committed_index) {
    int64_t last_applied_index = _last_applied_index.load(boost::memory_order_relaxed);

    // We can tolerate the disorder of committed_index
    if (last_applied_index >= committed_index) {
        return;
    }
    std::vector<Closure*> closure;
    int64_t first_closure_index = 0;
    CHECK_EQ(0, _closure_queue->pop_closure_until(committed_index, &closure,
                                                  &first_closure_index));
    Task tasks_store[64];
    LogEntry* entry_store[64];
    int64_t last_applied_term = 0;
    TaskBatcher tb(_fsm, tasks_store, entry_store, ARRAY_SIZE(tasks_store),
                  last_applied_index + 1);
    for (int64_t index = last_applied_index + 1; index <= committed_index; ++index) {
        LogEntry *entry = _log_manager->get_entry(index);
        CHECK(entry);
        CHECK(index == entry->id.index);
        Closure* done = NULL;
        if (index >= first_closure_index) {
            done = closure[index - first_closure_index];
        }
        tb.append(entry, done);
        last_applied_term = entry->id.term;
    }
    tb.apply_task();
    if (last_applied_term > 0) {
        _last_applied_term = last_applied_term;
    }
    LogId last_applied_id;
    last_applied_id.index = committed_index;
    last_applied_id.term = last_applied_term;
    _last_applied_index.store(committed_index, boost::memory_order_release);
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
    meta.last_included_index = last_applied_index;
    meta.last_included_term = _last_applied_term;
    ConfigurationPair conf_pair;
    _log_manager->get_configuration(last_applied_index, &conf_pair);
    meta.last_configuration = conf_pair.second;

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
        return;
    }

    ret = _fsm->on_snapshot_load(reader);
    if (ret != 0) {
        done->status().set_error(ret, "StateMachine on_snapshot_load failed");
        done->Run();
        return;
    }

    _last_applied_index.store(meta.last_included_index, boost::memory_order_release);
    _last_applied_term = meta.last_included_term;
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

void FSMCaller::describe(std::ostream &/*os*/, bool /*use_html*/) {
}

}  // namespace raft
