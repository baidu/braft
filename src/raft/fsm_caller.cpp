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

namespace raft {

FSMCaller::FSMCaller()
    : _node(NULL)
    , _log_manager(NULL)
    , _fsm(NULL)
    , _last_applied_index(0)
    , _last_applied_term(0)
{
}

FSMCaller::~FSMCaller() {
}

int FSMCaller::run(void* meta, google::protobuf::Closure** const tasks[], size_t tasks_size) {
    // stop
    /*
    FSMCaller* caller = static_cast<FSMCaller*>(meta);
    if (tasks_size == 0) {
        caller->_node->Release();
        return;
    }
    //*/
    // run closure
    for (size_t i = 0; i < tasks_size; i++) {
        google::protobuf::Closure* done = *tasks[i];
        if (done) {
            done->Run();
        }
    }
    return 0;
}

int FSMCaller::init(const FSMCallerOptions &options) {
    if (options.log_manager == NULL || options.fsm == NULL
            || options.last_applied_index < 0) {
        return EINVAL;
    }
    _node = options.node;
    _log_manager = options.log_manager;
    _fsm = options.fsm;
    _last_applied_index = options.last_applied_index;

    bthread::execution_queue_start(&_queue,
                                   NULL, /*queue_options*/
                                   FSMCaller::run,
                                   this);
    // bind lifecycle with node, need AddRef, shutdown is async
    _node->AddRef();
    return 0;
}

int FSMCaller::shutdown(Closure* done) {
    google::protobuf::Closure* new_done =
        google::protobuf::NewCallback(this, &FSMCaller::do_shutdown, done);
    bthread::execution_queue_execute(_queue, new_done);
    // call execution_queue_stop to release some resource
    return bthread::execution_queue_stop(_queue);
}

void FSMCaller::do_shutdown(Closure* done) {
    if (done) {
        done->Run();
    }
    // bind lifecycle with node, need AddRef, shutdown is async
    _node->Release();
}

int FSMCaller::on_committed(int64_t committed_index, void *context) {
    google::protobuf::Closure* done =
        google::protobuf::NewCallback(this, &FSMCaller::do_committed,
                                      committed_index, static_cast<Closure*>(context));
    return bthread::execution_queue_execute(_queue, done);
}

void FSMCaller::do_committed(int64_t committed_index, Closure* done) {
    CHECK((done == NULL && committed_index > _last_applied_index) ||
          (done != NULL && committed_index == (_last_applied_index + 1)));

    for (int64_t index = _last_applied_index + 1; index <= committed_index; ++index) {
        LogEntry *entry = _log_manager->get_entry(index);
        if (entry == NULL) {
            CHECK(done == NULL);
            break;
        }
        CHECK(index == entry->index);

        switch (entry->type) {
        case ENTRY_TYPE_DATA:
            //TODO: use result instead of done
            _fsm->on_apply(entry->data, index, done);
            break;
        case ENTRY_TYPE_ADD_PEER:
        case ENTRY_TYPE_REMOVE_PEER:
            // Notify that configuration change is successfully executed
            _node->on_configuration_change_done(entry->type, *(entry->peers));
            if (done) {
                done->Run();
            }
            break;
        case ENTRY_TYPE_NO_OP:
            break;
        default:
            CHECK(false) << "Unknown entry type" << entry->type;
        }

        _last_applied_index = index;
        _last_applied_term = entry->term;
        // Release: get_entry ref 1, leader append ref quorum
        // leader clear memory when both leader and quorum(inlcude leader or not) stable
        // follower's quorum = 0
        if (1 == entry->Release(entry->quorum + 1)) {
            _log_manager->clear_memory_logs(index);
        }
    }
}

int FSMCaller::on_cleared(int64_t log_index, void* context, int error_code) {
    google::protobuf::Closure* done =
        google::protobuf::NewCallback(this, &FSMCaller::do_cleared,
                                      log_index, static_cast<Closure*>(context), error_code);
    return bthread::execution_queue_execute(_queue, done);
}

void FSMCaller::do_cleared(int64_t log_index, Closure* done, int error_code) {
    log_index = log_index; // no used
    CHECK(done);
    done->set_error(error_code, "operation failed");
    done->Run();
}

int FSMCaller::on_snapshot_save(SaveSnapshotDone* done) {
    google::protobuf::Closure* new_done =
        google::protobuf::NewCallback(this, &FSMCaller::do_snapshot_save, done);
    return bthread::execution_queue_execute(_queue, new_done);
}

void FSMCaller::do_snapshot_save(SaveSnapshotDone* done) {
    CHECK(done);

    SnapshotMeta meta;
    meta.last_included_index = _last_applied_index;
    meta.last_included_term = _last_applied_term;
    ConfigurationPair pair = _log_manager->get_configuration(_last_applied_index);
    CHECK(pair.first > 0);
    meta.last_configuration = pair.second;

    SnapshotWriter* writer = done->start(meta);
    if (!writer) {
        done->set_error(EINVAL, "snapshot_storage create SnapshotWriter failed");
        done->Run();
        return;
    }

    int ret = _fsm->on_snapshot_save(writer, done);
    if (ret != 0) {
        done->set_error(ret, "StateMachine on_snapshot_save failed");
        done->Run();
        return;
    }

    return;
}

int FSMCaller::on_snapshot_load(InstallSnapshotDone* done) {
    google::protobuf::Closure* new_done =
        google::protobuf::NewCallback(this, &FSMCaller::do_snapshot_load, done);
    return bthread::execution_queue_execute(_queue, new_done);
}

void FSMCaller::do_snapshot_load(InstallSnapshotDone* done) {
    //TODO done_guard
    SnapshotReader* reader = done->start();
    if (!reader) {
        done->set_error(EINVAL, "open SnapshotReader failed");
        done->Run();
        return;
    }

    int ret = _fsm->on_snapshot_load(reader);
    if (ret != 0) {
        done->set_error(ret, "StateMachine on_snapshot_load failed");
        done->Run();
        return;
    }

    done->Run();
}

class LeaderStartClosure : public Closure {
public:
    LeaderStartClosure(StateMachine* fsm) : _fsm(fsm) {}
    void Run() {
        if (_err_code == 0) {
            _fsm->on_leader_start();
        }
        delete this;
    }
    StateMachine* _fsm;
};

Closure* FSMCaller::on_leader_start() {
    return new LeaderStartClosure(_fsm);
}

int FSMCaller::on_leader_stop() {
    google::protobuf::Closure* done =
        google::protobuf::NewCallback(this, &FSMCaller::do_leader_stop);
    return bthread::execution_queue_execute(_queue, done);
}

void FSMCaller::do_leader_stop() {
    _fsm->on_leader_stop();
}

}  // namespace raft
