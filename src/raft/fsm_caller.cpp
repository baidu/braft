// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/20 23:45:17

#include "raft/fsm_caller.h"

#include <base/logging.h>
#include "raft/raft.h"
#include "raft/log_manager.h"
#include "raft/node.h"
#include "raft/util.h"
#include "raft/raft.pb.h"
#include "raft/log_entry.h"

namespace raft {

FSMCaller::FSMCaller()
    : _node(NULL)
    , _log_manager(NULL)
    , _fsm(NULL)
    , _last_applied_index(0)
    , _last_committed_index(0)
    , _head(NULL)
    , _processing(false)
    , _state(NORMAL)
    , _shutdown_done(NULL)
{
    CHECK_EQ(0, bthread_mutex_init(&_mutex, NULL));
}

FSMCaller::~FSMCaller() {
    bthread_mutex_destroy(&_mutex);
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
    _last_committed_index = options.last_applied_index;
    _head = NULL;
    _processing = false;

    // bind lifecycle with node, AddRef
    _node->AddRef();
    return 0;
}

void FSMCaller::shutdown(Closure* done) {
    BAIDU_SCOPED_LOCK(_mutex);
    _state = SHUTINGDOWN;
    _shutdown_done = done;

    // check and start thread
    if (!_processing) {
        bthread_t tid;
        if (bthread_start_urgent(&tid, &BTHREAD_ATTR_NORMAL/*FIXME*/,
                                 call_user_fsm, this) != 0) {
            LOG(ERROR) << "Fail to start bthread, " << berror();
            call_user_fsm(this);
        }
    }
}

bool FSMCaller::should_shutdown(Closure** done) {
    //TODO: cas optimize
    BAIDU_SCOPED_LOCK(_mutex);
    // only one return true when reentrant
    bool ok = (_state == SHUTINGDOWN);
    if (ok) {
        _state = SHUTDOWN;
        *done = _shutdown_done;
        _shutdown_done = NULL;
    }
    return ok;
}

bool FSMCaller::more_tasks(ContextWithIndex **head,
                           int64_t *last_committed_index) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_processing);
    if (_last_applied_index != _last_committed_index) {
        *last_committed_index = _last_committed_index;
        *head = _head;
        _head = NULL;
        return true;
    }
    _processing = false;
    return false;
}

void* FSMCaller::call_user_fsm(void* arg) {
    FSMCaller* caller = (FSMCaller*)arg;
    ContextWithIndex *head = NULL;
    int64_t last_committed_index;
    while (caller->more_tasks(&head, &last_committed_index)) {
        int64_t next_applied_index = caller->_last_applied_index + 1;
        ContextWithIndex *next_log_with_context = head;
        while (next_applied_index <= last_committed_index) {
            void *context = NULL;
            if (next_log_with_context != NULL
                    && next_log_with_context->log_index == next_applied_index) {
                context = next_log_with_context->context;
                base::LinkNode<ContextWithIndex>* next = next_log_with_context->next();
                delete next_log_with_context;
                next_log_with_context = 
                        next != (base::LinkNode<ContextWithIndex>*)head 
                        ? next->value() : NULL;
            }
            while (true/*FIXME*/) {
                LogEntry *entry = caller->_log_manager->get_entry(next_applied_index);
                Closure* done = (Closure*)context;
                if (entry != NULL) {
                    switch (entry->type) {
                    case ENTRY_TYPE_DATA:
                        CHECK(next_applied_index == entry->index);
                        caller->_fsm->on_apply(entry->data, next_applied_index,
                                               done);
                        if (done) {
                            done->Run();
                        }
                        break;
                    case ENTRY_TYPE_ADD_PEER:
                    case ENTRY_TYPE_REMOVE_PEER:
                        // Notify that configuration change is successfully executed
                        caller->_node->on_configuration_change_done(entry->type, *(entry->peers));
                        if (done) {
                            done->Run();
                        }
                        break;
                    case ENTRY_TYPE_NO_OP:
                        break;
                    default:
                        CHECK(false) << "Unknown entry type" << entry->type;
                    }
                    entry->Release();

                    break;
                } else {
                    CHECK(false) << "committed_index=" << last_committed_index 
                                 << " is larger than last_log_index="
                                 << caller->_log_manager->last_log_index();
                    if (caller->_log_manager->wait(next_applied_index - 1, NULL) != 0) {
                        LOG(WARNING) << "Fail to wait _log_manager";
                    }
                }
            }
            ++next_applied_index;
        }
        caller->_last_applied_index = next_applied_index - 1;
    }

    // check should shutdown
    Closure* shutdown_done = NULL;
    if (caller->should_shutdown(&shutdown_done)) {
        caller->_fsm->on_shutdown();
        if (shutdown_done) {
            shutdown_done->Run();
        }
        // bind lifecycle with node, Release
        caller->_node->Release();
    }
    return NULL;
}

int FSMCaller::on_committed(int64_t committed_index, void *context) {
    ContextWithIndex *node = NULL;
    if (context != NULL) {
        node = new ContextWithIndex();
        node->log_index = committed_index;
        node->context = context;
    }
    bool start_bthread = false;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_state != NORMAL) {
            CHECK(context == NULL);
            return 0;
        }
        if (committed_index <= _last_committed_index) {
            CHECK(false) << "committed_index should be monotonic,"
                         << " actually committed=" << committed_index
                         << " last_committed_index=" << _last_committed_index;
            return -1;
        }
        _last_committed_index = committed_index;
        if (node != NULL) {
            if (_head == NULL) {
                _head = node;
            } else {
                node->InsertBefore(_head);
            }
        }
        if (!_processing) {
            _processing = true;
            start_bthread = true;
        }
    }  // out of the critical section
    if (start_bthread) {
        bthread_t tid;
        if (bthread_start_urgent(&tid, &BTHREAD_ATTR_NORMAL/*FIXME*/,
                                 call_user_fsm, this) != 0) {
            LOG(ERROR) << "Fail to start bthread, " << berror();
            call_user_fsm(this);
            return 0;
        }
    }
    return 0;
}

void* FSMCaller::call_cleared_cb(void* arg) {
    Closure* done = (Closure*) arg;
    done->Run();
    return NULL;
}

int FSMCaller::on_cleared(int64_t log_index, void* context, int error_code) {
    // handle failure, call apply/add_peer/remove_peer closure.
    // here NOT need notify configuration change failed, on_cleared triggered by step_down.
    // step_down will clear configuration change.
    //
    // TODO: need order with call_user_fsm?
    // TODO: need node AddRef?
    bthread_t tid;
    Closure* done = (Closure*) context;
    done->set_error(error_code, "leader stepdown, may majority die");
    if (bthread_start_urgent(&tid, &BTHREAD_ATTR_NORMAL/*FIXME*/,
                             call_cleared_cb, context) != 0) {
        LOG(ERROR) << "Fail to start bthread, " << berror();
        call_cleared_cb(context);
    }
    return 0;
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

void* FSMCaller::call_leader_stop_cb(void* arg) {
    FSMCaller* caller = (FSMCaller*)arg;
    caller->_fsm->on_leader_stop();
    caller->_node->Release();
    return NULL;
}

int FSMCaller::on_leader_stop() {
    {
        BAIDU_SCOPED_LOCK(_mutex);
        // avoid on_leader_stop callback after shutdown
        if (_state != NORMAL) {
            return EINVAL;
        }
    }
    // TODO: need order with call_user_fsm?
    _node->AddRef();
    bthread_t tid;
    if (bthread_start_urgent(&tid, &BTHREAD_ATTR_NORMAL/*FIXME*/,
                             call_leader_stop_cb, this) != 0) {
        LOG(ERROR) << "Fail to start bthread, " << berror();
        call_leader_stop_cb(this);
    }
    return 0;
}

}  // namespace raft
