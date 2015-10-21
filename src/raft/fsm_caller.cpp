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

namespace raft {

FSMCaller::FSMCaller()
    : _log_manager(NULL)
    , _node_user(NULL)
    , _last_applied_index(0)
    , _last_committed_index(0)
    , _head(NULL)
    , _processing(false) 
{
    CHECK_EQ(0, bthread_mutex_init(&_mutex, NULL));
}

FSMCaller::~FSMCaller() {
    bthread_mutex_destroy(&_mutex);
}

int FSMCaller::init(const FSMCallerOptions &options) {
    if (options.log_manager == NULL || options.node_user == NULL
            || options.last_applied_index < 0) {
        return -1;
    }
    _log_manager = options.log_manager;
    _node_user = options.node_user;
    _last_applied_index = options.last_applied_index;
    _last_committed_index = options.last_applied_index;
    _head = NULL;
    _processing = false;
    return 0;
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
                if (entry != NULL) {
                    switch (entry->type) {
                    case ENTRY_TYPE_DATA:
                        caller->_node_user->apply(*entry, (base::Closure*)context);
                        break;
                    case ENTRY_TYPE_ADD_PEER:
                    case ENTRY_TYPE_REMOVE_PEER:
                        // TODO(wangyao02): Notify that configuration change is
                        // successfully executed
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

int on_cleared(int64_t /*log_index*/, void* /*context*/, int /*error_code*/) {
    // TODO:  handle failure;
    LOG(WARNING) << "Not implemented yet";
    return 0;
}

}  // namespace raft
