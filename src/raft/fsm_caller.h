// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/20 23:22:45

#ifndef  PUBLIC_RAFT_FSM_CALLER_H
#define  PUBLIC_RAFT_FSM_CALLER_H

#include <base/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <base/containers/linked_list.h>        // base::LinkNode
#include <bthread.h>
#include "raft/commitment_manager.h"

namespace raft {

class NodeImpl;
class LogManager;
class StateMachine;

struct FSMCallerOptions {
    FSMCallerOptions() {}
    int64_t last_applied_index;
    NodeImpl* node;
    LogManager *log_manager;
    StateMachine *fsm;
};

class BAIDU_CACHELINE_ALIGNMENT FSMCaller : public CommitmentWaiter {
public:
    FSMCaller();
    virtual ~FSMCaller();
    int init(const FSMCallerOptions &options);
    int on_committed(int64_t committed_index, void *context);
    int on_cleared(int64_t log_index, void *context, int error_code);
private:

    struct ContextWithIndex : public base::LinkNode<ContextWithIndex> {
        void *context;
        int64_t log_index;
    };

    bool more_tasks(ContextWithIndex **head, int64_t* last_committed_index);

    static void* call_user_fsm(void* arg);
    static void* call_cleared_cb(void* arg);


    NodeImpl *_node;
    LogManager *_log_manager;
    StateMachine *_fsm;
    bthread_mutex_t _mutex;
    int64_t _last_applied_index;
    int64_t _last_committed_index;
    ContextWithIndex *_head;
    bool _processing;
};

};

#endif  //PUBLIC_RAFT_FSM_CALLER_H
