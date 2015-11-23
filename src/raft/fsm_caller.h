// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/20 23:22:45

#ifndef  PUBLIC_RAFT_FSM_CALLER_H
#define  PUBLIC_RAFT_FSM_CALLER_H

#include <base/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <bthread.h>
#include <bthread/execution_queue.h>
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

class InstallSnapshotDone;
class SaveSnapshotDone;
class BAIDU_CACHELINE_ALIGNMENT FSMCaller {
public:
    FSMCaller();
    virtual ~FSMCaller();
    int init(const FSMCallerOptions &options);
    int shutdown(Closure* done);
    int on_committed(int64_t committed_index, void *context);
    int on_cleared(int64_t log_index, void *context, int error_code);
    int on_snapshot_load(InstallSnapshotDone* done);
    int on_snapshot_save(SaveSnapshotDone* done);
    Closure* on_leader_start();
    int on_leader_stop();
    int64_t last_applied_index() {
        return _last_applied_index.load(boost::memory_order_relaxed);
    }
private:
    static int run(void* meta, google::protobuf::Closure** const tasks[], size_t tasks_size);
    void do_shutdown(Closure* done);
    void do_committed(int64_t committed_index, Closure* done);
    void do_cleared(int64_t log_index, Closure* done, int error_code);
    void do_snapshot_save(SaveSnapshotDone* done);
    void do_snapshot_load(InstallSnapshotDone* done);
    void do_leader_stop();

    bthread::ExecutionQueueId<google::protobuf::Closure*> _queue;
    NodeImpl *_node;
    LogManager *_log_manager;
    StateMachine *_fsm;
    boost::atomic<int64_t> _last_applied_index;
    int64_t _last_applied_term;
};

};

#endif  //PUBLIC_RAFT_FSM_CALLER_H
