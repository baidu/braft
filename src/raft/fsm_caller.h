// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/20 23:22:45

#ifndef  PUBLIC_RAFT_FSM_CALLER_H
#define  PUBLIC_RAFT_FSM_CALLER_H

#include <base/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <bthread.h>
#include <bthread/execution_queue.h>
#include "raft/commitment_manager.h"
#include "raft/closure_queue.h"
#include "raft/macros.h"

namespace raft {

class NodeImpl;
class LogManager;
class StateMachine;
class SnapshotMeta;

struct FSMCallerOptions {
    FSMCallerOptions() 
        : log_manager(NULL)
        , fsm(NULL)
        , after_shutdown(NULL)
        , closure_queue(NULL)
    {}
    LogManager *log_manager;
    StateMachine *fsm;
    google::protobuf::Closure* after_shutdown;
    ClosureQueue* closure_queue;
};

class SaveSnapshotClosure : public Closure {
public:
    // TODO: comments
    virtual SnapshotWriter* start(const SnapshotMeta& meta) = 0;
};

class LoadSnapshotClosure : public Closure {
public:
    // TODO: comments
    virtual SnapshotReader* start() = 0;
};

class BAIDU_CACHELINE_ALIGNMENT FSMCaller {
public:
    FSMCaller();
    RAFT_MOCK ~FSMCaller();
    int init(const FSMCallerOptions& options);
    int shutdown();
    RAFT_MOCK int on_committed(int64_t committed_index);
    int on_snapshot_load(LoadSnapshotClosure* done);
    int on_snapshot_save(SaveSnapshotClosure* done);
    //Closure* on_leader_start();
    int on_leader_stop();
    int64_t last_applied_index() const {
        return _last_applied_index.load(boost::memory_order_relaxed);
    }
    void describe(std::ostream& os, bool use_html);
private:

    enum TaskType {
        COMMITTED,
        SNAPSHOT_SAVE,
        SNAPSHOT_LOAD,
        LEADER_STOP,
    };

    struct ApplyTask {
        TaskType type;
        union {
            // For applying log entry (including configuartion change)
            int64_t committed_index;
            
            // For other operation
            raft::Closure* done;
        };
    };

    static double get_cumulated_cpu_time(void* arg);
    static int run(void* meta, bthread::TaskIterator<ApplyTask>& iter);
    void do_shutdown(); //Closure* done);
    void do_committed(int64_t committed_index);
    void do_cleared(int64_t log_index, Closure* done, int error_code);
    void do_snapshot_save(SaveSnapshotClosure* done);
    void do_snapshot_load(LoadSnapshotClosure* done);
    void do_leader_stop();

    bthread::ExecutionQueueId<ApplyTask> _queue_id;
    LogManager *_log_manager;
    StateMachine *_fsm;
    ClosureQueue* _closure_queue;
    boost::atomic<int64_t> _last_applied_index;
    int64_t _last_applied_term;
    google::protobuf::Closure* _after_shutdown;
};

};

#endif  //PUBLIC_RAFT_FSM_CALLER_H
