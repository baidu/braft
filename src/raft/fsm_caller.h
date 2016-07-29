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
class OnErrorClousre;
class LogEntry;
class NodeImpl;

// Backing implementation of Iterator
class IteratorImpl {
    DISALLOW_COPY_AND_ASSIGN(IteratorImpl);
public:
    // Move to the next
    void next();
    LogEntry* entry() const { return _cur_entry; }
    bool is_good() const { return _cur_index <= _committed_index && !has_error(); }
    Closure* done() const;
    void set_error_and_rollback(size_t ntail, const base::Status* st);
    bool has_error() const { return _error.type() != ERROR_TYPE_NONE; }
    const Error& error() const { return _error; }
    int64_t index() const { return _cur_index; }
    void run_the_rest_closure_with_error();
private:
    IteratorImpl(StateMachine* sm, LogManager* lm, 
                 std::vector<Closure*> *closure,
                 int64_t first_closure_index,
                 int64_t last_applied_index,
                 int64_t committed_index,
                 boost::atomic<int64_t>* applying_index);
    ~IteratorImpl() {}
friend class FSMCaller;
    StateMachine* _sm;
    LogManager* _lm;
    std::vector<Closure*> *_closure;
    int64_t _first_closure_index;
    int64_t _cur_index;
    int64_t _committed_index;
    LogEntry* _cur_entry;
    boost::atomic<int64_t>* _applying_index;
    Error _error;
};

struct FSMCallerOptions {
    FSMCallerOptions() 
        : log_manager(NULL)
        , fsm(NULL)
        , after_shutdown(NULL)
        , closure_queue(NULL)
        , node(NULL)
    {}
    LogManager *log_manager;
    StateMachine *fsm;
    google::protobuf::Closure* after_shutdown;
    ClosureQueue* closure_queue;
    NodeImpl* node;
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
    RAFT_MOCK int on_snapshot_load(LoadSnapshotClosure* done);
    RAFT_MOCK int on_snapshot_save(SaveSnapshotClosure* done);
    int on_leader_stop();
    RAFT_MOCK int on_error(const Error& e);
    int64_t last_applied_index() const {
        return _last_applied_index.load(boost::memory_order_relaxed);
    }
    void describe(std::ostream& os, bool use_html);
    void join();
private:

friend class IteratorImpl;

    enum TaskType {
        IDLE,
        COMMITTED,
        SNAPSHOT_SAVE,
        SNAPSHOT_LOAD,
        LEADER_STOP,
        ERROR,
    };

    struct ApplyTask {
        TaskType type;
        union {
            // For applying log entry (including configuartion change)
            int64_t committed_index;
            
            // For other operation
            Closure* done;
        };
    };

    static double get_cumulated_cpu_time(void* arg);
    static int run(void* meta, bthread::TaskIterator<ApplyTask>& iter);
    void do_shutdown(); //Closure* done);
    void do_committed(int64_t committed_index);
    void do_cleared(int64_t log_index, Closure* done, int error_code);
    void do_snapshot_save(SaveSnapshotClosure* done);
    void do_snapshot_load(LoadSnapshotClosure* done);
    void do_on_error(OnErrorClousre* done);
    void do_leader_stop();
    void set_error(const Error& e);
    bool pass_by_status(Closure* done);

    bthread::ExecutionQueueId<ApplyTask> _queue_id;
    LogManager *_log_manager;
    StateMachine *_fsm;
    ClosureQueue* _closure_queue;
    boost::atomic<int64_t> _last_applied_index;
    int64_t _last_applied_term;
    google::protobuf::Closure* _after_shutdown;
    NodeImpl* _node;
    TaskType _cur_task;
    boost::atomic<int64_t> _applying_index;
    Error _error;
};

};

#endif  //PUBLIC_RAFT_FSM_CALLER_H
