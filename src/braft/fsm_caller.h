// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Wang,Yao(wangyao02@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef  BRAFT_FSM_CALLER_H
#define  BRAFT_FSM_CALLER_H

#include <butil/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <bthread/bthread.h>
#include <bthread/execution_queue.h>
#include "braft/ballot_box.h"
#include "braft/closure_queue.h"
#include "braft/macros.h"
#include "braft/log_entry.h"
#include "braft/lease.h"

namespace braft {

class NodeImpl;
class LogManager;
class StateMachine;
class SnapshotMeta;
class OnErrorClousre;
struct LogEntry;
class LeaderChangeContext;

// Backing implementation of Iterator
class IteratorImpl {
    DISALLOW_COPY_AND_ASSIGN(IteratorImpl);
public:
    // Move to the next
    void next();
    LogEntry* entry() const { return _cur_entry; }
    bool is_good() const { return _cur_index <= _committed_index && !has_error(); }
    Closure* done() const;
    void set_error_and_rollback(size_t ntail, const butil::Status* st);
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
                 butil::atomic<int64_t>* applying_index);
    ~IteratorImpl() {}
friend class FSMCaller;
    StateMachine* _sm;
    LogManager* _lm;
    std::vector<Closure*> *_closure;
    int64_t _first_closure_index;
    int64_t _cur_index;
    int64_t _committed_index;
    LogEntry* _cur_entry;
    butil::atomic<int64_t>* _applying_index;
    Error _error;
};

struct FSMCallerOptions {
    FSMCallerOptions() 
        : log_manager(NULL)
        , fsm(NULL)
        , after_shutdown(NULL)
        , closure_queue(NULL)
        , node(NULL)
        , usercode_in_pthread(false)
        , bootstrap_id()
    {}
    LogManager *log_manager;
    StateMachine *fsm;
    google::protobuf::Closure* after_shutdown;
    ClosureQueue* closure_queue;
    NodeImpl* node;
    bool usercode_in_pthread;
    LogId bootstrap_id;
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
    BRAFT_MOCK ~FSMCaller();
    int init(const FSMCallerOptions& options);
    int shutdown();
    BRAFT_MOCK int on_committed(int64_t committed_index);
    BRAFT_MOCK int on_snapshot_load(LoadSnapshotClosure* done);
    BRAFT_MOCK int on_snapshot_save(SaveSnapshotClosure* done);
    int on_leader_stop(const butil::Status& status);
    int on_leader_start(int64_t term, int64_t lease_epoch);
    int on_start_following(const LeaderChangeContext& start_following_context);
    int on_stop_following(const LeaderChangeContext& stop_following_context);
    BRAFT_MOCK int on_error(const Error& e);
    int64_t last_applied_index() const {
        return _last_applied_index.load(butil::memory_order_relaxed);
    }
    int64_t applying_index() const;
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
        LEADER_START,
        START_FOLLOWING,
        STOP_FOLLOWING,
        ERROR,
    };

    struct LeaderStartContext {
        LeaderStartContext(int64_t term_, int64_t lease_epoch_)
            : term(term_), lease_epoch(lease_epoch_)
        {}

        int64_t term;
        int64_t lease_epoch;
    };

    struct ApplyTask {
        TaskType type;
        union {
            // For applying log entry (including configuration change)
            int64_t committed_index;
            
            // For on_leader_start
            LeaderStartContext* leader_start_context;
            
            // For on_leader_stop
            butil::Status* status;    

            // For on_start_following and on_stop_following
            LeaderChangeContext* leader_change_context;

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
    void do_leader_stop(const butil::Status& status);
    void do_leader_start(const LeaderStartContext& leader_start_context);
    void do_start_following(const LeaderChangeContext& start_following_context);
    void do_stop_following(const LeaderChangeContext& stop_following_context);
    void set_error(const Error& e);
    bool pass_by_status(Closure* done);

    bthread::ExecutionQueueId<ApplyTask> _queue_id;
    LogManager *_log_manager;
    StateMachine *_fsm;
    ClosureQueue* _closure_queue;
    butil::atomic<int64_t> _last_applied_index;
    int64_t _last_applied_term;
    google::protobuf::Closure* _after_shutdown;
    NodeImpl* _node;
    TaskType _cur_task;
    butil::atomic<int64_t> _applying_index;
    Error _error;
    bool _queue_started;
};

};

#endif  //BRAFT_FSM_CALLER_H
