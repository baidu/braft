// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
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

// Authors: Kai FAN(fankai@baidu.com)

#ifndef  BRAFT_RAFT_LEARNER_H
#define  BRAFT_RAFT_LEARNER_H

#include <brpc/server.h>
#include <brpc/parallel_channel.h>
#include <butil/atomic_ref_count.h>
#include <butil/memory/ref_counted.h>

#include <bthread/bthread.h>
#include "braft/raft.h"
#include "braft/log_manager.h"
#include "braft/storage.h"
#include "braft/snapshot_service.h"
#include "braft/snapshot_executor.h"
#include "braft/fsm_caller.h"
#include "braft/configuration_manager.h"
#include "braft/repeated_timer_task.h"
#include "braft/log_fetcher.h"

namespace braft {

DECLARE_int32(raft_learner_req_timeout_ms);

class LearnerTimer : public RepeatedTimerTask {
public:
    LearnerTimer() : _learner(NULL) {}
    virtual ~LearnerTimer() override {}
    int init(LearnerImpl* learner, int timeout_ms);
    virtual void run() override = 0;
protected:
    void on_destroy() override;
    LearnerImpl* _learner;
};

class FetchLogTimer: public LearnerTimer {
protected:
    void run() override;
};

class LearnerSnapshotTimer : public LearnerTimer {
public:
    LearnerSnapshotTimer() : _first_schedule(true) {}
protected:
    void run() override;
    int adjust_timeout_ms(int timeout_ms) override;
private:
    bool _first_schedule;
};

class LearnerStableClosure : public LogManager::StableClosure {
public:
    LearnerStableClosure(LearnerImpl* learner,
                         const size_t nentries);
    virtual ~LearnerStableClosure() override;
    virtual void Run() override;
private:
    LearnerImpl* _learner;
    size_t _nentries;
};

class BAIDU_CACHELINE_ALIGNMENT LearnerImpl
        : public butil::RefCountedThreadSafe<LearnerImpl> {
public:
    LearnerImpl(const GroupId& group_id, const PeerId& peer_id);
    virtual ~LearnerImpl();

    int init(const NodeOptions& options);

    void join();

    void shutdown(Closure* done);

    NodeId node_id() const {
        return NodeId(_group_id, _server_id);
    }

    void get_status(NodeStatus* status);
    void describe(std::ostream& os, bool use_html);

    void snapshot(Closure* done);

    butil::Status set_read_barrier(const int64_t index);

    void handle_tail_snapshot(brpc::Controller* cntl,
                              const TailSnapshotRequest* request,
                              TailSnapshotResponse* response);
    void fetch_log();

    static void on_learn(void* arg);
    static void* learn(void* arg);

    bool disable_cli() const { return _options.disable_cli; }

    void resume(Closure* done);
    void reset(Closure* done);

private:
friend class butil::RefCountedThreadSafe<LearnerImpl>;
friend class LearnerStableClosure;
friend class SnapshotServiceImpl;
friend class FetchLogClosure;
friend class LogFetcher;

    scoped_refptr<LogFetcher> get_log_fetcher();
    int tail_snapshot(const PeerId& peer, const int64_t last_snapshot_index, 
                      const int64_t peer_first_log_index);
    void step_down(const butil::Status& status);
    int init_log_fetcher(const PeerId& peer);
    int init_log_storage();
    int init_snapshot_storage();
    int init_fsm_caller();
    void on_fetch_log_error();
    int commit_at(int64_t first_log_index, int64_t last_log_index);
    int start_learn_timer();
    void stop_learn_timer();
    void unsafe_stop_learn_timer();
    int explore_learning_info(PeerId* peer, int64_t* committed_index, 
                              int64_t* last_snapshot_index, int64_t* peer_first_log_index);
    brpc::ParallelChannel* init_rpc_channel();
    ExploreResponse* explore_raft_group();
    int append_entries(std::vector<LogEntry*>* entries);
    void after_shutdown();
    static void after_shutdown(LearnerImpl* node);
    int64_t last_committed_index() { return _last_committed_index; }

    GroupId _group_id;
    PeerId _server_id;
    NodeOptions _options;
    ConfigurationEntry _conf;
    ConfigurationManager* _config_manager;
    LogStorage* _log_storage;
    LogManager* _log_manager;
    SnapshotExecutor* _snapshot_executor;
    FSMCaller* _fsm_caller;
    scoped_refptr<LogFetcher> _log_fetcher;
    ClosureQueue _closure_queue;
    std::vector<Closure*> _shutdown_continuations;

    raft_mutex_t _mutex;
    State _state;
    int64_t _last_committed_index;
    int64_t _last_log_index;
    int64_t _read_barrier;
    int64_t _fetching_snapshot_index;

    FetchLogTimer _fetch_log_timer;
    LearnerSnapshotTimer _snapshot_timer;
    bthread_timer_t _learn_timer;
};

} // namespace braft

#endif
