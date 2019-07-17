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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef BRAFT_RAFT_NODE_H
#define BRAFT_RAFT_NODE_H

#include <set>
#include <butil/atomic_ref_count.h>
#include <butil/memory/ref_counted.h>
#include <butil/iobuf.h>
#include <bthread/execution_queue.h>
#include <brpc/server.h>
#include "braft/raft.h"
#include "braft/log_manager.h"
#include "braft/ballot_box.h"
#include "braft/storage.h"
#include "braft/raft_service.h"
#include "braft/fsm_caller.h"
#include "braft/replicator.h"
#include "braft/util.h"
#include "braft/closure_queue.h"
#include "braft/configuration_manager.h"
#include "braft/repeated_timer_task.h"

namespace braft {

class LogStorage;
class RaftMetaStorage;
class SnapshotStorage;
class SnapshotExecutor;
class StopTransferArg;

class NodeImpl;
class NodeTimer : public RepeatedTimerTask {
public:
    NodeTimer() : _node(NULL) {}
    virtual ~NodeTimer() {}
    int init(NodeImpl* node, int timeout_ms);
    virtual void run() = 0;
protected:
    void on_destroy();
    NodeImpl* _node;
};

class ElectionTimer : public NodeTimer {
protected:
    void run();
    int adjust_timeout_ms(int timeout_ms);
};

class VoteTimer : public NodeTimer {
protected:
    void run();
    int adjust_timeout_ms(int timeout_ms);
};

class StepdownTimer : public NodeTimer {
protected:
    void run();
};

class SnapshotTimer : public NodeTimer {
public:
    SnapshotTimer() : _first_schedule(true) {}
protected:
    void run();
    int adjust_timeout_ms(int timeout_ms);
private:
    bool _first_schedule;
};

class BAIDU_CACHELINE_ALIGNMENT NodeImpl 
        : public butil::RefCountedThreadSafe<NodeImpl> {
friend class RaftServiceImpl;
friend class RaftStatImpl;
friend class FollowerStableClosure;
friend class ConfigurationChangeDone;
friend class VoteBallotCtx;
public:
    NodeImpl(const GroupId& group_id, const PeerId& peer_id);
    NodeImpl();

    NodeId node_id() const {
        return NodeId(_group_id, _server_id);
    }

    PeerId leader_id() {
        BAIDU_SCOPED_LOCK(_mutex);
        return _leader_id;
    }

    bool is_leader() {
        BAIDU_SCOPED_LOCK(_mutex);
        return _state == STATE_LEADER;
    }

    // public user api
    //
    // init node
    int init(const NodeOptions& options);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    void shutdown(Closure* done);

    // Block the thread until the node is successfully stopped.
    void join();

    // apply task to the replicated-state-machine
    //
    // About the ownership:
    // |task.data|: for the performance consideration, we will take way the 
    //              content. If you want keep the content, copy it before call
    //              this function
    // |task.done|: If the data is successfully committed to the raft group. We
    //              will pass the ownership to StateMachine::on_apply.
    //              Otherwise we will specifit the error and call it.
    //
    void apply(const Task& task);

    butil::Status list_peers(std::vector<PeerId>* peers);

    // @Node configuration change
    void add_peer(const PeerId& peer, Closure* done);
    void remove_peer(const PeerId& peer, Closure* done);
    void change_peers(const Configuration& new_peers, Closure* done);
    butil::Status reset_peers(const Configuration& new_peers);

    // trigger snapshot
    void snapshot(Closure* done);

    // trigger vote
    butil::Status vote(int election_timeout);

    // reset the election_timeout for the very node
    butil::Status reset_election_timeout_ms(int election_timeout_ms);
    void reset_election_timeout_ms(int election_timeout_ms, int max_clock_drift_ms);

    // rpc request proc func
    //
    // handle received PreVote
    int handle_pre_vote_request(const RequestVoteRequest* request,
                                RequestVoteResponse* response);
    // handle received RequestVote
    int handle_request_vote_request(const RequestVoteRequest* request,
                     RequestVoteResponse* response);

    // handle received AppendEntries
    void handle_append_entries_request(brpc::Controller* cntl,
                                      const AppendEntriesRequest* request,
                                      AppendEntriesResponse* response,
                                      google::protobuf::Closure* done,
                                      bool from_append_entries_cache = false);

    // handle received InstallSnapshot
    void handle_install_snapshot_request(brpc::Controller* controller,
                                        const InstallSnapshotRequest* request,
                                        InstallSnapshotResponse* response,
                                        google::protobuf::Closure* done);

    void handle_timeout_now_request(brpc::Controller* controller,
                                    const TimeoutNowRequest* request,
                                    TimeoutNowResponse* response,
                                    google::protobuf::Closure* done);
    // timer func
    void handle_election_timeout();
    void handle_vote_timeout();
    void handle_stepdown_timeout();
    void handle_snapshot_timeout();
    void handle_transfer_timeout(int64_t term, const PeerId& peer);

    // Closure call func
    //
    void handle_pre_vote_response(const PeerId& peer_id, const int64_t term,
                                  const int64_t ctx_version,
                                  const RequestVoteResponse& response);
    void handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                      const int64_t ctx_version,
                                      const RequestVoteResponse& response);
    void on_caughtup(const PeerId& peer, int64_t term, 
                     int64_t version, const butil::Status& st);
    // other func
    //
    // called when leader change configuration done, ref with FSMCaller
    void on_configuration_change_done(int64_t term);

    // Called when leader lease is safe to start.
    void leader_lease_start(int64_t lease_epoch);

    // called when leader recv greater term in AppendEntriesResponse, ref with Replicator
    int increase_term_to(int64_t new_term, const butil::Status& status);

    // Temporary solution
    void update_configuration_after_installing_snapshot();

    void describe(std::ostream& os, bool use_html);
 
    // Get the internal status of this node, the information is mostly the same as we
    // see from the website, which is generated by |describe| actually.
    void get_status(NodeStatus* status);

    // Readonly mode func
    void enter_readonly_mode();
    void leave_readonly_mode();
    bool readonly();
    int change_readonly_config(int64_t term, const PeerId& peer_id, bool readonly);
    void check_majority_nodes_readonly();
    void check_majority_nodes_readonly(const Configuration& conf);

    // Lease func
    bool is_leader_lease_valid();
    void get_leader_lease_status(LeaderLeaseStatus* status);

    // Call on_error when some error happens, after this is called.
    // After this point:
    //  - This node is to step down immediately if it was the leader.
    //  - Any futuer operation except shutdown would fail, including any RPC
    //    request.
    void on_error(const Error& e);

    int transfer_leadership_to(const PeerId& peer);
    
    butil::Status read_committed_user_log(const int64_t index, UserLog* user_log);

    int bootstrap(const BootstrapOptions& options);

    bool disable_cli() const { return _options.disable_cli; }

private:
friend class butil::RefCountedThreadSafe<NodeImpl>;

    virtual ~NodeImpl();
    // internal init func
    int init_snapshot_storage();
    int init_log_storage();
    int init_meta_storage();
    int init_fsm_caller(const LogId& bootstrap_index);
    void unsafe_register_conf_change(const Configuration& old_conf,
                                     const Configuration& new_conf,
                                     Closure* done);
    void stop_replicator(const std::set<PeerId>& keep,
                         const std::set<PeerId>& drop);

    // become leader
    void become_leader();

    // step down to follower, status give the reason
    void step_down(const int64_t term, bool wakeup_a_candidate,
                   const butil::Status& status);

    // reset leader_id. 
    // When new_leader_id is NULL, it means this node just stop following a leader; 
    // otherwise, it means setting this node's leader_id to new_leader_id.
    // status gives the situation under which this method is called.
    void reset_leader_id(const PeerId& new_leader_id, const butil::Status& status);

    // check weather to step_down when receiving append_entries/install_snapshot
    // requests.
    void check_step_down(const int64_t term, const PeerId& server_id);

    // pre vote before elect_self
    void pre_vote(std::unique_lock<raft_mutex_t>* lck);

    // elect self to candidate
    void elect_self(std::unique_lock<raft_mutex_t>* lck);

    // grant self a vote
    class VoteBallotCtx;
    void grant_self(VoteBallotCtx* vote_ctx, std::unique_lock<raft_mutex_t>* lck);
    static void on_grant_self_timedout(void* arg);
    static void* handle_grant_self_timedout(void* arg);

    // leader async apply configuration
    void unsafe_apply_configuration(const Configuration& new_conf,
                                    const Configuration* old_conf,
                                    bool leader_start);

    void do_snapshot(Closure* done);

    void after_shutdown();
    static void after_shutdown(NodeImpl* node);

    void do_apply(butil::IOBuf& data, Closure* done);

    struct LogEntryAndClosure;
    static int execute_applying_tasks(
                void* meta, bthread::TaskIterator<LogEntryAndClosure>& iter);
    void apply(LogEntryAndClosure tasks[], size_t size);
    void check_dead_nodes(const Configuration& conf, int64_t now_ms);

    bool handle_out_of_order_append_entries(brpc::Controller* cntl,
                                            const AppendEntriesRequest* request,
                                            AppendEntriesResponse* response,
                                            google::protobuf::Closure* done,
                                            int64_t local_last_index);
    void check_append_entries_cache(int64_t local_last_index);
    void clear_append_entries_cache();
    static void* handle_append_entries_from_cache(void* arg);
    static void on_append_entries_cache_timedout(void* arg);
    static void* handle_append_entries_cache_timedout(void* arg);

    int64_t last_leader_active_timestamp();
    int64_t last_leader_active_timestamp(const Configuration& conf);
    void unsafe_reset_election_timeout_ms(int election_timeout_ms,
                                          int max_clock_drift_ms);

private:

    class ConfigurationCtx {
    DISALLOW_COPY_AND_ASSIGN(ConfigurationCtx);
    public:
        enum Stage {
            // Don't change the order if you are not sure about the usage
            STAGE_NONE = 0,
            STAGE_CATCHING_UP = 1,
            STAGE_JOINT = 2,
            STAGE_STABLE = 3,
        };
        ConfigurationCtx(NodeImpl* node) :
            _node(node), _stage(STAGE_NONE), _version(0), _done(NULL) {}
        void list_new_peers(std::vector<PeerId>* new_peers) const {
            new_peers->clear();
            std::set<PeerId>::iterator it;
            for (it = _new_peers.begin(); it != _new_peers.end(); ++it) {
                new_peers->push_back(*it);
            }
        }
        void list_old_peers(std::vector<PeerId>* old_peers) const {
            old_peers->clear();
            std::set<PeerId>::iterator it;
            for (it = _old_peers.begin(); it != _old_peers.end(); ++it) {
                old_peers->push_back(*it);
            }
        }
        const char* stage_str() {
            const char* str[] = {"STAGE_NONE", "STAGE_CATCHING_UP", 
                                 "STAGE_JOINT", "STAGE_STABLE", };
            if (_stage <= STAGE_STABLE) {
                return str[(int)_stage];
            } else {
                return "UNKNOWN";
            }
        }
        int32_t stage() const { return _stage; }
        void reset(butil::Status* st = NULL);
        bool is_busy() const { return _stage != STAGE_NONE; }
        // Start change configuration.
        void start(const Configuration& old_conf, 
                   const Configuration& new_conf,
                   Closure * done);
        // Invoked when this node becomes the leader, write a configuration
        // change log as the first log
        void flush(const Configuration& conf,
                   const Configuration& old_conf);
        void next_stage();
        void on_caughtup(int64_t version, const PeerId& peer_id, bool succ);
    private:
        NodeImpl* _node;
        Stage _stage;
        int _nchanges;
        int64_t _version;
        std::set<PeerId> _new_peers;
        std::set<PeerId> _old_peers;
        std::set<PeerId> _adding_peers;
        Closure* _done;
    };

    struct LogEntryAndClosure {
        LogEntry* entry;
        Closure* done;
        int64_t expected_term;
    };

    struct AppendEntriesRpc : public butil::LinkNode<AppendEntriesRpc> {
        brpc::Controller* cntl;
        const AppendEntriesRequest* request;
        AppendEntriesResponse* response;
        google::protobuf::Closure* done;
        int64_t receive_time_ms;
    };

    struct HandleAppendEntriesFromCacheArg {
        NodeImpl* node;
        butil::LinkedList<AppendEntriesRpc> rpcs;
    };

    // A simple cache to temporaryly store out-of-order AppendEntries requests.
    class AppendEntriesCache {
    public:
        AppendEntriesCache(NodeImpl* node, int64_t version)
            : _node(node), _timer(bthread_timer_t())
            , _cache_version(0), _timer_version(0) {}

        int64_t first_index() const;
        int64_t cache_version() const;
        bool empty() const;
        bool store(AppendEntriesRpc* rpc);
        void process_runable_rpcs(int64_t local_last_index);
        void clear();
        void do_handle_append_entries_cache_timedout(
                int64_t timer_version, int64_t timer_start_ms);

    private:
        void ack_fail(AppendEntriesRpc* rpc);
        void start_to_handle(HandleAppendEntriesFromCacheArg* arg);
        bool start_timer();
        void stop_timer();

        NodeImpl* _node;
        butil::LinkedList<AppendEntriesRpc> _rpc_queue;
        std::map<int64_t, AppendEntriesRpc*> _rpc_map;
        bthread_timer_t _timer;
        int64_t _cache_version;
        int64_t _timer_version;
    };

    // A versioned ballot for vote and prevote
    struct GrantSelfArg;
    class VoteBallotCtx {
    public:
        VoteBallotCtx() : _timer(bthread_timer_t()), _version(0), _grant_self_arg(NULL) {
        }
        void init(NodeImpl* node);
        void grant(const PeerId& peer) {
            _ballot.grant(peer);
        }
        bool granted() {
            return _ballot.granted();
        }
        int64_t version() {
            return _version;
        }
        void start_grant_self_timer(int64_t wait_ms, NodeImpl* node);
        void stop_grant_self_timer(NodeImpl* node);
        void reset(NodeImpl* node);
    private:
        bthread_timer_t _timer;
        Ballot _ballot;
        // Each time the vote ctx restarted, increase the version to avoid
        // ABA problem.
        int64_t _version;
        GrantSelfArg* _grant_self_arg;
    };

    struct GrantSelfArg {
        NodeImpl* node;
        int64_t vote_ctx_version;
        VoteBallotCtx* vote_ctx;
    };

    State _state;
    int64_t _current_term;
    int64_t _last_leader_timestamp;
    PeerId _leader_id;
    PeerId _voted_id;
    VoteBallotCtx _vote_ctx; // candidate vote ctx
    VoteBallotCtx _pre_vote_ctx; // prevote ctx
    ConfigurationEntry _conf;

    GroupId _group_id;
    VersionedGroupId _v_group_id;
    PeerId _server_id;
    NodeOptions _options;

    raft_mutex_t _mutex;
    ConfigurationCtx _conf_ctx;
    LogStorage* _log_storage;
    RaftMetaStorage* _meta_storage;
    ClosureQueue* _closure_queue;
    ConfigurationManager* _config_manager;
    LogManager* _log_manager;
    FSMCaller* _fsm_caller;
    BallotBox* _ballot_box;
    SnapshotExecutor* _snapshot_executor;
    ReplicatorGroup _replicator_group;
    std::vector<Closure*> _shutdown_continuations;
    ElectionTimer _election_timer;
    VoteTimer _vote_timer;
    StepdownTimer _stepdown_timer;
    SnapshotTimer _snapshot_timer;
    bthread_timer_t _transfer_timer;
    StopTransferArg* _stop_transfer_arg;
    bool _vote_triggered;
    ReplicatorId _waking_candidate;
    bthread::ExecutionQueueId<LogEntryAndClosure> _apply_queue_id;
    bthread::ExecutionQueue<LogEntryAndClosure>::scoped_ptr_t _apply_queue;
    AppendEntriesCache* _append_entries_cache;
    int64_t _append_entries_cache_version;

    // for readonly mode
    bool _node_readonly;
    bool _majority_nodes_readonly;

    LeaderLease _leader_lease;
    FollowerLease _follower_lease;
};

}

#endif //~BRAFT_RAFT_NODE_H
