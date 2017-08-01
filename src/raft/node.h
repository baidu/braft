// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/08 16:57:44

#ifndef PUBLIC_RAFT_RAFT_NODE_H
#define PUBLIC_RAFT_RAFT_NODE_H

#include <set>
#include <base/atomic_ref_count.h>
#include <base/memory/ref_counted.h>
#include <base/iobuf.h>
#include <bthread/execution_queue.h>
#include <baidu/rpc/server.h>

#include "bthread.h"
#include "raft/raft.h"
#include "raft/log_manager.h"
#include "raft/commitment_manager.h"
#include "raft/storage.h"
#include "raft/raft_service.h"
#include "raft/fsm_caller.h"
#include "raft/replicator.h"
#include "raft/util.h"
#include "raft/closure_queue.h"
#include "raft/configuration_manager.h"
#include "raft/repeated_timer_task.h"

namespace raft {

class LogStorage;
class StableStorage;
class SnapshotStorage;
class SnapshotExecutor;
struct StopTransferArg;

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
protected:
    void run();
};

class BAIDU_CACHELINE_ALIGNMENT NodeImpl 
        : public base::RefCountedThreadSafe<NodeImpl> {
friend class RaftServiceImpl;
friend class RaftStatImpl;
friend class FollowerStableClosure;
public:
    NodeImpl(const GroupId& group_id, const PeerId& peer_id);

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

    base::Status list_peers(std::vector<PeerId>* peers);

    // add peer to replicated-state-machine
    // done is user defined function, maybe response to client
    void add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                         Closure* done);

    // remove peer from replicated-state-machine
    // done is user defined function, maybe response to client
    void remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                            Closure* done);

    // set peer to local replica
    // done is user defined function, maybe response to client
    // only used in major node is down, reduce peerset to make group available
    int set_peer(const std::vector<PeerId>& old_peers,
                 const std::vector<PeerId>& new_peers);

    // trigger snapshot
    void snapshot(Closure* done);

    // trigger vote
    void vote(int election_timeout);

    // reset the election_timeout for the very node
    void reset_election_timeout_ms(int election_timeout_ms);

    // rpc request proc func
    //
    // handle received PreVote
    int handle_pre_vote_request(const RequestVoteRequest* request,
                                RequestVoteResponse* response);
    // handle received RequestVote
    int handle_request_vote_request(const RequestVoteRequest* request,
                     RequestVoteResponse* response);

    // handle received AppendEntries
    void handle_append_entries_request(baidu::rpc::Controller* cntl,
                                      const AppendEntriesRequest* request,
                                      AppendEntriesResponse* response,
                                      google::protobuf::Closure* done);

    // handle received InstallSnapshot
    void handle_install_snapshot_request(baidu::rpc::Controller* controller,
                                        const InstallSnapshotRequest* request,
                                        InstallSnapshotResponse* response,
                                        google::protobuf::Closure* done);

    void handle_timeout_now_request(baidu::rpc::Controller* controller,
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
                                      const RequestVoteResponse& response);
    void handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                      const RequestVoteResponse& response);
    void on_caughtup(const PeerId& peer, int64_t term, 
                     const base::Status& st, Closure* done);
    // other func
    //
    // called when leader change configuration done, ref with FSMCaller
    void on_configuration_change_done(int64_t term);

    // called when leader recv greater term in AppendEntriesResponse, ref with Replicator
    int increase_term_to(int64_t new_term, const base::Status& status);

    // Temporary solution
    void update_configuration_after_installing_snapshot();

    void describe(std::ostream& os, bool use_html);

    // Call on_error when some error happens, after this is called.
    // After this point:
    //  - This node is to step down immediately if it was the leader.
    //  - Any futuer operation except shutdown would fail, including any RPC
    //    request.
    void on_error(const Error& e);

    int transfer_leadership_to(const PeerId& peer);

private:
    friend class base::RefCountedThreadSafe<NodeImpl>;
    virtual ~NodeImpl();

private:
    // internal init func
    int init_snapshot_storage();
    int init_log_storage();
    int init_stable_storage();
    bool unsafe_register_conf_change(const std::vector<PeerId>& old_peers,
                                     const std::vector<PeerId>& new_peers,
                                     Closure* done);

    // become leader
    void become_leader();

    // step down to follower, status give the reason
    void step_down(const int64_t term, bool wakeup_a_candidate, const base::Status& status);

    // reset leader_id. 
    // When new_leader_id is NULL, it means this node just stop following a leader; 
    // otherwise, it means setting this node's leader_id to new_leader_id.
    // status gives the situation under which this method is called.
    void reset_leader_id(const PeerId& new_leader_id, const base::Status& status);

    // check weather to step_down when receiving append_entries/install_snapshot
    // requests.
    void check_step_down(const int64_t term, const PeerId& server_id);

    // pre vote before elect_self
    void pre_vote(std::unique_lock<raft_mutex_t>* lck);

    // elect self to candidate
    void elect_self(std::unique_lock<raft_mutex_t>* lck);

    // leader async apply configuration
    void unsafe_apply_configuration(const Configuration& new_conf, 
                                    Closure* done);

    void do_snapshot(Closure* done);

    void after_shutdown();
    static void after_shutdown(NodeImpl* node);

    void do_apply(base::IOBuf& data, Closure* done);

    struct LogEntryAndClosure;
    static int execute_applying_tasks(
                void* meta, bthread::TaskIterator<LogEntryAndClosure>& iter);
    void apply(LogEntryAndClosure tasks[], size_t size);

private:
    struct VoteCtx {
        size_t peers;
        std::set<PeerId> ungranted;

        VoteCtx() {
            reset();
        }

        void set(const std::vector<PeerId>& peer_vec) {
            peers = peer_vec.size();
            for (size_t i = 0; i < peer_vec.size(); i++) {
                ungranted.insert(peer_vec[i]);
            }
        }

        void grant(PeerId peer) {
            ungranted.erase(peer);
        }
        bool quorum() {
            return (peers - ungranted.size()) >= (peers / 2 + 1);
        }
        void reset() {
            peers = 0;
            ungranted.clear();
        }
    };
    struct ConfigurationCtx {
        std::vector<PeerId> peers;
        void set(std::vector<PeerId>& peers_) {
            peers.swap(peers_);
        }
        void reset() {
            peers.clear();
        }
        bool empty() {
            return peers.empty();
        }
    };

    struct LogEntryAndClosure {
        LogEntry* entry;
        Closure* done;
        int64_t expected_term;
    };

    State _state;
    int64_t _current_term;
    int64_t _last_leader_timestamp;
    PeerId _leader_id;
    PeerId _voted_id;
    VoteCtx _vote_ctx; // candidate vote ctx
    VoteCtx _pre_vote_ctx; // prevote ctx
    ConfigurationPair _conf;

    GroupId _group_id;
    PeerId _server_id;
    NodeOptions _options;

    raft_mutex_t _mutex;
    ConfigurationCtx _conf_ctx;
    LogStorage* _log_storage;
    StableStorage* _stable_storage;
    ClosureQueue* _closure_queue;
    ConfigurationManager* _config_manager;
    LogManager* _log_manager;
    FSMCaller* _fsm_caller;
    CommitmentManager* _commit_manager;
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
};

}

#endif //~PUBLIC_RAFT_RAFT_NODE_H
