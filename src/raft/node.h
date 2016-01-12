/*
 * =====================================================================================
 *
 *       Filename:  node.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/08 16:57:44
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
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

namespace raft {

class LogStorage;
class StableStorage;
class SnapshotStorage;
class SnapshotExecutor;

class LeaderStableClosure : public LogManager::StableClosure {
public:
    void Run();
private:
    LeaderStableClosure(const NodeId& node_id, CommitmentManager* commit_manager);
friend class NodeImpl;
    NodeId _node_id;
    CommitmentManager* _commit_manager;
};

class BAIDU_CACHELINE_ALIGNMENT NodeImpl : public base::RefCountedThreadSafe<NodeImpl> {
friend class RaftServiceImpl;
friend class RaftStatImpl;
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
        return _state == LEADER;
    }

    // public user api
    //
    // init node
    int init(const NodeOptions& options);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    void shutdown(Closure* done);

    // apply data to replicated-state-machine
    // done is user defined function, maybe response to client, transform to on_applied
    void apply(const base::IOBuf& data, Closure* done);

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

    // rpc request proc func
    //
    // handle received PreVote
    int handle_pre_vote_request(const RequestVoteRequest* request,
                                RequestVoteResponse* response);
    // handle received RequestVote
    int handle_request_vote_request(const RequestVoteRequest* request,
                     RequestVoteResponse* response);

    // handle received AppendEntries
    int handle_append_entries_request(const base::IOBuf& data_buf,
                       const AppendEntriesRequest* request,
                       AppendEntriesResponse* response);

    // handle received InstallSnapshot
    void handle_install_snapshot_request(baidu::rpc::Controller* controller,
                                        const InstallSnapshotRequest* request,
                                        InstallSnapshotResponse* response,
                                        google::protobuf::Closure* done);

    // timer func
    //
    void handle_election_timeout();
    void handle_vote_timeout();
    void handle_stepdown_timeout();
    void handle_snapshot_timeout();

    // Closure call func
    //
    void handle_pre_vote_response(const PeerId& peer_id, const int64_t term,
                                      const RequestVoteResponse& response);
    void handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                      const RequestVoteResponse& response);
    void on_caughtup(const PeerId& peer, int error_code, Closure* done);
    // other func
    //
    // called when leader change configuration done, ref with FSMCaller
    void on_configuration_change_done(const EntryType type, const std::vector<PeerId>& peers);

    // called when leader recv greater term in AppendEntriesResponse, ref with Replicator
    int increase_term_to(int64_t new_term);

    // Temporary solution
    void update_configuration_after_installing_snapshot();

    void describe(std::ostream& os, bool use_html);

private:
    friend class base::RefCountedThreadSafe<NodeImpl>;
    virtual ~NodeImpl();

private:
    // internal init func
    int init_snapshot_storage();
    int init_log_storage();
    int init_stable_storage();

    // become leader
    void become_leader();

    // step down to follower
    void step_down(const int64_t term);

    // pre vote before elect_self
    void pre_vote();

    // elect self to candidate
    void elect_self();

    // leader async append log entry
    void append(LogEntry* entry, Closure* done);

    // candidate/follower sync append log entry
    int append(const std::vector<LogEntry*>& entries);

    void do_snapshot(Closure* done);

    void after_shutdown();
    static void after_shutdown(NodeImpl* node);

private:
    struct VoteCtx {
        size_t needed;
        std::set<PeerId> granted;

        VoteCtx() {
            reset();
        }

        void set(size_t peer_size) {
            needed = peer_size / 2 + 1;
        }

        void grant(PeerId peer) {
            granted.insert(peer);
        }
        bool quorum() {
            return granted.size() >= needed;
        }
        void reset() {
            needed = 0;
            granted.clear();
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

    State _state;
    int64_t _current_term;
    int64_t _last_leader_timestamp;
    PeerId _leader_id;
    PeerId _voted_id;
    VoteCtx _vote_ctx; // candidate vote ctx
    VoteCtx _pre_vote_ctx; // prevote ctx
    // Access of the fields before this line should in the critical section of
    // _state_mutex
    std::pair<int64_t, Configuration> _conf;

    GroupId _group_id;
    PeerId _server_id;
    NodeOptions _options;

    //int64_t _committed_index;
    raft_mutex_t _mutex;
    ConfigurationCtx _conf_ctx;
    LogStorage* _log_storage;
    StableStorage* _stable_storage;
    ConfigurationManager* _config_manager;
    LogManager* _log_manager;
    FSMCaller* _fsm_caller;
    CommitmentManager* _commit_manager;
    SnapshotExecutor* _snapshot_executor;
    ReplicatorGroup _replicator_group;
    std::vector<Closure*> _shutdown_continuations;
    bthread_timer_t _election_timer; // follower -> candidate timer
    bthread_timer_t _vote_timer; // candidate retry timer
    bthread_timer_t _stepdown_timer; // leader check quorum node ok
    bthread_timer_t _snapshot_timer; // snapshot timer
};

}

#endif //~PUBLIC_RAFT_RAFT_NODE_H
