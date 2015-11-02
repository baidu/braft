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
#include <base/memory/singleton.h>
#include <base/iobuf.h>
#include <baidu/rpc/server.h>

#include "bthread.h"
#include "raft/raft.h"
#include "raft/log_manager.h"
#include "raft/commitment_manager.h"
#include "raft/raft_service.h"
#include "raft/fsm_caller.h"
#include "raft/replicator.h"
#include "raft/util.h"

namespace raft {

class NodeImpl : public base::RefCountedThreadSafe<NodeImpl> {
friend class RaftServiceImpl;
public:
    NodeImpl(const GroupId& group_id, const ReplicaId& replica_id);

    NodeId node_id() {
        return NodeId(_group_id, _server_id);
    }

    PeerId leader_id() {
        std::lock_guard<bthread_mutex_t> guard(_mutex);
        return _leader_id;
    }

    // init node
    int init(const NodeOptions& options);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    void shutdown(Closure* done);

    // apply data to replicated-state-machine
    // done is user defined function, maybe response to client, transform to on_applied
    int apply(const base::IOBuf& data, Closure* done);

    // add peer to replicated-state-machine
    // done is user defined function, maybe response to client
    int add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                         Closure* done);

    // remove peer from replicated-state-machine
    // done is user defined function, maybe response to client
    int remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                            Closure* done);

    // set peer to local replica
    // done is user defined function, maybe response to client
    // only used in major node is down, reduce peerset to make group available
    int set_peer(const std::vector<PeerId>& old_peers,
                         const std::vector<PeerId>& new_peers);

    // handle received RequestVote
    int handle_request_vote_request(const RequestVoteRequest* request,
                     RequestVoteResponse* response);

    // handle received AppendEntries
    int handle_append_entries_request(base::IOBuf& data_buf,
                       const AppendEntriesRequest* request,
                       AppendEntriesResponse* response);

    // handle received InstallSnapshot
    int handle_install_snapshot_request(const InstallSnapshotRequest* request,
                       InstallSnapshotResponse* response);

    void on_configuration_change_done(const EntryType type, const std::vector<PeerId>& peers);
    void on_caughtup(const PeerId& peer, int error_code, Closure* done);

    int increase_term_to(int64_t new_term);

    // timer func
    void handle_election_timeout();
    void handle_vote_timeout();
    void handle_stepdown_timeout();

    // rpc response proc func
    void handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                      const RequestVoteResponse& response);

    // called when leader disk thread on_stable callback and peer thread replicate success
    void advance_commit_index(const PeerId& peer_id, const int64_t log_index);

    FSMCaller* fsm_caller() {
        return _fsm_caller;
    }
private:
    friend class base::RefCountedThreadSafe<NodeImpl>;
    virtual ~NodeImpl();

private:
    // become leader
    void become_leader();

    // step down to follower
    void step_down(const int64_t term);

    // elect self to candidate
    void elect_self();

    // get last log term, through log and snapshot
    int64_t last_log_term();

    // leader async append log entry
    int append(LogEntry* entry, Closure* done);

    // candidate/follower sync append log entry
    int append(const std::vector<LogEntry*>& entries);
private:
    enum State {
        LEADER = 1,
        CANDIDATE = 2,
        FOLLOWER = 3,
        SHUTDOWN = 4,
        STATE_END = 4,
    };
    const char* State2Str(State state) {
        const char* str[] = {"LEADER", "CANDIDATE", "FOLLOWER", "SHUTDOWN"};
        if (state <= STATE_END) {
            return str[(int)state - 1];
        } else {
            return "UNKNOWN";
        }
    }
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

    NodeOptions _options;
    bool _inited;
    GroupId _group_id;
    PeerId _server_id;
    State _state;
    int64_t _current_term;
    PeerId _leader_id;
    PeerId _voted_id;
    std::pair<int64_t, Configuration> _conf;

    //int64_t _committed_index;
    int64_t _last_snapshot_term;
    int64_t _last_snapshot_index;
    int64_t _last_leader_timestamp;

    bthread_mutex_t _mutex;
    VoteCtx _vote_ctx; // candidate vote ctx
    ConfigurationCtx _conf_ctx;
    bthread_timer_t _election_timer; // follower -> candidate timer
    bthread_timer_t _vote_timer; // candidate retry timer
    bthread_timer_t _stepdown_timer; // leader check quorum node ok

    LogStorage* _log_storage;
    StableStorage* _stable_storage;
    ConfigurationManager* _config_manager;
    LogManager* _log_manager;
    FSMCaller* _fsm_caller;
    CommitmentManager* _commit_manager;
    ReplicatorGroup _replicator_group;
};

class NodeManager {
public:
    static NodeManager* GetInstance() {
        return Singleton<NodeManager>::get();
    }

    int init(const char* ip_str, int start_port, int end_port);

    base::EndPoint address();

    // add raft node
    bool add(NodeImpl* node);

    // remove raft node
    void remove(NodeImpl* node);

    // get node by group_id and peer_id
    NodeImpl* get(const GroupId& group_id, const PeerId& peer_id);

private:
    NodeManager();
    ~NodeManager();
    DISALLOW_COPY_AND_ASSIGN(NodeManager);
    friend struct DefaultSingletonTraits<NodeManager>;

    bthread_mutex_t _mutex;
    typedef std::map<NodeId, NodeImpl*> NodeMap;
    NodeMap _nodes;

    base::EndPoint _address;
    baidu::rpc::Server _server;
    RaftServiceImpl _service_impl;
};

}

#endif //~PUBLIC_RAFT_RAFT_NODE_H
