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
#include "raft/raft.h"
#include "raft/log_manager.h"
#include "raft/commitment_manager.h"

namespace raft {

class LogEntryCommitmentWaiter : public CommitmentWaiter {
public:
    LogEntryCommitmentWaiter(const NodeId& id):_id(id) {}
    virtual ~LogEntryCommitmentWaiter() {}

    // Called when some logs are commited since the last time this method was
    // commited
    virtual int on_committed(int64_t last_commited_index, void *context);
    virtual int on_cleared(int64_t log_index, void *context, int error_code);
private:
    NodeId _id;
};

class NodeImpl : public Node {
public:
    NodeImpl(const GroupId& group_id, const PeerId& server_id, const NodeOptions* option);

    // apply data to replicated-state-machine
    // done is user defined function, maybe response to client, transform to on_applied
    virtual int apply(const void* data, const int len, base::Closure* done);

    // add peer to replicated-state-machine
    // done is user defined function, maybe response to client
    virtual int add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                         base::Closure* done);

    // remove peer from replicated-state-machine
    // done is user defined function, maybe response to client
    virtual int remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                            base::Closure* done);

    // set peer to local replica
    // done is user defined function, maybe response to client
    // only used in major node is down, reduce peerset to make group available
    virtual int set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers,
                 base::Closure* done);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    virtual int shutdown(base::Closure* done);

    // handle received RequestVote
    int handle_request_vote_request(const protocol::RequestVoteRequest* request,
                     protocol::RequestVoteResponse* response);

    // handle received AppendEntries
    int handle_append_entries_request(base::IOBuf& data_buf,
                       const protocol::AppendEntriesRequest* request,
                       protocol::AppendEntriesResponse* response);

    // handle received InstallSnapshot
    int handle_install_snapshot_request(const protocol::InstallSnapshotRequest* request,
                       protocol::InstallSnapshotResponse* response);

    enum State {
        FOLLOWER = 0,
        CANDIDATE = 1,
        LEADER = 2,
        SHUTDOWN = 3,
    };

    virtual NodeId node_id() {
        return NodeId(_group_id, _server_id);
    }

    GroupId group_id() {
        return _group_id;
    }

    PeerId server_id() {
        return _server_id;
    }

    void add_ref() {
        base::AtomicRefCountInc(&_ref_count);
    }
    void release() {
        if (!base::AtomicRefCountDec(&_ref_count)) {
            delete this;
        }
    }

    // timer func
    void handle_election_timeout();
    void handle_vote_timeout();

    // rpc response proc func
    void handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                      const protocol::RequestVoteResponse& response);

    // called when leader disk thread on_stable callback and peer thread replicate success
    int advance_commit_index(const PeerId& peer_id, const int64_t log_index);
private:
    //friend class base::RefCountedThreadSafe<NodeImpl>;
    virtual ~NodeImpl();

    // become leader
    void become_leader();

    // step down to follower
    void step_down(const int64_t term);

    // elect self to candidate
    void elect_self();

    // get last log term, through log and snapshot
    int64_t last_log_term();

    // leader async append log entry
    int append(LogEntry* entry, base::Closure* done);
private:
    struct VoteCtx {
        size_t needed;
        std::set<PeerId> granted;

        VoteCtx() {
            reset();
        }
        VoteCtx(size_t peer_size) {
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
    struct ReplicateCtx {
        bool exiting;
        int64_t next_id;
        int64_t last_timestamp;
    };
    typedef std::map<PeerId, ReplicateCtx> ReplicateCtxMap;

    NodeOptions _options;
    GroupId _group_id;
    PeerId _server_id;
    State _state;
    int64_t _current_term;
    PeerId _leader_id;
    PeerId _voted_id;
    Configuration _conf;

    //int64_t _committed_index;
    int64_t _last_snapshot_term;
    int64_t _last_snapshot_index;
    int64_t _last_leader_timestamp;

    bthread_mutex_t _mutex;
    VoteCtx _vote_ctx; // candidate vote ctx
    ReplicateCtxMap _replicate_ctx_map; // leader replicate ctx map
    bthread_timer_t _election_timer; // follower -> candidate timer
    bthread_timer_t _vote_timer; // candidate retry timer
    bthread_timer_t _lease_timer; // leader check lease timer

    LogManager* _log_manager;
    CommitmentManager* _commit_manager;
    StableStorage *_stable;
    mutable base::AtomicRefCount _ref_count;
};

}

#endif //~PUBLIC_RAFT_RAFT_NODE_H
