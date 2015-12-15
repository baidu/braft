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
#include <base/containers/doubly_buffered_data.h>
#include <base/iobuf.h>
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

class SaveSnapshotDone : public SaveSnapshotClosure {
public:
    SaveSnapshotDone(NodeImpl* node, SnapshotStorage* snapshot_storage, Closure* done);
    virtual ~SaveSnapshotDone();

    SnapshotWriter* start(const SnapshotMeta& meta);
    virtual void Run();

    NodeImpl* _node;
    SnapshotStorage* _snapshot_storage;
    SnapshotWriter* _writer;
    Closure* _done; // user done
    SnapshotMeta _meta;
};

class InstallSnapshotDone : public LoadSnapshotClosure {
public:
    InstallSnapshotDone(NodeImpl* node,
                        SnapshotStorage* snapshot_storage,
                        baidu::rpc::Controller* controller,
                        const InstallSnapshotRequest* request,
                        InstallSnapshotResponse* response,
                        google::protobuf::Closure* done);
    virtual ~InstallSnapshotDone();

    SnapshotReader* start();
    virtual void Run();

    NodeImpl* _node;
    SnapshotStorage* _snapshot_storage;
    SnapshotReader* _reader;
    baidu::rpc::Controller* _controller;
    const InstallSnapshotRequest* _request;
    InstallSnapshotResponse* _response;
    google::protobuf::Closure* _done;
};

class LeaderStableClosure : public LogManager::StableClosure {
public:
    void Run();
private:
    LeaderStableClosure(const NodeId& node_id, CommitmentManager* commit_manager);
friend class NodeImpl;
    NodeId _node_id;
    CommitmentManager* _commit_manager;
};

class NodeImpl : public base::RefCountedThreadSafe<NodeImpl> {
friend class RaftServiceImpl;
public:
    NodeImpl(const GroupId& group_id, const PeerId& peer_id);

    NodeId node_id() const {
        return NodeId(_group_id, _server_id);
    }

    PeerId leader_id() {
        BAIDU_SCOPED_LOCK(_mutex);
        return _leader_id;
    }

    NodeStats stats();

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
    int handle_install_snapshot_request(baidu::rpc::Controller* controller,
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
    void on_snapshot_load_done();
    int on_snapshot_save_done(const SnapshotMeta& meta, SnapshotWriter* writer);

    // other func
    //
    // called when leader change configuration done, ref with FSMCaller
    void on_configuration_change_done(const EntryType type, const std::vector<PeerId>& peers);

    // called when leader recv greater term in AppendEntriesResponse, ref with Replicator
    int increase_term_to(int64_t new_term);

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

    // get last log term, through log and snapshot
    int64_t last_log_term();

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

    NodeOptions _options;
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

    raft_mutex_t _mutex;
    VoteCtx _vote_ctx; // candidate vote ctx
    VoteCtx _pre_vote_ctx; // prevote ctx
    ConfigurationCtx _conf_ctx;
    bthread_timer_t _election_timer; // follower -> candidate timer
    bthread_timer_t _vote_timer; // candidate retry timer
    bthread_timer_t _stepdown_timer; // leader check quorum node ok
    bthread_timer_t _snapshot_timer; // snapshot timer

    bool _snapshot_saving;
    SnapshotMeta* _loading_snapshot_meta;

    LogStorage* _log_storage;
    StableStorage* _stable_storage;
    SnapshotStorage* _snapshot_storage;
    ConfigurationManager* _config_manager;
    LogManager* _log_manager;
    FSMCaller* _fsm_caller;
    CommitmentManager* _commit_manager;
    ReplicatorGroup _replicator_group;
    std::vector<Closure*> _shutdown_continuations;
};

class NodeManager {
public:
    static NodeManager* GetInstance() {
        return Singleton<NodeManager>::get();
    }

    int start(const base::EndPoint& listen_addr,
             baidu::rpc::Server* server, baidu::rpc::ServerOptions* options);
    baidu::rpc::Server* stop(const base::EndPoint& listen_addr);

    // add raft node
    bool add(NodeImpl* node);

    // remove raft node
    bool remove(NodeImpl* node);

    // get node by group_id and peer_id
    scoped_refptr<NodeImpl> get(const GroupId& group_id, const PeerId& peer_id);

    // get all the nodes of |group_id|
    void get_nodes_by_group_id(const GroupId& group_id, 
                               std::vector<scoped_refptr<NodeImpl> >* nodes);

    void get_all_nodes(std::vector<scoped_refptr<NodeImpl> >* nodes);

private:
    NodeManager();
    ~NodeManager();
    DISALLOW_COPY_AND_ASSIGN(NodeManager);
    friend struct DefaultSingletonTraits<NodeManager>;
    
    // TODO(chenzhangyi01): replace std::map with FlatMap
    // To make implementation simplicity, we use two maps here, although
    // it works practically with only one GroupMap
    typedef std::map<NodeId, NodeImpl*> NodeMap;
    typedef std::multimap<GroupId, NodeImpl*> GroupMap;
    struct Maps {
        NodeMap node_map;
        GroupMap group_map;
    };
    // Functor to modify DBD
    static size_t _add_node(Maps&, const NodeImpl* node);
    static size_t _remove_node(Maps&, const NodeImpl* node);

    base::DoublyBufferedData<Maps> _nodes;

    baidu::rpc::Server* get_server(const base::EndPoint& ip_and_port);
    void add_server(const base::EndPoint& ip_and_port, baidu::rpc::Server* server);
    baidu::rpc::Server* remove_server(const base::EndPoint& ip_and_port);

    typedef std::map<base::EndPoint, baidu::rpc::Server*> ServerMap;
    raft_mutex_t _mutex;
    ServerMap _servers;
    std::set<base::EndPoint> _own_servers;
    RaftServiceImpl _service_impl;
};

}

#endif //~PUBLIC_RAFT_RAFT_NODE_H
