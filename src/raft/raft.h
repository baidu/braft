// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/16 16:54:30
 
#ifndef PUBLIC_RAFT_RAFT_H
#define PUBLIC_RAFT_RAFT_H

#include <string>

#include <base/logging.h>
#include <base/iobuf.h>
#include <base/status.h>
#include "raft/configuration.h"
#include "raft/enum.pb.h"
#include "raft/errno.pb.h"

#ifdef RAFT_ENABLE_ROCKSDB_STORAGE
#include <raft/rocksdb.h>
#endif // RAFT_ENABLE_ROCKSDB_STORAGE

template <typename T> class scoped_refptr;

namespace baidu {
namespace rpc {
class Server;
}  // namespae rpc
}  // namespace baidu

namespace raft {

class SnapshotWriter;
class SnapshotReader;
class SnapshotHook;
class LeaderChangeContext;
class FileSystemAdaptor;

static const PeerId ANY_PEER(base::EndPoint(base::IP_ANY, 0), 0);

// Raft-specific closure which encloses a base::Status to report if the
// operation was successful.
class Closure : public google::protobuf::Closure {
public:
    base::Status& status() { return _st; }
    const base::Status& status() const { return _st; }
    
private:
    base::Status _st;
};

// Describe a specific error
class Error {
public:
    Error() : _type(ERROR_TYPE_NONE) {}
    Error(const Error& e) : _type(e._type), _st(e._st) {}
    ErrorType type() const { return _type; }
    const base::Status& status() const { return _st; }
    base::Status& status() { return _st; }
    void set_type(ErrorType type) { _type = type; }

    Error& operator=(const Error& rhs) {
        _type = rhs._type;
        _st = rhs._st;
        return *this;
    }
private:
    // Intentionally copyable
    ErrorType _type;
    base::Status _st;
};

inline const char* errortype2str(ErrorType t) {
    switch (t) {
    case ERROR_TYPE_NONE:
        return "None";
    case ERROR_TYPE_LOG:
        return "LogError";
    case ERROR_TYPE_STABLE:
        return "StableError";
    case ERROR_TYPE_SNAPSHOT:
        return "SnapshotError";
    case ERROR_TYPE_STATE_MACHINE:
        return "StateMachineError";
    }
    return "Unknown";
}

inline std::ostream& operator<<(std::ostream& os, const Error& e) {
    os << "{type=" << errortype2str(e.type()) 
       << ", error_code=" << e.status().error_code()
       << ", error_text=`" << e.status().error_cstr()
       << "'}";
    return os;
}

// Basic message structure of libraft
struct Task {
    Task() : data(NULL), done(NULL), expected_term(-1) {}

    // The data applied to StateMachine
    base::IOBuf* data;

    // Continuation when the data is applied to StateMachine or error occurs.
    Closure* done;

    // Reject this task if expected_term doesn't match the current term of
    // this Node if the value is not -1
    // Default: -1
    int64_t expected_term;
};

class IteratorImpl;

// Iterator over a batch of committed tasks
//
// Example:
// void YouStateMachine::on_apply(raft::Iterator& iter) {
//     for (; iter.valid(); iter.next()) {
//         baidu::rpc::ClosureGuard done_guard(iter.done());
//         process(iter.data());
//     }
// }
class Iterator {
    DISALLOW_COPY_AND_ASSIGN(Iterator);
public:
    // Move to the next task.
    void next();

    // Return a unique and monotonically increasing identifier of the current 
    // task:
    //  - Uniqueness guarantees that committed tasks in different peers with 
    //    the same index are always the same and kept unchanged.
    //  - Monotonicity guarantees that for any index pair i, j (i < j), task 
    //    at index |i| must be applied before task at index |j| in all the 
    //    peers from the group.
    int64_t index() const;

    // Returns the term of the leader which to task was applied to.
    int64_t term() const;

    // Return the data whose content is the same as what was passed to
    // Node::apply in the leader node.
    const base::IOBuf& data() const;

    // If done() is non-NULL, you must call done()->Run() after applying this
    // task no matter this operation succeeds or fails, otherwise the
    // corresponding resources would leak.
    //
    // If this task is proposed by this Node when it was the leader of this 
    // group and the leadership has not changed before this point, done() is 
    // exactly what was passed to Node::apply which may stand for some 
    // continuation (such as respond to the client) after updating the 
    // StateMachine with the given task. Otherweise done() must be NULL.
    Closure* done() const;

    // Return true this iterator is currently references to a valid task, false
    // otherwise, indicating that the iterator has reached the end of this
    // batch of tasks or some error has occured
    bool valid() const;

    // Invoked when some critical error occured. And we will consider the last 
    // |ntail| tasks (starting from the last iterated one) as not applied. After
    // this point, no futher changes on the StateMachine as well as the Node 
    // would be allowed and you should try to repair this replica or just drop 
    // it.
    //
    // If |st| is not NULL, it should describe the detail of the error.
    void set_error_and_rollback(size_t ntail = 1, const base::Status* st = NULL);

private:
friend class FSMCaller;
    Iterator(IteratorImpl* impl) : _impl(impl) {}
    ~Iterator() {};

    // The ownership of _impl belongs to FSMCaller;
    IteratorImpl* _impl;
};

// |StateMachine| is the sink of all the events of a very raft node.
// Implement a specific StateMachine for your own bussiness logic.
//
// NOTE: All the interfaces are not guaranteed to be thread safe and they are 
// called sequentially, saying that every single operation will block all the 
// following ones.
class StateMachine {
public:
    virtual ~StateMachine();

    // Update the StateMachine with a batch a tasks that you can access
    // through |iterator|.
    //
    // Invoked when one or more tasks that were passed to Node::apply have been
    // committed to the raft group (quorum of the group peers have received 
    // those tasks and stored them on the backing storage).
    //
    // Once this function returns to the caller, we will regard all the iterated
    // tasks through |iter| have been successfully applied. And if you didn't
    // apply all the the given tasks, we would regard this as a critical error
    // and report a error whose type is ERROR_TYPE_STATE_MACHINE.
    virtual void on_apply(::raft::Iterator& iter) = 0;

    // Invoked once when the raft node was shut down.
    // Default do nothing
    virtual void on_shutdown();

    // user defined snapshot generate function, this method will block on_apply.
    // user can make snapshot async when fsm can be cow(copy-on-write).
    // call done->Run() when snapshot finised.
    // success return 0, fail return errno
    // Default: Save nothing and returns error, no log would be compacted
    virtual void on_snapshot_save(::raft::SnapshotWriter* writer,
                                  ::raft::Closure* done);

    // user defined snapshot load function
    // get and load snapshot
    // success return 0, fail return errno
    // Default: Load nothing and returns error.
    virtual int on_snapshot_load(::raft::SnapshotReader* reader);

    // user defined leader start function
    // [NOTE] user can direct append to node ignore this callback.
    //        this callback can ensure read-consistency, after leader's first NO_OP committed
    // Default: did nothing
    virtual void on_leader_start();
    virtual void on_leader_start(int64_t term);

    // user defined leader start function
    // [NOTE] this method called immediately when leader stepdown,
    //        maybe before some method: apply success on_apply or fail done.
    //        user sure resource available.
    virtual void on_leader_stop();
    virtual void on_leader_stop(const base::Status& status);

    // on_error is called when  
    virtual void on_error(const ::raft::Error& e);

    // Invoked when a configuration has been committed to the group
    virtual void on_configuration_committed(const ::raft::Configuration& conf);

    // this method is called when a follower stops following a leader and its leader_id becomes NULL,
    // situations including: 
    // 1. handle election_timeout and start pre_vote 
    // 2. receive requests with higher term such as vote_request from a candidate
    // or append_entires_request from a new leader
    // 3. receive timeout_now_request from current leader and start request_vote
    // the parameter stop_following_context gives the information(leader_id, term and status) about the
    // very leader whom the follower followed before.
    // User can reset the node's information as it stops following some leader. 
    virtual void on_stop_following(const ::raft::LeaderChangeContext& stop_following_context);

    // this method is called when a follower or candidate starts following a leader and its leader_id
    // (should be NULL before the method is called) is set to the leader's id,
    // situations including:
    // 1. a candidate receives append_entries from a leader
    // 2. a follower(without leader) receives append_entries from a leader
    // the parameter start_following_context gives the information(leader_id, term and status) about 
    // the very leader whom the follower starts to follow.
    // User can reset the node's information as it starts to follow some leader.
    virtual void on_start_following(const ::raft::LeaderChangeContext& start_following_context);

};

enum State {
    // Don't change the order if you are not sure about the usage.
    STATE_LEADER = 1,
    STATE_TRANSFERING = 2,
    STATE_CANDIDATE = 3,
    STATE_FOLLOWER = 4,
    STATE_ERROR = 5,
    STATE_UNINITIALIZED = 6,
    STATE_SHUTTING = 7,
    STATE_SHUTDOWN = 8,
    STATE_END,
};

inline const char* state2str(State state) {
    const char* str[] = {"LEADER", "TRANSFERING", "CANDIDATE", "FOLLOWER", 
                         "ERROR", "UNINITIALIZED", "SHUTTING", "SHUTDOWN", };
    if (state < STATE_END) {
        return str[(int)state - 1];
    } else {
        return "UNKNOWN";
    }
}

// Return true if |s| indicates the node is active
inline bool is_active_state(State s) {
    // This should be as fast as possible
    return s < STATE_ERROR;
}

// This class encapsulates the parameter of on_start_following and on_stop_following interfaces.
class LeaderChangeContext {
    DISALLOW_COPY_AND_ASSIGN(LeaderChangeContext);
public:
    LeaderChangeContext(const PeerId& leader_id, int64_t term, const base::Status& status)
        : _leader_id(leader_id)
        , _term(term) 
        , _st(status)
    {};
    // for on_start_following, the leader_id and term are of the new leader;
    // for on_stop_following, the leader_id and term are of the old leader.
    const PeerId& leader_id() const { return _leader_id; }
    int64_t term() const { return _term; }
    // return the information about why on_start_following or on_stop_following is called.
    const base::Status& status() const { return _st; }
        
private:
    PeerId _leader_id;
    int64_t _term;
    base::Status _st;
};

inline std::ostream& operator<<(std::ostream& os, const LeaderChangeContext& context) {
    os << "{leader_id=" << context.leader_id()
       << ", term=" << context.term() << ", error_code=" << context.status().error_code()
       << ", error_text=" << context.status().error_cstr()
       << "}";
    return os;
}

struct NodeOptions {
    // A follower would become a candidate if it doesn't receive any message 
    // from the leader in |election_timeout_ms| milliseconds
    // Default: 1000 (1s)
    int election_timeout_ms; //follower to candidate timeout

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds
    // if this was reset as a positive number
    // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
    //
    // Default: 3600 (1 hour)
    int snapshot_interval_s;

    // We will regard a adding peer as caught up if the margin between the
    // last_log_index of this peer and the last_log_index of leader is less than
    // |catchup_margin|
    //
    // Default: 1000
    int catchup_margin;

    // If |pipelined_replication| is true, leader will replicate following log
    // entries through network to followers before receiving the ack of previous 
    // ones. 
    //
    // Default: false
    bool pipelined_replication;

    // If node is starting from a empty environment (both LogStorage and
    // SnapshotStorage are empty), it would use |initial_conf| as the
    // configuration of the group, otherwise it would load configuration from
    // the existing environment.
    //
    // Default: A empty group
    Configuration initial_conf;

    // The specific StateMachine implemented your bussiness logic, which must be
    // a valid instance.
    StateMachine* fsm;

    // If |node_owns_fsm| is true. |fms| would be destroyed when the backing
    // Node is no longer referenced.
    //
    // Default: false
    bool node_owns_fsm;

    // Run the user callbacks and user closures in pthread rather than bthread
    // 
    // Default: false
    bool usercode_in_pthread;

    // Describe a specific LogStorage in format ${type}://${parameters}
    std::string log_uri;

    // Describe a specific StableStorage in format ${type}://${parameters}
    std::string stable_uri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    std::string snapshot_uri;

    // If enable, we will filter duplicate files before copy remote snapshot,
    // to avoid useless transmission. Two files in local and remote are duplicate,
    // only if they has the same filename and the same checksum (stored in file meta).
    // Default: false
    bool filter_before_copy_remote;

    // If non-null, we will pass this snapshot_file_system_adaptor to SnapshotStorage
    // Default: NULL
    scoped_refptr<FileSystemAdaptor> *snapshot_file_system_adaptor;

    // Construct a default instance
    NodeOptions();
};

inline NodeOptions::NodeOptions() 
    : election_timeout_ms(1000)
    , snapshot_interval_s(3600)
    , catchup_margin(1000)
    , pipelined_replication(false)
    , fsm(NULL)
    , node_owns_fsm(false)
    , usercode_in_pthread(false)
    , filter_before_copy_remote(false)
    , snapshot_file_system_adaptor(NULL)
{}

class NodeImpl;
class Node {
public:
    Node(const GroupId& group_id, const PeerId& peer_id);
    virtual ~Node();

    // get node id
    NodeId node_id();

    // get leader PeerId, for redirect
    PeerId leader_id();

    // Return true if this is the leader of the belonging group
    bool is_leader();

    // init node
    int init(const NodeOptions& options);

    // shutdown local replica.
    // done is user defined function, maybe response to client or clean some resource
    // [NOTE] code after apply can't access resource in done
    void shutdown(Closure* done);

    // Block the thread until the node is successfully stopped.
    void join();

    // [Thread-safe and wait-free]
    // apply task to the replicated-state-machine
    //
    // About the ownership:
    // |task.data|: for the performance consideration, we will take away the 
    //              content. If you want keep the content, copy it before call
    //              this function
    // |task.done|: If the data is successfully committed to the raft group. We
    //              will pass the ownership to StateMachine::on_apply.
    //              Otherwise we will specify the error and call it.
    //
    void apply(const Task& task);

    // list peers of this raft group, only leader retruns ok
    // [NOTE] when list_peers concurrency with add_peer/remove_peer, maybe return peers is staled.
    // because add_peer/remove_peer immediately modify configuration in memory
    base::Status list_peers(std::vector<PeerId>* peers);

    // add peer to replicated-state-machine [thread-safe]
    // done is user defined function, maybe response to client
    // [NOTE] code after apply can't access resource in done
    void add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done);

    // remove peer from replicated-state-machine [thread-safe]
    // done is user defined function, maybe response to client
    // [NOTE] code after apply can't access resource in done
    void remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done);

    // set peer to local replica [thread-safe]
    // done is user defined function, maybe response to client
    // only used in major node is down, reduce peerset to make group available
    int set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers);
    int set_peer(const std::vector<PeerId>& new_peers);

    // user trigger snapshot
    // done is user defined function, maybe response to client
    void snapshot(Closure* done);

    // user trigger vote
    // reset election_timeout, suggest some peer to become the leader in a
    // higher probability
    void vote(int election_timeout);

    // Try transfering leadership to |peer|.
    // If peer is ANY_PEER, we will choose a peer with the largest last_log_id
    // among peers in |current_conf| to be the possible candidate.
    // The definition of ANY_PEER is:
    // addr.ip == 0.0.0.0 && addr.port == 0 && idx == 0
    // Returns 0 on success, -1 otherwise.
    int transfer_leadership_to(const PeerId& peer);

private:
    NodeImpl* _impl;
};

// Attach raft services to |server|, this makes the raft services share the same
// listen address with the user services.
//
// NOTE: Now we only allow the backing Server to be started with a specific
// listen address, if the Server is going to be started from a range of ports, 
// the behavior is undefined.
// Returns 0 on success, -1 otherwise.
int add_service(baidu::rpc::Server* server, const base::EndPoint& listen_addr);
int add_service(baidu::rpc::Server* server, int port);
int add_service(baidu::rpc::Server* server, const char* listen_ip_and_port);

}  // namespace raft

#endif //~PUBLIC_RAFT_RAFT_H
