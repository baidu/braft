// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/16 16:54:30
 
#ifndef PUBLIC_RAFT_RAFT_H
#define PUBLIC_RAFT_RAFT_H

#include <string>
#include <gflags/gflags.h>

#include <base/logging.h>
#include <base/iobuf.h>
#include "raft/configuration.h"

namespace baidu {
namespace rpc {
class Server;
class ServerOptions;
}
}

namespace raft {

class LogEntry;
class SnapshotWriter;
class SnapshotReader;

class Closure: public google::protobuf::Closure {
public:
    Closure() : _err_code(0) {}
    virtual ~Closure() {}

    void set_error(int err_code, const char* reason_fmt, ...);
    void set_error(int err_code, const std::string &error_text);

    virtual void Run() = 0;
protected:
    int _err_code;
    std::string _err_text;
};

// Basic message structure of libraft
struct Task {
    Task() : data(NULL), done(NULL) {}

    // The data applied to StateMachine
    base::IOBuf* data;

    // Continuation when the data is applied to StateMachine or error occurs.
    // Only valid when the belonging node is the leader of this group.
    Closure* done;
};

// |StateMachine| is the sink of all the events of a very raft node.
// Implement a specific StateMachine for your own bussiness logic.
//
// NOTE: All the interfaces are called sequentially, saying that every single
// operation will block all the following 
//
class StateMachine {
public:

    // Update the StateMachine with |task|
    //
    // Called when the task passed to Node::apply has been committed to the raft
    // group (quorum of the cluster have received this task and stored it in the
    // storage). If the belonging node is the leader of the group, |task->done| 
    // is excactly what was passed to Node::apply which may stand for some
    // continuation after updating the StateMachine, otherwise |task->done| 
    // must be NULL and you have nothing to do besides updating the StateMachine
    // 
    // 1: |index| stands for the universally unique and monotonically increasing 
    //    identifier of |task|.
    //  - Uniqueness guarantees that committed tasks in different peers with 
    //    the same index are always the same and unchanged.
    //  - Monotonicity guarantees that for any index pair i, j (i < j), task at
    //    index |i| must be applied before task at index |j| in all the peers 
    //    from the group.
    //
    // 2: About the ownership:
    //  - |task.buf| is readonly, users should not modify the content
    //  - |task.done| belongs to users.
    //
    virtual void on_apply(const int64_t index/*1*/, const Task& task/*2*/) = 0;

    // Like on_apply, except for that we call this function with a sequence of
    // tasks. |first_index| is the index of the first task.
    //
    // We guarantee that |size| is always a positive number.
    //
    // NOTE: This interface has default implementation. If you want to imporve
    // the perfromance, you can override it.
    //
    virtual void on_apply_in_batch(const int64_t first_index, 
                                   const Task tasks[],
                                   size_t size) {
        for (size_t i = 0; i < size; ++i) {
            on_apply(first_index + i, tasks[i]);
        }
    }

    // Called once when the raft node shut down.
    virtual void on_shutdown() {}

    // user defined snapshot generate function, this method will block on_apply.
    // user can make snapshot async when fsm can be cow(copy-on-write).
    // call done->Run() when snapshot finised.
    // success return 0, fail return errno
    virtual void on_snapshot_save(SnapshotWriter* writer, Closure* done) {
        (void)writer;
        CHECK(done);
        done->set_error(-1, "Not implemented");
    }

    // user defined snapshot load function
    // get and load snapshot
    // success return 0, fail return errno
    virtual int on_snapshot_load(SnapshotReader* reader) {
        (void)reader;
        LOG(ERROR) << "Not implemented";
        return -1;
    }

    // user defined leader start function
    // [NOTE] user can direct append to node ignore this callback.
    //        this callback can sure read consistency, after leader's first NO_OP committed
    virtual void on_leader_start() {}

    // user defined leader start function
    // [NOTE] this method called immediately when leader stepdown,
    //        maybe before some method: apply success on_apply or fail done.
    //        user sure resource available.
    virtual void on_leader_stop() {}

    virtual ~StateMachine() {}

};

enum State {
    LEADER = 1,
    CANDIDATE = 2,
    FOLLOWER = 3,
    SHUTDOWN = 4,
    SHUTTING = 5,
    STATE_END = 6,
};

inline const char* state2str(State state) {
    const char* str[] = {"LEADER", "CANDIDATE", "FOLLOWER", "SHUTDOWN", "SHUTTING"};
    if (state < STATE_END) {
        return str[(int)state - 1];
    } else {
        return "UNKNOWN";
    }
}

inline bool is_active_state(State s) {
    // This should be as fast as possible
    switch (s) {
    case LEADER:
    case CANDIDATE:
    case FOLLOWER:
        return true;
    default:
        return false;
    }
}

struct NodeOptions {
    int election_timeout; //ms, follower to candidate timeout
    int snapshot_interval; // s, snapshot interval. 0 is disable internal snapshot timer
    int catchup_margin; // catchup margin to judge finish
    bool enable_pipeline; // pipeline switch
    Configuration conf; // peer conf
    StateMachine* fsm; // user defined function [MUST]
    std::string log_uri;
    std::string stable_uri;
    std::string snapshot_uri;

    NodeOptions()
        : election_timeout(1000),
        snapshot_interval(86400),
        catchup_margin(1000),
        enable_pipeline(false),
        fsm(NULL) {}
};

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

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    // [NOTE] code after apply can't access resource in done
    void shutdown(Closure* done);

    // [Thread-safe and wait-free]
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

    // user trigger snapshot
    // done is user defined function, maybe response to client
    void snapshot(Closure* done);

private:
    NodeImpl* _impl;
};

// start raft server, MUST assign listen_addr or server_desc.
// server and options can be NULL, create internal baidu::rpc::Server
int start_raft(const base::EndPoint& listen_addr,
              baidu::rpc::Server* server, baidu::rpc::ServerOptions* options);
int start_raft(const char* server_desc,
              baidu::rpc::Server* server, baidu::rpc::ServerOptions* options);
// stop raft server
int stop_raft(const base::EndPoint& listen_addr, baidu::rpc::Server** server);
int stop_raft(const char* server_desc, baidu::rpc::Server** server_ptr);

};

#endif //~PUBLIC_RAFT_RAFT_H
