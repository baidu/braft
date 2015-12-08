/*
 * =====================================================================================
 *
 *       Filename:  raft.h
 *
 *    Description:  raft consensus replicate library
 *
 *        Version:  1.0
 *        Created:  2015/09/16 16:54:30
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_RAFT_H
#define PUBLIC_RAFT_RAFT_H

#include <string>
#include <gflags/gflags.h>

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

class StateMachine {
public:
    // user defined logentry proc function
    // [NOTE] index: realize follower read strong consistency
    // [NOTE] done: on_apply return some result to done, user call it when finish
    virtual void on_apply(const base::IOBuf& buf, const int64_t index, Closure* done) = 0;

    // user define shutdown function
    virtual void on_shutdown() = 0;

    // user defined snapshot generate function, this method will block on_apply.
    // user can make snapshot async when fsm can be cow(copy-on-write).
    // call done->Run() when snapshot finised.
    // success return 0, fail return errno
    virtual int on_snapshot_save(SnapshotWriter* writer, Closure* done);

    // user defined snapshot load function
    // get and load snapshot
    // success return 0, fail return errno
    virtual int on_snapshot_load(SnapshotReader* reader);

    // user defined leader start function
    // [NOTE] user can direct append to node ignore this callback.
    //        this callback can sure read consistency, after leader's first NO_OP committed
    virtual void on_leader_start();

    // user defined leader start function
    // [NOTE] this method called immediately when leader stepdown,
    //        maybe before some method: apply success on_apply or fail done.
    //        user sure resource available.
    virtual void on_leader_stop();
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

struct NodeStats {
    State state;
    int64_t term;
    int64_t last_log_index;
    int64_t last_log_term;
    int64_t committed_index;
    int64_t applied_index;
    int64_t last_snapshot_index;
    int64_t last_snapshot_term;
    Configuration configuration;

    NodeStats() : state(SHUTDOWN), term(0), last_log_index(0), last_log_term(0),
            committed_index(0), applied_index(0), last_snapshot_index(0), last_snapshot_term(0) {
    }
};

struct NodeOptions {
    int election_timeout; //ms, follower to candidate timeout
    int snapshot_interval; // s, snapshot interval. 0 is disable internal snapshot timer
    bool enable_pipeline; // pipeline switch
    Configuration conf; // peer conf
    StateMachine* fsm; // user defined function [MUST]
    std::string log_uri;
    std::string stable_uri;
    std::string snapshot_uri;

    NodeOptions()
        : election_timeout(1000),
        snapshot_interval(86400),
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

    // init node
    int init(const NodeOptions& options);

    // get stats
    NodeStats stats();

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    // [NOTE] code after apply can't access resource in done
    void shutdown(Closure* done);

    // apply data to replicated-state-machine [thread-safe]
    // done is user defined function, maybe response to client, transform to on_applied
    // [NOTE] code after apply can't access resource in done
    void apply(const base::IOBuf& data, Closure* done);

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
