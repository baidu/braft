// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/21 14:32:24

#ifndef  PUBLIC_RAFT_REPLICATOR_H
#define  PUBLIC_RAFT_REPLICATOR_H

#include <stdint.h>
#include <bthread.h>
#include <baidu/rpc/channel.h>                  // baidu::rpc::Controller
#include <baidu/rpc/controller.h>               // baidu::rpc::Controller
#include "raft/configuration.h"
#include "raft/raft.pb.h"

namespace raft {

class LogManager;
class CommitmentManager;
class NodeImpl;

struct ReplicatorOptions {
    ReplicatorOptions();
    int64_t heartbeat_timeout_ms;
    GroupId group_id;
    PeerId server_id;
    PeerId peer_id;
    LogManager* log_manager;
    CommitmentManager* commit_manager;
    NodeImpl *node;
    int64_t term;
};

typedef uint64_t ReplicatorId;

class Closure;
struct OnCaughtUp {
    OnCaughtUp();

    void (*on_caught_up)(
        void* arg, const PeerId &pid,
        int error_code, Closure* done);
    
    // default: NULL
    void* arg;

    // default: NULL
    Closure* done;

    // The minimal margin between |last_log_index| from leader and the very peer.
    // default: 0
    uint32_t min_margin;
private:
friend class Replicator;
    PeerId pid;
    bthread_timer_t timer;
    bool has_timer;
    int error_code;
    void _run();
};

class Replicator {
public:
    Replicator();
    ~Replicator();
    // Called by the leader, otherwise the behavior is undefined
    // Start to replicate the log to the given follower
    static int start(const ReplicatorOptions&, ReplicatorId* id);

    // Called by leader, otherwise the behavior is undefined.
    // Let the replicator stop replicating to the very follower, but send
    // hearbeat message to avoid the follower becoming a candidate.
    // This method should be called When the very follower is about to be
    // removed (has not been committed), it's not necessary to replicate the 
    // logs since the removing task. 
    static int stop_appending_after(ReplicatorId, int64_t log_index);

    // Called when the leader steps down, otherwise the behavior is undefined
    // Stop replicating
    static int stop(ReplicatorId);

    static int join(ReplicatorId);

    static int64_t last_response_timestamp(ReplicatorId id);

    // Called on_caugth_up when the last_log_index of the very follower reaches
    // |baseline| or error occurs (timedout or the replicator quits)
    static void wait_for_caught_up(ReplicatorId, const OnCaughtUp& on_caugth_up,
                                   const timespec* due_time);

private:

    int _prepare_entry(int offset, EntryMeta* em, base::IOBuf *data);
    void _wait_more_entries(long start_time_us);
    void _send_heartbeat();
    void _send_entries(long start_time_us);
    void _notify_on_caught_up(int error_code, bool);
    void _fill_common_fields(AppendEntriesRequest*);

    static void _on_rpc_returned(ReplicatorId id, baidu::rpc::Controller* cntl,
                                 AppendEntriesRequest* request, 
                                 AppendEntriesResponse* response);
    static int _on_failed(bthread_id_t id, void* arg, int error_code);
    static int _continue_sending(void* arg, int error_code);
    static void* _run_on_caught_up(void*);
    static void _on_catch_up_timedout(void*);

private:
    
    baidu::rpc::Channel _sending_channel;
    int64_t _next_index;
    baidu::rpc::CallId _rpc_in_fly;
    bthread_id_t _id;
    ReplicatorOptions _options;
    OnCaughtUp *_on_caught_up;
    int64_t _last_response_timestamp;
};

struct ReplicatorGroupOptions {
    ReplicatorGroupOptions();
    int64_t heartbeat_timeout_ms;
    LogManager* log_manager;
    CommitmentManager* commit_manager;
    NodeImpl* node;
    int64_t term;
};

// Create a ReplicatorGroup when a candidate becomes leader and delete when it
// steps down.
// The methods of ReplicatorGroup are NOTthread-safe
class ReplicatorGroup {
public:
    ReplicatorGroup();
    ~ReplicatorGroup();
    int init(const NodeId& node_id, const ReplicatorGroupOptions&);
    
    // Add a replicator attached with |peer|
    // will be a notification when the replicator catches up according to the
    // arguments.
    // NOTE: when calling this function, the replicatos starts to work
    // immediately, annd might call node->step_down which might have race with
    // the caller, you should deal with this situation.
    int add_replicator(const PeerId &peer);
    
    // wait the very peer catchup
    int wait_caughtup(const PeerId& peer, const OnCaughtUp& on_caught_up,
                      const timespec* due_time);

    int64_t last_response_timestamp(const PeerId& peer);

    // Stop all the replicators
    int stop_all();

    int stop_replicator(const PeerId &peer);

    int stop_appending_after(const PeerId &peer, int64_t log_index);
private:

    int _add_replicator(const PeerId& peer, ReplicatorId *rid);

    std::map<PeerId, ReplicatorId> _rmap;
    ReplicatorOptions _common_options;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_REPLICATOR_H
