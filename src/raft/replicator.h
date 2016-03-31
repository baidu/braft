// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/21 14:32:24

#ifndef  PUBLIC_RAFT_REPLICATOR_H
#define  PUBLIC_RAFT_REPLICATOR_H

#include <stdint.h>
#include <bthread.h>
#include <baidu/rpc/channel.h>                  // baidu::rpc::Channel
#include <baidu/rpc/controller.h>               // baidu::rpc::Controller
#include "raft/raft.h"
#include "raft/configuration.h"
#include "raft/raft.pb.h"
#include "raft/storage.h"

namespace raft {

class LogManager;
class CommitmentManager;
class NodeImpl;

struct ReplicatorOptions {
    ReplicatorOptions();
    int heartbeat_timeout_ms;
    GroupId group_id;
    PeerId server_id;
    PeerId peer_id;
    LogManager* log_manager;
    CommitmentManager* commit_manager;
    NodeImpl *node;
    int64_t term;
    SnapshotStorage* snapshot_storage;
};

typedef uint64_t ReplicatorId;

class CatchupClosure : public Closure {
public:
    virtual void Run() = 0;
protected:
    CatchupClosure()
        : _max_margin(0)
        , _timer(0)
        , _has_timer(false)
        , _error_was_set(false)
    {}
private:
friend class Replicator;
    int64_t _max_margin;
    bthread_timer_t _timer;
    bool _has_timer;
    bool _error_was_set;
    void _run();
};

class BAIDU_CACHELINE_ALIGNMENT Replicator {
public:
    Replicator();
    ~Replicator();
    // Called by the leader, otherwise the behavior is undefined
    // Start to replicate the log to the given follower
    static int start(const ReplicatorOptions&, ReplicatorId* id);

    // Called when the leader steps down, otherwise the behavior is undefined
    // Stop replicating
    static int stop(ReplicatorId);

    static int join(ReplicatorId);

    static int64_t last_response_timestamp(ReplicatorId id);

    // Wait until the margin between |last_log_index| from leader and the peer
    // is less than |max_margin| or error occurs. 
    // |done| can't be NULL and it is called after waiting fnishies.
    static void wait_for_caught_up(ReplicatorId, int64_t max_margin,
                                   const timespec* due_time,
                                   CatchupClosure* done);

private:

    int _prepare_entry(int offset, EntryMeta* em, base::IOBuf* data);
    void _wait_more_entries(long start_time_us);
    void _send_heartbeat();
    void _send_entries(long start_time_us);
    void _notify_on_caught_up(int error_code, bool);
    int _fill_common_fields(AppendEntriesRequest* request, int64_t prev_log_index);
    void _block(long start_time_us, int error_code);
    void _install_snapshot();

    static void _on_rpc_returned(ReplicatorId id, baidu::rpc::Controller* cntl,
                                 AppendEntriesRequest* request, 
                                 AppendEntriesResponse* response);
    static int _on_failed(bthread_id_t id, void* arg, int error_code);
    static int _continue_sending(void* arg, int error_code);
    static void* _run_on_caught_up(void*);
    static void _on_catch_up_timedout(void*);
    static void _on_block_timedout(void *arg);
    static void _on_install_snapshot_returned(
                ReplicatorId id, baidu::rpc::Controller* cntl,
                InstallSnapshotRequest* request, 
                InstallSnapshotResponse* response);

private:
    
    baidu::rpc::Channel _sending_channel;
    int64_t _next_index;
    baidu::rpc::CallId _rpc_in_fly;
    bthread_id_t _id;
    ReplicatorOptions _options;
    CatchupClosure *_catchup_closure;
    int64_t _last_response_timestamp;
    bool _installing_snapshot;
    int _consecutive_error_times;
};

struct ReplicatorGroupOptions {
    ReplicatorGroupOptions();
    int heartbeat_timeout_ms;
    LogManager* log_manager;
    CommitmentManager* commit_manager;
    NodeImpl* node;
    SnapshotStorage* snapshot_storage;
};

// Create a ReplicatorGroup when a candidate becomes leader and delete when it
// steps down.
// The methods of ReplicatorGroup are NOT thread-safe
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
    int wait_caughtup(const PeerId& peer, int64_t max_margin,
                      const timespec* due_time, CatchupClosure* done);

    int64_t last_response_timestamp(const PeerId& peer);

    // Stop all the replicators
    int stop_all();

    int stop_replicator(const PeerId &peer);

    // Reset the term of all to-add replicators.
    // This method is supposed to be called when the very candidate becomes the
    // leader, so we suppose that there are no running replicators.
    // Return 0 on success, -1 otherwise
    int reset_term(int64_t new_term);

    // Reset the interval of heartbeat
    // This method is supposed to be called when the very candidate becomes the
    // leader, use new heartbeat_interval, maybe call vote() reset election_timeout
    // Return 0 on success, -1 otherwise
    int reset_heartbeat_interval(int new_interval_ms);

    // Returns true if the there's a replicator attached to the given |peer|
    bool contains(const PeerId& peer) const;
private:


    int _add_replicator(const PeerId& peer, ReplicatorId *rid);

    std::map<PeerId, ReplicatorId> _rmap;
    ReplicatorOptions _common_options;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_REPLICATOR_H
