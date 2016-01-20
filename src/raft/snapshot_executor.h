// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/24 23:41:36

#ifndef  PUBLIC_RAFT_SNAPSHOT_EXECUTOR_H
#define  PUBLIC_RAFT_SNAPSHOT_EXECUTOR_H

#include <baidu/rpc/controller.h>

#include "raft/raft.h"
#include "raft/util.h"
#include "raft/snapshot.h"
#include "raft/storage.h"
#include "raft/raft.pb.h"
#include "raft/fsm_caller.h"
#include "raft/log_manager.h"

namespace raft {
class NodeImpl;

struct SnapshotExecutorOptions {
    SnapshotExecutorOptions();
    // URI of SnapshotStorage
    std::string uri;
    
    FSMCaller* fsm_caller;
    NodeImpl* node;
    LogManager* log_manager;
    int64_t init_term;
};

// Executing Snapshot related stuff
class BAIDU_CACHELINE_ALIGNMENT SnapshotExecutor {
    DISALLOW_COPY_AND_ASSIGN(SnapshotExecutor);
public:
    SnapshotExecutor();
    ~SnapshotExecutor();

    int init(const SnapshotExecutorOptions& options);

    // Return the owner NodeImpl
    NodeImpl* node() const { return _node; }

    // Start to snapshot StateMachine, and |done| is called after the execution
    // finishes or fails.
    void do_snapshot(Closure* done);

    // Install snapshot according to the very RPC from leader
    // After the installing succeeds (StateMachine is reset with the snapshot)
    // or fails, done will be called to respond
    // 
    // Errors:
    //  - Term dismatches: which happens interrupt_downloading_snapshot was 
    //    called before install_snapshot, indicating that this RPC was issued by
    //    the old leader.
    //  - Interrupted: happens when interrupt_downloading_snapshot is called or
    //    a new RPC with the same or newer snapshot arrives
    //  - Busy: the state machine is saving or loading snapshot
    void install_snapshot(baidu::rpc::Controller* controller,
                         const InstallSnapshotRequest* request,
                         InstallSnapshotResponse* response,
                         google::protobuf::Closure* done);

    // Interrupt the downloading if possible.
    // This is called when the term of node increased to |new_term|, which
    // happens when receiving RPC from new peer. In this case, it's hard to
    // determine whether to keep downloading snapshot as the new leader
    // possibly contains the missing logs and is going to send AppendEntries. To
    // make things simplicity and leader changing and snapshot installing are
    // both very rare. So we interrupt snapshot downloading when leader changes.
    // And let the new leader decide whether to install a new snapshot or
    // continue appending log entries.
    //
    // NOTE: we can't interrupt the snapshot insalling which has finsihed
    // downloading and is reseting the State Machine.
    void interrupt_downloading_snapshot(int64_t new_term);

    // Return true is this is currently installing a snapshot, either
    // downloading or loading.
    bool is_installing_snapshot() const { 
        // 1: acquire fence makes this thread sees the latest change when seeing
        //    the lastest _loading_snapshot
        // _downloading_snapshot is NULL when then downloading was successfully 
        // interrupted or installing has finished
        return _downloading_snapshot.load(boost::memory_order_acquire/*1*/);
    }

    // Return the backing snapshot storage
    SnapshotStorage* snapshot_storage() { return _snapshot_storage; }

    void describe(std::ostream& os, bool use_html);

private:
friend class SaveSnapshotDone;
friend class FirstSnapshotLoadDone;
friend class InstallSnapshotDone;

    void on_snapshot_load_done(int error_code, const std::string& error_text);
    int on_snapshot_save_done(int error_code, 
                              const SnapshotMeta& meta, 
                              SnapshotWriter* writer);

    struct DownloadingSnapshot {
        const InstallSnapshotRequest* request;
        InstallSnapshotResponse* response;
        baidu::rpc::Controller* cntl;
        google::protobuf::Closure* done;
    };

    int register_downloading_snapshot(DownloadingSnapshot* ds,
                                     int64_t* saved_version);
    int parse_install_snapshot_request(
            const InstallSnapshotRequest* request,
            SnapshotMeta* meta);
    void load_downloading_snapshot(DownloadingSnapshot* ds,
                                  const SnapshotMeta& meta,
                                  int64_t expected_version,
                                  SnapshotWriter* writer);

    raft_mutex_t _mutex;
    int64_t _last_snapshot_term;
    int64_t _last_snapshot_index;
    // TODO: unify _term and _version
    int64_t _term;
    int64_t _version;
    bool _saving_snapshot;
    bool _loading_snapshot;
    SnapshotStorage* _snapshot_storage;
    FSMCaller* _fsm_caller;
    NodeImpl* _node;
    LogManager* _log_manager;
    // The ownership of _downloading_snapshot is a little messy:
    // - Before we start to replace the FSM with the downloaded one. The
    //   ownership belongs with the downloding thread
    // - After we push the load task to FSMCaller, the ownership belongs to the
    //   closure which is called after the Snapshot replaces FSM
    boost::atomic<DownloadingSnapshot*> _downloading_snapshot;
    SnapshotMeta _loading_snapshot_meta;
};

inline SnapshotExecutorOptions::SnapshotExecutorOptions() 
    : fsm_caller(NULL)
    , node(NULL)
    , log_manager(NULL)
    , init_term(0)
{}

}  // namespace raft

#endif  // PUBLIC_RAFT_SNAPSHOT_EXECUTOR_H
