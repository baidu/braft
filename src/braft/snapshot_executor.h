// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  BRAFT_SNAPSHOT_EXECUTOR_H
#define  BRAFT_SNAPSHOT_EXECUTOR_H

#include <brpc/controller.h>

#include "braft/raft.h"
#include "braft/util.h"
#include "braft/snapshot.h"
#include "braft/storage.h"
#include "braft/raft.pb.h"
#include "braft/snapshot_service.pb.h"
#include "braft/fsm_caller.h"
#include "braft/log_manager.h"

namespace braft {
class NodeImpl;
class SnapshotHook;
class FileSystemAdaptor;

struct SnapshotExecutorOptions {
    SnapshotExecutorOptions();
    // URI of SnapshotStorage
    std::string uri;
   
    FSMCaller* fsm_caller;
    NodeImpl* node;
    LogManager* log_manager;
    int64_t init_term;
    butil::EndPoint addr;
    bool filter_before_copy_remote;
    bool usercode_in_pthread;
    bool copy_file = true;
    scoped_refptr<FileSystemAdaptor> file_system_adaptor;
    scoped_refptr<SnapshotThrottle> snapshot_throttle;

    bool learner_auto_load_applied_logs = true;
};

class SnapshotExecutor;
struct SnapshotFetcherCtx {
    bthread_t tid;
    bthread_id_t resource_id;
    int64_t max_index;
    GroupId group_id;
    PeerId peer_id;
    SnapshotExecutor* se;
    SnapshotWriter* writer;
    Closure* done;

    SnapshotFetcherCtx()
        : tid(INVALID_BTHREAD)
        , resource_id(INVALID_BTHREAD_ID)
        , max_index(INT64_MAX)
        , se(NULL)
        , writer(NULL)
        , done(NULL) {}
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
    void install_snapshot(brpc::Controller* controller,
                          const InstallSnapshotRequest* request,
                          InstallSnapshotResponse* response,
                          google::protobuf::Closure* done);

    // Fetch snapshot from remote node and restore.
    // Return 0 if fetch and restore successful.
    int fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                       const int64_t max_snapshot_index);
    void fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                        const int64_t max_snapshot_index,
                        SnapshotWriter* writer, Closure* done);
    void cancel_fetch_snapshot(const bool is_wait);

    void handle_tail_snapshot(brpc::Controller* cntl,
                              const TailSnapshotRequest* request,
                              TailSnapshotResponse* response);

    // Interrupt the downloading if possible.
    // This is called when the term of node increased to |new_term|, which
    // happens when receiving RPC from new peer. In this case, it's hard to
    // determine whether to keep downloading snapshot as the new leader
    // possibly contains the missing logs and is going to send AppendEntries. To
    // make things simplicity and leader changing during snapshot installing is 
    // very rare. So we interrupt snapshot downloading when leader changes, and
    // let the new leader decide whether to install a new snapshot or continue 
    // appending log entries.
    //
    // NOTE: we can't interrupt the snapshot insalling which has finsihed
    // downloading and is reseting the State Machine.
    void interrupt_downloading_snapshot(int64_t new_term);

    // Return true if this is currently installing a snapshot, either
    // downloading or loading.
    bool is_installing_snapshot() const { 
        // 1: acquire fence makes this thread sees the latest change when seeing
        //    the lastest _loading_snapshot
        // _downloading_snapshot is NULL when then downloading was successfully 
        // interrupted or installing has finished
        return _downloading_snapshot.load(butil::memory_order_acquire/*1*/);
    }

    // Return the backing snapshot storage
    SnapshotStorage* snapshot_storage() { return _snapshot_storage; }

    int64_t last_snapshot_index() {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        return _last_snapshot_index;
    }

    void describe(std::ostream& os, bool use_html);

    // Shutdown the SnapshotExecutor and all the following jobs would be refused
    void shutdown();

    // Block the current thread until all the running job finishes (including
    // failure)
    void join();

    int load_snapshot(int64_t commited_index, Closure* done);
    void check_consistency();
    butil::Status check_snapshot_consistency();

private:
friend class SaveSnapshotDone;
friend class FirstSnapshotLoadDone;
friend class SnapshotLoadDone;
friend class InstallSnapshotDone;
friend class FetchSnapshotDone;

    static void unlock_snapshot_fetcher_id(bthread_id_t context_id);
    int fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                       const int64_t snapshot_index,
                       const bthread_id_t& context_id,
                       bool load_snapshot);

    void on_snapshot_load_done(const butil::Status& st);
    int on_snapshot_save_done(const butil::Status& st,
                              const SnapshotMeta& meta,
                              SnapshotWriter* writer);
    void on_load_fetched_snapshot_done(const butil::Status& st);

    struct DownloadingSnapshot {
        const InstallSnapshotRequest* request;
        InstallSnapshotResponse* response;
        brpc::Controller* cntl;
        google::protobuf::Closure* done;
    };

    int register_downloading_snapshot(DownloadingSnapshot* ds);
    int parse_install_snapshot_request(
            const InstallSnapshotRequest* request,
            SnapshotMeta* meta);
    void load_downloading_snapshot(DownloadingSnapshot* ds,
                                  const SnapshotMeta& meta);
    int load_fetched_snapshot(const std::string& uri, bool load_snapshot);
    int start_reader_expire_timer();
    int reset_reader_expire_timer();
    void destroy_fetch_snapshot_reader();
    void unsafe_destroy_fetch_snapshot_reader();
    static void* run_snapshot_fetcher(void* arg);
    static void on_snapshot_reader_expired(void* arg);
    void report_error(int error_code, const char* fmt, ...);
    static bool destroy_snapshot_fetcher_id(const bthread_id_t& id);
    bool reset_snapshot_fetcher_id(const bthread_id_t& id);

    raft_mutex_t _mutex;
    int64_t _last_snapshot_term;
    int64_t _last_snapshot_index;
    int64_t _term;
    bool _saving_snapshot;
    bool _loading_snapshot;
    bool _stopped;
    bool _usercode_in_pthread;
    SnapshotStorage* _snapshot_storage;
    SnapshotCopier* _cur_copier;
    FSMCaller* _fsm_caller;
    NodeImpl* _node;
    LogManager* _log_manager;
    // The ownership of _downloading_snapshot is a little messy:
    // - Before we start to replace the FSM with the downloaded one. The
    //   ownership belongs with the downloding thread
    // - After we push the load task to FSMCaller, the ownership belongs to the
    //   closure which is called after the Snapshot replaces FSM
    butil::atomic<DownloadingSnapshot*> _downloading_snapshot;
    SnapshotMeta _loading_snapshot_meta;
    bthread::CountdownEvent _running_jobs;
    scoped_refptr<SnapshotThrottle> _snapshot_throttle;
    SnapshotReader* _fetch_snapshot_reader;
    bthread_timer_t _reader_expire_timer;
    bthread_id_t _snapshot_fetcher_id;
};

inline SnapshotExecutorOptions::SnapshotExecutorOptions() 
    : fsm_caller(NULL)
    , node(NULL)
    , log_manager(NULL)
    , init_term(0)
    , filter_before_copy_remote(false)
    , usercode_in_pthread(false)
{}

}  //  namespace braft

#endif  // BRAFT_SNAPSHOT_EXECUTOR_H
