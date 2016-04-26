// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/12 15:35:41

#ifndef  PUBLIC_RAFT_LOG_MANAGER_H
#define  PUBLIC_RAFT_LOG_MANAGER_H

#include <base/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <base/containers/flat_map.h>           // base::FlatMap
#include <deque>                                // std::deque
#include <bthread/execution_queue.h>            // bthread::ExecutionQueueId

#include "raft/raft.h"                          // Closure
#include "raft/util.h"                          // raft_mutex_t
#include "raft/log_entry.h"                     // LogEntry
#include "raft/configuration_manager.h"         // ConfigurationManager

namespace raft {

class LogStorage;

struct LogManagerOptions {
    LogManagerOptions();
    LogStorage* log_storage;
    ConfigurationManager* configuration_manager;
};

class NodeImpl;
class SnapshotMeta;

class BAIDU_CACHELINE_ALIGNMENT LogManager {
public:
    typedef int64_t WaitId;

    class StableClosure : public Closure {
    public:
        StableClosure() : _first_log_index(0) {}
    protected:
        int64_t _first_log_index;
    private:
    friend class LogManager;
    friend class AppendBatcher;
        std::vector<LogEntry*> _entries;
    };

    LogManager();
    ~LogManager();
    int init(const LogManagerOptions& options);

    void shutdown();

    // Append log entry vector and wait until it's stable (NOT COMMITTED!)
    // success return 0, fail return errno
    void append_entries(std::vector<LogEntry*> *entries, StableClosure* done);

    // Notify the log manager about the latest snapshot, which indicates the
    // logs which can be safely truncated.
    void set_snapshot(const SnapshotMeta* meta);

    // We don't delete all the logs before last snapshot to avoid installing
    // snapshot on slow replica. Call this method to drop all the logs before
    // last snapshot immediately.
    void clear_bufferred_logs();

    // Get the log at |index|
    // Returns:
    //  success return ptr, fail return null
    LogEntry* get_entry(const int64_t index);

    // Get the log term at |index|
    // Returns:
    //  success return term > 0, fail return 0
    int64_t get_term(const int64_t index);

    // Get the first log index of log
    // Returns:
    //  success return first log index, empty return 0
    int64_t first_log_index();

    // Get the last log index of log
    // Returns:
    //  success return last memory and logstorage index, empty return 0
    int64_t last_log_index(bool is_flush = false);
    
    // Return the id the last log.
    LogId last_log_id(bool is_flush = false);

    void get_configuration(const int64_t index, ConfigurationPair* conf);

    // Check if |current| should be updated to the latest configuration
    // Returns true and |current| is assigned to the lastest configuration, returns
    // false otherweise
    // FIXME: It's not ABA free
    bool check_and_set_configuration(ConfigurationPair* current);

    // Wait until there are more logs since |last_log_index| and |on_new_log| 
    // would be called after there are new logs or error occurs
    WaitId wait(int64_t expected_last_log_index,
                int (*on_new_log)(void *arg, int error_code), void *arg);

    // Remove a waiter
    // Returns:
    //  - 0: success
    //  - -1: id is Invalid
    int remove_waiter(WaitId id);
    
    
    // Set the applied id, indicating that the log and all the previose ones
    // can be droped from memory logs
    void set_applied_id(const LogId& applied_id);

    // get disk id, call some case need sync log_id, e.g. pre_vote/vote request and response
    LogId get_disk_id();

    void describe(std::ostream& os, bool use_html);

private:
friend class AppendBatcher;
    struct WaitMeta {
        int (*on_new_log)(void *arg, int error_code);
        void* arg;
        int error_code;
    };

    void append_to_storage(std::vector<LogEntry*>* to_append, LogId* last_id);

    static int disk_thread(void* meta,
                           bthread::TaskIterator<StableClosure*>& iter);
    
    // delete logs from storage's head, [1, first_index_kept) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_prefix(const int64_t first_index_kept,
                        std::unique_lock<raft_mutex_t>& lck);
    
    int reset(const int64_t next_log_index,
              std::unique_lock<raft_mutex_t>& lck);

    // Must be called in the disk thread, otherwise the
    // behavior is undefined
    void set_disk_id(const LogId& disk_id);

    LogEntry* get_entry_from_memory(const int64_t index);

    WaitId notify_on_new_log(int64_t expected_last_log_index, WaitMeta* wm);

    int check_and_resolve_confliction(std::vector<LogEntry*>* entries, 
                                      StableClosure* done);

    void unsafe_truncate_suffix(const int64_t last_index_kept);

    // Clear the logs in memory whose id <= the given |id|
    void clear_memory_logs(const LogId& id);

    int64_t unsafe_get_term(const int64_t index);

    // Start a independent thread to append log to LogStorage
    int start_disk_thread();
    int stop_disk_thread();

    void wakeup_all_waiter(std::unique_lock<raft_mutex_t>& lck);
    static void *run_on_new_log(void* arg);

    // Fast implementation with one lock
    // TODO(chenzhangyi01): reduce the critical section
    LogStorage* _log_storage;
    ConfigurationManager* _config_manager;

    raft_mutex_t _mutex;
    base::FlatMap<int64_t, WaitMeta*> _wait_map;
    bool _stopped;
    WaitId _next_wait_id;

    LogId _disk_id;
    LogId _applied_id;
    // TODO(chenzhangyi01): replace deque with a thread-safe data structure
    std::deque<LogEntry* /*FIXME*/> _logs_in_memory;
    int64_t _first_log_index;
    int64_t _last_log_index;
    LogId _last_snapshot_id;

    bthread::ExecutionQueueId<StableClosure*> _disk_queue;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_LOG_MANAGER_H
