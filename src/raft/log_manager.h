// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/12 15:35:41

#ifndef  PUBLIC_RAFT_LOG_MANAGER_H
#define  PUBLIC_RAFT_LOG_MANAGER_H

#include <base/macros.h>
#include <deque>
#include <bthread.h>
#include <bthread/execution_queue.h>

#include "raft/raft.h"
#include "raft/util.h"

namespace raft {

class LogStorage;
struct LogManagerOptions {
    LogManagerOptions();
    LogStorage* log_storage;
    ConfigurationManager* configuration_manager;
};

class NodeImpl;
class LeaderStableClosure;
class SnapshotMeta;
class BAIDU_CACHELINE_ALIGNMENT LogManager {
public:
    LogManager();
    ~LogManager();
    int init(const LogManagerOptions& options);

    void shutdown();

    // Start a independent thread to append log to LogStorage
    int start_disk_thread();
    int stop_disk_thread();

    // Append log entry vector and wait until it's stable (NOT COMMITTED!)
    // success return 0, fail return errno
    int append_entries(const std::vector<LogEntry *>& entries);
    // Append a log entry and call closure when it's stable
    void append_entry(LogEntry* log_entry, LeaderStableClosure* done);

    void clear_memory_logs(const int64_t index);

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail, (first_index_kept, infinity) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_suffix(const int64_t last_index_kept);

    void set_snapshot(const SnapshotMeta* meta);
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
    int64_t last_log_index();

    ConfigurationPair get_configuration(const int64_t index);

    // check and set configuration
    // Returns:
    //  change return true; else return false
    bool check_and_set_configuration(std::pair<int64_t, Configuration>& current);

    // Wait until there are more logs since |last_log_index| or error occurs
    // Returns:
    //  0: success, indicating that there are more logs
    //  ETIMEDOUT: time expires
    int wait(int64_t expected_last_log_index,
             const timespec* due_time);

    // Like the previous method, except that this method returns immediately and
    // |on_writable| would be called after there are new logs or error occurs
    void wait(int64_t expected_last_log_index,
              const timespec *due_time,
              int (*on_new_log)(void *arg, int error_code), void *arg);

private:
    static int leader_disk_run(void* meta,
                               LeaderStableClosure** const tasks[], size_t tasks_size);

    LogEntry* get_entry_from_memory(const int64_t index);

    void notify_on_new_log(int64_t expected_last_log_index, bthread_id_t wait_id);
    // Fast implementation with one lock
    // TODO(chenzhangyi01): reduce the critical section
    LogStorage* _log_storage;
    ConfigurationManager* _config_manager;

    bthread_mutex_t _mutex;
    bthread_id_list_t _wait_list;

    // TODO(chenzhangyi01): replace deque with a thread-safe data struture
    std::deque<LogEntry* /*FIXME*/> _logs_in_memory;
    int64_t _last_log_index;
    int64_t _last_snapshot_index;
    int64_t _last_snapshot_term;

    bthread::ExecutionQueueId<LeaderStableClosure*> _leader_disk_queue;
    bool _leader_disk_thread_running;
    bool _stopped;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_LOG_MANAGER_H