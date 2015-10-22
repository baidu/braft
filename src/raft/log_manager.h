// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/12 15:35:41

#ifndef  PUBLIC_RAFT_LOG_MANAGER_H
#define  PUBLIC_RAFT_LOG_MANAGER_H

#include <base/macros.h>
#include <bthread.h>

#include "raft.h"

namespace raft {

struct LogManagerOptions {
    LogManagerOptions();
    LogStorage* log_storage;
    ConfigurationManager* configuration_manager;
};

class BAIDU_CACHELINE_ALIGNMENT LogManager {
public:
    LogManager();
    ~LogManager();
    int init(const LogManagerOptions& options);

    // Start a independent thread to append log to LogStorage
    int start_disk_thread();
    int stop_dist_thread();

    // Append a log entry and wait until it's stable (NOT COMMITTED!)
    int append_entry(LogEntry* log_entry);
    // Append log entry vector and wait until it's stable (NOT COMMITTED!)
    // return success number
    int append_entries(const std::vector<LogEntry *>& entries);
    // Append a log entry and call on_stable when it's stable
    void append(LogEntry* log_entry,
                int (*on_stable)(void* arg, int64_t log_index, int error_code),
                void* arg);

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail, (first_index_kept, infinity) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_suffix(const int64_t last_index_kept);

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
};

}  // namespace raft

#endif  //PUBLIC_RAFT_LOG_MANAGER_H
