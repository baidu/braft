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
    int64_t initial_term;
}

class BAIDU_CACHELINE_ALIGNMENT LogStorage {
public:
    LogManager();
    ~LogManager();
    int init(const LogManager& options);
    
    // [Not thread-safe]
    // Increase the term to |new_term|
    // Returns 0 on success, -1 on failure indicating the internal term is
    // larger than |new_term|
    // Note: when this method is successfully called, all the operations with
    // pervious terms will immediately fail
    int increase_term_to(int64_t new_term);

    // Start a independent thread to append log to LogStorage, which
    // automatically quits when the term changes
    int start_disk_thread_at_current_term();

    // [Not Thread-safe]
    // Append a log entry and wait until it's stable (NOT COMMITTED!)
    int append(const LogEntry& log_entry);
    size_t append_in_batch(const LogEntry *log_entry[], size_t size);
    void append(const LogEntry& log_entry,
                int (*on_stable)(void* arg, int64_t log_index, int error_code),
                void* arg);

    int truncate_prefix(const int64_t first_index_kept);

    int truncate_suffix(int64_t last_index_kept);

    // [Thread safe] 
    // Get the log at |index|
    // Returns:
    //  0: success and the log is stored in |entry|
    //  ENODATA: No data at |index|
    //  ECOMPACTED: The log at |index| was compacted
    //  EINVAL: index or entry is invalid
    int get_log(int64_t index, LogEntry *entry);

    int get_term_of_log(int64_t expected_cur_term, int64_t index, int64_t *term);

    // [Thread safe]
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
    void notify_on_new_log(int64_t expected_last_log_index, bthread_id_t wait_id);
    // Fast implementation with one lock
    // TODO(chenzhangyi01): reduce the critical section
    LogStorage *_log_storage;   
    bthread_mutex_t _mutex;
    bthread_id_list _wait_list;
    
    // TODO(chenzhangyi01): replace deque with a thread-safe data struture
    std::deque<LogEntry> _logs_in_memory;
    int64_t _last_log_index;
    int64_t _first_log_index;
    int64_t _first_log_index_in_memory;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_LOG_MANAGER_H
