// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
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

// Authors: Xiong,Kai(xiongkai@baidu.com)

#ifndef  BRAFT_SNAPSHOT_THROTTLE_H
#define  BRAFT_SNAPSHOT_THROTTLE_H

#include <butil/memory/ref_counted.h>                // butil::RefCountedThreadSafe
#include "braft/util.h"

namespace braft {

// Abstract class with the function of throttling during heavy disk reading/writing
class SnapshotThrottle : public butil::RefCountedThreadSafe<SnapshotThrottle> {
public:
    SnapshotThrottle() {}
    virtual ~SnapshotThrottle() {}
    // Get available throughput after throttled 
    // Must be thread-safe
    virtual size_t throttled_by_throughput(int64_t bytes) = 0;
    virtual bool add_one_more_task(bool is_leader) = 0;
    virtual void finish_one_task(bool is_leader) = 0;
    virtual int64_t get_retry_interval_ms() = 0;

    // After a throttled request finish, |return_unused_throughput| is called to
    // return back unsed tokens (throghput). Default implementation do nothing.
    // There are two situations we can optimize:
    // case 1: The follower and leader both try to throttle the same request, and
    //         only one of them permit the request. No actual IO and bandwidth consumed,
    //         the acquired tokens are wasted.
    // case 2: We acquired some tokens, but only part of them are used, because of
    //         the file reach the eof, or the file contains holes.
    virtual void return_unused_throughput(
            int64_t acquired, int64_t consumed, int64_t elaspe_time_us) {}
private:
    DISALLOW_COPY_AND_ASSIGN(SnapshotThrottle);
    friend class butil::RefCountedThreadSafe<SnapshotThrottle>;
};

// SnapshotThrottle with throughput threshold used in install_snapshot
class ThroughputSnapshotThrottle : public SnapshotThrottle {
public:
    ThroughputSnapshotThrottle(int64_t throttle_throughput_bytes, int64_t check_cycle);
    int64_t get_throughput() const { return _throttle_throughput_bytes; }
    int64_t get_cycle() const { return _check_cycle; }
    size_t throttled_by_throughput(int64_t bytes);
    bool add_one_more_task(bool is_leader);
    void finish_one_task(bool is_leader);
    int64_t get_retry_interval_ms() { return 1000 / _check_cycle + 1;}
    void return_unused_throughput(
            int64_t acquired, int64_t consumed, int64_t elaspe_time_us);

private:
    ~ThroughputSnapshotThrottle();
    // user defined throughput threshold for raft, bytes per second
    int64_t _throttle_throughput_bytes;
    // user defined check cycles of throughput per second
    int64_t _check_cycle;
    // the num of tasks doing install_snapshot
    int _snapshot_task_num;
    int64_t _last_throughput_check_time_us;
    int64_t _cur_throughput_bytes;
    raft_mutex_t _mutex;
};

inline int64_t caculate_check_time_us(int64_t current_time_us, 
        int64_t check_cycle) {
    int64_t base_aligning_time_us = 1000 * 1000 / check_cycle;
    return current_time_us / base_aligning_time_us * base_aligning_time_us;
}

} //  namespace braft

#endif  // BRAFT_SNAPSHOT_THROTTLE_H
