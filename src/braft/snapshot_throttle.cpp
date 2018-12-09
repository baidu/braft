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

#include <butil/time.h>
#include <gflags/gflags.h>
#include <brpc/reloadable_flags.h>
#include "braft/snapshot_throttle.h"
#include "braft/util.h"

namespace braft {

// used to increase throttle threshold dynamically when user-defined
// threshold is too small in extreme cases.
// notice that this flag does not distinguish disk types(sata or ssd, and so on)
DEFINE_int64(raft_minimal_throttle_threshold_mb, 0,
            "minimal throttle throughput threshold per second");
BRPC_VALIDATE_GFLAG(raft_minimal_throttle_threshold_mb,
                    brpc::NonNegativeInteger);
DEFINE_int32(raft_max_install_snapshot_tasks_num, 1000, 
             "Max num of install_snapshot tasks per disk at the same time");
BRPC_VALIDATE_GFLAG(raft_max_install_snapshot_tasks_num, 
                    brpc::PositiveInteger);

ThroughputSnapshotThrottle::ThroughputSnapshotThrottle(
        int64_t throttle_throughput_bytes, int64_t check_cycle) 
    : _throttle_throughput_bytes(throttle_throughput_bytes)
    , _check_cycle(check_cycle)
    , _snapshot_task_num(0)
    , _last_throughput_check_time_us(
            caculate_check_time_us(butil::cpuwide_time_us(), check_cycle))
    , _cur_throughput_bytes(0)
{}

ThroughputSnapshotThrottle::~ThroughputSnapshotThrottle() {}

size_t ThroughputSnapshotThrottle::throttled_by_throughput(int64_t bytes) {
    size_t available_size = bytes;
    int64_t now = butil::cpuwide_time_us();
    int64_t limit_throughput_bytes_s = std::max(_throttle_throughput_bytes,
                FLAGS_raft_minimal_throttle_threshold_mb * 1024 *1024);
    int64_t limit_per_cycle = limit_throughput_bytes_s / _check_cycle;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_cur_throughput_bytes + bytes > limit_per_cycle) {
        // reading another |bytes| excceds the limit
        if (now - _last_throughput_check_time_us <= 
            1 * 1000 * 1000 / _check_cycle) {
            // if time interval is less than or equal to a cycle, read more data
            // to make full use of the throughput of current cycle.
            available_size = limit_per_cycle - _cur_throughput_bytes;
            _cur_throughput_bytes = limit_per_cycle;
        } else {
            // otherwise, read the data in the next cycle.
            available_size = bytes > limit_per_cycle ? limit_per_cycle : bytes;
            _cur_throughput_bytes = available_size;
            _last_throughput_check_time_us = 
                caculate_check_time_us(now, _check_cycle);
        }
    } else {
        // reading another |bytes| doesn't excced limit(less than or equal to), 
        // put it in current cycle
        available_size = bytes;
        _cur_throughput_bytes += available_size;
    }
    lck.unlock();
    return available_size;
}

bool ThroughputSnapshotThrottle::add_one_more_task(bool is_leader) { 
    // Don't throttle leader, let follower do it
    if (is_leader) {
        return true;
    }
    int task_num_threshold = FLAGS_raft_max_install_snapshot_tasks_num;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    int saved_task_num = _snapshot_task_num;
    if (_snapshot_task_num >= task_num_threshold) {
        lck.unlock();
        LOG(WARNING) << "Fail to add one more task when current task num is: " 
                     << saved_task_num << ", task num threshold: " << task_num_threshold;
        return false;
    }
    saved_task_num = ++_snapshot_task_num;
    lck.unlock();
    LOG(INFO) << "Succed to add one more task, new task num is: " << saved_task_num
              << ", task num threshold: " << task_num_threshold;
    return true;
}

void ThroughputSnapshotThrottle::finish_one_task(bool is_leader) {
    if (is_leader) {
        return;
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    int saved_task_num = --_snapshot_task_num;
    // _snapshot_task_num should not be negative
    CHECK_GE(_snapshot_task_num, 0) << "Finishing task cause wrong task num: "
                                    << saved_task_num;
    lck.unlock();
    LOG(INFO) << "Finish one task, new task num is: " << saved_task_num;
    return;
}

void ThroughputSnapshotThrottle::return_unused_throughput(
            int64_t acquired, int64_t consumed, int64_t elaspe_time_us) {
    int64_t now = butil::cpuwide_time_us();
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (now - elaspe_time_us < _last_throughput_check_time_us) {
        // Tokens are aqured in last cycle, ignore
        return;
    }
    _cur_throughput_bytes = std::max(
            _cur_throughput_bytes - (acquired - consumed), int64_t(0));
}

}  //  namespace braft
