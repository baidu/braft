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

ThroughputSnapshotThrottle::ThroughputSnapshotThrottle(
        int64_t throttle_throughput_bytes, int64_t check_cycle) 
    : _throttle_throughput_bytes(throttle_throughput_bytes)
    , _check_cycle(check_cycle)
    , _last_throughput_check_time_us(
            caculate_check_time_us(butil::cpuwide_time_us(), check_cycle))
    , _cur_throughput_bytes(0)
{}

ThroughputSnapshotThrottle::~ThroughputSnapshotThrottle() {}

size_t ThroughputSnapshotThrottle::throttled_by_throughput(int64_t bytes) {
    size_t available_size = bytes;
    int64_t now = butil::cpuwide_time_us();
    int64_t limit_per_cycle = _throttle_throughput_bytes / _check_cycle;
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

}  //  namespace braft


