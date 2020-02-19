// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
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

// Authors: Pengfei Zheng (zhengpengfei@baidu.com)

#include <gflags/gflags.h>
#include <brpc/reloadable_flags.h>
#include "braft/lease.h"

namespace braft {

DEFINE_bool(raft_enable_leader_lease, false,
            "Enable or disable leader lease. only when all peers in a raft group "
            "set this configuration to true, leader lease check and vote are safe.");
BRPC_VALIDATE_GFLAG(raft_enable_leader_lease, ::brpc::PassValidate);

void LeaderLease::init(int64_t election_timeout_ms) {
    _election_timeout_ms = election_timeout_ms;
}

void LeaderLease::on_leader_start(int64_t term) {
    BAIDU_SCOPED_LOCK(_mutex);
    ++_lease_epoch;
    _term = term;
    _last_active_timestamp = 0;
}

void LeaderLease::on_leader_stop() {
    BAIDU_SCOPED_LOCK(_mutex);
    _last_active_timestamp = 0;
    _term = 0;
}

void LeaderLease::on_lease_start(int64_t expect_lease_epoch, int64_t last_active_timestamp) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_term == 0 || expect_lease_epoch != _lease_epoch) {
        return;
    }
    _last_active_timestamp = last_active_timestamp;
}

void LeaderLease::renew(int64_t last_active_timestamp) {
    BAIDU_SCOPED_LOCK(_mutex);
    _last_active_timestamp = last_active_timestamp;
}

void LeaderLease::get_lease_info(LeaseInfo* lease_info) {
    lease_info->term = 0;
    lease_info->lease_epoch = 0;
    if (!FLAGS_raft_enable_leader_lease) {
        lease_info->state = LeaderLease::DISABLED;
        return;
    }

    BAIDU_SCOPED_LOCK(_mutex);
    if (_term == 0) {
        lease_info->state = LeaderLease::EXPIRED;
        return;
    }
    if (_last_active_timestamp == 0) {
        lease_info->state = LeaderLease::NOT_READY;
        return;
    }
    if (butil::monotonic_time_ms() < _last_active_timestamp + _election_timeout_ms) {
        lease_info->term = _term;
        lease_info->lease_epoch = _lease_epoch;
        lease_info->state = LeaderLease::VALID;
    } else {
        lease_info->state = LeaderLease::SUSPECT;
    }
}

int64_t LeaderLease::lease_epoch() {
    BAIDU_SCOPED_LOCK(_mutex);
    return _lease_epoch;
}

void LeaderLease::reset_election_timeout_ms(int64_t election_timeout_ms) {
    BAIDU_SCOPED_LOCK(_mutex);
    _election_timeout_ms = election_timeout_ms;
}

void FollowerLease::init(int64_t election_timeout_ms, int64_t max_clock_drift_ms) {
    _election_timeout_ms = election_timeout_ms;
    _max_clock_drift_ms = max_clock_drift_ms;
    // When the node restart, we are not sure when the lease will be expired actually,
    // so just be conservative.
    _last_leader_timestamp = butil::monotonic_time_ms();
}

void FollowerLease::renew(const PeerId& leader_id) {
    _last_leader = leader_id;
    _last_leader_timestamp = butil::monotonic_time_ms();
}

int64_t FollowerLease::votable_time_from_now() {
    if (!FLAGS_raft_enable_leader_lease) {
        return 0;
    }

    int64_t now = butil::monotonic_time_ms();
    int64_t votable_timestamp = _last_leader_timestamp + _election_timeout_ms +
                                _max_clock_drift_ms;
    if (now >= votable_timestamp) {
        return 0;
    }
    return votable_timestamp - now;
}

const PeerId& FollowerLease::last_leader() {
    return _last_leader;
}

bool FollowerLease::expired() {
    return butil::monotonic_time_ms() - _last_leader_timestamp
                >= _election_timeout_ms + _max_clock_drift_ms;
}

void FollowerLease::reset() {
    _last_leader = PeerId();
    _last_leader_timestamp = 0;
}

void FollowerLease::reset_election_timeout_ms(int64_t election_timeout_ms,
                                              int64_t max_clock_drift_ms) {
    _election_timeout_ms = election_timeout_ms;
    _max_clock_drift_ms = max_clock_drift_ms;
}

} // namespace braft
