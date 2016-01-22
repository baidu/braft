// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/15 16:36:52

#ifndef  PUBLIC_RAFT_COMMITMENT_MANAGER_H
#define  PUBLIC_RAFT_COMMITMENT_MANAGER_H

#include <stdint.h>                             // int64_t
#include <set>                                  // std::set
#include <deque>
#include <boost/atomic.hpp>                     // boost::atomic
#include <bthread.h>                            // raft_mutex_t
#include "raft/raft.h"
#include "raft/util.h"

namespace raft {

class FSMCaller;
struct CommitmentManagerOptions {
    CommitmentManagerOptions() {}
    FSMCaller* waiter;
};

class CommitmentManager {
public:
    CommitmentManager();
    ~CommitmentManager();

    int init(const CommitmentManagerOptions& options);

    // Called by leader, otherwise the behavior is undefined
    // Set log at |index| is stable at |peer|.
    int set_stable_at_peer_reentrant(int64_t log_index, const PeerId& peer);

    // Called when the leader steps down, otherwise the behavior is undefined
    // When a leader steps down, the uncommitted user applications should 
    // fail immediately, which the new leader will deal whether to commit or
    // truncate.
    int clear_pending_applications();

    // Called when a candidate becomes the new leader, otherwise the behavior is
    // undefined.
    // According the the raft algorithm, the logs from pervious terms can't be 
    // committed until a log at the new term becomes committed, so 
    // |new_pending_index| should be |last_log_index| + 1.
    int reset_pending_index(int64_t new_pending_index);

    // Called by leader, otherwise the behavior is undefined
    // Store application context before replication.
    int append_pending_application(const Configuration& conf, void *context);

    // Called by follower, otherwise the behavior is undefined.
    // Set commited index received from leader
    int set_last_committed_index(int64_t last_committed_index);

    int64_t last_committed_index() 
    { return _last_committed_index.load(boost::memory_order_acquire); }

    void describe(std::ostream& os, bool use_html);

private:
    struct PendingMeta {
        std::set<PeerId> peers;
        void *context;
        int quorum;
    };

    raft_mutex_t                                     _mutex;
    FSMCaller*                                          _waiter;
    boost::atomic<int64_t>                              _last_committed_index;
    int64_t                                             _pending_index;
    std::deque<PendingMeta*>                            _pending_apps;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_COMMITMENT_MANAGER_H
