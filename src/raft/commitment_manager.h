// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/15 16:36:52

#ifndef  PUBLIC_RAFT_COMMITMENT_MANAGER_H
#define  PUBLIC_RAFT_COMMITMENT_MANAGER_H

#include <stdint.h>                             // int64_t
#include <set>                                  // std::set
#include <boost/atomic.hpp>                     // boost::atomic
#include <base/containers/bounded_queue.h>      // base::BoundedQueue
#include <bthread.h>                            // bthread_mutex_t
#include "raft/raft.h"

namespace raft {

class CommitmentWaiter {
public:
    // Called when some logs are commited since the last time this method was
    // called
    virtual int on_committed(int64_t last_commited_index, void *context) = 0;
    virtual int on_cleared(int64_t log_index, void *context, 
                           int error_code) = 0;
};

struct CommitmentManagerOptions {
    CommitmentManagerOptions() {}
    uint32_t max_pending_size;
    CommitmentWaiter *waiter;
    int64_t last_committed_index;
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

private:
    struct PendingMeta {
        std::set<PeerId> peers;
        void *context;
        int quorum;
    };

    bthread_mutex_t                                     _mutex;
    CommitmentWaiter*                                   _waiter;
    boost::atomic<int64_t>                              _last_committed_index;
    int64_t                                             _pending_index;
    base::BoundedQueue<PendingMeta*>                    _pending_apps;
};

class OnAppliedCaller : public CommitmentWaiter {
};

}  // namespace raft

#endif  //PUBLIC_RAFT_COMMITMENT_MANAGER_H
