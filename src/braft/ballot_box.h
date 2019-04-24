// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  BRAFT_BALLOT_BOX_H
#define  BRAFT_BALLOT_BOX_H

#include <stdint.h>                             // int64_t
#include <set>                                  // std::set
#include <deque>
#include <butil/atomicops.h>                     // butil::atomic
#include "braft/raft.h"
#include "braft/util.h"
#include "braft/ballot.h"

namespace braft {

class FSMCaller;
class ClosureQueue;

struct BallotBoxOptions {
    BallotBoxOptions() 
        : waiter(NULL)
        , closure_queue(NULL)
    {}
    FSMCaller* waiter;
    ClosureQueue* closure_queue;
};

struct BallotBoxStatus {
    BallotBoxStatus()
        : committed_index(0), pending_index(0), pending_queue_size(0)
    {}
    int64_t committed_index;
    int64_t pending_index;
    int64_t pending_queue_size;
};

class BallotBox {
public:
    BallotBox();
    ~BallotBox();

    int init(const BallotBoxOptions& options);

    // Called by leader, otherwise the behavior is undefined
    // Set logs in [first_log_index, last_log_index] are stable at |peer|.
    int commit_at(int64_t first_log_index, int64_t last_log_index,
                  const PeerId& peer);

    // Called when the leader steps down, otherwise the behavior is undefined
    // When a leader steps down, the uncommitted user applications should 
    // fail immediately, which the new leader will deal whether to commit or
    // truncate.
    int clear_pending_tasks();
    
    // Called when a candidate becomes the new leader, otherwise the behavior is
    // undefined.
    // According the the raft algorithm, the logs from pervious terms can't be 
    // committed until a log at the new term becomes committed, so 
    // |new_pending_index| should be |last_log_index| + 1.
    int reset_pending_index(int64_t new_pending_index);

    // Called by leader, otherwise the behavior is undefined
    // Store application context before replication.
    int append_pending_task(const Configuration& conf, 
                            const Configuration* old_conf,
                            Closure* closure);

    // Called by follower, otherwise the behavior is undefined.
    // Set commited index received from leader
    int set_last_committed_index(int64_t last_committed_index);

    int64_t last_committed_index() 
    { return _last_committed_index.load(butil::memory_order_acquire); }

    void describe(std::ostream& os, bool use_html);

    void get_status(BallotBoxStatus* ballot_box_status);

private:

    FSMCaller*                                      _waiter;
    ClosureQueue*                                   _closure_queue;                            
    raft_mutex_t                                    _mutex;
    butil::atomic<int64_t>                          _last_committed_index;
    int64_t                                         _pending_index;
    std::deque<Ballot>                              _pending_meta_queue;

};

}  //  namespace braft

#endif  //BRAFT_BALLOT_BOX_H
