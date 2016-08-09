// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/02/01 16:30:08

#ifndef  PUBLIC_RAFT_CLOSURE_QUEUE_H
#define  PUBLIC_RAFT_CLOSURE_QUEUE_H

#include "raft/util.h"

namespace raft {

// Holding the closure waiting for the commitment of logs
class ClosureQueue {
public:
    explicit ClosureQueue(bool usercode_in_pthread);
    ~ClosureQueue();

    // Clear all the pending closure and run done
    void clear();

    // Called when a candidate becomes the new leader, otherwise the behavior is
    // undefined.
    // Reset the first index of the coming pending closures to |first_index|
    void reset_first_index(int64_t first_index);

    // Called by leader, otherwise the behavior is undefined
    // Append the closure
    void append_pending_closure(Closure* c);

    // Pop all the closure until |index| (included) into out in the same order
    // of thier indexes, |out_first_index| would be assigned the index of out[0] if
    // out is not empyt, index + 1 otherwise.
    int pop_closure_until(int64_t index, 
                          std::vector<Closure*> *out, int64_t *out_first_index);
private:
    // TODO: a spsc lock-free queue would help
    raft_mutex_t                                    _mutex;
    int64_t                                         _first_index;
    std::deque<Closure*>                            _queue;
    bool                                            _usercode_in_pthread;

};

} // namespace raft

#endif  //PUBLIC_RAFT_CLOSURE_QUEUE_H
