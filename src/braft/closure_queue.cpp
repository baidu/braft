// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
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

#include <bthread/unstable.h>
#include "braft/closure_queue.h"
#include "braft/raft.h"

namespace braft {

ClosureQueue::ClosureQueue(bool usercode_in_pthread) 
    : _first_index(0)
    , _usercode_in_pthread(usercode_in_pthread)
{}

ClosureQueue::~ClosureQueue() {
    clear();
}

void ClosureQueue::clear() {
    std::deque<Closure*> saved_queue;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        saved_queue.swap(_queue);
        _first_index = 0;
    }
    bool run_bthread = false;
    for (std::deque<Closure*>::iterator 
            it = saved_queue.begin(); it != saved_queue.end(); ++it) {
        if (*it) {
            (*it)->status().set_error(EPERM, "leader stepped down");
            run_closure_in_bthread_nosig(*it, _usercode_in_pthread);
            run_bthread = true;
        }
    }
    if (run_bthread) {
        bthread_flush();
    }
}

void ClosureQueue::reset_first_index(int64_t first_index) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_queue.empty());
    _first_index = first_index;
}

void ClosureQueue::append_pending_closure(Closure* c) {
    BAIDU_SCOPED_LOCK(_mutex);
    _queue.push_back(c);
}

int ClosureQueue::pop_closure_until(int64_t index,
                                    std::vector<Closure*> *out, int64_t *out_first_index) {
    out->clear();
    BAIDU_SCOPED_LOCK(_mutex);
    if (_queue.empty() || index < _first_index) {
        *out_first_index = index + 1;
        return 0;
    }
    if (index > _first_index + (int64_t)_queue.size() - 1) {
        CHECK(false) << "Invalid index=" << index
                     << " _first_index=" << _first_index
                     << " _closure_queue_size=" << _queue.size();
        return -1;
    }
    *out_first_index = _first_index;
    for (int64_t i = _first_index; i <= index; ++i) {
        out->push_back(_queue.front());
        _queue.pop_front();
    }
    _first_index = index + 1;
    return 0;
}

} //  namespace braft
