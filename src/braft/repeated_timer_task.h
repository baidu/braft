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
//          Ma,Jingwei(majingwei@baidu.com)

#ifndef  BRAFT_REPEATED_TIMER_TASK_H
#define  BRAFT_REPEATED_TIMER_TASK_H

#include <bthread/unstable.h>
#include "braft/macros.h"

namespace braft {

// Repeated scheduled timer task
class RepeatedTimerTask{
DISALLOW_COPY_AND_ASSIGN(RepeatedTimerTask);
public:
    RepeatedTimerTask();
    virtual ~RepeatedTimerTask();
    // Initialize timer task
    int init(int timeout_ms);

    // Start the timer
    void start();

    // Run timer function once now
    void run_once_now();

    // Stop the timer
    void stop();

    // Reset the timer, and schedule it in the initial timeout_ms
    void reset();

    // Reset the timer and schedule it in |timeout_ms|
    void reset(int timeout_ms);

    // Destroy the timer
    void destroy();

    // Describe the current status of timer
    void describe(std::ostream& os, bool use_html);

protected:

    // Invoked everytime when it reaches the timeout
    virtual void run() = 0;

    // Invoked when the timer is finally destroyed
    virtual void on_destroy() = 0;

    virtual int adjust_timeout_ms(int timeout_ms) {
        return timeout_ms;
    }

private:

    static void on_timedout(void* arg);
    static void* run_on_timedout_in_new_thread(void* arg);
    void on_timedout();
    void schedule(std::unique_lock<raft_mutex_t>& lck);

    raft_mutex_t _mutex;
    bthread_timer_t _timer;
    timespec _next_duetime;
    int  _timeout_ms;
    bool _stopped;
    bool _running;
    bool _destroyed;
    bool _invoking;
};

}  //  namespace braft

#endif  //BRAFT_REPEATED_TIMER_TASK_H
