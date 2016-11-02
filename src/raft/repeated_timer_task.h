// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/11/01 15:44:30

#ifndef  PUBLIC_RAFT_REPEATED_TIMER_TASK_H
#define  PUBLIC_RAFT_REPEATED_TIMER_TASK_H

#include "raft/timer.h"
#include "raft/macros.h"

namespace raft {

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
    raft_timer_t _timer;
    timespec _next_duetime;
    int  _timeout_ms;
    bool _stopped;
    bool _running;
    bool _destroyed;
    bool _invoking;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_REPEATED_TIMER_TASK_H
