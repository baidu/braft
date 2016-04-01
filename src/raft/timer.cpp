// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2016/03/31 15:42:37

#include <com_log.h>
#include <bthread_unstable.h>
#include <base/string_printf.h>
#include <base/logging.h>
#include <gflags/gflags.h>
#include "raft/timer.h"

namespace raft {

DEFINE_int32(raft_timer_threads, 4, "raft timer threads");

TimerManager::TimerManager() {
    memset(_timer_threads, 0, sizeof(_timer_threads));
    for (int i = 0; i < FLAGS_raft_timer_threads; i++) {
        bthread::TimerThread* tt = new (std::nothrow) bthread::TimerThread;
        init(tt, i);
        _timer_threads[i] = tt;
    }
}

TimerManager::~TimerManager() {
    for (int i = 0; i < FLAGS_raft_timer_threads; i++) {
        delete _timer_threads[i];
        _timer_threads[i] = NULL;
    }
}

int TimerManager::add(raft_timer_t* id, timespec abstime,
                   void (*on_timer)(void*), void* arg) {
    bthread::TimerThread* tt = next_thread();

    bthread_timer_t tmp = tt->schedule(on_timer, arg, abstime);
    if (tmp != 0) {
        id->thread = tt;
        id->id = tmp;
        return 0;
    }
    return ESTOP;
}

int TimerManager::del(const raft_timer_t& id) {
    bthread::TimerThread* tt = id.thread;
    const int state = tt->unschedule(id.id);
    if (state >= 0) {
        return state;
    } else {
        return EINVAL;
    }
}

static void open_comlog_at_beginning(void*) {
    // Initialize comlog for this thread
    if (com_logstatus() != LOG_NOT_DEFINED) {
        com_openlog_r();
    }
}

static void close_comlog_at_end(void*) {
    if (com_logstatus() != LOG_NOT_DEFINED) {
        com_closelog_r();
    }
}

int TimerManager::init(bthread::TimerThread* timer_thread, int index) {
    bthread::TimerThreadOptions options;
    base::string_printf(&options.bvar_prefix, "raft_timer%d", index);
    //options.bvar_prefix = "raft_timer";
    options.begin_fn = open_comlog_at_beginning;
    options.end_fn = close_comlog_at_end;
    const int rc = timer_thread->start(&options);
    CHECK_EQ(0, rc) << "Fail to start timer_thread, " << berror(rc);

    return rc;
}

static uint64_t s_timer_thread_key = 0;
bthread::TimerThread* TimerManager::next_thread() {
    uint64_t key = ((boost::atomic<uint64_t>&)s_timer_thread_key).fetch_add(
            1, boost::memory_order_relaxed);
    return _timer_threads[key % static_cast<uint64_t>(FLAGS_raft_timer_threads)];
}

}
