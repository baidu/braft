// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2016/03/31 15:42:37

#ifndef PUBLIC_RAFT_RAFT_TIMER_H
#define PUBLIC_RAFT_RAFT_TIMER_H

#include <base/memory/singleton.h>
#include <bthread_types.h>
#include <bthread/timer_thread.h>

namespace raft {

struct raft_timer_t {
    bthread_timer_t id;
    bthread::TimerThread* thread;

    raft_timer_t() : id(0), thread(NULL) {}
};

static const int MAX_TIMER_THREAD_SIZE = 16;

class TimerManager {
public:
    static TimerManager* GetInstance() {
        return Singleton<TimerManager>::get();
    }

    // Run `on_timer(arg)' at or after real-time `abstime'. Put identifier of the
    // timer into *id.
    // Return 0 on success, errno otherwise.
    int add(raft_timer_t* id, timespec abstime,
                       void (*on_timer)(void*), void* arg);

    // Unschedule the timer associated with `id'.
    // Returns: 0 - exist & not-run; 1 - still running; EINVAL - not exist.
    int del(const raft_timer_t& id);
private:
    TimerManager();
    ~TimerManager();
    DISALLOW_COPY_AND_ASSIGN(TimerManager);
    friend struct DefaultSingletonTraits<TimerManager>;

    int init(bthread::TimerThread* timer_thread, int index);
    bthread::TimerThread* next_thread();

    bthread::TimerThread* _timer_threads[MAX_TIMER_THREAD_SIZE];
};

static inline int raft_timer_add(raft_timer_t* id, timespec abstime,
                   void (*on_timer)(void*), void* arg) {
    return TimerManager::GetInstance()->add(id, abstime, on_timer, arg);
}

static inline int raft_timer_del(const raft_timer_t& id) {
    return TimerManager::GetInstance()->del(id);
}

}

#endif //~PUBLIC_RAFT_RAFT_TIMER_H



