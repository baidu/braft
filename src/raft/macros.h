// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/01 17:21:31

#ifndef  PUBLIC_RAFT_MACROS_H
#define  PUBLIC_RAFT_MACROS_H

#include <base/macros.h>
#include <base/logging.h>
#include <bvar/utils/lock_timer.h>

#define RAFT_VLOG_IS_ON     VLOG_IS_ON(89)
#define RAFT_VLOG           VLOG(89)
#define RAFT_VPLOG          VPLOG(89)
#define RAFT_VLOG_IF(cond)  VLOG_IF(89, (cond))
#define RAFT_VPLOG_IF(cond) VPLOG_IF(89, (cond))

#define USE_BTHREAD_MUTEX

#ifdef USE_BTHREAD_MUTEX

#include <bthread/mutex.h>

typedef ::bvar::MutexWithLatencyRecorder<bthread_mutex_t> raft_mutex_t;
#define raft_mutex_init bthread_mutex_init
#define raft_mutex_destroy bthread_mutex_destroy
#define raft_mutex_lock bthread_mutex_lock
#define raft_mutex_unlock bthread_mutex_unlock

#define raft_cond_t bthread_cond_t
#define raft_cond_init bthread_cond_init
#define raft_cond_destroy bthread_cond_destroy
#define raft_cond_signal bthread_cond_signal
#define raft_cond_broadcast bthread_cond_broadcast
#define raft_cond_wait bthread_cond_wait

#else   // USE_BTHREAD_MUTEX

typedef ::bvar::MutexWithLatencyRecorder<pthread_mutex_t> raft_mutex_t;

#define raft_mutex_init pthread_mutex_init
#define raft_mutex_destroy  pthread_mutex_destroy
#define raft_mutex_lock pthread_mutex_lock
#define raft_mutex_unlock  pthread_mutex_unlock

#define raft_cond_t pthread_cond_t
#define raft_cond_init pthread_cond_init
#define raft_cond_destroy pthread_cond_destroy
#define raft_cond_signal pthread_cond_signal
#define raft_cond_broadcast pthread_cond_broadcast
#define raft_cond_wait pthread_cond_wait

#endif  // USE_BTHREAD_MUTEX

#endif  //PUBLIC_RAFT_MACROS_H
