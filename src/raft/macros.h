// libraft - Quorum-based replication of states across machines.
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

typedef ::bthread::Mutex raft_mutex_t;

#else   // USE_BTHREAD_MUTEX

typedef ::base::Mutex raft_mutex_t;

#endif  // USE_BTHREAD_MUTEX

#ifdef UNIT_TEST
#define RAFT_MOCK virtual
#else
#define RAFT_MOCK
#endif

#endif  //PUBLIC_RAFT_MACROS_H
