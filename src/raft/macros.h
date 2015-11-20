// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/01 17:21:31

#ifndef  PUBLIC_RAFT_MACROS_H
#define  PUBLIC_RAFT_MACROS_H

#include <base/macros.h>
#include <base/logging.h>

#define RAFT_VLOG_IS_ON     VLOG_IS_ON(89)
#define RAFT_VLOG           VLOG(89)
#define RAFT_VPLOG          VPLOG(89)
#define RAFT_VLOG_IF(cond)  VLOG_IF(89, (cond))
#define RAFT_VPLOG_IF(cond) VPLOG_IF(89, (cond))

#define USE_BTHREAD

#ifndef USE_BTHREAD
#define bthread_mutex_t pthread_mutex_t
#define bthread_mutex_init pthread_mutex_init
#define bthread_mutex_destroy pthread_mutex_destroy
#define bthread_mutex_lock pthread_mutex_lock
#define bthread_mutex_unlock pthread_mutex_unlock
#endif

#endif  //PUBLIC_RAFT_MACROS_H
