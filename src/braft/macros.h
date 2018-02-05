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

#ifndef  BRAFT_MACROS_H
#define  BRAFT_MACROS_H

#include <butil/macros.h>
#include <butil/logging.h>
#include <bvar/utils/lock_timer.h>

#define BRAFT_VLOG_IS_ON     VLOG_IS_ON(89)
#define BRAFT_VLOG           VLOG(89)
#define BRAFT_VPLOG          VPLOG(89)
#define BRAFT_VLOG_IF(cond)  VLOG_IF(89, (cond))
#define BRAFT_VPLOG_IF(cond) VPLOG_IF(89, (cond))

//#define USE_BTHREAD_MUTEX

#ifdef USE_BTHREAD_MUTEX

#include <bthread/mutex.h>

namespace braft {
typedef ::bthread::Mutex raft_mutex_t;
}  // namespace braft

#else   // USE_BTHREAD_MUTEX

#include <butil/synchronization/lock.h>
namespace braft {
typedef ::butil::Mutex raft_mutex_t;
}  // namespace braft

#endif  // USE_BTHREAD_MUTEX

#ifdef UNIT_TEST
#define BRAFT_MOCK virtual
#else
#define BRAFT_MOCK
#endif

#define BRAFT_GET_ARG3(arg1, arg2, arg3, ...)  arg3

#define BRAFT_RETURN_IF1(expr, rc)       \
    do {                                \
        if ((expr)) {                   \
            return (rc);                \
        }                               \
    } while (0)

#define BRAFT_RETURN_IF0(expr)           \
    do {                                \
        if ((expr)) {                   \
            return;                     \
        }                               \
    } while (0)

#define BRAFT_RETURN_IF(expr, args...)   \
        BRAFT_GET_ARG3(1, ##args, BRAFT_RETURN_IF1, BRAFT_RETURN_IF0)(expr, ##args)

#endif  //BRAFT_MACROS_H
