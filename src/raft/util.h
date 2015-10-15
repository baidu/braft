/*
 * =====================================================================================
 *
 *       Filename:  util.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月15日 15时42分37秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <base/scoped_lock.h>
#include <base/rand_util.h>
#include <base/time.h>
#include <bthread.h>

namespace std {

template <> class lock_guard<bthread_mutex_t> {
public:
    explicit lock_guard(bthread_mutex_t & mutex) : _pmutex(&mutex) {
#if !defined(NDEBUG)
        const int rc = bthread_mutex_lock(_pmutex);
        if (rc) {
            char buf[64] = "";
            strerror_r(rc, buf, sizeof(buf));
            LOG(FATAL) << "Fail to lock bthread_mutex_t=" << _pmutex << ", " << buf;
            _pmutex = NULL;
        }
#else
        bthread_mutex_lock(_pmutex);
#endif  // NDEBUG
    }
    
    ~lock_guard() {
#ifndef NDEBUG
        if (_pmutex) {
            bthread_mutex_unlock(_pmutex);
        }
#else
        bthread_mutex_unlock(_pmutex);
#endif
    }
    
private:
    DISALLOW_COPY_AND_ASSIGN(lock_guard);
    bthread_mutex_t* _pmutex;
};

}

namespace raft {

inline int64_t random_timeout(int64_t timeout_ms) {
    return timeout_ms + (base::RandUint64() % (timeout_ms));
}

}

