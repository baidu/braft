/*
 * =====================================================================================
 *
 *       Filename:  bthread_sync.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/12/07 17:41:12
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef BTHREAD_SYNC_H
#define BTHREAD_SYNC_H

class BthreadCond {
public:
    BthreadCond() {
        Init(1);
    }
    ~BthreadCond() {
        bthread_cond_destroy(&_cond);
        bthread_mutex_destroy(&_mutex);
    }

    void Init(int count = 1) {
        LOG(TRACE) << "cond(" << this << ") init " << count;
        _value = count;
        bthread_mutex_init(&_mutex, NULL);
        bthread_cond_init(&_cond, NULL);
    }
    int Signal() {
        LOG(TRACE) << "cond(" << this << ") signal";
        bthread_mutex_lock(&_mutex);
        _value--;
        raft_cond_signal(&_cond);
        bthread_mutex_unlock(&_mutex);
        return 0;
    }

    int Wait() {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_value > 0) {
            ret = raft_cond_wait(&_cond, &_mutex);
        }
        bthread_mutex_unlock(&_mutex);
        LOG(TRACE) << "cond(" << this << ") wait";
        return ret;
    }
private:
    int _value;
    bthread_cond_t _cond;
    bthread_mutex_t _mutex;
};

#endif //~BTHREAD_SYNC_H
