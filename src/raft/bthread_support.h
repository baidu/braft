// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
// Date: 2016/02/02 16:51:58

#ifndef PUBLIC_RAFT_BTHREAD_SUPPORT_H
#define PUBLIC_RAFT_BTHREAD_SUPPORT_H

#include "bthread.h"

namespace raft {

class BthreadCond {
public:
    BthreadCond() {
        bthread_mutex_init(&_mutex, NULL);
        bthread_cond_init(&_cond, NULL);
        _count = 1;
    }
    ~BthreadCond() {
        bthread_cond_destroy(&_cond);
        bthread_mutex_destroy(&_mutex);
    }

    void init(int count = 1) {
        _count = count;
    }

    int signal() {
        bthread_mutex_lock(&_mutex);
        _count--;
        bthread_cond_signal(&_cond);
        bthread_mutex_unlock(&_mutex);
        return 0;
    }

    int wait() {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_count > 0) {
            ret = bthread_cond_wait(&_cond, &_mutex);
        }
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
private:
    int _count;
    bthread_cond_t _cond;
    bthread_mutex_t _mutex;
};

class BthreadRWLock {
public:
    BthreadRWLock() : _readers(0), _writers(0), _read_waiters(0), _write_waiters(0) {
        bthread_mutex_init(&_mutex, NULL);
        bthread_cond_init(&_read_cond, NULL);
        bthread_cond_init(&_write_cond, NULL);
    }
    ~BthreadRWLock() {
        bthread_cond_destroy(&_write_cond);
        bthread_cond_destroy(&_read_cond);
        bthread_mutex_destroy(&_mutex);
    }

    void read() {
        bthread_mutex_lock(&_mutex);
        //read wait writer and write_waiter
        if (_writers || _write_waiters) {
            _read_waiters++;
            do {
                bthread_cond_wait(&_read_cond, &_mutex);
            } while (_writers || _write_waiters);
            _read_waiters--;
        }
        _readers++;
        bthread_mutex_unlock(&_mutex);
    }

    void write() {
        bthread_mutex_lock(&_mutex);
        // write write reader and writer
        if (_readers || _writers) {
            _write_waiters++;
            do {
                bthread_cond_wait(&_write_cond, &_mutex);
            } while (_readers || _writers);
            _write_waiters--;
        }
        _writers = 1;
        bthread_mutex_unlock(&_mutex);
    }

    void unlock() {
        bthread_mutex_lock(&_mutex);
        if (_writers == 1) {
            _writers = 0;

            // signal write waiter or broadcast read waiter
            if (_write_waiters) {
                bthread_cond_signal(&_write_cond);
            } else if (_read_waiters) {
                bthread_cond_broadcast(&_read_cond);
            }
        } else {
            _readers--;

            //signal write waiter
            if (_write_waiters && 0 == _readers) {
                bthread_cond_signal(&_write_cond);
            }
        }
        bthread_mutex_unlock(&_mutex);
    }

private:
    bthread_mutex_t _mutex;
    bthread_cond_t _read_cond;
    bthread_cond_t _write_cond;
    int _readers; // run readers
    int _writers; // run writers
    int _read_waiters; // wait readers [wait writer]
    int _write_waiters; // wait writers [wait writer]
};

template<typename T>
class BthreadTLS {
public:
    BthreadTLS() {
        bthread_key_create(&_key, NULL);
    }
    ~BthreadTLS() {
        bthread_key_delete(_key);
    }

    T* get() {
        void* ptr = bthread_getspecific(_key);
        if (!ptr) {
            ptr = new T;
            bthread_setspecific(_key, ptr);
        }
        return static_cast<T*>(ptr);
    }
private:
    bthread_key_t _key;
};

}

#endif
