// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
// Date: 2016/03/13 20:04:38

#ifndef PUBLIC_RAFT_IO_MAN_H
#define PUBLIC_RAFT_IO_MAN_H

#include <base/memory/singleton.h>
#include "base/threading/thread.h"
#include "base/threading/sequenced_worker_pool.h"
#include "base/memory/ref_counted.h"
#include <bthread/countdown_event.h>

namespace raft {

class IOMan : public base::RefCountedThreadSafe<IOMan> {
public:
    static IOMan* GetInstance() {
        return Singleton<IOMan>::get();
    }

    ssize_t write(int fd, const void* buf, size_t count);

    ssize_t read(int fd, void* buf, size_t count);

    ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset);

    ssize_t pread(int fd, void* buf, size_t count, off_t offset);

private:
    IOMan();
    ~IOMan();
    DISALLOW_COPY_AND_ASSIGN(IOMan);
    friend struct DefaultSingletonTraits<IOMan>;
    friend class base::RefCountedThreadSafe<IOMan>;

    struct IOCtx {
        ssize_t ret;
        int err;
        bthread::CountdownEvent cond;

        IOCtx() : ret(0), err(0) {}
    };

    void do_write(int fd, const void* buf, size_t count, IOCtx* ctx);
    void do_read(int fd, void* buf, size_t count, IOCtx* ctx);
    void do_pwrite(int fd, const void* buf, size_t count, off_t offset, IOCtx* ctx);
    void do_pread(int fd, void* buf, size_t count, off_t offset, IOCtx* ctx);

    scoped_refptr<base::SequencedWorkerPool> _worker_pool;
};


// bthread_io wrapper
static inline ssize_t bthread_write(int fd, const void* buf, size_t count) {
    return IOMan::GetInstance()->write(fd, buf, count);
}

static inline ssize_t bthread_read(int fd, void* buf, size_t count) {
    return IOMan::GetInstance()->read(fd, buf, count);
}

static inline ssize_t bthread_pwrite(int fd, const void* buf, size_t count, off_t offset) {
    return IOMan::GetInstance()->pwrite(fd, buf, count, offset);
}

static inline ssize_t bthread_pread(int fd, void* buf, size_t count, off_t offset) {
    return IOMan::GetInstance()->pread(fd, buf, count, offset);
}

}

#endif //~PUBLIC_RAFT_IO_MAN_H
