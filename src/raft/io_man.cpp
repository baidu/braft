// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
// Date: 2016/03/13 20:04:38

#include <unistd.h>
#include "google/gflags.h"
#include "base/threading/thread.h"
#include "base/threading/sequenced_worker_pool.h"
#include "base/memory/ref_counted.h"
#include "base/bind.h"
#include "io_man.h"

namespace raft {

DEFINE_int32(ioman_threads, 24, "ioman threads");

IOMan::IOMan()
    : _worker_pool(new base::SequencedWorkerPool(FLAGS_ioman_threads, "io_worker_pool")) {
    AddRef();
}

IOMan::~IOMan() {
    // Shutdown() must call in base::Thread, has MessageLoop
    //_worker_pool->Shutdown();
}

// write and read function
ssize_t IOMan::write(int fd, const void* buf, size_t count) {
    IOCtx ctx;
    _worker_pool->PostWorkerTask(FROM_HERE, base::Bind(&IOMan::do_write, this,
                                                       fd, buf, count, &ctx));
    ctx.cond.wait();
    errno = ctx.err;

    return ctx.ret;
}

void IOMan::do_write(int fd, const void* buf, size_t count, IOCtx* ctx) {
    int ret = ::write(fd, buf, count);
    ctx->ret = ret;
    ctx->err = errno;
    ctx->cond.signal();
}

ssize_t IOMan::read(int fd, void* buf, size_t count) {
    IOCtx ctx;
    _worker_pool->PostWorkerTask(FROM_HERE, base::Bind(&IOMan::do_read, this,
                                                       fd, buf, count, &ctx));
    ctx.cond.wait();
    errno = ctx.err;

    return ctx.ret;
}

void IOMan::do_read(int fd, void* buf, size_t count, IOCtx* ctx) {
    int ret = ::read(fd, buf, count);
    ctx->ret = ret;
    ctx->err = errno;
    ctx->cond.signal();
}

ssize_t IOMan::pwrite(int fd, const void* buf, size_t count, off_t offset) {
    IOCtx ctx;
    _worker_pool->PostWorkerTask(FROM_HERE, base::Bind(&IOMan::do_pwrite, this,
                                                       fd, buf, count, offset, &ctx));
    ctx.cond.wait();
    errno = ctx.err;

    return ctx.ret;
}

void IOMan::do_pwrite(int fd, const void* buf, size_t count, off_t offset, IOCtx* ctx) {
    int ret = ::pwrite(fd, buf, count, offset);
    ctx->ret = ret;
    ctx->err = errno;
    ctx->cond.signal();
}

ssize_t IOMan::pread(int fd, void* buf, size_t count, off_t offset) {
    IOCtx ctx;
    _worker_pool->PostWorkerTask(FROM_HERE, base::Bind(&IOMan::do_pread, this,
                                                       fd, buf, count, offset, &ctx));
    ctx.cond.wait();
    errno = ctx.err;

    return ctx.ret;
}

void IOMan::do_pread(int fd, void* buf, size_t count, off_t offset, IOCtx* ctx) {
    int ret = ::pread(fd, buf, count, offset);
    ctx->ret = ret;
    ctx->err = errno;
    ctx->cond.signal();
}

}
