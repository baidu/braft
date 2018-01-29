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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef BRAFT_RAFT_UTIL_H
#define BRAFT_RAFT_UTIL_H

#include <zlib.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <limits.h>
#include <stdlib.h>
#include <string>
#include <set>
#include <butil/third_party/murmurhash3/murmurhash3.h>
#include <butil/endpoint.h>
#include <butil/scoped_lock.h>
#include <butil/fast_rand.h>
#include <butil/time.h>
#include <butil/logging.h>
#include <butil/iobuf.h>
#include <butil/unique_ptr.h>
#include <butil/memory/singleton.h>
#include <butil/containers/doubly_buffered_data.h>
#include <butil/crc32c.h>
#include <butil/file_util.h>
#include <bthread/bthread.h>
#include <bthread/unstable.h>
#include <bthread/countdown_event.h>
#include "braft/macros.h"
#include "braft/raft.h"

namespace braft {
class Closure;

// http://stackoverflow.com/questions/1493936/faster-approach-to-checking-for-an-all-zero-buffer-in-c
inline bool is_zero(const char* buff, const size_t size) {
    if (size >= sizeof(uint64_t)) {
        return (0 == *(uint64_t*)buff) &&
            (0 == memcmp(buff, buff + sizeof(uint64_t), size - sizeof(uint64_t)));
    } else if (size > 0) {
        return (0 == *(uint8_t*)buff) &&
            (0 == memcmp(buff, buff + sizeof(uint8_t), size - sizeof(uint8_t)));
    } else {
        return 0;
    }
}

inline uint32_t murmurhash32(const void *key, int len) {
    uint32_t hash = 0;
    butil::MurmurHash3_x86_32(key, len, 0, &hash);
    return hash;
}

inline uint32_t murmurhash32(const butil::IOBuf& buf) {
    butil::MurmurHash3_x86_32_Context ctx;
    butil::MurmurHash3_x86_32_Init(&ctx, 0);
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        butil::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            butil::MurmurHash3_x86_32_Update(&ctx, sp.data(), sp.size());
        }
    }
    uint32_t hash = 0;
    butil::MurmurHash3_x86_32_Final(&hash, &ctx);
    return hash;
}

inline uint32_t crc32(const void* key, int len) {
    return butil::crc32c::Value((const char*)key, len);
}

inline uint32_t crc32(const butil::IOBuf& buf) {
    uint32_t hash = 0;
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        butil::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            hash = butil::crc32c::Extend(hash, sp.data(), sp.size());
        }
    }
    return hash;
}

// Start a bthread to run closure
void run_closure_in_bthread(::google::protobuf::Closure* closure,
                           bool in_pthread = false);

struct RunClosureInBthread {
    void operator()(google::protobuf::Closure* done) {
        return run_closure_in_bthread(done);
    }
};

typedef std::unique_ptr<google::protobuf::Closure, RunClosureInBthread>
        AsyncClosureGuard;

// Start a bthread to run closure without signal other worker thread to steal
// it. You should call bthread_flush() at last.
void run_closure_in_bthread_nosig(::google::protobuf::Closure* closure,
                                  bool in_pthread = false);

struct RunClosureInBthreadNoSig {
    void operator()(google::protobuf::Closure* done) {
        return run_closure_in_bthread_nosig(done);
    }
};

ssize_t file_pread(butil::IOPortal* portal, int fd, off_t offset, size_t size);

ssize_t file_pwrite(const butil::IOBuf& data, int fd, off_t offset);

// unsequence file data, reduce the overhead of copy some files have hole.
class FileSegData {
public:
    // for reader
    FileSegData(const butil::IOBuf& data)
        : _data(data), _seg_header(butil::IOBuf::INVALID_AREA), _seg_offset(0), _seg_len(0) {}
    // for writer
    FileSegData() : _seg_header(butil::IOBuf::INVALID_AREA), _seg_offset(0), _seg_len(0) {}
    ~FileSegData() {
        close();
    }

    // writer append
    void append(const butil::IOBuf& data, uint64_t offset);
    void append(void* data, uint64_t offset, uint32_t len);

    // writer get
    butil::IOBuf& data() {
        close();
        return _data;
    }

    // read next, NEED clear data when call next in loop
    size_t next(uint64_t* offset, butil::IOBuf* data);

private:
    void close();

    butil::IOBuf _data;
    butil::IOBuf::Area _seg_header;
    uint64_t _seg_offset;
    uint32_t _seg_len;
};

// A special Closure which provides synchronization primitives
class SynchronizedClosure : public Closure {
public:
    SynchronizedClosure() : _event(1) {}

    SynchronizedClosure(int num_signal) : _event(num_signal) {}
    // Implements braft::Closure
    void Run() {
        _event.signal();
    }
    // Block the thread until Run() has been called
    void wait() { _event.wait(); }
    // Reset the event
    void reset() {
        status().reset();
        _event.reset();
    }
private:
    bthread::CountdownEvent _event;
};

}  //  namespace braft

#endif // BRAFT_RAFT_UTIL_H
