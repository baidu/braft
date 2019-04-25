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
#include <bvar/bvar.h>
#include "braft/macros.h"
#include "braft/raft.h"

namespace bvar {
namespace detail {

class Percentile;

typedef Window<IntRecorder, SERIES_IN_SECOND> RecorderWindow;
typedef Window<Maxer<uint64_t>, SERIES_IN_SECOND> MaxUint64Window;
typedef Window<Percentile, SERIES_IN_SECOND> PercentileWindow;

// For mimic constructor inheritance.
class CounterRecorderBase {
public:
    explicit CounterRecorderBase(time_t window_size);
    time_t window_size() const { return _avg_counter_window.window_size(); }
protected:
    IntRecorder _avg_counter;
    Maxer<uint64_t> _max_counter;
    Percentile _counter_percentile;
    RecorderWindow _avg_counter_window;
    MaxUint64Window _max_counter_window;
    PercentileWindow _counter_percentile_window;

    PassiveStatus<int64_t> _total_times;
    PassiveStatus<int64_t> _qps;
    PassiveStatus<int64_t> _counter_p1;
    PassiveStatus<int64_t> _counter_p2;
    PassiveStatus<int64_t> _counter_p3;
    PassiveStatus<int64_t> _counter_999;  // 99.9%
    PassiveStatus<int64_t> _counter_9999; // 99.99%
    CDF _counter_cdf;
    PassiveStatus<Vector<int64_t, 4> > _counter_percentiles;
};
} // namespace detail

// Specialized structure to record counter.
// It's not a Variable, but it contains multiple bvar inside.
class CounterRecorder : public detail::CounterRecorderBase {
    typedef detail::CounterRecorderBase Base;
public:
    CounterRecorder() : Base(-1) {}
    explicit CounterRecorder(time_t window_size) : Base(window_size) {}
    explicit CounterRecorder(const butil::StringPiece& prefix) : Base(-1) {
        expose(prefix);
    }
    CounterRecorder(const butil::StringPiece& prefix,
                    time_t window_size) : Base(window_size) {
        expose(prefix);
    }
    CounterRecorder(const butil::StringPiece& prefix1,
                    const butil::StringPiece& prefix2) : Base(-1) {
        expose(prefix1, prefix2);
    }
    CounterRecorder(const butil::StringPiece& prefix1,
                    const butil::StringPiece& prefix2,
                    time_t window_size) : Base(window_size) {
        expose(prefix1, prefix2);
    }

    ~CounterRecorder() { hide(); }

    // Record the counter num.
    CounterRecorder& operator<<(int64_t count_num);
        
    // Expose all internal variables using `prefix' as prefix.
    // Returns 0 on success, -1 otherwise.
    // Example:
    //   CounterRecorder rec;
    //   rec.expose("foo_bar_add");     // foo_bar_add_avg_counter
    //                                    // foo_bar_add_max_counter
    //                                    // foo_bar_add_total_times
    //                                    // foo_bar_add_qps
    //   rec.expose("foo_bar", "apply");   // foo_bar_apply_avg_counter
    //                                    // foo_bar_apply_max_counter
    //                                    // foo_bar_apply_total_times
    //                                    // foo_bar_apply_qps
    int expose(const butil::StringPiece& prefix) {
        return expose(butil::StringPiece(), prefix);
    }
    int expose(const butil::StringPiece& prefix1,
               const butil::StringPiece& prefix2);
    
    // Hide all internal variables, called in dtor as well.
    void hide();

    // Get the average counter num in recent |window_size| seconds
    // If |window_size| is absent, use the window_size to ctor.
    int64_t avg_counter(time_t window_size) const
    { return _avg_counter_window.get_value(window_size).get_average_int(); }
    int64_t avg_counter() const
    { return _avg_counter_window.get_value().get_average_int(); }

    // Get p1/p2/p3/99.9-ile counter num in recent window_size-to-ctor seconds.
    Vector<int64_t, 4> counter_percentiles() const;

    // Get the max counter numer in recent window_size-to-ctor seconds.
    int64_t max_counter() const { return _max_counter_window.get_value(); }

    // Get the total number of recorded counter nums
    int64_t total_times() const { return _avg_counter.get_value().num; }

    // Get qps in recent |window_size| seconds. The `q' means counter nums.
    // recorded by operator<<().
    // If |window_size| is absent, use the window_size to ctor.
    int64_t qps(time_t window_size) const;
    int64_t qps() const { return _qps.get_value(); }

    // Get |ratio|-ile counter num in recent |window_size| seconds
    // E.g. 0.99 means 99%-ile
    int64_t counter_percentile(double ratio) const;

    // Get name of a sub-bvar.
    const std::string& avg_counter_name() const { return _avg_counter_window.name(); }
    const std::string& counter_percentiles_name() const
    { return _counter_percentiles.name(); }
    const std::string& counter_cdf_name() const { return _counter_cdf.name(); }
    const std::string& max_counter_name() const
    { return _max_counter_window.name(); }
    const std::string& total_times_name() const { return _total_times.name(); }
    const std::string& qps_name() const { return _qps.name(); }
};

std::ostream& operator<<(std::ostream& os, const CounterRecorder&);

}  // namespace bvar

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
