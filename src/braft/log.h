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
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef BRAFT_LOG_H
#define BRAFT_LOG_H

#include <vector>
#include <map>
#include <butil/memory/ref_counted.h>
#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "braft/log_entry.h"
#include "braft/storage.h"
#include "braft/util.h"

namespace braft {

class BAIDU_CACHELINE_ALIGNMENT Segment 
        : public butil::RefCountedThreadSafe<Segment> {
public:
    Segment(const std::string& path, const int64_t first_index, int checksum_type)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(true),
        _first_index(first_index), _last_index(first_index - 1),
        _checksum_type(checksum_type)
    {}
    Segment(const std::string& path, const int64_t first_index, const int64_t last_index,
            int checksum_type)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(false),
        _first_index(first_index), _last_index(last_index),
        _checksum_type(checksum_type)
    {}

    struct EntryHeader;

    // create open segment
    int create();

    // load open or closed segment
    // open fd, load index, truncate uncompleted entry
    int load(ConfigurationManager* configuration_manager);

    // serialize entry, and append to open segment
    int append(const LogEntry* entry);

    // get entry by index
    LogEntry* get(const int64_t index) const;

    // get entry's term by index
    int64_t get_term(const int64_t index) const;

    // close open segment
    int close(bool will_sync = true);

    // sync open segment
    int sync(bool will_sync);

    // unlink segment
    int unlink();

    // truncate segment to last_index_kept
    int truncate(const int64_t last_index_kept);

    bool is_open() const {
        return _is_open;
    }

    int64_t bytes() const {
        return _bytes;
    }

    int64_t first_index() const {
        return _first_index;
    }

    int64_t last_index() const {
        return _last_index.load(butil::memory_order_consume);
    }

    std::string file_name();
private:
friend class butil::RefCountedThreadSafe<Segment>;
    ~Segment() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
    }

    struct LogMeta {
        off_t offset;
        size_t length;
        int64_t term;
    };

    int _load_entry(off_t offset, EntryHeader *head, butil::IOBuf *body, 
                    size_t size_hint) const;

    int _get_meta(int64_t index, LogMeta* meta) const;

    int _truncate_meta_and_get_last(int64_t last);

    std::string _path;
    int64_t _bytes;
    mutable raft_mutex_t _mutex;
    int _fd;
    bool _is_open;
    const int64_t _first_index;
    butil::atomic<int64_t> _last_index;
    int _checksum_type;
    std::vector<std::pair<int64_t/*offset*/, int64_t/*term*/> > _offset_and_term;
};

// LogStorage use segmented append-only file, all data in disk, all index in memory.
// append one log entry, only cause one disk write, every disk write will call fsync().
//
// SegmentLog layout:
//      log_meta: record start_log
//      log_000001-0001000: closed segment
//      log_inprogress_0001001: open segment
class SegmentLogStorage : public LogStorage {
public:
    typedef std::map<int64_t, scoped_refptr<Segment> > SegmentMap;

    explicit SegmentLogStorage(const std::string& path, bool enable_sync = true)
        : _path(path)
        , _first_log_index(1)
        , _last_log_index(0)
        , _checksum_type(0)
        , _enable_sync(enable_sync)
    {} 

    SegmentLogStorage()
        : _first_log_index(1)
        , _last_log_index(0)
        , _checksum_type(0)
        , _enable_sync(true)
    {}

    virtual ~SegmentLogStorage() {}

    // init logstorage, check consistency and integrity
    virtual int init(ConfigurationManager* configuration_manager);

    // first log index in log
    virtual int64_t first_log_index() {
        return _first_log_index.load(butil::memory_order_acquire);
    }

    // last log index in log
    virtual int64_t last_log_index();

    // get logentry by index
    virtual LogEntry* get_entry(const int64_t index);

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index);

    // append entry to log
    int append_entry(const LogEntry* entry);

    // append entries to log and update IOMetric, return success append number
    virtual int append_entries(const std::vector<LogEntry*>& entries, IOMetric* metric);

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    virtual int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    virtual int truncate_suffix(const int64_t last_index_kept);

    virtual int reset(const int64_t next_log_index);

    LogStorage* new_instance(const std::string& uri) const;
    
    butil::Status gc_instance(const std::string& uri) const;

    SegmentMap& segments() {
        return _segments;
    }

    void list_files(std::vector<std::string>* seg_files);

    void sync();
private:
    scoped_refptr<Segment> open_segment();
    int save_meta(const int64_t log_index);
    int load_meta();
    int list_segments(bool is_empty);
    int load_segments(ConfigurationManager* configuration_manager);
    int get_segment(int64_t log_index, scoped_refptr<Segment>* ptr);
    void pop_segments(
            int64_t first_index_kept, 
            std::vector<scoped_refptr<Segment> >* poped);
    void pop_segments_from_back(
            const int64_t first_index_kept,
            std::vector<scoped_refptr<Segment> >* popped,
            scoped_refptr<Segment>* last_segment);


    std::string _path;
    butil::atomic<int64_t> _first_log_index;
    butil::atomic<int64_t> _last_log_index;
    raft_mutex_t _mutex;
    SegmentMap _segments;
    scoped_refptr<Segment> _open_segment;
    int _checksum_type;
    bool _enable_sync;
};

}  //  namespace braft

#endif //~BRAFT_LOG_H
