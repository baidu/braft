// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 09/17/2015 02:47:02 PM

#ifndef PUBLIC_RAFT_LOG_H
#define PUBLIC_RAFT_LOG_H

#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/atomic.hpp>
#include <base/iobuf.h>
#include <base/logging.h>
#include "raft/log_entry.h"
#include "raft/storage.h"
#include "raft/util.h"

namespace raft {

class BAIDU_CACHELINE_ALIGNMENT Segment {
public:
    Segment(const std::string& path, const int64_t first_index)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(true),
        _first_index(first_index), _last_index(first_index - 1) {
    }
    Segment(const std::string& path, const int64_t first_index, const int64_t last_index)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(false),
        _first_index(first_index), _last_index(last_index) {
    }
    ~Segment() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
    }

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
    int close();

    // sync open segment
    int sync();

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
        return _last_index.load(boost::memory_order_consume);
    }

private:

    struct LogMeta {
        off_t offset;
        size_t length;
        int64_t term;
    };

    int _load_entry(off_t offset, EntryHeader *head, base::IOBuf *body, 
                    size_t size_hint) const;

    int _get_meta(int64_t index, LogMeta* meta) const;

    int _truncate_meta_and_get_last(int64_t last);

    std::string _path;
    int64_t _bytes;
    mutable raft_mutex_t _mutex;
    int _fd;
    bool _is_open;
    const int64_t _first_index;
    boost::atomic<int64_t> _last_index;
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
    typedef std::map<int64_t, boost::shared_ptr<Segment> > SegmentMap;

    explicit SegmentLogStorage(const std::string& path)
        : _path(path),
          _first_log_index(1),
          _last_log_index(0) {
    }

    SegmentLogStorage()
        : _first_log_index(1), _last_log_index(0) {
    }

    virtual ~SegmentLogStorage() {
        _segments.clear();

        if (_open_segment) {
            _open_segment.reset();
        }
    }

    // init logstorage, check consistency and integrity
    virtual int init(ConfigurationManager* configuration_manager);

    // first log index in log
    virtual int64_t first_log_index() {
        return _first_log_index.load(boost::memory_order_acquire);
    }

    // last log index in log
    virtual int64_t last_log_index();

    // get logentry by index
    virtual LogEntry* get_entry(const int64_t index);

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index);

    // append entry to log
    int append_entry(const LogEntry* entry);

    // append entries to log, return success append number
    virtual int append_entries(const std::vector<LogEntry*>& entries);

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    virtual int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail, (first_index_kept, infinity) will be discarded
    virtual int truncate_suffix(const int64_t last_index_kept);

    virtual int reset(const int64_t next_log_index);

    LogStorage* new_instance(const std::string& uri) const;

    SegmentMap& segments() {
        return _segments;
    }

private:
    Segment* open_segment();
    int save_meta(const int64_t log_index);
    int load_meta();
    int list_segments(bool is_empty);
    int load_segments(ConfigurationManager* configuration_manager);
    int get_segment(int64_t log_index, boost::shared_ptr<Segment>* ptr);
    void pop_segments(
            int64_t first_index_kept, 
            std::vector<boost::shared_ptr<Segment> >* poped);
    void pop_segments_from_back(
            const int64_t first_index_kept,
            std::vector<boost::shared_ptr<Segment> >* popped,
            boost::shared_ptr<Segment>* last_segment);


    std::string _path;
    boost::atomic<int64_t> _first_log_index;
    boost::atomic<int64_t> _last_log_index;
    raft_mutex_t _mutex;
    SegmentMap _segments;
    boost::shared_ptr<Segment> _open_segment;
};

}  // namespace raft

#endif //~PUBLIC_RAFT_LOG_H
