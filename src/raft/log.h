/*
 * =====================================================================================
 *
 *       Filename:  log.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  09/17/2015 02:47:02 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_LOG_H
#define PUBLIC_RAFT_LOG_H

#include <vector>
#include <map>
#include <base/callback.h>
#include <base/iobuf.h>
#include "raft/log_entry.h"
#include "raft/storage.h"

namespace raft {

class Segment {
public:
    Segment(const std::string& path, const int64_t start_index)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(true),
        _start_index(start_index), _end_index(start_index - 1) {
    }
    Segment(const std::string& path, const int64_t start_index, const int64_t end_index)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(false),
        _start_index(start_index), _end_index(end_index) {
    }
    virtual ~Segment() {
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
    int load(const base::Callback<void(int64_t, const Configuration&)>& configuration_cb);

    // serialize entry, and append to open segment
    int append(const LogEntry* entry);

    // get entry by index
    LogEntry* get(const int64_t index);

    // get entry's term by index
    int64_t get_term(const int64_t index);

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

    int64_t start_index() const {
        return _start_index;
    }

    int64_t end_index() const {
        return _end_index;
    }

private:

    int _load_entry(off_t offset, EntryHeader *head, base::IOBuf *body, 
                    size_t size_hint);
    ssize_t _read_up(base::IOPortal* buf, size_t count, off_t offset);

    std::string _path;
    int64_t _bytes;
    int _fd;
    bool _is_open;
    int64_t _start_index;
    int64_t _end_index;
    std::vector<int64_t> _offset;
};

// LogStorage use segmented append-only file, all data in disk, all index in memory.
// append one log entry, only cause one disk write, every disk write will call fsync().
//
// SegmentLog layout:
//      log_meta: record start_log
//      log_000001-0001000: closed segment
//      log_inprogress_0001001: open segment
class SegmentLogStorage : public LogStorage, public base::RefCountedThreadSafe<SegmentLogStorage>{
public:
    typedef std::map<int64_t, Segment*> SegmentMap;

    SegmentLogStorage(const std::string& path)
        : LogStorage(path), _path(path),
        _committed_log_index(0), _start_log_index(1),
        _is_inited(false), _open_segment(NULL) {
            AddRef();
    }

    // init logstorage, check consistency and integrity
    virtual int init(ConfigurationManager* configuration_manager);

    // first log index in log
    virtual int64_t first_log_index() {
        return _start_log_index;
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

    int64_t committed_log_index() const {
        return _committed_log_index;
    }

    void mark_committed(const int64_t committed_index);

    SegmentMap& segments() {
        return _segments;
    }

    scoped_refptr<ConfigurationManager> configuration_manager() {
        return _configuration_manager;
    }

    void add_configuration(int64_t index, const Configuration& config);

private:
    friend class base::RefCountedThreadSafe<SegmentLogStorage>;
    virtual ~SegmentLogStorage() {
        SegmentMap::iterator it;
        for (it = _segments.begin(); it != _segments.end(); ++it) {
            delete it->second;
        }
        _segments.clear();

        if (_open_segment) {
            delete _open_segment;
            _open_segment = NULL;
        }

        _is_inited = false;
    }

    Segment* open_segment();
    int save_meta(const int64_t log_index);
    int load_meta();
    int list_segments(bool is_empty);
    int load_segments();

    std::string _path;
    int64_t _committed_log_index;
    int64_t _start_log_index;
    bool _is_inited;
    SegmentMap _segments;
    Segment* _open_segment;
    scoped_refptr<ConfigurationManager> _configuration_manager;
};

LogStorage* create_local_log_storage(const std::string& uri);

}

#endif //~PUBLIC_RAFT_LOG_H
