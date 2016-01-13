/*
 * =====================================================================================
 *
 *       Filename:  log.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/18 14:56:29
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include "raft/log.h"

#include <gflags/gflags.h>
#include <base/files/dir_reader_posix.h>            // base::DirReaderPosix
#include <base/file_util.h>                         // base::CreateDirectory
#include <base/string_printf.h>                     // base::string_appendf
#include <base/time.h>
#include <base/raw_pack.h>                          // base::RawPacker
#include <baidu/rpc/reloadable_flags.h>             // 

#include "raft/local_storage.pb.h"
#include "raft/log_entry.h"
#include "raft/protobuf_file.h"
#include "raft/util.h"

#define RAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020ld"
#define RAFT_SEGMENT_CLOSED_PATTERN "log_%020ld_%020ld"
#define RAFT_SEGMENT_META_FILE  "log_meta"

namespace raft {

using ::base::RawPacker;
using ::base::RawUnpacker;

DEFINE_int32(raft_max_segment_size, 8 * 1024 * 1024 /*8M*/, 
             "Max size of one segment file");
BAIDU_RPC_VALIDATE_GFLAG(raft_max_segment_size, baidu::rpc::PositiveInteger);

int ftruncate_uninterrupted(int fd, off_t length) {
    int rc = 0;
    do {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}


// Format of Header, all fields are in network order
// | ----------------- term (64bits) ----------------- |
// | type (8bits) | -------- data len (56bits) ------- |
// | data_checksum (32bits) | header checksum (32bits) |

const static size_t ENTRY_HEADER_SIZE = 24;

struct Segment::EntryHeader {
    int64_t term;
    int type;
    uint64_t data_len;
    uint32_t data_checksum;
};

std::ostream& operator<<(std::ostream& os, const Segment::EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", data_checksum=" << h.data_checksum << '}';
    return os;
}

int Segment::create() {
    if (!_is_open) {
        CHECK(false) << "Create on a closed segment at first_index=" 
                     << _first_index << " in " << _path;
        return -1;
    }

    std::string path(_path);
    base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN, _first_index);
    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        return -1;
    }

    _fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    LOG_IF(INFO, _fd >= 0) << "Created new segment `" << path 
                           << "' with fd=" << _fd ;
    return _fd >= 0 ? 0 : -1;
}

int Segment::_load_entry(off_t offset, EntryHeader* head, base::IOBuf* data,
                         size_t size_hint) const {
    base::IOPortal buf;
    size_t to_read = std::max(size_hint, ENTRY_HEADER_SIZE);
    const ssize_t n = file_pread(&buf, _fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }
    char header_buf[ENTRY_HEADER_SIZE];
    const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t term = 0;
    uint64_t type_and_data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    RawUnpacker(p).unpack64((uint64_t&)term)
                  .unpack64(type_and_data_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    if (header_checksum != murmurhash32(p, ENTRY_HEADER_SIZE - 4)) {
        int type = type_and_data_len >> 56;
        uint64_t data_len = type_and_data_len & 0xFFFFFFFFFFFFFFUL;
        EntryHeader dummy;
        dummy.term = term;
        dummy.type = type;
        dummy.data_len = data_len;
        dummy.data_checksum = data_checksum;
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << " dummy=" << dummy;
        return -1;
    }
    uint64_t data_len = type_and_data_len & 0xFFFFFFFFFFFFFFUL;
    int type = type_and_data_len >> 56;
    if (head != NULL) {
        head->term = term;
        head->type = type;
        head->data_len = data_len;
        head->data_checksum = data_checksum;
    }
    if (data != NULL) {
        if (buf.length() < ENTRY_HEADER_SIZE + data_len) {
            const size_t to_read = ENTRY_HEADER_SIZE + data_len - buf.length();
            const ssize_t n = file_pread(&buf, _fd, offset + buf.length(), to_read);
            if (n != (ssize_t)to_read) {
                return n < 0 ? -1 : 1;
            }
        } else if (buf.length() > ENTRY_HEADER_SIZE + data_len) {
            buf.pop_back(buf.length() - ENTRY_HEADER_SIZE - data_len);
        }
        CHECK_EQ(buf.length(), ENTRY_HEADER_SIZE + data_len);
        buf.pop_front(ENTRY_HEADER_SIZE);
        if (murmurhash32(buf) != data_checksum) {
            LOG(ERROR) << "Found corrupted data at offset=" 
                       << offset + ENTRY_HEADER_SIZE
                       << " data_len=" << data_len;
            // TODO: abort()?
            return -1;
        }
        data->swap(buf);
    }
    return 0;
}

int Segment::_get_meta(int64_t index, LogMeta* meta) const {
    BAIDU_SCOPED_LOCK(_mutex);
    if (index > _last_index.load(boost::memory_order_relaxed) 
                    || index < _first_index) {
        // out of range
        RAFT_VLOG << "_last_index=" << _last_index.load(boost::memory_order_relaxed)
                  << " _first_index=" << _first_index;
        return -1;
    } else if (_last_index == _first_index - 1) {
        RAFT_VLOG << "_last_index=" << _last_index.load(boost::memory_order_relaxed)
                  << " _first_index=" << _first_index;
        // empty
        return -1;
    }
    int64_t meta_index = index - _first_index;
    int64_t entry_cursor = _offset_and_term[meta_index].first;
    int64_t next_cursor = (index < _last_index.load(boost::memory_order_relaxed))
                          ? _offset_and_term[meta_index + 1].first : _bytes;
    DCHECK_LT(entry_cursor, next_cursor);
    meta->offset = entry_cursor;
    meta->term = _offset_and_term[meta_index].second;
    meta->length = next_cursor - entry_cursor;
    return 0;
}

int Segment::load(ConfigurationManager* configuration_manager) {
    int ret = 0;

    std::string path(_path);
    // create fd
    if (_is_open) {
        base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN, _first_index);
    } else {
        base::string_appendf(&path, "/" RAFT_SEGMENT_CLOSED_PATTERN, 
                             _first_index, _last_index.load());
    }
    _fd = ::open(path.c_str(), O_RDWR);
    if (_fd < 0) {
        LOG(ERROR) << "Fail to open " << path << ", " << berror();
        return -1;
    }

    // get file size
    struct stat st_buf;
    if (fstat(_fd, &st_buf) != 0) {
        LOG(ERROR) << "Fail to get the stat of " << path << ", " << berror();
        ::close(_fd);
        _fd = -1;
        return -1;
    }

    // load entry index
    int64_t file_size = st_buf.st_size;
    int64_t entry_off = 0;
    for (int64_t i = _first_index; entry_off < file_size; i++) {
        EntryHeader header;
        const int rc = _load_entry(entry_off, &header, NULL, ENTRY_HEADER_SIZE);
        if (rc > 0) {
            // The last log was not completely written, which should be truncated
            break;
        }
        if (rc < 0) {
            ret = rc;
            break;
        }
        // rc == 0
        const int64_t skip_len = ENTRY_HEADER_SIZE + header.data_len;
        if (entry_off + skip_len > file_size) {
            // The last log was not completely written which should be truncated
            break;
        }
        if (header.type == ENTRY_TYPE_ADD_PEER 
                || header.type == ENTRY_TYPE_REMOVE_PEER) {
            base::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            if (_load_entry(entry_off, NULL, &data, skip_len) != 0) {
                break;
            }
            ConfigurationPBMeta meta;
            base::IOBufAsZeroCopyInputStream wrapper(data);
            if (!meta.ParseFromZeroCopyStream(&wrapper)) {
                LOG(WARNING) << "Fail to parse ConfigurationPBMeta";
                break;
            }
            bool meta_ok = true;
            std::vector<PeerId> peers;
            for (int j = 0; j < meta.peers_size(); ++j) {
                PeerId peer_id;
                if (peer_id.parse(meta.peers(j)) != 0) {
                    LOG(ERROR) << "Fail to parse ConfigurationPBMeta";
                    meta_ok = false;
                    break;
                }
                peers.push_back(peer_id);
            }
            if (meta_ok) {
                configuration_manager->add(i, Configuration(peers));
            } else {
                break;
            }
        }
        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        if (_is_open) {
            ++_last_index;
        }
        entry_off += skip_len;
    }

    // truncate last uncompleted entry
    if (ret == 0 && entry_off != file_size) {
        LOG(INFO) << "truncate last uncompleted write entry, path: " << _path
            << " first_index: " << _first_index
            << " old_size: " << file_size << " new_size: " << entry_off;
        ret = ftruncate_uninterrupted(_fd, entry_off);
    }

    // seek to end, for opening segment
    ::lseek(_fd, entry_off, SEEK_SET);

    _bytes = entry_off;
    return ret;
}

int Segment::append(const LogEntry* entry) {

    if (BAIDU_UNLIKELY(!entry || !_is_open)) {
        return EINVAL;
    } else if (BAIDU_UNLIKELY(entry->index <= _last_index.load(boost::memory_order_consume))) {
        return EEXIST;
    } else if (BAIDU_UNLIKELY(entry->index != _last_index.load(boost::memory_order_consume) + 1)) {
        CHECK(false) << "entry->index=" << entry->index
                  << " _last_index=" << _last_index
                  << " _first_index=" << _first_index;
        return ERANGE;
    }

    base::IOBuf data;
    switch (entry->type) {
    case ENTRY_TYPE_DATA:
        data.append(entry->data);
        break;
    case ENTRY_TYPE_NO_OP:
        break;
    case ENTRY_TYPE_ADD_PEER:
    case ENTRY_TYPE_REMOVE_PEER: {
            ConfigurationPBMeta meta;
            const std::vector<PeerId>& peers = *(entry->peers);
            for (size_t i = 0; i < peers.size(); i++) {
                meta.add_peers(peers[i].to_string());
            }
            base::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!meta.SerializeToZeroCopyStream(&wrapper)) {
                LOG(ERROR) << "Fail to serialize ConfigurationPBMeta";
                return -1;
            }
        }
        break;
    default:
        LOG(FATAL) << "unknow entry type: " << entry->type;
        return -1;
    }
    CHECK_LE(data.length(), 1ul << 56ul);
    char header_buf[ENTRY_HEADER_SIZE];
    RawPacker packer(header_buf);
    packer.pack64(entry->term)
          .pack64((uint64_t)entry->type << 56ul | data.length())
          .pack32(murmurhash32(data));
    packer.pack32(murmurhash32(header_buf, ENTRY_HEADER_SIZE - 4));
    base::IOBuf header;
    header.append(header_buf, ENTRY_HEADER_SIZE);
    const size_t to_write = header.length() + data.length();
    base::IOBuf* pieces[2] = { &header, &data };
    size_t start = 0;
    ssize_t written = 0;
    while (written < (ssize_t)to_write) {
        const ssize_t n = base::IOBuf::cut_multiple_into_file_descriptor(
                _fd, pieces, ARRAY_SIZE(pieces) - start);
        if (n < 0) {
            LOG(ERROR) << "Fail to write to fd=" << _fd << ", " << berror();
            return -1;
        }
        written += n;
        for (;start < ARRAY_SIZE(pieces) && pieces[start]->empty(); ++start) {}
    }
    BAIDU_SCOPED_LOCK(_mutex);
    _offset_and_term.push_back(std::make_pair(_bytes, entry->term));
    _last_index.fetch_add(1, boost::memory_order_relaxed);
    _bytes += to_write;

    return 0;
}

int Segment::sync() {
    if (_last_index > _first_index) {
        CHECK(_is_open);
        if (FLAGS_raft_sync) {
            return ::fsync(_fd);
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}

LogEntry* Segment::get(const int64_t index) const {

    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return NULL;
    }

    bool ok = true;
    LogEntry* entry = NULL;
    do {
        ConfigurationPBMeta configuration_meta;
        EntryHeader header;
        base::IOBuf data;
        if (_load_entry(meta.offset, &header, &data, 
                        meta.length) != 0) {
            ok = false;
            break;
        }
        CHECK_EQ(meta.term, header.term);
        entry = new LogEntry();
        switch (header.type) {
        case ENTRY_TYPE_DATA:
            entry->data.swap(data);
            break;
        case ENTRY_TYPE_NO_OP:
            CHECK(data.empty()) << "Data of NO_OP must be empty";
            break;
        case ENTRY_TYPE_ADD_PEER:
        case ENTRY_TYPE_REMOVE_PEER:
            {
                base::IOBufAsZeroCopyInputStream wrapper(data);
                if (!configuration_meta.ParseFromZeroCopyStream(&wrapper)) {
                    ok = false;
                    break;
                }
                entry->peers = new std::vector<PeerId>;
                for (int i = 0; i < configuration_meta.peers_size(); i++) {
                    entry->peers->push_back(PeerId(configuration_meta.peers(i)));
                }
            }
            break;
        default:
            CHECK(false) << "Unknown entry type";
            break;
        }

        if (!ok) { 
            break;
        }
        entry->index = index;
        entry->term = header.term;
        entry->type = (EntryType)header.type;
    } while (0);

    if (!ok && entry != NULL) {
        entry->Release();
        entry = NULL;
    }
    return entry;
}

int64_t Segment::get_term(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return 0;
    }
    return meta.term;
}

int Segment::close() {
    CHECK(_is_open);

    std::string old_path(_path);
    base::string_appendf(&old_path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                         _first_index);
    std::string new_path(_path);
    base::string_appendf(&new_path, "/" RAFT_SEGMENT_CLOSED_PATTERN, 
                         _first_index, _last_index.load());

    // TODO: optimize index memory usage by reconstruct vector
    int ret = this->sync();
    if (ret == 0) {
        _is_open = false;
        const int rc = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, rc == 0) << "Renamed `" << old_path
                              << "' to `" << new_path <<'\'';
        LOG_IF(INFO, rc != 0) << "Fail to rename `" << old_path
                              << "' to `" << new_path <<"\', "
                              << berror();
        return rc;
    }
    return ret;
}

static void* run_unlink(void* arg) {
    std::string* file_path = (std::string*) arg;
    base::Timer timer;
    timer.start();
    int ret = ::unlink(file_path->c_str());
    timer.stop();
    RAFT_VLOG << "unlink " << file_path << " ret " << ret << " time: " << timer.u_elapsed();
    delete file_path;

    return NULL;
}

int Segment::unlink() {
    int ret = 0;
    do {
        std::string path(_path);
        if (_is_open) {
            base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                                 _first_index);
        } else {
            base::string_appendf(&path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                                _first_index, _last_index.load());
        }

        std::string tmp_path(path);
        tmp_path.append(".tmp");
        ret = ::rename(path.c_str(), tmp_path.c_str());
        if (ret != 0) {
            PLOG(ERROR) << "Fail to rename " << path << " to " << tmp_path;
            break;
        }

        // start bthread to unlink
        // TODO unlink follow control
        std::string* file_path = new std::string(tmp_path);
        bthread_t tid;
        if (bthread_start_urgent(&tid, &BTHREAD_ATTR_NORMAL, run_unlink, file_path) != 0) {
            run_unlink(file_path);
        }

        LOG(INFO) << "Unlinked segment `" << path << '\'';
    } while (0);

    return ret;
}

int Segment::truncate(const int64_t last_index_kept) {
    int64_t truncate_size = 0;
    int64_t first_truncate_in_offset = 0;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (last_index_kept >= _last_index) {
        return 0;
    }
    first_truncate_in_offset = last_index_kept + 1 - _first_index;
    truncate_size = _offset_and_term[first_truncate_in_offset].first;
    LOG(INFO) << "Truncating " << _path << " first_index: " << _first_index
              << " last_index from " << _last_index << " to " << last_index_kept
              << " truncate size to " << truncate_size;
    lck.unlock();

    // truncate fd
    int ret = ftruncate_uninterrupted(_fd, truncate_size);
    if (ret < 0) {
        return ret;
    }

    // seek fd
    off_t ret_off = ::lseek(_fd, truncate_size, SEEK_SET);
    if (ret_off < 0) {
        PLOG(ERROR) << "Fail to lseek fd=" << _fd << " to size=" << truncate_size;
        return -1;
    }

    // rename
    if (!_is_open) {
        std::string old_path(_path);
        base::string_appendf(&old_path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());

        std::string new_path(_path);
        base::string_appendf(&new_path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                             _first_index, last_index_kept);
        ret = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, ret == 0) << "Renamed `" << old_path << "' to `"
                               << new_path << '\'';
        LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                << new_path << "', " << berror();
    }

    lck.lock();
    // update memory var
    _offset_and_term.resize(first_truncate_in_offset);
    _last_index.store(last_index_kept, boost::memory_order_relaxed);
    _bytes = truncate_size;
    return ret;
}

int SegmentLogStorage::init(ConfigurationManager* configuration_manager) {

    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        LOG(ERROR) << "Fail to create " << dir_path.AsUTF8Unsafe();
        return -1;
    }

    int ret = 0;
    bool is_empty = false;
    do {
        ret = load_meta();
        if (ret != 0 && errno == ENOENT) {
            is_empty = true;
        } else if (ret != 0) {
            break;
        }

        ret = list_segments(is_empty);
        if (ret != 0) {
            break;
        }

        ret = load_segments(configuration_manager);
        if (ret != 0) {
            break;
        }
    } while (0);

    if (is_empty) {
        _first_log_index.store(1);
        _last_log_index.store(0);
        ret = save_meta(1);
    }
    return ret;
}

int64_t SegmentLogStorage::last_log_index() {
    return _last_log_index.load(boost::memory_order_acquire);
}

int SegmentLogStorage::append_entries(const std::vector<LogEntry*>& entries) {
    Segment* last_segment = NULL;
    for (size_t i = 0; i < entries.size(); i++) {
        LogEntry* entry = entries[i];

        Segment* segment = open_segment();
        int ret = segment->append(entry);
        if (0 != ret && EEXIST != ret) {
            return i;
        }
        if (EEXIST == ret && entry->term != get_term(entry->index)) {
            return i;
        }
        _last_log_index.fetch_add(1, boost::memory_order_release);
        last_segment = segment;
    }
    last_segment->sync();
    return entries.size();
}

int SegmentLogStorage::append_entry(const LogEntry* entry) {
    Segment* segment = open_segment();
    int ret = segment->append(entry);
    if (ret != 0 && ret != EEXIST) {
        return ret;
    }
    if (EEXIST == ret && entry->term != get_term(entry->index)) {
        return EINVAL;
    }
    _last_log_index.fetch_add(1, boost::memory_order_release);

    return segment->sync();
}

LogEntry* SegmentLogStorage::get_entry(const int64_t index) {
    boost::shared_ptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return NULL;
    }
    return ptr->get(index);
}

int64_t SegmentLogStorage::get_term(const int64_t index) {
    boost::shared_ptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return 0;
    }
    return ptr->get_term(index);
}

void SegmentLogStorage::pop_segments(
        const int64_t first_index_kept,
        std::vector<boost::shared_ptr<Segment> >* popped) {
    popped->clear();
    popped->reserve(32);
    BAIDU_SCOPED_LOCK(_mutex);
    _first_log_index.store(first_index_kept, boost::memory_order_release);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end();) {
        boost::shared_ptr<Segment>& segment = it->second;
        if (segment->last_index() < first_index_kept) {
            popped->push_back(segment);
            _segments.erase(it++);
        } else {
            return;
        }
    }
    if (_open_segment) {
        if (_open_segment->last_index() < first_index_kept) {
            popped->push_back(_open_segment);
            _open_segment.reset();
            // _log_storage is empty
            _last_log_index.store(first_index_kept - 1);
        } else {
            CHECK(_open_segment->first_index() <= first_index_kept);
        }
    } else {
        // _log_storage is empty
        _last_log_index.store(first_index_kept - 1);
    }
}

int SegmentLogStorage::truncate_prefix(const int64_t first_index_kept) {
    // segment files
    if (_first_log_index.load(boost::memory_order_acquire) >= first_index_kept) {
        LOG(WARNING) << "Nothing is going to happen since _first_log_index=" 
                     << _first_log_index.load(boost::memory_order_relaxed)
                     << " >= first_index_kept="
                     << first_index_kept;
        return 0;
    }
    // NOTE: truncate_prefix is not important, as it has nothing to do with 
    // consensus. We try to save meta on the disk first to make sure even if
    // the deleting fails or the process crashes (which is unlikely to happen).
    // The new process would see the latest `first_log_index'
    if (save_meta(first_index_kept) != 0) { // NOTE
        PLOG(ERROR) << "Fail to save meta";
        return -1;
    }
    std::vector<boost::shared_ptr<Segment> > popped;
    pop_segments(first_index_kept, &popped);
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i].reset();
    }
    return 0;
}

void SegmentLogStorage::pop_segments_from_back(
        const int64_t last_index_kept,
        std::vector<boost::shared_ptr<Segment> >* popped,
        boost::shared_ptr<Segment>* last_segment) {
    popped->clear();
    popped->reserve(32);
    last_segment->reset();
    BAIDU_SCOPED_LOCK(_mutex);
    _last_log_index.store(last_index_kept, boost::memory_order_release);
    if (_open_segment) {
        if (_open_segment->first_index() <= last_index_kept) {
            *last_segment = _open_segment;
            return;
        }
        popped->push_back(_open_segment);
        _open_segment.reset();
    }
    for (SegmentMap::reverse_iterator 
            it = _segments.rbegin(); it != _segments.rend(); ++it) {
        if (it->second->first_index() <= last_index_kept) {
            // Not return as we need to maintain _segments at the end of this
            // routine
            break;
        }
        popped->push_back(it->second);
        //XXX: C++03 not support erase reverse_iterator
    }
    for (size_t i = 0; i < popped->size(); i++) {
        _segments.erase((*popped)[i]->first_index());
    }
    if (_segments.rbegin() != _segments.rend()) {
        *last_segment = _segments.rbegin()->second;
    } else {
        // all the logs have been cleared, the we move _first_log_index to the
        // next index
        _first_log_index.store(last_index_kept + 1, boost::memory_order_release);
    }
}

int SegmentLogStorage::truncate_suffix(const int64_t last_index_kept) {
    // segment files
    std::vector<boost::shared_ptr<Segment> > popped;
    boost::shared_ptr<Segment> last_segment;
    pop_segments_from_back(last_index_kept, &popped, &last_segment);
    if (last_segment) {
        if (_first_log_index.load(boost::memory_order_relaxed) <=
            _last_log_index.load(boost::memory_order_relaxed)) {
            last_segment->truncate(last_index_kept);
        } else {
            // trucate_prefix() and truncate_suffix() to discard entire logs
            BAIDU_SCOPED_LOCK(_mutex);
            popped.push_back(last_segment);
            _segments.erase(last_segment->first_index());
            if (_open_segment) {
                CHECK(_open_segment == last_segment);
                _open_segment.reset();
            }
        }
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i].reset();
    }
    return 0;
}

int SegmentLogStorage::reset(const int64_t next_log_index) {
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index;
        return EINVAL;
    }
    std::vector<boost::shared_ptr<Segment> > popped;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    popped.reserve(_segments.size());
    for (SegmentMap::const_iterator 
            it = _segments.begin(); it != _segments.end(); ++it) {
        popped.push_back(it->second);
    }
    _segments.clear();
    if (_open_segment) {
        popped.push_back(_open_segment);
        _open_segment.reset();
    }
    _first_log_index.store(next_log_index, boost::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, boost::memory_order_relaxed);
    lck.unlock();
    // NOTE: see the comments in truncate_prefix
    if (!save_meta(next_log_index)) {
        PLOG(ERROR) << "Fail to save meta";
        return -1;
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i].reset();
    }
    return 0;
}

int SegmentLogStorage::list_segments(bool is_empty) {
    base::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION. path: " << _path;
        return -1;
    }

    // restore segment meta
    while (dir_reader.Next()) {
        // unlink unneed segments and unfinished unlinked segments
        if ((is_empty && 0 == strncmp(dir_reader.name(), "log_", strlen("log_"))) ||
            (0 == strncmp(dir_reader.name() + (strlen(dir_reader.name()) - strlen(".tmp")),
                          ".tmp", strlen(".tmp")))) {
            std::string segment_path(_path);
            segment_path.append("/");
            segment_path.append(dir_reader.name());
            ::unlink(segment_path.c_str());

            LOG(WARNING) << "unlink unused segment, path: " << segment_path;

            continue;
        }

        int match = 0;
        int64_t first_index = 0;
        int64_t last_index = 0;
        match = sscanf(dir_reader.name(), RAFT_SEGMENT_CLOSED_PATTERN, 
                       &first_index, &last_index);
        if (match == 2) {
            RAFT_VLOG << "restore closed segment, path: " << _path
                      << " first_index: " << first_index
                      << " last_index: " << last_index;
            Segment* segment = new Segment(_path, first_index, last_index);
            _segments[first_index].reset(segment);
            continue;
        }

        match = sscanf(dir_reader.name(), RAFT_SEGMENT_OPEN_PATTERN, 
                       &first_index);
        if (match == 1) {
            LOG(INFO) << "restore open segment, path: " << _path
                << " first_index: " << first_index;
            Segment* segment = new Segment(_path, first_index);
            if (!_open_segment) {
                _open_segment.reset(segment);
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                    << " first_index: " << first_index;
                return -1;
            }
        }
    }

    // check segment
    int64_t last_log_index = -1;
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end();) {
        Segment* segment = it->second.get();
        if (segment->first_index() >= segment->last_index()) {
            LOG(WARNING) << "closed segment is bad, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index != -1 &&
                                  segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "closed segment not in order, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_log_index: " << last_log_index;
            return -1;
        } else if (last_log_index == -1 &&
                      _first_log_index.load(boost::memory_order_acquire) 
                      < segment->first_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(boost::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index == -1 &&
                                  _first_log_index > segment->last_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load(boost::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            segment->unlink();
            _segments.erase(it++);
            continue;
        }

        last_log_index = segment->last_index();
        it++;
    }
    if (_open_segment) {
        if (last_log_index == -1 &&
                _first_log_index.load(boost::memory_order_relaxed) < _open_segment->first_index()) {
        LOG(WARNING) << "open segment has hole, path: " << _path
            << " first_log_index: " << _first_log_index.load(boost::memory_order_relaxed)
            << " first_index: " << _open_segment->first_index();
        } else if (last_log_index != -1 && _open_segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(boost::memory_order_relaxed)
                << " first_index: " << _open_segment->first_index();
        }
        CHECK_LE(last_log_index, _open_segment->last_index());
    }

    return 0;
}

int SegmentLogStorage::load_segments(ConfigurationManager* configuration_manager) {
    int ret = 0;

    // closed segments
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ++it) {
        Segment* segment = it->second.get();
        LOG(TRACE) << "load closed segment, path: " << _path
            << " first_index: " << segment->first_index()
            << " last_index: " << segment->last_index();
        ret = segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        _last_log_index.store(segment->last_index(), boost::memory_order_release);
    }

    // open segment
    if (_open_segment) {
        LOG(TRACE) << "load open segment, path: " << _path
            << " first_index: " << _open_segment->first_index();
        ret = _open_segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        if (_first_log_index.load() > _open_segment->last_index()) {
            LOG(WARNING) << "open segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load()
                << " first_index: " << _open_segment->first_index()
                << " last_index: " << _open_segment->last_index();
            _open_segment->unlink();
            _open_segment.reset();
        } else {
            _last_log_index.store(_open_segment->last_index(), 
                                 boost::memory_order_release);
        }
    }
    if (_last_log_index == 0) {
        _last_log_index = _first_log_index - 1;
    }
    return 0;
}

int SegmentLogStorage::save_meta(const int64_t log_index) {
    base::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" RAFT_SEGMENT_META_FILE);

    ProtoBufFile pb_file(meta_path);
    LogPBMeta meta;
    meta.set_first_log_index(log_index);
    int ret = pb_file.save(&meta, FLAGS_raft_sync /*true*/);

    timer.stop();
    RAFT_VLOG << "log save_meta " << meta_path << " time: " << timer.u_elapsed();
    return ret;
}

int SegmentLogStorage::load_meta() {
    base::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" RAFT_SEGMENT_META_FILE);

    ProtoBufFile pb_file(meta_path);
    LogPBMeta meta;
    if (0 != pb_file.load(&meta)) {
        return -1;
    }

    _first_log_index.store(meta.first_log_index());

    timer.stop();
    RAFT_VLOG << "log load_meta " << meta_path << " time: " << timer.u_elapsed();
    return 0;
}

Segment* SegmentLogStorage::open_segment() {
    boost::shared_ptr<Segment> prev_open_segment;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_open_segment) {
            _open_segment.reset(new Segment(_path, last_log_index() + 1));
            if (_open_segment->create() != 0) {
                _open_segment.reset();
                return NULL;
            }
        }
        if (_open_segment->bytes() > FLAGS_raft_max_segment_size) {
            _segments[_open_segment->first_index()] = _open_segment;
            prev_open_segment.swap(_open_segment);
        }
    }
    do {
        if (prev_open_segment) {
            if (prev_open_segment->close() == 0) {
                BAIDU_SCOPED_LOCK(_mutex);
                _open_segment.reset(new Segment(_path, last_log_index() + 1));
                if (_open_segment->create() == 0) {
                    // success
                    break;
                }
            }
            CHECK(false) << "Fail to create close old open_segment or create new open_segment";
            // Failed, revert former changes
            BAIDU_SCOPED_LOCK(_mutex);
            _segments.erase(prev_open_segment->first_index());
            _open_segment.swap(prev_open_segment);
        }
    } while (0);
    return _open_segment.get();
}

int SegmentLogStorage::get_segment(int64_t index, boost::shared_ptr<Segment>* ptr) {
    BAIDU_SCOPED_LOCK(_mutex);
    int64_t first_index = first_log_index();
    int64_t last_index = last_log_index();
    if (index < first_index || index > last_index + 1) {
        LOG_IF(WARNING, index > last_index) << "Attempted to access entry " << index << " outside of log, "
            << " first_log_index: " << first_index
            << " last_log_index: " << last_index;
        return -1;
    } else if (BAIDU_UNLIKELY(index == last_index + 1)) {
        return -1;
    }

    if (_open_segment && index >= _open_segment->first_index()) {
        *ptr = _open_segment;
        CHECK(ptr->get() != NULL);
    } else {
        SegmentMap::iterator it = _segments.upper_bound(index);
        SegmentMap::iterator saved_it = it;
        --it;
        CHECK(it != saved_it);
        *ptr = it->second;
    }
    return 0;
}

LogStorage* create_local_log_storage(const std::string& uri) {
    std::string local_path = fileuri2path(uri);
    if (local_path.empty()) {
        return NULL;
    }

    SegmentLogStorage* storage = new SegmentLogStorage(local_path);
    return storage;
}

}
