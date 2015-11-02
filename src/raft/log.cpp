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
#include <base/callback.h>                          // base::Callback
#include <base/bind.h>                              // base::Bind
#include <baidu/rpc/raw_pack.h>                     // baidu::rpc::RawPacker
#include <baidu/rpc/reloadable_flags.h>             // 

#include "raft/local_storage.pb.h"
#include "raft/log_entry.h"
#include "raft/protobuf_file.h"
#include "raft/util.h"

#define RAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020ld"
#define RAFT_SEGMENT_CLOSED_PATTERN "log_%020ld_%020ld"
#define RAFT_SEGMENT_META_FILE  "log_meta"

namespace raft {

using ::baidu::rpc::RawPacker;
using ::baidu::rpc::RawUnpacker;

DEFINE_int32(raft_max_segment_size, 8 * 1024 * 1024 /*8M*/, 
             "Max size of one segment file");
BAIDU_RPC_VALIDATE_GFLAG(raft_max_segment_size, baidu::rpc::PositiveInteger);


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
        CHECK(false) << "Create on a closed segment at start_index=" 
                     << _start_index << " in " << _path;
        return -1;
    }

    std::string path(_path);
    base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN, _start_index);
    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        return -1;
    }

    _fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    LOG_IF(INFO, _fd >= 0) << "Created new segment `" << path 
                           << "' with fd=" << _fd ;
    return _fd >= 0 ? 0 : -1;
}

ssize_t Segment::_read_up(base::IOPortal* buf, size_t count, off_t offset) {
    size_t nread = 0;
    while (nread < count) {
        ssize_t n = buf->append_from_file_descriptor(
                         _fd, count - nread, offset + nread);
        if (n >= 0) {
            nread += n;
        } else {
            if (n < 0) {
                LOG(ERROR) << "Fail to read from fd=" << _fd << ", " 
                           <<  berror();
                return -1;
            } else {
                return nread;
            }
        }
    }
    return (ssize_t)nread;
}

int Segment::_load_entry(off_t offset, EntryHeader* head, base::IOBuf* data,
                         size_t size_hint) {
    base::IOPortal buf;
    size_t to_read = std::max(size_hint, ENTRY_HEADER_SIZE);
    const ssize_t n = _read_up(&buf, to_read, offset);
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
            const ssize_t n = _read_up(
                    &buf, to_read,
                    offset + buf.length()); 
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

int Segment::load(const base::Callback<void(int64_t, const Configuration&)>& configuration_cb) {
    int ret = 0;

    std::string path(_path);
    // create fd
    if (_is_open) {
        base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN, _start_index);
    } else {
        base::string_appendf(&path, "/" RAFT_SEGMENT_CLOSED_PATTERN, 
                             _start_index, _end_index);
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
    for (int64_t i = _start_index; entry_off < file_size; i++) {
        EntryHeader header;
        const int rc = _load_entry(entry_off, &header, NULL, ENTRY_HEADER_SIZE);
        if (rc > 0) {
            // The last log was not completely written which should be truncated
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
            ConfigurationMeta meta;
            base::IOBufAsZeroCopyInputStream wrapper(data);
            if (!meta.ParseFromZeroCopyStream(&wrapper)) {
                LOG(WARNING) << "Fail to parse ConfigurationMeta";
                break;
            }
            bool meta_ok = true;
            std::vector<PeerId> peers;
            for (int j = 0; j < meta.peers_size(); ++j) {
                PeerId peer_id;
                if (peer_id.parse(meta.peers(j)) != 0) {
                    LOG(WARNING) << "Fail to parse ConfigurationMeta";
                    meta_ok = false;
                    break;
                }
            }
            if (meta_ok) {
                configuration_cb.Run(i, Configuration(peers));
            } else {
                break;
            }
        }
        _offset.push_back(entry_off);
        if (_is_open) {
            ++_end_index;
        }
        entry_off += skip_len;
    }

    // truncate last uncompleted entry
    if (ret == 0 && entry_off != file_size) {
        LOG(INFO) << "truncate last uncompleted write entry, path: " << _path
            << " start_index: " << _start_index
            << " old_size: " << file_size << " new_size: " << entry_off;
        ret = ::ftruncate(_fd, entry_off);
    }
    _bytes = entry_off;
    return ret;
}

int Segment::append(const LogEntry* entry) {

    if (BAIDU_UNLIKELY(!entry || !_is_open)) {
        return -1;
    } else if (BAIDU_UNLIKELY(entry->index != _end_index + 1)) {
        LOG(INFO) << "entry->index=" << entry->index
                  << " _end_index=" << _end_index
                  << " _start_index=" << _start_index;
        return -1;
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
            ConfigurationMeta meta;
            const std::vector<PeerId>& peers = *(entry->peers);
            for (size_t i = 0; i < peers.size(); i++) {
                meta.add_peers(peers[i].to_string());
            }
            base::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!meta.SerializeToZeroCopyStream(&wrapper)) {
                LOG(ERROR) << "Fail to serialize ConfigurationMeta";
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
    
    _offset.push_back(_bytes);
    _end_index++;
    _bytes += to_write;

    return 0;
}

int Segment::sync() {
    if (_end_index > _start_index) {
        assert(_is_open);
        return ::fsync(_fd);
    } else {
        return 0;
    }
}

LogEntry* Segment::get(const int64_t index) {
    if (index > _end_index || index < _start_index) {
        // out of range
        LOG(INFO) << "_end_index=" << _end_index
                  << " _start_index=" << _start_index;
        return NULL;
    } else if (_end_index == _start_index - 1) {
        LOG(INFO) << "_end_index=" << _end_index
                  << " _start_index=" << _start_index;
        // empty
        return NULL;
    }

    bool ok = true;
    LogEntry* entry = NULL;
    do {
        ConfigurationMeta configuration_meta;
        int64_t entry_cursor = _offset[index - _start_index];
        int64_t next_cursor = index < _end_index 
                              ? _offset[index - _start_index + 1] : _bytes;
        EntryHeader header;
        base::IOBuf data;
        if (_load_entry(entry_cursor, &header, &data, 
                        next_cursor - entry_cursor) != 0) {
            ok = false;
            break;
        }
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

int64_t Segment::get_term(const int64_t index) {
    if (BAIDU_UNLIKELY(index > _end_index || index < _start_index)) {
        // out of range
        return 0;
    } else if (BAIDU_UNLIKELY(_end_index == _start_index - 1)) {
        // empty
        return 0;
    }

    int64_t entry_cursor = _offset[index - _start_index];

    EntryHeader header;
    if (_load_entry(entry_cursor, &header, NULL, ENTRY_HEADER_SIZE) != 0) {
        LOG(WARNING) << "Fail to load header";
        return 0;
    }
    return header.term;
}

int Segment::close() {
    CHECK(_is_open);

    std::string old_path(_path);
    base::string_appendf(&old_path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                         _start_index);
    std::string new_path(_path);
    base::string_appendf(&new_path, "/" RAFT_SEGMENT_CLOSED_PATTERN, 
                         _start_index, _end_index);

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
    } else {
        return ret;
    }
}

int Segment::unlink() {
    int ret = 0;
    do {
        ret = ::close(_fd);
        if (ret != 0) {
            break;
        }
        _fd = -1;

        std::string path(_path);
        if (_is_open) {
            base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                                 _start_index);
        } else {
            base::string_appendf(&path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                                _start_index, _end_index);
        }

        ret = ::unlink(path.c_str());
        if (ret != 0) {
            break;
        }

        LOG(INFO) << "Unlinked segment `" << path << '\'';
    } while (0);


    if (ret == 0) {
        delete this;
        return 0;
    } else {
        return ret;
    }
}

int Segment::truncate(const int64_t last_index_kept) {
    int64_t first_truncate_in_offset = last_index_kept + 1 - _start_index;
    int64_t truncate_size = _offset[first_truncate_in_offset];
    LOG(INFO) << "Truncating " << _path << " start_index: " << _start_index
              << " end_index from " << _end_index << " to " << last_index_kept;

    int ret = 0;
    do {
        // truncate fd
        ret = ::ftruncate(_fd, truncate_size);
        if (ret < 0) {
            break;
        }

        // seek fd
        off_t ret_off = ::lseek(_fd, truncate_size, SEEK_SET);
        if (ret_off < 0) {
            ret = -1;
            break;
        }

        // rename
        if (!_is_open) {
            std::string old_path(_path);
            base::string_appendf(&old_path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                                 _start_index, _end_index);

            std::string new_path(_path);
            base::string_appendf(&new_path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                                 _start_index, last_index_kept);
            ret = ::rename(old_path.c_str(), new_path.c_str());
            LOG_IF(INFO, ret == 0) << "Renamed `" << old_path << "' to `"
                                   << new_path << '\'';
            LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                    << new_path << "', " << berror();
        }
        // update memory var
        _offset.erase(_offset.begin() + first_truncate_in_offset, _offset.end());
        _end_index = last_index_kept;
        _bytes = truncate_size;
    } while (0);

    return ret;
}

int SegmentLogStorage::init(ConfigurationManager* configuration_manager) {
    if (_is_inited) {
        return 0;
    }

    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        return -1;
    }

    _configuration_manager = make_scoped_refptr<ConfigurationManager>(configuration_manager);

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

        ret = load_segments();
        if (ret != 0) {
            break;
        }
    } while (0);

    if (ret == 0) {
        _is_inited = true;
    }
    if (is_empty) {
        ret = save_meta(1);
    }
    return ret;
}

int64_t SegmentLogStorage::last_log_index() {
    assert(_is_inited);

    Segment* segment = NULL;
    if (_open_segment) {
        segment = _open_segment;
    } else {
        SegmentMap::reverse_iterator it = _segments.rbegin();
        if (it != _segments.rend()) {
            segment = it->second;
        }
    }

    if (segment) {
        return segment->end_index();
    } else {
        return _start_log_index - 1;
    }
}

int SegmentLogStorage::append_entries(const std::vector<LogEntry*>& entries) {
    if (BAIDU_UNLIKELY(!_is_inited)) {
        LOG(WARNING) << "SegmentLogStorage not init(), path: " << _path;
        return 0;
    }

    Segment* last_segment = NULL;
    // FIXME: Should flush all the entries in one system call
    for (size_t i = 0; i < entries.size(); i++) {
        LogEntry* entry = entries[i];

        Segment* segment = open_segment();
        if (0 != segment->append(entry)) {
            return i;
        }
        last_segment = segment;
    }
    last_segment->sync();
    return entries.size();
}

void SegmentLogStorage::mark_committed(const int64_t committed_index) {
    if (_committed_log_index < committed_index && committed_index <= last_log_index()) {
        _committed_log_index = committed_index;
    }
}

int SegmentLogStorage::append_entry(const LogEntry* entry) {
    if (BAIDU_UNLIKELY(!_is_inited)) {
        LOG(WARNING) << "SegmentLogStorage not init(), path: " << _path;
        return -1;
    }

    Segment* segment = open_segment();
    int ret = segment->append(entry);
    if (ret != 0) {
        return ret;
    }

    return segment->sync();
}

LogEntry* SegmentLogStorage::get_entry(const int64_t index) {
    if (BAIDU_UNLIKELY(!_is_inited)) {
        LOG(WARNING) << "SegmentLogStorage not init(), path: " << _path;
        return NULL;
    } else if (BAIDU_UNLIKELY(index < first_log_index() || index > last_log_index())) {
        LOG(WARNING) << "Attempted to access entry " << index << " outside of log, "
            << " first_log_index: " << first_log_index()
            << " last_log_index: " << last_log_index();
        return NULL;
    }

    Segment* segment = NULL;
    if (_open_segment && index >= _open_segment->start_index()) {
        segment = _open_segment;
    } else {
        SegmentMap::iterator it = _segments.upper_bound(index);
        --it;
        segment = it->second;
    }

    return segment->get(index);
}

int64_t SegmentLogStorage::get_term(const int64_t index) {
    if (BAIDU_UNLIKELY(!_is_inited)) {
        LOG(WARNING) << "SegmentLogStorage not init(), path: " << _path;
        return 0;
    } else if (BAIDU_UNLIKELY(index < first_log_index() || index > last_log_index())) {
        LOG(WARNING) << "Attempted to access entry " << index << " outside of log, "
            << " first_log_index: " << first_log_index()
            << " last_log_index: " << last_log_index();
        return 0;
    }

    Segment* segment = NULL;
    if (_open_segment && index >= _open_segment->start_index()) {
        segment = _open_segment;
    } else {
        SegmentMap::iterator it = _segments.upper_bound(index);
        --it;
        segment = it->second;
    }

    return segment->get_term(index);
}

int SegmentLogStorage::truncate_prefix(const int64_t first_index_kept) {
    if (!_is_inited) {
        LOG(WARNING) << "SegmentLogStorage not init(), path: " << _path;
        return -1;
    }

    // segment files
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end();) {
        Segment* segment = it->second;
        if (segment->end_index() < first_index_kept) {
            _segments.erase(it++);
            segment->unlink();
        } else {
            break;
        }
    }

    // configurations
    _configuration_manager->truncate_prefix(first_index_kept);

    return save_meta(first_index_kept);
}

int SegmentLogStorage::truncate_suffix(const int64_t last_index_kept) {
    if (!_is_inited) {
        LOG(WARNING) << "SegmentLogStorage not init(), path: " << _path;
        return -1;
    }

    int ret = 0;
    if (_open_segment) {
        if (_open_segment->end_index() <= last_index_kept) {
            return 0;
        } else if (_open_segment->start_index() > last_index_kept) {
            ret = _open_segment->unlink();
            _open_segment = NULL;
        } else {
            ret = _open_segment->truncate(last_index_kept);
        }
    }

    std::vector<Segment*> delete_segments;
    SegmentMap::reverse_iterator it;
    for (it = _segments.rbegin(); it != _segments.rend() && ret == 0;) {
        Segment* segment = it->second;
        if (segment->end_index() <= last_index_kept) {
            break;
        } else if (segment->start_index() > last_index_kept) {
            //XXX: C++03 not support erase reverse_iterator
            delete_segments.push_back(segment);
            ++it;
            /*
            SegmentMap::iterator delete_it(it.base());
            ++it;
            _segments.erase(delete_it);
            ret = segment->unlink();
            //*/
        } else {
            ret = segment->truncate(last_index_kept);
            break;
        }
    }
    for (size_t i = 0; i < delete_segments.size() && ret == 0; i++) {
        Segment* segment = delete_segments[i];
        _segments.erase(segment->start_index());
        ret = segment->unlink();
    }

    // configurations
    _configuration_manager->truncate_suffix(last_index_kept);

    return ret;
}

int SegmentLogStorage::list_segments(bool is_empty) {
    base::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION. path: " << _path;
        return -1;
    }

    // restore segment meta
    while (dir_reader.Next()) {
        if (is_empty) {
            if (0 == strncmp(dir_reader.name(), "log_", strlen("log_"))) {
                std::string segment_path(_path);
                segment_path.append("/");
                segment_path.append(dir_reader.name());
                ::unlink(segment_path.c_str());

                LOG(WARNING) << "unlink unused segment, path: " << segment_path;
            }
            continue;
        }

        int match = 0;
        int64_t start_index = 0;
        int64_t end_index = 0;
        match = sscanf(dir_reader.name(), RAFT_SEGMENT_CLOSED_PATTERN, 
                       &start_index, &end_index);
        if (match == 2) {
            RAFT_VLOG << "restore closed segment, path: " << _path
                      << " start_index: " << start_index
                      << " end_index: " << end_index;
            Segment* segment = new Segment(_path, start_index, end_index);
            _segments.insert(std::pair<int64_t, Segment*>(start_index, segment));
            continue;
        }

        match = sscanf(dir_reader.name(), RAFT_SEGMENT_OPEN_PATTERN, 
                       &start_index);
        if (match == 1) {
            LOG(DEBUG) << "restore open segment, path: " << _path
                << " start_index: " << start_index;
            Segment* segment = new Segment(_path, start_index);
            if (!_open_segment) {
                _open_segment = segment;
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                    << " start_index: " << start_index;
                return -1;
            }
        }

        //LOG(WARNING) << "no recognize log file: " << _path << "/" << dir_reader.name();
    }

    // check segment
    int64_t last_end_index = -1;
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end();) {
        Segment* segment = it->second;
        if (segment->start_index() >= segment->end_index()) {
            LOG(WARNING) << "closed segment is bad, path: " << _path
                << " start_index: " << segment->start_index()
                << " end_index: " << segment->end_index();
            return -1;
        } else if (last_end_index != -1 &&
                                  segment->start_index() != last_end_index + 1) {
            LOG(WARNING) << "closed segment not in order, path: " << _path
                << " start_index: " << segment->start_index()
                << " last_end_index: " << last_end_index;
            return -1;
        } else if (last_end_index == -1 &&
                                  _start_log_index < segment->start_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                << " start_log_index: " << _start_log_index
                << " start_index: " << segment->start_index()
                << " end_index: " << segment->end_index();
            return -1;
        } else if (last_end_index == -1 &&
                                  _start_log_index > segment->end_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                << " start_log_index: " << _start_log_index
                << " start_index: " << segment->start_index()
                << " end_index: " << segment->end_index();
            _segments.erase(it++);
            segment->unlink();
            continue;
        }

        last_end_index = segment->end_index();
        it++;
    }
    if (_open_segment) {
        if (last_end_index == -1 && _start_log_index < _open_segment->start_index()) {
        LOG(WARNING) << "open segment has hole, path: " << _path
            << " start_log_index: " << _start_log_index
            << " start_index: " << _open_segment->start_index();
        } else if (last_end_index != -1 && _open_segment->start_index() != last_end_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                << " start_log_index: " << _start_log_index
                << " start_index: " << _open_segment->start_index();
        }
    }

    return 0;
}

int SegmentLogStorage::load_segments() {
    int ret = 0;

    base::Callback<void(const int64_t, const Configuration&)> configuration_cb =
        base::Bind(&SegmentLogStorage::add_configuration, this);
    // closed segments
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ++it) {
        Segment* segment = it->second;
        LOG(TRACE) << "load closed segment, path: " << _path
            << " start_index: " << segment->start_index()
            << " end_index: " << segment->end_index();
        ret = segment->load(configuration_cb);
        if (ret != 0) {
            return ret;
        }
    }

    // open segment
    if (_open_segment) {
        LOG(TRACE) << "load open segment, path: " << _path
            << " start_index: " << _open_segment->start_index();
        ret = _open_segment->load(configuration_cb);
        if (ret != 0) {
            return ret;
        }
        if (_start_log_index > _open_segment->end_index()) {
            LOG(WARNING) << "open segment need discard, path: " << _path
                << " start_log_index: " << _start_log_index
                << " start_index: " << _open_segment->start_index()
                << " end_index: " << _open_segment->end_index();
            _open_segment->unlink();
            _open_segment = NULL;
        }
    }

    return 0;
}

void SegmentLogStorage::add_configuration(const int64_t index, const Configuration& config) {
    _configuration_manager->add(index, config);
}

int SegmentLogStorage::save_meta(const int64_t log_index) {
    std::string meta_path(_path);
    meta_path.append("/" RAFT_SEGMENT_META_FILE);

    _start_log_index = log_index;

    ProtoBufFile pb_file(meta_path);
    LogMeta meta;
    meta.set_start_log_index(log_index);
    return pb_file.save(&meta, true);
}

int SegmentLogStorage::load_meta() {
    std::string meta_path(_path);
    meta_path.append("/" RAFT_SEGMENT_META_FILE);

    ProtoBufFile pb_file(meta_path);
    LogMeta meta;
    if (0 != pb_file.load(&meta)) {
        return -1;
    }

    _start_log_index = meta.start_log_index();
    return 0;
}

Segment* SegmentLogStorage::open_segment() {
    int ret = 0;

    do {
        if (!_open_segment) {
            _open_segment = new Segment(_path, last_log_index() + 1);
            ret = _open_segment->create();
            break;
        }

        if (_open_segment->bytes() > FLAGS_raft_max_segment_size) {
            ret = _open_segment->close();
            if (ret == 0) {
                _segments.insert(std::pair<int64_t, Segment*>(_open_segment->start_index(),
                                                              _open_segment));
                _open_segment = new Segment(_path, last_log_index() + 1);
                ret = _open_segment->create();
            }
        }
    } while (0);

    assert(ret == 0);
    return _open_segment;
}

}
