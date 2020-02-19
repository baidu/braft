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

#include "braft/log.h"

#include <gflags/gflags.h>
#include <butil/files/dir_reader_posix.h>            // butil::DirReaderPosix
#include <butil/file_util.h>                         // butil::CreateDirectory
#include <butil/string_printf.h>                     // butil::string_appendf
#include <butil/time.h>
#include <butil/raw_pack.h>                          // butil::RawPacker
#include <butil/fd_utility.h>                        // butil::make_close_on_exec
#include <brpc/reloadable_flags.h>             // 

#include "braft/local_storage.pb.h"
#include "braft/log_entry.h"
#include "braft/protobuf_file.h"
#include "braft/util.h"
#include "braft/fsync.h"

//#define BRAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020ld"
//#define BRAFT_SEGMENT_CLOSED_PATTERN "log_%020ld_%020ld"
#define BRAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020" PRId64
#define BRAFT_SEGMENT_CLOSED_PATTERN "log_%020" PRId64 "_%020" PRId64
#define BRAFT_SEGMENT_META_FILE  "log_meta"

namespace braft {

using ::butil::RawPacker;
using ::butil::RawUnpacker;

DECLARE_bool(raft_trace_append_entry_latency);
DEFINE_int32(raft_max_segment_size, 8 * 1024 * 1024 /*8M*/, 
             "Max size of one segment file");
BRPC_VALIDATE_GFLAG(raft_max_segment_size, brpc::PositiveInteger);

DEFINE_bool(raft_sync_segments, false, "call fsync when a segment is closed");
BRPC_VALIDATE_GFLAG(raft_sync_segments, ::brpc::PassValidate);

static bvar::LatencyRecorder g_open_segment_latency("raft_open_segment");
static bvar::LatencyRecorder g_segment_append_entry_latency("raft_segment_append_entry");
static bvar::LatencyRecorder g_sync_segment_latency("raft_sync_segment");

int ftruncate_uninterrupted(int fd, off_t length) {
    int rc = 0;
    do {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

enum CheckSumType {
    CHECKSUM_MURMURHASH32 = 0,
    CHECKSUM_CRC32 = 1,   
};

// Format of Header, all fields are in network order
// | -------------------- term (64bits) -------------------------  |
// | entry-type (8bits) | checksum_type (8bits) | reserved(16bits) |
// | ------------------ data len (32bits) -----------------------  |
// | data_checksum (32bits) | header checksum (32bits)             |

const static size_t ENTRY_HEADER_SIZE = 24;

struct Segment::EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    uint32_t data_len;
    uint32_t data_checksum;
};

std::ostream& operator<<(std::ostream& os, const Segment::EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", checksum_type=" << h.checksum_type
       << ", data_checksum=" << h.data_checksum << '}';
    return os;
}

int Segment::create() {
    if (!_is_open) {
        CHECK(false) << "Create on a closed segment at first_index=" 
                     << _first_index << " in " << _path;
        return -1;
    }

    std::string path(_path);
    butil::string_appendf(&path, "/" BRAFT_SEGMENT_OPEN_PATTERN, _first_index);
    _fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (_fd >= 0) {
        butil::make_close_on_exec(_fd);
    }
    LOG_IF(INFO, _fd >= 0) << "Created new segment `" << path 
                           << "' with fd=" << _fd ;
    return _fd >= 0 ? 0 : -1;
}

inline bool verify_checksum(int checksum_type,
                            const char* data, size_t len, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == murmurhash32(data, len));
    case CHECKSUM_CRC32:
        return (value == crc32(data, len));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline bool verify_checksum(int checksum_type, 
                            const butil::IOBuf& data, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == murmurhash32(data));
    case CHECKSUM_CRC32:
        return (value == crc32(data));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline uint32_t get_checksum(int checksum_type, const char* data, size_t len) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return murmurhash32(data, len);
    case CHECKSUM_CRC32:
        return crc32(data, len);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

inline uint32_t get_checksum(int checksum_type, const butil::IOBuf& data) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return murmurhash32(data);
    case CHECKSUM_CRC32:
        return crc32(data);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

int Segment::_load_entry(off_t offset, EntryHeader* head, butil::IOBuf* data,
                         size_t size_hint) const {
    butil::IOPortal buf;
    size_t to_read = std::max(size_hint, ENTRY_HEADER_SIZE);
    const ssize_t n = file_pread(&buf, _fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }
    char header_buf[ENTRY_HEADER_SIZE];
    const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    RawUnpacker(p).unpack64((uint64_t&)term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.data_len = data_len;
    tmp.data_checksum = data_checksum;
    if (!verify_checksum(tmp.checksum_type, 
                        p, ENTRY_HEADER_SIZE - 4, header_checksum)) {
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << ", header=" << tmp << ", path: " << _path;
        return -1;
    }
    if (head != NULL) {
        *head = tmp;
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
        if (!verify_checksum(tmp.checksum_type, buf, tmp.data_checksum)) {
            LOG(ERROR) << "Found corrupted data at offset=" 
                       << offset + ENTRY_HEADER_SIZE
                       << " header=" << tmp
                       << " path: " << _path;
            // TODO: abort()?
            return -1;
        }
        data->swap(buf);
    }
    return 0;
}

int Segment::_get_meta(int64_t index, LogMeta* meta) const {
    BAIDU_SCOPED_LOCK(_mutex);
    if (index > _last_index.load(butil::memory_order_relaxed) 
                    || index < _first_index) {
        // out of range
        BRAFT_VLOG << "_last_index=" << _last_index.load(butil::memory_order_relaxed)
                  << " _first_index=" << _first_index;
        return -1;
    } else if (_last_index == _first_index - 1) {
        BRAFT_VLOG << "_last_index=" << _last_index.load(butil::memory_order_relaxed)
                  << " _first_index=" << _first_index;
        // empty
        return -1;
    }
    int64_t meta_index = index - _first_index;
    int64_t entry_cursor = _offset_and_term[meta_index].first;
    int64_t next_cursor = (index < _last_index.load(butil::memory_order_relaxed))
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
        butil::string_appendf(&path, "/" BRAFT_SEGMENT_OPEN_PATTERN, _first_index);
    } else {
        butil::string_appendf(&path, "/" BRAFT_SEGMENT_CLOSED_PATTERN, 
                             _first_index, _last_index.load());
    }
    _fd = ::open(path.c_str(), O_RDWR);
    if (_fd < 0) {
        LOG(ERROR) << "Fail to open " << path << ", " << berror();
        return -1;
    }
    butil::make_close_on_exec(_fd);

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
    int64_t actual_last_index = _first_index - 1;
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
            // The last log was not completely written and it should be
            // truncated
            break;
        }
        if (header.type == ENTRY_TYPE_CONFIGURATION) {
            butil::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            if (_load_entry(entry_off, NULL, &data, skip_len) != 0) {
                break;
            }
            scoped_refptr<LogEntry> entry = new LogEntry();
            entry->id.index = i;
            entry->id.term = header.term;
            butil::Status status = parse_configuration_meta(data, entry);
            if (status.ok()) {
                ConfigurationEntry conf_entry(*entry);
                configuration_manager->add(conf_entry); 
            } else {
                LOG(ERROR) << "fail to parse configuration meta, path: " << _path
                    << " entry_off " << entry_off;
                ret = -1;
                break;
            }
        }
        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        ++actual_last_index;
        entry_off += skip_len;
    }

    const int64_t last_index = _last_index.load(butil::memory_order_relaxed);
    if (ret == 0 && !_is_open) {
        if (actual_last_index < last_index) {
            LOG(ERROR) << "data lost in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        } else if (actual_last_index > last_index) {
            // FIXME(zhengpengfei): should we ignore garbage entries silently
            LOG(ERROR) << "found garbage in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        }
    }

    if (ret != 0) {
        return ret;
    }

    if (_is_open) {
        _last_index = actual_last_index;
    }

    // truncate last uncompleted entry
    if (entry_off != file_size) {
        LOG(INFO) << "truncate last uncompleted write entry, path: " << _path
            << " first_index: " << _first_index << " old_size: " << file_size << " new_size: " << entry_off;
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
    } else if (entry->id.index != 
                    _last_index.load(butil::memory_order_consume) + 1) {
        CHECK(false) << "entry->index=" << entry->id.index
                  << " _last_index=" << _last_index
                  << " _first_index=" << _first_index;
        return ERANGE;
    }

    butil::IOBuf data;
    switch (entry->type) {
    case ENTRY_TYPE_DATA:
        data.append(entry->data);
        break;
    case ENTRY_TYPE_NO_OP:
        break;
    case ENTRY_TYPE_CONFIGURATION: 
        {
            butil::Status status = serialize_configuration_meta(entry, data);
            if (!status.ok()) {
                LOG(ERROR) << "Fail to serialize ConfigurationPBMeta, path: " 
                           << _path;
                return -1; 
            }
        }
        break;
    default:
        LOG(FATAL) << "unknow entry type: " << entry->type
                   << ", path: " << _path;
        return -1;
    }
    CHECK_LE(data.length(), 1ul << 56ul);
    char header_buf[ENTRY_HEADER_SIZE];
    const uint32_t meta_field = (entry->type << 24 ) | (_checksum_type << 16);
    RawPacker packer(header_buf);
    packer.pack64(entry->id.term)
          .pack32(meta_field)
          .pack32((uint32_t)data.length())
          .pack32(get_checksum(_checksum_type, data));
    packer.pack32(get_checksum(
                  _checksum_type, header_buf, ENTRY_HEADER_SIZE - 4));
    butil::IOBuf header;
    header.append(header_buf, ENTRY_HEADER_SIZE);
    const size_t to_write = header.length() + data.length();
    butil::IOBuf* pieces[2] = { &header, &data };
    size_t start = 0;
    ssize_t written = 0;
    while (written < (ssize_t)to_write) {
        const ssize_t n = butil::IOBuf::cut_multiple_into_file_descriptor(
                _fd, pieces + start, ARRAY_SIZE(pieces) - start);
        if (n < 0) {
            LOG(ERROR) << "Fail to write to fd=" << _fd 
                       << ", path: " << _path << berror();
            return -1;
        }
        written += n;
        for (;start < ARRAY_SIZE(pieces) && pieces[start]->empty(); ++start) {}
    }
    BAIDU_SCOPED_LOCK(_mutex);
    _offset_and_term.push_back(std::make_pair(_bytes, entry->id.term));
    _last_index.fetch_add(1, butil::memory_order_relaxed);
    _bytes += to_write;

    return 0;
}

int Segment::sync(bool will_sync) {
    if (_last_index > _first_index) {
        //CHECK(_is_open);
        if (FLAGS_raft_sync && will_sync) {
            return raft_fsync(_fd);
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
        butil::IOBuf data;
        if (_load_entry(meta.offset, &header, &data, 
                        meta.length) != 0) {
            ok = false;
            break;
        }
        CHECK_EQ(meta.term, header.term);
        entry = new LogEntry();
        entry->AddRef();
        switch (header.type) {
        case ENTRY_TYPE_DATA:
            entry->data.swap(data);
            break;
        case ENTRY_TYPE_NO_OP:
            CHECK(data.empty()) << "Data of NO_OP must be empty";
            break;
        case ENTRY_TYPE_CONFIGURATION:
            {
                butil::Status status = parse_configuration_meta(data, entry); 
                if (!status.ok()) {
                    LOG(WARNING) << "Fail to parse ConfigurationPBMeta, path: "
                                 << _path;
                    ok = false;
                    break;
                }
            }
            break;
        default:
            CHECK(false) << "Unknown entry type, path: " << _path;
            break;
        }

        if (!ok) { 
            break;
        }
        entry->id.index = index;
        entry->id.term = header.term;
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

int Segment::close(bool will_sync) {
    CHECK(_is_open);
    
    std::string old_path(_path);
    butil::string_appendf(&old_path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                         _first_index);
    std::string new_path(_path);
    butil::string_appendf(&new_path, "/" BRAFT_SEGMENT_CLOSED_PATTERN, 
                         _first_index, _last_index.load());

    // TODO: optimize index memory usage by reconstruct vector
    LOG(INFO) << "close a full segment. Current first_index: " << _first_index 
              << " last_index: " << _last_index 
              << " raft_sync_segments: " << FLAGS_raft_sync_segments 
              << " will_sync: " << will_sync 
              << " path: " << new_path;
    int ret = 0;
    if (_last_index > _first_index) {
        if (FLAGS_raft_sync_segments && will_sync) {
            ret = raft_fsync(_fd);
        }
    }
    if (ret == 0) {
        _is_open = false;
        const int rc = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, rc == 0) << "Renamed `" << old_path
                              << "' to `" << new_path <<'\'';
        LOG_IF(ERROR, rc != 0) << "Fail to rename `" << old_path
                               << "' to `" << new_path <<"\', "
                               << berror();
        return rc;
    }
    return ret;
}

std::string Segment::file_name() {
    if (!_is_open) {
        return butil::string_printf(BRAFT_SEGMENT_CLOSED_PATTERN, _first_index, _last_index.load());
    } else {
        return butil::string_printf(BRAFT_SEGMENT_OPEN_PATTERN, _first_index);
    }
}

static void* run_unlink(void* arg) {
    std::string* file_path = (std::string*) arg;
    butil::Timer timer;
    timer.start();
    int ret = ::unlink(file_path->c_str());
    timer.stop();
    BRAFT_VLOG << "unlink " << *file_path << " ret " << ret << " time: " << timer.u_elapsed();
    delete file_path;

    return NULL;
}

int Segment::unlink() {
    int ret = 0;
    do {
        std::string path(_path);
        if (_is_open) {
            butil::string_appendf(&path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                                 _first_index);
        } else {
            butil::string_appendf(&path, "/" BRAFT_SEGMENT_CLOSED_PATTERN,
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
        if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, run_unlink, file_path) != 0) {
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
    BRAFT_VLOG << "Truncating " << _path << " first_index: " << _first_index
              << " last_index from " << _last_index << " to " << last_index_kept
              << " truncate size to " << truncate_size;
    lck.unlock();

    // Truncate on a full segment need to rename back to inprogess segment again,
    // because the node may crash before truncate.
    if (!_is_open) {
        std::string old_path(_path);
        butil::string_appendf(&old_path, "/" BRAFT_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());

        std::string new_path(_path);
        butil::string_appendf(&new_path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                             _first_index);
        int ret = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, ret == 0) << "Renamed `" << old_path << "' to `"
                               << new_path << '\'';
        LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                << new_path << "', " << berror();
        if (ret != 0) {
            return ret;
        }
        _is_open = true;
    }

    // truncate fd
    int ret = ftruncate_uninterrupted(_fd, truncate_size);
    if (ret < 0) {
        return ret;
    }

    // seek fd
    off_t ret_off = ::lseek(_fd, truncate_size, SEEK_SET);
    if (ret_off < 0) {
        PLOG(ERROR) << "Fail to lseek fd=" << _fd << " to size=" << truncate_size
                    << " path: " << _path;
        return -1;
    }

    lck.lock();
    // update memory var
    _offset_and_term.resize(first_truncate_in_offset);
    _last_index.store(last_index_kept, butil::memory_order_relaxed);
    _bytes = truncate_size;
    return ret;
}

int SegmentLogStorage::init(ConfigurationManager* configuration_manager) {
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        return -1;
    }

    if (butil::crc32c::IsFastCrc32Supported()) {
        _checksum_type = CHECKSUM_CRC32;
        LOG_ONCE(INFO) << "Use crc32c as the checksum type of appending entries";
    } else {
        _checksum_type = CHECKSUM_MURMURHASH32;
        LOG_ONCE(INFO) << "Use murmurhash32 as the checksum type of appending entries";
    }

    int ret = 0;
    bool is_empty = false;
    do {
        ret = load_meta();
        if (ret != 0 && errno == ENOENT) {
            LOG(WARNING) << _path << " is empty";
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
    return _last_log_index.load(butil::memory_order_acquire);
}

int SegmentLogStorage::append_entries(const std::vector<LogEntry*>& entries, IOMetric* metric) {
    if (entries.empty()) {
        return 0;
    }
    if (_last_log_index.load(butil::memory_order_relaxed) + 1
            != entries.front()->id.index) {
        LOG(FATAL) << "There's gap between appending entries and _last_log_index"
                   << " path: " << _path;
        return -1;
    }
    scoped_refptr<Segment> last_segment = NULL;
    int64_t now = 0;
    int64_t delta_time_us = 0;
    for (size_t i = 0; i < entries.size(); i++) {
        now = butil::cpuwide_time_us();
        LogEntry* entry = entries[i];
        
        scoped_refptr<Segment> segment = open_segment();
        if (FLAGS_raft_trace_append_entry_latency && metric) {
            delta_time_us = butil::cpuwide_time_us() - now;
            metric->open_segment_time_us += delta_time_us;
            g_open_segment_latency << delta_time_us;
        }
        if (NULL == segment) {
            return i;
        }
        int ret = segment->append(entry);
        if (0 != ret) {
            return i;
        }
        if (FLAGS_raft_trace_append_entry_latency && metric) {
            delta_time_us = butil::cpuwide_time_us() - now;
            metric->append_entry_time_us += delta_time_us;
            g_segment_append_entry_latency << delta_time_us;
        }
        _last_log_index.fetch_add(1, butil::memory_order_release);
        last_segment = segment;
    }
    now = butil::cpuwide_time_us();
    last_segment->sync(_enable_sync);
    if (FLAGS_raft_trace_append_entry_latency && metric) {
        delta_time_us = butil::cpuwide_time_us() - now;
        metric->sync_segment_time_us += delta_time_us;
        g_sync_segment_latency << delta_time_us; 
    }
    return entries.size();
}

int SegmentLogStorage::append_entry(const LogEntry* entry) {
    scoped_refptr<Segment> segment = open_segment();
    if (NULL == segment) {
        return EIO;
    }
    int ret = segment->append(entry);
    if (ret != 0 && ret != EEXIST) {
        return ret;
    }
    if (EEXIST == ret && entry->id.term != get_term(entry->id.index)) {
        return EINVAL;
    }
    _last_log_index.fetch_add(1, butil::memory_order_release);

    return segment->sync(_enable_sync);
}

LogEntry* SegmentLogStorage::get_entry(const int64_t index) {
    scoped_refptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return NULL;
    }
    return ptr->get(index);
}

int64_t SegmentLogStorage::get_term(const int64_t index) {
    scoped_refptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return 0;
    }
    return ptr->get_term(index);
}

void SegmentLogStorage::pop_segments(
        const int64_t first_index_kept,
        std::vector<scoped_refptr<Segment> >* popped) {
    popped->clear();
    popped->reserve(32);
    BAIDU_SCOPED_LOCK(_mutex);
    _first_log_index.store(first_index_kept, butil::memory_order_release);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end();) {
        scoped_refptr<Segment>& segment = it->second;
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
            _open_segment = NULL;
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
    if (_first_log_index.load(butil::memory_order_acquire) >= first_index_kept) {
      BRAFT_VLOG << "Nothing is going to happen since _first_log_index=" 
                     << _first_log_index.load(butil::memory_order_relaxed)
                     << " >= first_index_kept="
                     << first_index_kept;
        return 0;
    }
    // NOTE: truncate_prefix is not important, as it has nothing to do with 
    // consensus. We try to save meta on the disk first to make sure even if
    // the deleting fails or the process crashes (which is unlikely to happen).
    // The new process would see the latest `first_log_index'
    if (save_meta(first_index_kept) != 0) { // NOTE
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    std::vector<scoped_refptr<Segment> > popped;
    pop_segments(first_index_kept, &popped);
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
    return 0;
}

void SegmentLogStorage::pop_segments_from_back(
        const int64_t last_index_kept,
        std::vector<scoped_refptr<Segment> >* popped,
        scoped_refptr<Segment>* last_segment) {
    popped->clear();
    popped->reserve(32);
    *last_segment = NULL;
    BAIDU_SCOPED_LOCK(_mutex);
    _last_log_index.store(last_index_kept, butil::memory_order_release);
    if (_open_segment) {
        if (_open_segment->first_index() <= last_index_kept) {
            *last_segment = _open_segment;
            return;
        }
        popped->push_back(_open_segment);
        _open_segment = NULL;
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
        _first_log_index.store(last_index_kept + 1, butil::memory_order_release);
    }
}

int SegmentLogStorage::truncate_suffix(const int64_t last_index_kept) {
    // segment files
    std::vector<scoped_refptr<Segment> > popped;
    scoped_refptr<Segment> last_segment;
    pop_segments_from_back(last_index_kept, &popped, &last_segment);
    bool truncate_last_segment = false;
    int ret = -1;

    if (last_segment) {
        if (_first_log_index.load(butil::memory_order_relaxed) <=
            _last_log_index.load(butil::memory_order_relaxed)) {
            truncate_last_segment = true;
        } else {
            // trucate_prefix() and truncate_suffix() to discard entire logs
            BAIDU_SCOPED_LOCK(_mutex);
            popped.push_back(last_segment);
            _segments.erase(last_segment->first_index());
            if (_open_segment) {
                CHECK(_open_segment.get() == last_segment.get());
                _open_segment = NULL;
            }
        }
    }

    // The truncate suffix order is crucial to satisfy log matching property of raft
    // log must be truncated from back to front.
    for (size_t i = 0; i < popped.size(); ++i) {
        ret = popped[i]->unlink();
        if (ret != 0) {
            return ret;
        }
        popped[i] = NULL;
    }
    if (truncate_last_segment) {
        bool closed = !last_segment->is_open();
        ret = last_segment->truncate(last_index_kept);
        if (ret == 0 && closed && last_segment->is_open()) {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK(!_open_segment);
            _open_segment.swap(last_segment);
        }
    }

    return ret;
}

int SegmentLogStorage::reset(const int64_t next_log_index) {
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index
                   << " path: " << _path;
        return EINVAL;
    }
    std::vector<scoped_refptr<Segment> > popped;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    popped.reserve(_segments.size());
    for (SegmentMap::const_iterator 
            it = _segments.begin(); it != _segments.end(); ++it) {
        popped.push_back(it->second);
    }
    _segments.clear();
    if (_open_segment) {
        popped.push_back(_open_segment);
        _open_segment = NULL;
    }
    _first_log_index.store(next_log_index, butil::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, butil::memory_order_relaxed);
    lck.unlock();
    // NOTE: see the comments in truncate_prefix
    if (save_meta(next_log_index) != 0) {
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
    return 0;
}

int SegmentLogStorage::list_segments(bool is_empty) {
    butil::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION."
                     << " path: " << _path;
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
        match = sscanf(dir_reader.name(), BRAFT_SEGMENT_CLOSED_PATTERN, 
                       &first_index, &last_index);
        if (match == 2) {
            LOG(INFO) << "restore closed segment, path: " << _path
                      << " first_index: " << first_index
                      << " last_index: " << last_index;
            Segment* segment = new Segment(_path, first_index, last_index, _checksum_type);
            _segments[first_index] = segment;
            continue;
        }

        match = sscanf(dir_reader.name(), BRAFT_SEGMENT_OPEN_PATTERN, 
                       &first_index);
        if (match == 1) {
            BRAFT_VLOG << "restore open segment, path: " << _path
                << " first_index: " << first_index;
            if (!_open_segment) {
                _open_segment = new Segment(_path, first_index, _checksum_type);
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
    for (it = _segments.begin(); it != _segments.end(); ) {
        Segment* segment = it->second.get();
        if (segment->first_index() > segment->last_index()) {
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
                    _first_log_index.load(butil::memory_order_acquire) 
                    < segment->first_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index == -1 &&
                   _first_log_index > segment->last_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            segment->unlink();
            _segments.erase(it++);
            continue;
        }

        last_log_index = segment->last_index();
        ++it;
    }
    if (_open_segment) {
        if (last_log_index == -1 &&
                _first_log_index.load(butil::memory_order_relaxed) < _open_segment->first_index()) {
        LOG(WARNING) << "open segment has hole, path: " << _path
            << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
            << " first_index: " << _open_segment->first_index();
        } else if (last_log_index != -1 && _open_segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
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
        LOG(INFO) << "load closed segment, path: " << _path
            << " first_index: " << segment->first_index()
            << " last_index: " << segment->last_index();
        ret = segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        } 
        _last_log_index.store(segment->last_index(), butil::memory_order_release);
    }

    // open segment
    if (_open_segment) {
        LOG(INFO) << "load open segment, path: " << _path
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
            _open_segment = NULL;
        } else {
            _last_log_index.store(_open_segment->last_index(), 
                                 butil::memory_order_release);
        }
    }
    if (_last_log_index == 0) {
        _last_log_index = _first_log_index - 1;
    }
    return 0;
}

int SegmentLogStorage::save_meta(const int64_t log_index) {
    butil::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" BRAFT_SEGMENT_META_FILE);

    LogPBMeta meta;
    meta.set_first_log_index(log_index);
    ProtoBufFile pb_file(meta_path);
    int ret = pb_file.save(&meta, raft_sync_meta());

    timer.stop();
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << meta_path;
    LOG(INFO) << "log save_meta " << meta_path << " first_log_index: " << log_index
              << " time: " << timer.u_elapsed();
    return ret;
}

int SegmentLogStorage::load_meta() {
    butil::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" BRAFT_SEGMENT_META_FILE);

    ProtoBufFile pb_file(meta_path);
    LogPBMeta meta;
    if (0 != pb_file.load(&meta)) {
        PLOG_IF(ERROR, errno != ENOENT) << "Fail to load meta from " << meta_path;
        return -1;
    }

    _first_log_index.store(meta.first_log_index());

    timer.stop();
    LOG(INFO) << "log load_meta " << meta_path << " first_log_index: " << meta.first_log_index()
              << " time: " << timer.u_elapsed();
    return 0;
}

scoped_refptr<Segment> SegmentLogStorage::open_segment() {
    scoped_refptr<Segment> prev_open_segment;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_open_segment) {
            _open_segment = new Segment(_path, last_log_index() + 1, _checksum_type);
            if (_open_segment->create() != 0) {
                _open_segment = NULL;
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
            if (prev_open_segment->close(_enable_sync) == 0) {
                BAIDU_SCOPED_LOCK(_mutex);
                _open_segment = new Segment(_path, last_log_index() + 1, _checksum_type);
                if (_open_segment->create() == 0) {
                    // success
                    break;
                }
            }
            PLOG(ERROR) << "Fail to close old open_segment or create new open_segment"
                        << " path: " << _path;
            // Failed, revert former changes
            BAIDU_SCOPED_LOCK(_mutex);
            _segments.erase(prev_open_segment->first_index());
            _open_segment.swap(prev_open_segment);
            return NULL;
        }
    } while (0);
    return _open_segment;
}

int SegmentLogStorage::get_segment(int64_t index, scoped_refptr<Segment>* ptr) {
    BAIDU_SCOPED_LOCK(_mutex);
    int64_t first_index = first_log_index();
    int64_t last_index = last_log_index();
    if (first_index == last_index + 1) {
        return -1;
    }
    if (index < first_index || index > last_index + 1) {
        LOG_IF(WARNING, index > last_index) << "Attempted to access entry " << index << " outside of log, "
            << " first_log_index: " << first_index
            << " last_log_index: " << last_index;
        return -1;
    } else if (index == last_index + 1) {
        return -1;
    }

    if (_open_segment && index >= _open_segment->first_index()) {
        *ptr = _open_segment;
        CHECK(ptr->get() != NULL);
    } else {
        CHECK(!_segments.empty());
        SegmentMap::iterator it = _segments.upper_bound(index);
        SegmentMap::iterator saved_it = it;
        --it;
        CHECK(it != saved_it);
        *ptr = it->second;
    }
    return 0;
}

void SegmentLogStorage::list_files(std::vector<std::string>* seg_files) {
    BAIDU_SCOPED_LOCK(_mutex);
    seg_files->push_back(BRAFT_SEGMENT_META_FILE);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end(); ++it) {
        scoped_refptr<Segment>& segment = it->second;
        seg_files->push_back(segment->file_name());
    }
    if (_open_segment) {
        seg_files->push_back(_open_segment->file_name());
    }
}

void SegmentLogStorage::sync() {
    std::vector<scoped_refptr<Segment> > segments;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        for (SegmentMap::iterator it = _segments.begin(); it != _segments.end(); ++it) {
            segments.push_back(it->second);
        }
    }

    for (size_t i = 0; i < segments.size(); i++) {
        segments[i]->sync(true);
    }
}

LogStorage* SegmentLogStorage::new_instance(const std::string& uri) const {
    return new SegmentLogStorage(uri);
}

butil::Status SegmentLogStorage::gc_instance(const std::string& uri) const {
    butil::Status status;
    if (gc_dir(uri) != 0) {
        LOG(WARNING) << "Failed to gc log storage from path " << _path;
        status.set_error(EINVAL, "Failed to gc log storage from path %s", 
                         uri.c_str());
        return status;
    }
    LOG(INFO) << "Succeed to gc log storage from path " << uri;
    return status;
}

}
