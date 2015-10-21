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

#include <zlib.h>
#include <base/bind.h>
#include <base/compiler_specific.h>
#include <base/macros.h>
#include <base/sys_byteorder.h>
#include <base/files/dir_reader_posix.h>
#include <base/logging.h>
#include <base/file_util.h>

#include "raft/local_storage.pb.h"
#include "raft/log.h"
#include "raft/protobuf_file.h"

namespace raft {

const char* Segment::_s_meta_file = "log_meta";
const char* Segment::_s_closed_pattern = "log_%020u-%020u";
const char* Segment::_s_open_pattern = "log_inprogress_%020u";
const char* Segment::_s_entry_magic = "LSF1";
const int32_t SegmentLogStorage::_s_max_segment_size = 8*1024*1024; // 8M

int Segment::create() {
    if (!_is_open) {
        LOG(WARNING) << "closed segment can not call create(), path: " << _path
            << " start: " << _start_index;
        return -1;
    }

    char segment_name[128];
    snprintf(segment_name, sizeof(segment_name), _s_open_pattern, _start_index);
    std::string path(_path);
    path.append("/");
    path.append(segment_name);

    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        return -1;
    }

    _fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (_fd >= 0) {
        LOG(NOTICE) << "create new segment, path: " << path << " start: " << _start_index;
        return 0;
    } else {
        return -1;
    }
}

int Segment::load(const base::Callback<void(int64_t, const Configuration&)>& configuration_cb) {
    int ret = 0;

    std::string path(_path);
    // create fd
    if (_is_open) {
        char segment_name[128];
        snprintf(segment_name, sizeof(segment_name), _s_open_pattern, _start_index);
        path.append("/");
        path.append(segment_name);
    } else {
        char segment_name[128];
        snprintf(segment_name, sizeof(segment_name), _s_closed_pattern, _start_index, _end_index);
        path.append("/");
        path.append(segment_name);
    }
    _fd = ::open(path.c_str(), O_RDWR);
    if (_fd < 0) {
        return -1;
    }

    // get file size
    struct stat st_buf;
    ret = fstat(_fd, &st_buf);
    if (ret != 0) {
        return ret;
    }

    // load entry index
    int64_t file_size = st_buf.st_size;
    int64_t entry_off = 0;
    int64_t load_end_index = _start_index - 1;
    for (int64_t i = _start_index; entry_off < file_size; i++) {
        EntryHeader header;
        ssize_t nr = ::pread(_fd, &header, sizeof(header), entry_off);
        if (nr == sizeof(header)) {
            int64_t skip_len = sizeof(header) +
                base::NetToHost32(header.meta_len) + base::NetToHost32(header.data_len);
            if (BAIDU_UNLIKELY(0 != strncmp(header.magic, _s_entry_magic,
                                            strlen(_s_entry_magic)))) {
                LOG(WARNING) << "magic check failed, path: " << _path << " index: " << i;
                break;
            }

            if (entry_off + skip_len <= file_size) {
                // parse meta
                bool meta_ok = true;
                if (base::NetToHost32(header.type) == ENTRY_TYPE_ADD_PEER ||
                    base::NetToHost32(header.type) == ENTRY_TYPE_REMOVE_PEER) {
                    int meta_len = base::NetToHost32(header.meta_len);
                    DEFINE_SMALL_ARRAY(char, meta_buf, meta_len, 128);
                    do {
                        nr = ::pread(_fd, meta_buf, meta_len, entry_off + sizeof(header));
                        if (BAIDU_UNLIKELY(nr != meta_len)) {
                            meta_ok = false;
                            break;
                        }

                        ConfigurationMeta meta;
                        if (BAIDU_UNLIKELY(!meta.ParseFromArray(meta_buf, meta_len))) {
                            LOG(WARNING) << "EntryMeta parse failed, path: " << _path
                                << " index: " << i;
                            meta_ok = false;
                            break;
                        }

                        EntryType entry_type = static_cast<EntryType>(
                                base::NetToHost32(header.type));
                        if (entry_type == ENTRY_TYPE_ADD_PEER 
                                || entry_type == ENTRY_TYPE_REMOVE_PEER) {
                            std::vector<PeerId> peers;
                            for (int j = 0; j < meta.peers_size(); j++) {
                                PeerId peer_id;
                                if (0 != peer_id.parse(meta.peers(j))) {
                                    LOG(WARNING) << "ConfigurationMeta parse peers"
                                        " failed, path: " << _path << " index: " << i;
                                    meta_ok = false;
                                    break;
                                }
                                peers.push_back(peer_id);
                            }

                            if (meta_ok) {
                                configuration_cb.Run(i, Configuration(peers));
                            }
                        }
                    } while (0);
                }
                if (BAIDU_UNLIKELY(!meta_ok)) {
                    break;
                }

                // index
                _offset.push_back(entry_off);
                if (_is_open) {
                    _end_index++;
                }
                entry_off += skip_len;
            } else {
                // last entry not write complete
                break;
            }
        } else if (nr >= 0) {
            // last entry not write complete
            break;
        } else {
            ret = nr;
            break;
        }
    }

    // truncate last uncompleted entry
    if (ret == 0 && entry_off != file_size) {
        LOG(NOTICE) << "truncate last uncompleted write entry, path: " << _path
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
        return -1;
    }

    EntryHeader header;
    header.term = base::HostToNet64(entry->term);
    header.data_len = base::HostToNet32(entry->len);

    std::string meta_str;
    switch (entry->type) {
    case ENTRY_TYPE_DATA:
        header.type = base::HostToNet32(ENTRY_TYPE_DATA);
        break;
    case ENTRY_TYPE_NO_OP:
        header.type = base::HostToNet32(ENTRY_TYPE_NO_OP);
        break;
    case ENTRY_TYPE_ADD_PEER: {
            header.type = base::HostToNet32(ENTRY_TYPE_ADD_PEER);
            ConfigurationMeta meta;
            const std::vector<std::string>& peers = *(entry->peers);
            for (size_t i = 0; i < peers.size(); i++) {
                meta.add_peers(peers[i]);
            }
            meta.SerializeToString(&meta_str);
            header.meta_len = base::HostToNet32(meta_str.size());
        }
        break;
    case ENTRY_TYPE_REMOVE_PEER: {
            header.type = base::HostToNet32(ENTRY_TYPE_REMOVE_PEER);
            ConfigurationMeta meta;
            const std::vector<std::string>& peers = *(entry->peers);
            for (size_t i = 0; i < peers.size(); i++) {
                meta.add_peers(peers[i]);
            }
            meta.SerializeToString(&meta_str);
            header.meta_len = base::HostToNet32(meta_str.size());
        }
        break;
    default:
        LOG(FATAL) << "unknow entry type: " << entry->type;
        return -1;
    }

    // crc32 checksum
    int32_t checksum = crc32(0, (const Bytef*)(&header.checksum) + sizeof(int32_t),
                             sizeof(header) - sizeof(int32_t) - sizeof(int32_t));
    if (BAIDU_UNLIKELY(meta_str.size() > 0)) {
        checksum = crc32(checksum, (const Bytef*)(meta_str.data()), meta_str.size());
    }
    if (entry->len > 0) {
        checksum = crc32(checksum, (const Bytef*)(entry->data), entry->len);
    }
    header.checksum = base::HostToNet32(checksum);

    int ret = 0;
    int64_t left_len = sizeof(header) + meta_str.size() + entry->len;
    // header, meta, data
    do {
        // prepare iovec
        struct iovec vec[3];
        int nvec = 0;
        if (static_cast<size_t>(left_len) > meta_str.size() + entry->len) {
            vec[0].iov_len = left_len - meta_str.size() - entry->len;
            vec[0].iov_base = &header + (sizeof(header) - vec[0].iov_len);
            vec[1].iov_len = meta_str.size();
            vec[1].iov_base = (void*)meta_str.data();
            nvec = 2;
            if (entry->len > 0) {
                vec[2].iov_len = entry->len;
                vec[2].iov_base = entry->data;
                nvec = 3;
            }
        } else if (left_len > entry->len) {
            vec[0].iov_len = left_len - entry->len;
            vec[0].iov_base = (void*)(meta_str.data() + (meta_str.size() - vec[0].iov_len));
            nvec = 1;
            if (entry->len > 0) {
                vec[1].iov_len = entry->len;
                vec[1].iov_base = entry->data;
                nvec = 2;
            }
        } else {
            assert(entry->len > 0);
            vec[0].iov_len = left_len;
            vec[0].iov_base = (void *)((char *)entry->data + (entry->len - left_len));
            nvec = 1;
        }

        // write iovec
        ssize_t nw = ::writev(_fd, vec, nvec);
        if (nw >= 0) {
            left_len -= nw;
        } else {
            ret = nw;
            break;
        }
    } while (left_len > 0);

    if (ret == 0) {
        _offset.push_back(_bytes);
        _end_index++;
        _bytes += (sizeof(header) + meta_str.size() + entry->len);
    }

    return ret;
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
    if (BAIDU_UNLIKELY(index > _end_index || index < _start_index)) {
        // out of range
        return NULL;
    } else if (BAIDU_UNLIKELY(_end_index == _start_index - 1)) {
        // empty
        return NULL;
    }

    bool ok = false;
    char* data_buf = NULL;
    LogEntry* entry = NULL;
    ConfigurationMeta configuration_meta;
    do {
        int64_t entry_cursor = _offset[index - _start_index];

        EntryHeader header;
        ssize_t nr = ::pread(_fd, &header, sizeof(header), entry_cursor);
        if (BAIDU_UNLIKELY(nr != sizeof(header))) {
            LOG(WARNING) << "pread Entry Header failed, path: " << _path << " index: " << index;
            break;
        }
        if (BAIDU_UNLIKELY(0 != strncmp(header.magic, _s_entry_magic, strlen(_s_entry_magic)))) {
            LOG(WARNING) << "magic check failed, path: " << _path << " index: " << index;
            break;
        }
        int32_t checksum = crc32(0, (const Bytef*)(&header.checksum) + sizeof(int32_t),
                         sizeof(header) - sizeof(int32_t) - sizeof(int32_t));
        entry_cursor += sizeof(header);

        int meta_len = base::NetToHost32(header.meta_len);
        if (meta_len > 0) {
            DEFINE_SMALL_ARRAY(char, meta_buf, meta_len, 128);
            nr = ::pread(_fd, meta_buf, meta_len, entry_cursor);
            if (BAIDU_UNLIKELY(nr != meta_len)) {
                LOG(WARNING) << "pread Entry Meta failed, path: " << _path << " index: " << index;
                break;
            }
            if (BAIDU_UNLIKELY(!configuration_meta.ParseFromArray(meta_buf, meta_len))) {
                LOG(WARNING) << "parse Entry Meta failed, path: " << _path << " index: " << index;
                break;
            }
            checksum = crc32(checksum, (const Bytef*)meta_buf, meta_len);
            entry_cursor += meta_len;
        }

        int data_len = base::NetToHost32(header.data_len);
        if (data_len > 0) {
            data_buf = (char*)malloc(data_len);
            nr = ::pread(_fd, data_buf, data_len, entry_cursor);
            if (BAIDU_UNLIKELY(nr != data_len)) {
                LOG(WARNING) << "pread data failed, path: " << _path << " index " << index;
                break;
            }

            checksum = crc32(checksum, (const Bytef*)data_buf, data_len);
            entry_cursor += data_len;
        }

        if (BAIDU_UNLIKELY(static_cast<uint32_t>(checksum) != base::NetToHost32(header.checksum))) {
            LOG(WARNING) << "checksum dismatch, path: " << _path << " index " << index;
            break;
        }

        entry = new LogEntry();
        switch (base::NetToHost32(header.type)) {
        case ENTRY_TYPE_DATA:
            entry->type = ENTRY_TYPE_DATA;
            break;
        case ENTRY_TYPE_NO_OP:
            entry->type = ENTRY_TYPE_NO_OP;
            break;
        case ENTRY_TYPE_ADD_PEER:
            entry->type = ENTRY_TYPE_ADD_PEER;
            entry->peers = new std::vector<std::string>;
            for (int i = 0; i < configuration_meta.peers_size(); i++) {
                entry->peers->push_back(configuration_meta.peers(i));
            }
            break;
        case ENTRY_TYPE_REMOVE_PEER:
            entry->type = ENTRY_TYPE_REMOVE_PEER;
            entry->peers = new std::vector<std::string>;
            for (int i = 0; i < configuration_meta.peers_size(); i++) {
                entry->peers->push_back(configuration_meta.peers(i));
            }
            break;
        default:
            assert(false);
            break;
        }
        entry->index = index;
        entry->term = base::NetToHost64(header.term);
        entry->len = data_len;
        entry->data = data_buf;
        ok = true;
    } while (0);

    if (BAIDU_UNLIKELY(!ok)) {
        free(data_buf);
        entry->data = NULL;
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
    ssize_t nr = ::pread(_fd, &header, sizeof(header), entry_cursor);
    if (BAIDU_UNLIKELY(nr != sizeof(header))) {
        LOG(WARNING) << "pread Entry Header failed, path: " << _path << " index: " << index;
        return 0;
    }
    if (BAIDU_UNLIKELY(0 != strncmp(header.magic, _s_entry_magic, strlen(_s_entry_magic)))) {
        LOG(WARNING) << "magic check failed, path: " << _path << " index: " << index;
        return 0;
    }

    return base::NetToHost64(header.term);
}

int Segment::close() {
    assert(_is_open);

    char open_name[128];
    snprintf(open_name, sizeof(open_name), _s_open_pattern, _start_index);
    char closed_name[128];
    snprintf(closed_name, sizeof(closed_name), _s_closed_pattern, _start_index, _end_index);

    std::string old_path(_path);
    std::string new_path(_path);
    old_path.append("/");
    old_path.append(open_name);
    new_path.append("/");
    new_path.append(closed_name);

    // TODO: optimize index memory usage by reconstruct vector
    int ret = this->sync();
    if (ret == 0) {
        _is_open = false;
        LOG(NOTICE) << "close segment, path: " << new_path
            << " start: " << _start_index << " end: " << _end_index;
        return ::rename(old_path.c_str(), new_path.c_str());
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
        path.append("/");
        if (_is_open) {
            char segment_name[128];
            snprintf(segment_name, sizeof(segment_name), _s_open_pattern, _start_index);
            path.append(segment_name);
        } else {
            char segment_name[128];
            snprintf(segment_name, sizeof(segment_name), _s_closed_pattern,
                     _start_index, _end_index);
            path.append(segment_name);
        }

        ret = ::unlink(path.c_str());
        if (ret != 0) {
            break;
        }

    } while (0);

    LOG(NOTICE) << "unlink segment, path: " << _path
        << " start_index: " << _start_index << " end_index" << _end_index;

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
    LOG(NOTICE) << "truncate " << _path << " start_index: " << _start_index
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
            std::string new_path(_path);
            char segment_name[128];
            snprintf(segment_name, sizeof(segment_name), _s_closed_pattern,
                     _start_index, _end_index);
            old_path.append("/");
            old_path.append(segment_name);

            snprintf(segment_name, sizeof(segment_name), _s_closed_pattern,
                     _start_index, last_index_kept);
            new_path.append("/");
            new_path.append(segment_name);
            ret = ::rename(old_path.c_str(), new_path.c_str());
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

    int64_t old_start_index = 0;
    Segment* last_segment = NULL;
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
        match = sscanf(dir_reader.name(), Segment::_s_closed_pattern, &start_index, &end_index);
        if (match == 2) {
            LOG(DEBUG) << "restore closed segment, path: " << _path
                << " start_index: " << start_index
                << " end_index: " << end_index;
            Segment* segment = new Segment(_path, start_index, end_index);
            _segments.insert(std::pair<int64_t, Segment*>(start_index, segment));
            continue;
        }

        match = sscanf(dir_reader.name(), Segment::_s_open_pattern, &start_index);
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
    meta_path.append("/");
    meta_path.append(Segment::_s_meta_file);

    _start_log_index = log_index;

    ProtoBufFile pb_file(meta_path);
    LogMeta meta;
    meta.set_start_log_index(log_index);
    return pb_file.save(&meta, true);
}

int SegmentLogStorage::load_meta() {
    std::string meta_path(_path);
    meta_path.append("/");
    meta_path.append(Segment::_s_meta_file);

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

        if (_open_segment->bytes() > _s_max_segment_size) {
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
