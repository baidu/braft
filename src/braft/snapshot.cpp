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
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include <butil/time.h>
#include <butil/string_printf.h>                     // butil::string_appendf
#include <brpc/uri.h>
#include "braft/util.h"
#include "braft/protobuf_file.h"
#include "braft/local_storage.pb.h"
#include "braft/remote_file_copier.h"
#include "braft/snapshot.h"
#include "braft/node.h"
#include "braft/file_service.h"

//#define BRAFT_SNAPSHOT_PATTERN "snapshot_%020ld"
#define BRAFT_SNAPSHOT_PATTERN "snapshot_%020" PRId64
#define BRAFT_SNAPSHOT_META_FILE "__raft_snapshot_meta"

namespace braft {

const char* LocalSnapshotStorage::_s_temp_path = "temp";

LocalSnapshotMetaTable::LocalSnapshotMetaTable() {}

LocalSnapshotMetaTable::~LocalSnapshotMetaTable() {}

int LocalSnapshotMetaTable::add_file(const std::string& filename, 
                                const LocalFileMeta& meta) {
    Map::value_type value(filename, meta);
    std::pair<Map::iterator, bool> ret = _file_map.insert(value);
    LOG_IF(WARNING, !ret.second)
            << "file=" << filename << " already exists in snapshot";
    return ret.second ? 0 : -1;
}

int LocalSnapshotMetaTable::remove_file(const std::string& filename) {
    Map::iterator iter = _file_map.find(filename);
    if (iter == _file_map.end()) {
        return -1;
    }
    _file_map.erase(iter);
    return 0;
}

int LocalSnapshotMetaTable::save_to_file(FileSystemAdaptor* fs, const std::string& path) const {
    LocalSnapshotPbMeta pb_meta;
    if (_meta.IsInitialized()) {
        *pb_meta.mutable_meta() = _meta;
    }
    for (Map::const_iterator
            iter = _file_map.begin(); iter != _file_map.end(); ++iter) {
        LocalSnapshotPbMeta::File *f = pb_meta.add_files();
        f->set_name(iter->first);
        *f->mutable_meta() = iter->second;
    }
    ProtoBufFile pb_file(path, fs);
    int ret = pb_file.save(&pb_meta, raft_sync_meta());
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << path;
    return ret;
}

int LocalSnapshotMetaTable::load_from_file(FileSystemAdaptor* fs, const std::string& path) {
    ProtoBufFile pb_file(path, fs);
    LocalSnapshotPbMeta pb_meta;
    if (pb_file.load(&pb_meta) != 0) {
        PLOG(ERROR) << "Fail to load meta from " << path;
        return -1;
    }
    if (pb_meta.has_meta()) {
        _meta = pb_meta.meta();
    } else {
        _meta.Clear();
    }
    _file_map.clear();
    for (int i = 0; i < pb_meta.files_size(); ++i) {
        const LocalSnapshotPbMeta::File& f = pb_meta.files(i);
        _file_map[f.name()] = f.meta();
    }
    return 0;
}

int LocalSnapshotMetaTable::save_to_iobuf_as_remote(butil::IOBuf* buf) const {
    LocalSnapshotPbMeta pb_meta;
    if (_meta.IsInitialized()) {
        *pb_meta.mutable_meta() = _meta;
    }
    for (Map::const_iterator
            iter = _file_map.begin(); iter != _file_map.end(); ++iter) {
        LocalSnapshotPbMeta::File *f = pb_meta.add_files();
        f->set_name(iter->first);
        *f->mutable_meta() = iter->second;
        f->mutable_meta()->clear_source();
    }
    buf->clear();
    butil::IOBufAsZeroCopyOutputStream wrapper(buf);
    return pb_meta.SerializeToZeroCopyStream(&wrapper) ? 0 : -1;
}

int LocalSnapshotMetaTable::load_from_iobuf_as_remote(const butil::IOBuf& buf) {
    LocalSnapshotPbMeta pb_meta;
    butil::IOBufAsZeroCopyInputStream wrapper(buf);
    if (!pb_meta.ParseFromZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to parse LocalSnapshotPbMeta";
        return -1;
    }
    if (pb_meta.has_meta()) {
        _meta = pb_meta.meta();
    } else {
        _meta.Clear();
    }
    _file_map.clear();
    for (int i = 0; i < pb_meta.files_size(); ++i) {
        const LocalSnapshotPbMeta::File& f = pb_meta.files(i);
        _file_map[f.name()] = f.meta();
    }
    return 0;
}

void LocalSnapshotMetaTable::list_files(std::vector<std::string>* files) const {
    if (!files) {
        return;
    }
    files->clear();
    files->reserve(_file_map.size());
    for (Map::const_iterator
            iter = _file_map.begin(); iter != _file_map.end(); ++iter) {
        files->push_back(iter->first);
    }
}

int LocalSnapshotMetaTable::get_file_meta(const std::string& filename, 
                                          LocalFileMeta* file_meta) const {
    Map::const_iterator iter = _file_map.find(filename);
    if (iter == _file_map.end()) {
        return -1;
    }
    if (file_meta) {
        *file_meta = iter->second;
    }
    return 0;
}

std::string LocalSnapshot::get_path() { return std::string(); }

void LocalSnapshot::list_files(std::vector<std::string> *files) {
    return _meta_table.list_files(files);
}

int LocalSnapshot::get_file_meta(const std::string& filename, 
                                       ::google::protobuf::Message* file_meta) {
    LocalFileMeta* meta = NULL;
    if (file_meta) {
        meta = dynamic_cast<LocalFileMeta*>(file_meta);
        if (meta == NULL) {
            return -1;
        }
    }
    return _meta_table.get_file_meta(filename, meta);
}

LocalSnapshotWriter::LocalSnapshotWriter(const std::string& path,
                                         FileSystemAdaptor* fs)
    : _path(path), _fs(fs) {
}

LocalSnapshotWriter::~LocalSnapshotWriter() {
}

int LocalSnapshotWriter::init() {
    butil::File::Error e;
    if (!_fs->create_directory(_path, &e, false)) {
        set_error(EIO, "CreateDirectory failed, path: %s", _path.c_str());
        return EIO;
    }
    std::string meta_path = _path + "/" BRAFT_SNAPSHOT_META_FILE;
    if (_fs->path_exists(meta_path) && 
                _meta_table.load_from_file(_fs, meta_path) != 0) {
        set_error(EIO, "Fail to load metatable from %s", meta_path.c_str());
        return EIO;
    }

    // remove file if meta_path not exist or it's not in _meta_table 
    // to avoid dirty data
    {
         std::vector<std::string> to_remove;
         DirReader* dir_reader = _fs->directory_reader(_path);
         if (!dir_reader->is_valid()) {
             LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION,"
                 " path: " << _path;
             delete dir_reader;
             return EIO;
         }
         while (dir_reader->next()) {
             std::string filename = dir_reader->name();
             if (filename != BRAFT_SNAPSHOT_META_FILE) {
                 if (get_file_meta(filename, NULL) != 0) {
                     to_remove.push_back(filename);
                 }
             }
         }
         delete dir_reader;
         for (size_t i = 0; i < to_remove.size(); ++i) {
             std::string file_path = _path + "/" + to_remove[i];
             _fs->delete_file(file_path, false);
             LOG(WARNING) << "Snapshot file exist but meta not found so delete it,"
                 " path: " << file_path;
         }
    }

    return 0;
}

int64_t LocalSnapshotWriter::snapshot_index() {
    return _meta_table.has_meta() ? _meta_table.meta().last_included_index() : 0;
}

int LocalSnapshotWriter::remove_file(const std::string& filename) {
    return _meta_table.remove_file(filename);
}

int LocalSnapshotWriter::add_file(
        const std::string& filename, 
        const ::google::protobuf::Message* file_meta) {
    // TODO: normalize filename
    LocalFileMeta meta;
    if (file_meta) {
        meta.CopyFrom(*file_meta);
    }
    // TODO: Check file_meta
    return _meta_table.add_file(filename, meta);
}

void LocalSnapshotWriter::list_files(std::vector<std::string> *files) {
    return _meta_table.list_files(files);
}

int LocalSnapshotWriter::get_file_meta(const std::string& filename, 
                                       ::google::protobuf::Message* file_meta) {
    LocalFileMeta* meta = NULL;
    if (file_meta) {
        meta = dynamic_cast<LocalFileMeta*>(file_meta);
        if (meta == NULL) {
            return -1;
        }
    }
    return _meta_table.get_file_meta(filename, meta);
}

int LocalSnapshotWriter::save_meta(const SnapshotMeta& meta) {
    _meta_table.set_meta(meta);
    return 0;
}

int LocalSnapshotWriter::sync() {
    const int rc = _meta_table.save_to_file(_fs, _path + "/" BRAFT_SNAPSHOT_META_FILE);
    if (rc != 0 && ok()) {
        LOG(ERROR) << "Fail to sync, path: " << _path;
        set_error(rc, "Fail to sync : %s", berror(rc));
    }
    return rc;
}

LocalSnapshotReader::LocalSnapshotReader(const std::string& path,
                                         butil::EndPoint server_addr,
                                         FileSystemAdaptor* fs,
                                         SnapshotThrottle* snapshot_throttle)
    : _path(path)
    , _addr(server_addr)
    , _reader_id(0)
    , _fs(fs)
    , _snapshot_throttle(snapshot_throttle)
{}

LocalSnapshotReader::~LocalSnapshotReader() {
    destroy_reader_in_file_service();
}

int LocalSnapshotReader::init() {
    if (!_fs->directory_exists(_path)) {
        set_error(ENOENT, "Not such _path : %s", _path.c_str());
        return ENOENT;
    }
    std::string meta_path = _path + "/" BRAFT_SNAPSHOT_META_FILE;
    if (_meta_table.load_from_file(_fs, meta_path) != 0) {
        set_error(EIO, "Fail to load meta");
        return EIO;
    }
    return 0;
}

int LocalSnapshotReader::load_meta(SnapshotMeta* meta) {
    if (!_meta_table.has_meta()) {
        return -1;
    }
    *meta = _meta_table.meta();
    return 0;
}

int64_t LocalSnapshotReader::snapshot_index() {
    butil::FilePath path(_path);
    int64_t index = 0;
    int ret = sscanf(path.BaseName().value().c_str(), BRAFT_SNAPSHOT_PATTERN, &index);
    CHECK_EQ(ret, 1);
    return index;
}

void LocalSnapshotReader::list_files(std::vector<std::string> *files) {
    return _meta_table.list_files(files);
}

int LocalSnapshotReader::get_file_meta(const std::string& filename, 
                                       ::google::protobuf::Message* file_meta) {
    LocalFileMeta* meta = NULL;
    if (file_meta) {
        meta = dynamic_cast<LocalFileMeta*>(file_meta);
        if (meta == NULL) {
            return -1;
        }
    }
    return _meta_table.get_file_meta(filename, meta);
}

class SnapshotFileReader : public LocalDirReader {
public:
    SnapshotFileReader(FileSystemAdaptor* fs,
                       const std::string& path,
                       SnapshotThrottle* snapshot_throttle)
            : LocalDirReader(fs, path)
            , _snapshot_throttle(snapshot_throttle)
    {}

    void set_meta_table(const LocalSnapshotMetaTable &meta_table) {
        _meta_table = meta_table;
    }

    int read_file(butil::IOBuf* out,
                  const std::string &filename,
                  off_t offset,
                  size_t max_count,
                  bool read_partly,
                  size_t* read_count,
                  bool* is_eof) const {
        if (filename == BRAFT_SNAPSHOT_META_FILE) {
            int ret = _meta_table.save_to_iobuf_as_remote(out);
            if (ret == 0) {
                *read_count = out->size();
                *is_eof = true;
            }
            return ret;
        }
        LocalFileMeta file_meta;
        if (_meta_table.get_file_meta(filename, &file_meta) != 0) {
            return EPERM;
        }
        // go through throttle
        size_t new_max_count = max_count;
        if (_snapshot_throttle && FLAGS_raft_enable_throttle_when_install_snapshot) {
            int ret = 0;
            int64_t start = butil::cpuwide_time_us();
            int64_t used_count = 0;
            new_max_count = _snapshot_throttle->throttled_by_throughput(max_count);
            if (new_max_count < max_count) {
                // if it's not allowed to read partly or it's allowed but
                // throughput is throttled to 0, try again.
                if (!read_partly || new_max_count == 0) {
                    BRAFT_VLOG << "Read file throttled, path: " << path();
                    ret = EAGAIN;
                }
            }
            if (ret == 0) {
                ret = LocalDirReader::read_file_with_meta(
                    out, filename, &file_meta, offset, new_max_count, read_count, is_eof);
                used_count = out->size();
            }
            if ((ret == 0 || ret == EAGAIN) && used_count < (int64_t)new_max_count) {
                _snapshot_throttle->return_unused_throughput(
                        new_max_count, used_count, butil::cpuwide_time_us() - start);
            }
            return ret;
        }
        return LocalDirReader::read_file_with_meta(
                out, filename, &file_meta, offset, new_max_count, read_count, is_eof);
    }
   
private:
    LocalSnapshotMetaTable _meta_table;
    scoped_refptr<SnapshotThrottle> _snapshot_throttle;
};

std::string LocalSnapshotReader::generate_uri_for_copy() {
    if (_addr == butil::EndPoint()) {
        LOG(ERROR) << "Address is not specified, path:" << _path;
        return std::string();
    }
    if (_reader_id == 0) {
        // TODO: handler referenced files
        scoped_refptr<SnapshotFileReader> reader(
                new SnapshotFileReader(_fs.get(), _path, _snapshot_throttle.get()));
        reader->set_meta_table(_meta_table);
        if (!reader->open()) {
            LOG(ERROR) << "Open snapshot=" << _path << " failed";
            return std::string();
        }
        if (file_service_add(reader.get(), &_reader_id) != 0) {
            LOG(ERROR) << "Fail to add reader to file_service, path: " << _path;
            return std::string();
        }
    }
    std::ostringstream oss;
    oss << "remote://" << _addr << "/" << _reader_id;
    return oss.str();
}

void LocalSnapshotReader::destroy_reader_in_file_service() {
    if (_reader_id != 0) {
        CHECK_EQ(0, file_service_remove(_reader_id));
        _reader_id = 0;
    }
}

LocalSnapshotStorage::LocalSnapshotStorage(const std::string& path)
    : _path(path)
    , _last_snapshot_index(0)
{}

LocalSnapshotStorage::~LocalSnapshotStorage() {
}

int LocalSnapshotStorage::init() {
    butil::File::Error e;
    if (_fs == NULL) {
        _fs = default_file_system();
    }
    if (!_fs->create_directory(
                _path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << _path << " : " << e;
        return -1;
    }
    // delete temp snapshot
    if (!_filter_before_copy_remote) {
        std::string temp_snapshot_path(_path);
        temp_snapshot_path.append("/");
        temp_snapshot_path.append(_s_temp_path);
        LOG(INFO) << "Deleting " << temp_snapshot_path;
        if (!_fs->delete_file(temp_snapshot_path, true)) {
            LOG(WARNING) << "delete temp snapshot path failed, path " << temp_snapshot_path;
            return EIO;
        }
    }

    // delete old snapshot
    DirReader* dir_reader = _fs->directory_reader(_path);
    if (!dir_reader->is_valid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION. path: " << _path;
        delete dir_reader;
        return EIO;
    }
    std::set<int64_t> snapshots;
    while (dir_reader->next()) {
        int64_t index = 0;
        int match = sscanf(dir_reader->name(), BRAFT_SNAPSHOT_PATTERN, &index);
        if (match == 1) {
            snapshots.insert(index);
        }
    }
    delete dir_reader;

    // TODO: add snapshot watcher

    // get last_snapshot_index
    if (snapshots.size() > 0) {
        size_t snapshot_count = snapshots.size();
        for (size_t i = 0; i < snapshot_count - 1; i++) {
            int64_t index = *snapshots.begin();
            snapshots.erase(index);

            std::string snapshot_path(_path);
            butil::string_appendf(&snapshot_path, "/" BRAFT_SNAPSHOT_PATTERN, index);
            LOG(INFO) << "Deleting snapshot `" << snapshot_path << "'";
            // TODO: Notify Watcher before delete directories.
            if (!_fs->delete_file(snapshot_path, true)) {
                LOG(WARNING) << "delete old snapshot path failed, path " << snapshot_path;
                return EIO;
            }
        }

        _last_snapshot_index = *snapshots.begin();
        ref(_last_snapshot_index);
    }

    return 0;
}

void LocalSnapshotStorage::ref(const int64_t index) {
    BAIDU_SCOPED_LOCK(_mutex);
    _ref_map[index]++;
}

int LocalSnapshotStorage::destroy_snapshot(const std::string& path) {
    LOG(INFO) << "Deleting "  << path;
    if (!_fs->delete_file(path, true)) {
        LOG(WARNING) << "delete old snapshot path failed, path " << path;
        return -1;
    }
    return 0;
}

void LocalSnapshotStorage::unref(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    std::map<int64_t, int>::iterator it = _ref_map.find(index);
    if (it != _ref_map.end()) {
        it->second--;

        if (it->second == 0) {
            _ref_map.erase(it);
            lck.unlock();
            std::string old_path(_path);
            butil::string_appendf(&old_path, "/" BRAFT_SNAPSHOT_PATTERN, index);
            destroy_snapshot(old_path);
        }
    }
}

SnapshotWriter* LocalSnapshotStorage::create() {
    return create(true);
}

SnapshotWriter* LocalSnapshotStorage::create(bool from_empty) {
    LocalSnapshotWriter* writer = NULL;

    do {
        std::string snapshot_path(_path);
        snapshot_path.append("/");
        snapshot_path.append(_s_temp_path);

        // delete temp
        // TODO: Notify watcher before deleting
        if (_fs->path_exists(snapshot_path) && from_empty) {
            if (destroy_snapshot(snapshot_path) != 0) {
                break;
            }
        }

        writer = new LocalSnapshotWriter(snapshot_path, _fs.get());
        if (writer->init() != 0) {
            LOG(ERROR) << "Fail to init writer, path: " << snapshot_path;
            delete writer;
            writer = NULL;
            break;
        }
        BRAFT_VLOG << "Create writer success, path: " << snapshot_path;
    } while (0);

    return writer;
}

SnapshotCopier* LocalSnapshotStorage::start_to_copy_from(const std::string& uri) {
    LocalSnapshotCopier* copier = new LocalSnapshotCopier();
    copier->_storage = this;
    copier->_filter_before_copy_remote = _filter_before_copy_remote;
    copier->_fs = _fs.get();
    copier->_throttle = _snapshot_throttle.get();
    if (copier->init(uri) != 0) {
        LOG(ERROR) << "Fail to init copier from " << uri
                   << " path: " << _path;
        delete copier;
        return NULL;
    }
    copier->start();
    return copier;
}

int LocalSnapshotStorage::close(SnapshotCopier* copier) {
    delete copier;
    return 0;
}

SnapshotReader* LocalSnapshotStorage::copy_from(const std::string& uri) {
    SnapshotCopier* c = start_to_copy_from(uri);
    if (c == NULL) {
        return NULL;
    }
    c->join();
    SnapshotReader* reader = c->get_reader();
    close(c);
    return reader;
}

int LocalSnapshotStorage::close(SnapshotWriter* writer) {
    return close(writer, false);
}

int LocalSnapshotStorage::close(SnapshotWriter* writer_base,
                                bool keep_data_on_error) {
    LocalSnapshotWriter* writer = dynamic_cast<LocalSnapshotWriter*>(writer_base);
    int ret = writer->error_code();
    do {
        if (0 != ret) {
            break;
        }
        ret = writer->sync();
        if (ret != 0) {
            break;
        }
        int64_t old_index = 0;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            old_index = _last_snapshot_index;
        }
        int64_t new_index = writer->snapshot_index();
        if (new_index == old_index) {
            ret = EEXIST;
            break;
        }

        // rename temp to new
        std::string temp_path(_path);
        temp_path.append("/");
        temp_path.append(_s_temp_path);
        std::string new_path(_path);
        butil::string_appendf(&new_path, "/" BRAFT_SNAPSHOT_PATTERN, new_index);
        LOG(INFO) << "Deleting " << new_path;
        if (!_fs->delete_file(new_path, true)) {
            LOG(WARNING) << "delete new snapshot path failed, path " << new_path;
            ret = EIO;
            break;
        }
        LOG(INFO) << "Renaming " << temp_path << " to " << new_path;
        if (!_fs->rename(temp_path, new_path)) {
            LOG(WARNING) << "rename temp snapshot failed, from_path " << temp_path
                         << " to_path " << new_path;
            ret = EIO;
            break;
        }

        ref(new_index);
        {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK_EQ(old_index, _last_snapshot_index);
            _last_snapshot_index = new_index;
        }
        // unref old_index, ref new_index
        unref(old_index);
    } while (0);

    if (ret != 0 && !keep_data_on_error) {
        destroy_snapshot(writer->get_path());
    }
    delete writer;
    return ret != EIO ? 0 : -1;
}

SnapshotReader* LocalSnapshotStorage::open() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_last_snapshot_index != 0) {
        const int64_t last_snapshot_index = _last_snapshot_index;
        ++_ref_map[last_snapshot_index];
        lck.unlock();
        std::string snapshot_path(_path);
        butil::string_appendf(&snapshot_path, "/" BRAFT_SNAPSHOT_PATTERN, last_snapshot_index);
        LocalSnapshotReader* reader = new LocalSnapshotReader(snapshot_path, _addr, 
                _fs.get(), _snapshot_throttle.get());
        if (reader->init() != 0) {
            CHECK(!lck.owns_lock());
            unref(last_snapshot_index);
            delete reader;
            return NULL;
        }
        return reader;
    } else {
        errno = ENODATA;
        return NULL;
    }
}

int LocalSnapshotStorage::close(SnapshotReader* reader_) {
    LocalSnapshotReader* reader = dynamic_cast<LocalSnapshotReader*>(reader_);
    unref(reader->snapshot_index());
    delete reader;
    return 0;
}

int LocalSnapshotStorage::set_filter_before_copy_remote() {
    _filter_before_copy_remote = true;
    return 0;
}

int LocalSnapshotStorage::set_file_system_adaptor(FileSystemAdaptor* fs) {
    if (fs == NULL) {
        LOG(ERROR) << "file system is NULL, path: " << _path;
        return -1;
    }
    _fs = fs;
    return 0;
}

int LocalSnapshotStorage::set_snapshot_throttle(SnapshotThrottle* snapshot_throttle) {
    _snapshot_throttle = snapshot_throttle;
    return 0;
}

SnapshotStorage* LocalSnapshotStorage::new_instance(const std::string& uri) const {
    return new LocalSnapshotStorage(uri);
}

// LocalSnapshotCopier

LocalSnapshotCopier::LocalSnapshotCopier() 
    : _tid(INVALID_BTHREAD)
    , _cancelled(false)
    , _filter_before_copy_remote(false)
    , _fs(NULL)
    , _throttle(NULL)
    , _writer(NULL)
    , _storage(NULL)
    , _reader(NULL)
    , _cur_session(NULL)
{}

LocalSnapshotCopier::~LocalSnapshotCopier() {
    CHECK(!_writer);
}

void *LocalSnapshotCopier::start_copy(void* arg) {
    LocalSnapshotCopier* c = (LocalSnapshotCopier*)arg;
    c->copy();
    return NULL;
}

void LocalSnapshotCopier::copy() {
    do {
        load_meta_table();
        if (!ok()) {
            break;
        }
        filter();
        if (!ok()) {
            break;
        }
        std::vector<std::string> files;
        _remote_snapshot.list_files(&files);
        for (size_t i = 0; i < files.size() && ok(); ++i) {
            copy_file(files[i]);
        }
    } while (0);
    if (!ok() && _writer && _writer->ok()) {
        LOG(WARNING) << "Fail to copy, error_code " << error_code()
                     << " error_msg " << error_cstr() 
                     << " writer path " << _writer->get_path();
        _writer->set_error(error_code(), error_cstr());
    }
    if (_writer) {
        // set_error for copier only when failed to close writer and copier was 
        // ok before this moment 
        if (_storage->close(_writer, _filter_before_copy_remote) != 0 && ok()) {
            set_error(EIO, "Fail to close writer");
        }
        _writer = NULL;
    }
    if (ok()) {
        _reader = _storage->open();
    }
}

void LocalSnapshotCopier::load_meta_table() {
    butil::IOBuf meta_buf;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_cancelled) {
        set_error(ECANCELED, "%s", berror(ECANCELED));
        return;
    }
    scoped_refptr<RemoteFileCopier::Session> session
            = _copier.start_to_copy_to_iobuf(BRAFT_SNAPSHOT_META_FILE,
                                            &meta_buf, NULL);
    _cur_session = session.get();
    lck.unlock();
    session->join();
    lck.lock();
    _cur_session = NULL;
    lck.unlock();
    if (!session->status().ok()) {
        LOG(WARNING) << "Fail to copy meta file : " << session->status();
        set_error(session->status().error_code(), session->status().error_cstr());
        return;
    }
    if (_remote_snapshot._meta_table.load_from_iobuf_as_remote(meta_buf) != 0) {
        LOG(WARNING) << "Bad meta_table format";
        set_error(-1, "Bad meta_table format");
        return;
    }
    CHECK(_remote_snapshot._meta_table.has_meta());
}

int LocalSnapshotCopier::filter_before_copy(LocalSnapshotWriter* writer, 
                                            SnapshotReader* last_snapshot) {
    std::vector<std::string> existing_files;
    writer->list_files(&existing_files);
    std::vector<std::string> to_remove;

    for (size_t i = 0; i < existing_files.size(); ++i) {
        if (_remote_snapshot.get_file_meta(existing_files[i], NULL) != 0) {
            to_remove.push_back(existing_files[i]);
            writer->remove_file(existing_files[i]);
        }
    }

    std::vector<std::string> remote_files;
    _remote_snapshot.list_files(&remote_files);
    for (size_t i = 0; i < remote_files.size(); ++i) {
        const std::string& filename = remote_files[i];
        LocalFileMeta remote_meta;
        CHECK_EQ(0, _remote_snapshot.get_file_meta(
                filename, &remote_meta));
        if (!remote_meta.has_checksum()) {
            // Redownload file if this file doen't have checksum
            writer->remove_file(filename);
            to_remove.push_back(filename);
            continue;
        }

        LocalFileMeta local_meta;
        if (writer->get_file_meta(filename, &local_meta) == 0) {
            if (local_meta.has_checksum() &&
                local_meta.checksum() == remote_meta.checksum()) {
                LOG(INFO) << "Keep file=" << filename
                          << " checksum=" << remote_meta.checksum()
                          << " in " << writer->get_path();
                continue;
            }
            // Remove files from writer so that the file is to be copied from
            // remote_snapshot or last_snapshot
            writer->remove_file(filename);
            to_remove.push_back(filename);
        }

        // Try find files in last_snapshot
        if (!last_snapshot) {
            continue;
        }
        if (last_snapshot->get_file_meta(filename, &local_meta) != 0) {
            continue;
        }
        if (!local_meta.has_checksum() || local_meta.checksum() != remote_meta.checksum()) {
            continue;
        }
        LOG(INFO) << "Found the same file=" << filename
                  << " checksum=" << remote_meta.checksum()
                  << " in last_snapshot=" << last_snapshot->get_path();
        if (local_meta.source() == braft::FILE_SOURCE_LOCAL) {
            std::string source_path = last_snapshot->get_path() + '/'
                                      + filename;
            std::string dest_path = writer->get_path() + '/'
                                      + filename;
            _fs->delete_file(dest_path, false);
            if (!_fs->link(source_path, dest_path)) {
                PLOG(ERROR) << "Fail to link " << source_path
                            << " to " << dest_path;
                continue;
            }
            // Don't delete linked file
            if (!to_remove.empty() && to_remove.back() == filename) {
                to_remove.pop_back();
            }
        }
        // Copy file from last_snapshot
        writer->add_file(filename, &local_meta);
    }

    if (writer->sync() != 0) {
        LOG(ERROR) << "Fail to sync writer on path=" << writer->get_path();
        return -1;
    }

    for (size_t i = 0; i < to_remove.size(); ++i) {
        std::string file_path = writer->get_path() + "/" + to_remove[i];
        _fs->delete_file(file_path, false);
    }

    return 0;
}

void LocalSnapshotCopier::filter() {
    _writer = (LocalSnapshotWriter*)_storage->create(!_filter_before_copy_remote);
    if (_writer == NULL) {
        set_error(EIO, "Fail to create snapshot writer");
        return;
    }

    if (_filter_before_copy_remote) {
        SnapshotReader* reader = _storage->open();
        if (filter_before_copy(_writer, reader) != 0) {
            LOG(WARNING) << "Fail to filter writer before copying"
                            ", path: " << _writer->get_path() 
                         << ", destroy and create a new writer";
            _writer->set_error(-1, "Fail to filter");
            _storage->close(_writer, false);
            _writer = (LocalSnapshotWriter*)_storage->create(true);
        }
        if (reader) {
            _storage->close(reader);
        }
        if (_writer == NULL) {
            set_error(EIO, "Fail to create snapshot writer");
            return;
        }
    }
    _writer->save_meta(_remote_snapshot._meta_table.meta());
    if (_writer->sync() != 0) {
        set_error(EIO, "Fail to sync snapshot writer");
        return;
    }
}

void LocalSnapshotCopier::copy_file(const std::string& filename) {
    if (_writer->get_file_meta(filename, NULL) == 0) {
        LOG(INFO) << "Skipped downloading " << filename
                  << " path: " << _writer->get_path();
        return;
    }
    std::string file_path = _writer->get_path() + '/' + filename;
    butil::FilePath sub_path(filename);
    if (sub_path != sub_path.DirName() && sub_path.DirName().value() != ".") {
        butil::File::Error e;
        bool rc = false;
        if (FLAGS_raft_create_parent_directories) {
            butil::FilePath sub_dir =
                    butil::FilePath(_writer->get_path()).Append(sub_path.DirName());
            rc = _fs->create_directory(sub_dir.value(), &e, true);
        } else {
            rc = create_sub_directory(
                    _writer->get_path(), sub_path.DirName().value(), _fs, &e);
        }
        if (!rc) {
            LOG(ERROR) << "Fail to create directory for " << file_path
                       << " : " << butil::File::ErrorToString(e);
            set_error(file_error_to_os_error(e), 
                      "Fail to create directory");
        }
    }
    LocalFileMeta meta;
    _remote_snapshot.get_file_meta(filename, &meta);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_cancelled) {
        set_error(ECANCELED, "%s", berror(ECANCELED));
        return;
    }
    scoped_refptr<RemoteFileCopier::Session> session
        = _copier.start_to_copy_to_file(filename, file_path, NULL);
    if (session == NULL) {
        LOG(WARNING) << "Fail to copy " << filename
                     << " path: " << _writer->get_path();
        set_error(-1, "Fail to copy %s", filename.c_str());
        return;
    }
    _cur_session = session.get();
    lck.unlock();
    session->join();
    lck.lock();
    _cur_session = NULL;
    lck.unlock();
    if (!session->status().ok()) {
        set_error(session->status().error_code(), session->status().error_cstr());
        return;
    }
    if (_writer->add_file(filename, &meta) != 0) {
        set_error(EIO, "Fail to add file to writer");
        return;
    }
    if (_writer->sync() != 0) {
        set_error(EIO, "Fail to sync writer");
        return;
    }
}

void LocalSnapshotCopier::start() {
    if (bthread_start_background(
                &_tid, NULL, start_copy, this) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        copy();
    }
}

void LocalSnapshotCopier::join() {
    bthread_join(_tid, NULL);
}

void LocalSnapshotCopier::cancel() {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_cancelled) {
        return;
    }
    _cancelled = true;
    if (_cur_session) {
        _cur_session->cancel();
    }
}

int LocalSnapshotCopier::init(const std::string& uri) {
    return _copier.init(uri, _fs, _throttle);
}

}  //  namespace braft
