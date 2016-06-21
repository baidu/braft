// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/11/05 11:34:03

#include <base/time.h>
#include <base/file_util.h>                         // base::CreateDirectory
#include <base/files/dir_reader_posix.h>            // base::DirReaderPosix
#include <base/string_printf.h>                     // base::string_appendf
#include <baidu/rpc/uri.h>
#include "raft/util.h"
#include "raft/protobuf_file.h"
#include "raft/local_storage.pb.h"
#include "raft/remote_path_copier.h"
#include "raft/snapshot.h"
#include "raft/node.h"
#include "raft/file_service.h"

#define RAFT_SNAPSHOT_PATTERN "snapshot_%020ld"
#define RAFT_SNAPSHOT_META_FILE "__raft_snapshot_meta"

namespace raft {

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

int LocalSnapshotMetaTable::save_to_file(const std::string& path) const {
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
    ProtoBufFile pb_file(path);
    return pb_file.save(&pb_meta, FLAGS_raft_sync);
}

int LocalSnapshotMetaTable::load_from_file(const std::string& path) {
    ProtoBufFile pb_file(path);
    LocalSnapshotPbMeta pb_meta;
    if (pb_file.load(&pb_meta) != 0) {
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

int LocalSnapshotMetaTable::save_to_iobuf_as_remote(base::IOBuf* buf) const {
    LocalSnapshotPbMeta pb_meta;
    if (_meta.IsInitialized()) {
        *pb_meta.mutable_meta() = _meta;
    }
    for (Map::const_iterator
            iter = _file_map.begin(); iter != _file_map.end(); ++iter) {
        LocalSnapshotPbMeta::File *f = pb_meta.add_files();
        f->set_name(iter->first);
        *f->mutable_meta() = iter->second;
        f->mutable_meta()->clear_reference();
    }
    buf->clear();
    base::IOBufAsZeroCopyOutputStream wrapper(buf);
    return pb_meta.SerializeToZeroCopyStream(&wrapper) ? 0 : -1;
}

int LocalSnapshotMetaTable::load_from_iobuf_as_remote(const base::IOBuf& buf) {
    LocalSnapshotPbMeta pb_meta;
    base::IOBufAsZeroCopyInputStream wrapper(buf);
    if (!pb_meta.ParseFromZeroCopyStream(&wrapper)) {
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

std::string RemoteSnapshot::get_path() { return std::string(); }

void RemoteSnapshot::list_files(std::vector<std::string> *files) {
    return _meta_table.list_files(files);
}

int RemoteSnapshot::get_file_meta(const std::string& filename, 
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

LocalSnapshotWriter::LocalSnapshotWriter(const std::string& path)
    : _path(path) {
}

LocalSnapshotWriter::~LocalSnapshotWriter() {
}

int LocalSnapshotWriter::init() {
    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        set_error(EIO, "CreateDirectory failed, path: %s %m", _path.c_str());
        return EIO;
    }
    base::FilePath meta_path = dir_path.Append(RAFT_SNAPSHOT_META_FILE);
    if (base::PathExists(meta_path) && 
                _meta_table.load_from_file(meta_path.value()) != 0) {
        set_error(EIO, "Fail to load metatable from %s", meta_path.value().c_str());
        return EIO;
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
    return _meta_table.save_to_file(_path + "/" + RAFT_SNAPSHOT_META_FILE);
}

LocalSnapshotReader::LocalSnapshotReader(const std::string& path,
                                         base::EndPoint server_addr)
    : _path(path)
    , _addr(server_addr)
    , _reader_id(0) {
}

LocalSnapshotReader::~LocalSnapshotReader() {
    destroy_reader_in_file_service();
}

int LocalSnapshotReader::init() {
    base::FilePath dir_path(_path);
    if (!base::DirectoryExists(dir_path)) {
        set_error(ENOENT, "Not such _path : %s", _path.c_str());
        return ENOENT;
    }
    if (_meta_table.load_from_file(
                dir_path.Append(RAFT_SNAPSHOT_META_FILE).value()) != 0) {
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
    base::FilePath path(_path);
    int64_t index = 0;
    int ret = sscanf(path.BaseName().value().c_str(), RAFT_SNAPSHOT_PATTERN, &index);
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

class SnapshotDirReader : public LocalDirReader {
public:
    SnapshotDirReader(const std::string& path)
            : LocalDirReader(path)
    {}
    void set_meta_table(const LocalSnapshotMetaTable &meta_table) {
        _meta_table = meta_table;
    }
    int read_file(base::IOBuf* out,
                  const std::string &filename,
                  off_t offset,
                  size_t max_count,
                  bool* is_eof) const {
        if (filename == RAFT_SNAPSHOT_META_FILE) {
            *is_eof = true;
            return _meta_table.save_to_iobuf_as_remote(out);
        }
        return LocalDirReader::read_file(
                out, filename, offset, max_count, is_eof);
    }
    
private:
    LocalSnapshotMetaTable _meta_table;
};

std::string LocalSnapshotReader::generate_uri_for_copy() {
    if (_addr == base::EndPoint()) {
        LOG(ERROR) << "Address is not specified";
        return std::string();
    }
    if (_reader_id == 0) {
        // TODO: handler referenced files
        scoped_refptr<SnapshotDirReader> reader(new SnapshotDirReader(_path));
        reader->set_meta_table(_meta_table);
        std::vector<std::string> filelist;
        list_files(&filelist);
        for (size_t i = 0; i < filelist.size(); ++i) {
            reader->add_white_list(filelist[i]);
        }
        if (file_service_add(reader.get(), &_reader_id) != 0) {
            LOG(ERROR) << "Fail to add reader to file_service";
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
    : _path(path),
    _last_snapshot_index(0) {
    if (!PathACL::GetInstance()->add(_path)) {
        LOG(WARNING) << "LocalSnapshotStorage add PathACL failed, path: " << path;
    }
}

LocalSnapshotStorage::~LocalSnapshotStorage() {
    PathACL::GetInstance()->remove(_path);
}

int LocalSnapshotStorage::init() {
    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        LOG(ERROR) << "CreateDirectory failed, path: " << _path;
        return EIO;
    }

    // delete temp snapshot
    std::string temp_snapshot_path(_path);
    temp_snapshot_path.append("/");
    temp_snapshot_path.append(_s_temp_path);
    LOG(INFO) << "Deleting " << temp_snapshot_path;
    if (!base::DeleteFile(base::FilePath(temp_snapshot_path), true)) {
        LOG(WARNING) << "delete temp snapshot path failed, path " << temp_snapshot_path;
        return EIO;
    }

    // delete old snapshot
    base::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION. path: " << _path;
        return EIO;
    }
    std::set<int64_t> snapshots;
    while (dir_reader.Next()) {
        int64_t index = 0;
        int match = sscanf(dir_reader.name(), RAFT_SNAPSHOT_PATTERN, &index);
        if (match == 1) {
            snapshots.insert(index);
        }
    }

    // TODO: add snapshot watcher

    // get last_snapshot_index
    if (snapshots.size() > 0) {
        size_t snapshot_count = snapshots.size();
        for (size_t i = 0; i < snapshot_count - 1; i++) {
            int64_t index = *snapshots.begin();
            snapshots.erase(index);

            std::string snapshot_path(_path);
            base::string_appendf(&snapshot_path, "/" RAFT_SNAPSHOT_PATTERN, index);
            LOG(INFO) << "Deleting snapshot `" << snapshot_path << "'";
            // TODO: Notify Watcher before delete directories.
            if (!base::DeleteFile(base::FilePath(snapshot_path), true)) {
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
    _ref_map[index] ++;
}

void LocalSnapshotStorage::unref(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    std::map<int64_t, int>::iterator it = _ref_map.find(index);
    if (it != _ref_map.end()) {
        it->second--;

        if (it->second == 0) {
            lck.unlock();
            std::string old_path(_path);
            base::string_appendf(&old_path, "/" RAFT_SNAPSHOT_PATTERN, index);
            LOG(INFO) << "Deleting snapshot `" << old_path << "'";
            bool ok = base::DeleteFile(base::FilePath(old_path), true);
            CHECK(ok) << "delete old snapshot path failed, path " << old_path;
        }
    }
}

SnapshotWriter* LocalSnapshotStorage::create() {
    LocalSnapshotWriter* writer = NULL;

    do {
        std::string snapshot_path(_path);
        snapshot_path.append("/");
        snapshot_path.append(_s_temp_path);

        // delete temp
        // TODO: Notify watcher before deleting
        base::FilePath temp_snapshot_path(snapshot_path);
        LOG(INFO) << "Deleting " << snapshot_path;
        if (!base::DeleteFile(base::FilePath(snapshot_path), true)) {
            LOG(WARNING) << "delete temp snapshot path failed, path " << snapshot_path;
            break;
        }

        // create temp
        if (!base::CreateDirectory(base::FilePath(snapshot_path))) {
            LOG(WARNING) << "create temp snapshot path failed, path " << snapshot_path;
            break;
        }
        writer = new LocalSnapshotWriter(snapshot_path);
        if (writer->init() != 0) {
            LOG(ERROR) << "Fail to init writer";
            delete writer;
            writer = NULL;
            break;
        }
    } while (0);

    return writer;
}

SnapshotReader* LocalSnapshotStorage::copy_from(const std::string& uri) {
    // TODO: keep temp files and 
    RemotePathCopier copier;
    if (copier.init(uri) != 0) {
        LOG(WARNING) << "Fail to init RemotePathCopier to " << uri;
        return NULL;
    }
    base::IOBuf meta_buf;
    if (copier.copy_to_iobuf(RAFT_SNAPSHOT_META_FILE, &meta_buf, NULL) != 0) {
        LOG(WARNING) << "Fail to copy";
        return NULL;
    }
    LocalSnapshotMetaTable meta_table;
    if (meta_table.load_from_iobuf_as_remote(meta_buf) != 0) {
        LOG(WARNING) << "Bad meta_table format";
        return NULL;
    }
    std::vector<std::string> files;
    SnapshotWriter* writer = create();
    if (meta_table.has_meta()) {
        writer->save_meta(meta_table.meta());
    }
    meta_table.list_files(&files);
    for (size_t i = 0; i < files.size(); ++i) {
        std::string file_path = writer->get_path() + '/' + files[i];
        LocalFileMeta meta;
        meta_table.get_file_meta(files[i], &meta);
        if (copier.copy_to_file(files[i], file_path, NULL) != 0) {
            LOG(WARNING) << "Fail to copy " << files[i];
            writer->set_error(-1, "Fail to copy %s", files[i].c_str());
            close(writer);
            return NULL;
        }
        if (writer->add_file(files[i], &meta) != 0) {
            if (writer->ok()) {
                writer->set_error(-1, "Fail to add file=", files[i].c_str());
            }
            close(writer);
            return NULL;
        }
    }
    if (close(writer) != 0) {
        return NULL;
    }
    return open();
}

int LocalSnapshotStorage::close(SnapshotWriter* writer_) {
    LocalSnapshotWriter* writer = dynamic_cast<LocalSnapshotWriter*>(writer_);
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
        base::string_appendf(&new_path, "/" RAFT_SNAPSHOT_PATTERN, new_index);
        LOG(INFO) << "Deleting " << new_path;
        if (!base::DeleteFile(base::FilePath(new_path), true)) {
            LOG(WARNING) << "delete new snapshot path failed, path " << new_path;
            ret = EIO;
            break;
        }
        LOG(INFO) << "Renaming " << temp_path << " to " << new_path;
        if (0 != ::rename(temp_path.c_str(), new_path.c_str())) {
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

    if (ret != 0) {
        std::string temp_path(_path);
        temp_path.append("/");
        temp_path.append(_s_temp_path);
        LOG(INFO) << "Deleting " << temp_path;
        base::DeleteFile(base::FilePath(temp_path), true);
    }
    delete writer;
    return ret == EEXIST ? 0 : ret;;
}

SnapshotReader* LocalSnapshotStorage::open() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_last_snapshot_index != 0) {
        const int64_t last_snapshot_index = _last_snapshot_index;
        ++_ref_map[last_snapshot_index];
        lck.unlock();
        std::string snapshot_path(_path);
        base::string_appendf(&snapshot_path, "/" RAFT_SNAPSHOT_PATTERN, last_snapshot_index);
        LocalSnapshotReader* reader = new LocalSnapshotReader(snapshot_path, _addr);
        if (reader->init() != 0) {
            CHECK(!lck.owns_lock());
            unref(last_snapshot_index);
            delete reader;
            return NULL;
        }
        return reader;
    } else {
        return NULL;
    }
}

int LocalSnapshotStorage::close(SnapshotReader* reader_) {
    LocalSnapshotReader* reader = dynamic_cast<LocalSnapshotReader*>(reader_);
    unref(reader->snapshot_index());
    delete reader;
    return 0;
}

SnapshotStorage* LocalSnapshotStorage::new_instance(const std::string& uri) const {
    return new LocalSnapshotStorage(uri);
}

}  // namespace raft
