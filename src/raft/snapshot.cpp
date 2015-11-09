/*
 * =====================================================================================
 *
 *       Filename:  snapshot.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年11月04日 14时07分03秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <base/file_util.h>                         // base::CreateDirectory
#include <base/files/dir_reader_posix.h>            // base::DirReaderPosix
#include <base/string_printf.h>                     // base::string_appendf
#include <baidu/rpc/uri.h>
#include "raft/util.h"
#include "raft/protobuf_file.h"
#include "raft/local_storage.pb.h"
#include "raft/remote_path_copier.h"
#include "raft/snapshot.h"

#define RAFT_SNAPSHOT_PATTERN "snapshot_%020ld"

namespace raft {

const char* LocalSnapshotWriter::_s_snapshot_meta = "snapshot_meta";
const char* LocalSnapshotReader::_s_snapshot_meta = "snapshot_meta";
const char* LocalSnapshotStorage::_s_temp_path = "temp";
const char* LocalSnapshotStorage::_s_lock_path = "lock";

LocalSnapshotWriter::LocalSnapshotWriter(const std::string& path, const SnapshotMeta& meta)
    : SnapshotWriter(meta), _path(path) {
}

LocalSnapshotWriter::~LocalSnapshotWriter() {
}

int LocalSnapshotWriter::init() {
    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        set_error(EIO, "CreateDirectory failed, path: %s %m", _path.c_str());
        return EIO;
    }

    return 0;
}

int64_t LocalSnapshotWriter::snapshot_index() {
    return _meta.last_included_index;
}

int LocalSnapshotWriter::copy(const std::string& uri) {
    RemotePathCopier copier;
    int ret = 0;
    do {
        base::EndPoint remote_addr;
        std::string remote_path;
        ret = fileuri_parse(uri, &remote_addr, &remote_path);
        if (0 != ret) {
            LOG(WARNING) << "LocalSnapshotWriter copy failed, path " << _path << " uri " << uri;
            break;
        }

        std::string temp_path(_path);
        temp_path.append("/");
        temp_path.append(LocalSnapshotStorage::_s_temp_path);

        ret = copier.init(remote_addr, temp_path);
        if (0 != ret) {
            LOG(WARNING) << "LocalSnapshotWriter copy failed, path " << _path << " uri " << uri;
            break;
        }

        ret = copier.copy(remote_path, _path, NULL);
        if (0 != ret) {
            LOG(WARNING) << "LocalSnapshotWriter copy failed, path " << _path << " uri " << uri;
            break;
        }
    } while (0);
    return ret;
}

int LocalSnapshotWriter::save_meta() {
    std::string meta_path(_path);
    meta_path.append("/");
    meta_path.append(_s_snapshot_meta);

    SnapshotPBMeta pb_meta;
    pb_meta.set_last_included_index(_meta.last_included_index);
    pb_meta.set_last_included_term(_meta.last_included_term);
    std::vector<PeerId> peers;
    _meta.last_configuration.peer_vector(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        pb_meta.add_peers(peers[i].to_string());
    }

    ProtoBufFile pb_file(meta_path);
    int ret = pb_file.save(&pb_meta, true);
    if (0 != ret) {
        set_error(EIO, "PBFile save failed, path: %s %m", meta_path.c_str());
        ret = EIO;
    }
    return ret;
}

LocalSnapshotReader::LocalSnapshotReader(const std::string& path)
    : _path(path) {
}

LocalSnapshotReader::~LocalSnapshotReader() {
}

int LocalSnapshotReader::init() {
    base::FilePath dir_path(_path);
    if (!base::CreateDirectory(dir_path)) {
        set_error(EIO, "CreateDirectory failed, path: %s %m", _path.c_str());
        return EIO;
    }

    return 0;
}

int LocalSnapshotReader::load_meta(SnapshotMeta* meta) {
    std::string meta_path(_path);
    meta_path.append("/");
    meta_path.append(_s_snapshot_meta);

    SnapshotPBMeta pb_meta;
    pb_meta.set_last_included_index(meta->last_included_index);
    pb_meta.set_last_included_term(meta->last_included_term);
    std::vector<PeerId> peers;
    meta->last_configuration.peer_vector(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        pb_meta.add_peers(peers[i].to_string());
    }

    ProtoBufFile pb_file(meta_path);
    int ret = pb_file.load(&pb_meta);
    if (ret == 0) {
        meta->last_included_index = pb_meta.last_included_index();
        meta->last_included_term = pb_meta.last_included_term();
        for (int i = 0; i < pb_meta.peers_size(); i++) {
            meta->last_configuration.add_peer(PeerId(pb_meta.peers(i)));
        }
    } else {
        set_error(EIO, "PBFile load failed, path: %s %m", meta_path.c_str());
        ret = EIO;
    }
    return ret;
}

std::string LocalSnapshotReader::get_uri() {
    return std::string("file://") + _path;
}

LocalSnapshotStorage::LocalSnapshotStorage(const std::string& path)
    : SnapshotStorage(path), _path(path),
    _lock_fd(-1), _last_snapshot_index(0) {
}

LocalSnapshotStorage::~LocalSnapshotStorage() {
    if (_lock_fd >= 0) {
        ::close(_lock_fd);
        _lock_fd = -1;
    }
}

int LocalSnapshotStorage::init() {
    // open lock fd
    std::string lock_path(_path);
    lock_path.append("/");
    lock_path.append(_s_lock_path);
    _lock_fd = ::open(lock_path.c_str(), O_CREAT | O_WRONLY, 0600);
    if (_lock_fd < 0) {
        LOG(WARNING) << "open snapshot lockfile failed, path " << lock_path;
        return EIO;
    }

    // delete temp snapshot
    std::string temp_snapshot_path(_path);
    temp_snapshot_path.append("/");
    temp_snapshot_path.append(_s_temp_path);
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

    // get last_snapshot_index
    if (snapshots.size() > 0) {
        size_t snapshot_count = snapshots.size();
        for (size_t i = 0; i < snapshot_count - 1; i++) {
            int64_t index = *snapshots.begin();
            snapshots.erase(index);

            std::string snapshot_path(_path);
            base::string_appendf(&snapshot_path, "/" RAFT_SNAPSHOT_PATTERN, index);
            if (!base::DeleteFile(base::FilePath(snapshot_path), true)) {
                LOG(WARNING) << "delete old snapshot path failed, path " << snapshot_path;
                return EIO;
            }
        }

        _last_snapshot_index = *snapshots.begin();
    }
    return 0;
}

SnapshotWriter* LocalSnapshotStorage::create(const SnapshotMeta& meta) {
    LocalSnapshotWriter* writer = NULL;

    do {
        if (0 != ::lockf(_lock_fd, F_TLOCK, 0)) {
            LOG(WARNING) << "lock file failed, path " << _path;
            break;
        }

        std::string snapshot_path(_path);
        snapshot_path.append("/");
        snapshot_path.append(_s_temp_path);

        // delete temp
        base::FilePath temp_snapshot_path(snapshot_path);
        if (!base::DeleteFile(base::FilePath(snapshot_path), true)) {
            LOG(WARNING) << "delete temp snapshot path failed, path " << snapshot_path;
            break;
        }

        // create temp
        if (!base::CreateDirectory(base::FilePath(snapshot_path))) {
            LOG(WARNING) << "create temp snapshot path failed, path " << snapshot_path;
            break;
        }
        writer = new LocalSnapshotWriter(snapshot_path, meta);
    } while (0);

    return writer;
}

int LocalSnapshotStorage::close(SnapshotWriter* writer_) {
    LocalSnapshotWriter* writer = dynamic_cast<LocalSnapshotWriter*>(writer_);
    int ret = writer->error_code();
    do {
        if (0 != ret) {
            break;
        }

        int64_t old_index = _last_snapshot_index;
        int64_t new_index = writer->snapshot_index();

        // rename temp to new
        std::string temp_path(_path);
        temp_path.append("/");
        temp_path.append(_s_temp_path);
        std::string new_path(_path);
        base::string_appendf(&new_path, "/" RAFT_SNAPSHOT_PATTERN, new_index);

        if (0 != ::rename(temp_path.c_str(), new_path.c_str())) {
            LOG(WARNING) << "rename temp snapshot failed, from_path " << temp_path
                << " to_path " << new_path;
            ret = EIO;
            break;
        }

        std::string old_path(_path);
        base::string_appendf(&old_path, "/" RAFT_SNAPSHOT_PATTERN, old_index);
        if (!base::DeleteFile(base::FilePath(old_path), true)) {
            LOG(WARNING) << "delete old snapshot path failed, path " << old_path;
            ret = EIO;
            break;
        }

        _last_snapshot_index = new_index;
    } while (0);

    if (0 != ::lockf(_lock_fd, F_ULOCK, 0)) {
        LOG(WARNING) << "unlock file failed, path " << _path;
        ret = EIO;
    }
    delete writer;
    return ret;
}

SnapshotReader* LocalSnapshotStorage::open() {
    if (_last_snapshot_index != 0) {
        std::string snapshot_path(_path);
        base::string_appendf(&snapshot_path, "/" RAFT_SNAPSHOT_PATTERN, _last_snapshot_index);
        return new LocalSnapshotReader(snapshot_path);
    } else {
        return NULL;
    }
}

int LocalSnapshotStorage::close(SnapshotReader* reader) {
    delete reader;
    return 0;
}

SnapshotStorage* create_local_snapshot_storage(const std::string& uri) {
    std::string local_path = fileuri2path(uri);
    if (local_path.empty()) {
        return NULL;
    }

    LocalSnapshotStorage* storage = new LocalSnapshotStorage(local_path);
    return storage;
}

}
