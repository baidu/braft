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

#define RAFT_SNAPSHOT_PATTERN "snapshot_%020ld"

namespace raft {

const char* LocalSnapshotWriter::_s_snapshot_meta = "snapshot_meta";
const char* LocalSnapshotReader::_s_snapshot_meta = "snapshot_meta";
const char* LocalSnapshotStorage::_s_temp_path = "temp";

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

        ret = copier.init(remote_addr);
        if (0 != ret) {
            LOG(WARNING) << "LocalSnapshotWriter copy failed, path " << _path << " uri " << uri;
            break;
        }

        LOG(INFO) << "copy from " << remote_addr << " " << remote_path << " to " << _path;
        ret = copier.copy(remote_path, _path, NULL);
        if (0 != ret) {
            LOG(WARNING) << "LocalSnapshotWriter copy failed, path " << _path << " uri " << uri;
            break;
        }
    } while (0);
    return ret;
}

int LocalSnapshotWriter::save_meta(const SnapshotMeta& meta) {
    base::Timer timer;
    timer.start();

    _meta = meta;
    std::string meta_path(_path);
    meta_path.append("/");
    meta_path.append(_s_snapshot_meta);

    SnapshotPBMeta pb_meta;
    pb_meta.set_last_included_index(_meta.last_included_index);
    pb_meta.set_last_included_term(_meta.last_included_term);
    std::vector<PeerId> peers;
    _meta.last_configuration.list_peers(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        pb_meta.add_peers(peers[i].to_string());
    }

    ProtoBufFile pb_file(meta_path);
    int ret = pb_file.save(&pb_meta, FLAGS_raft_sync /*true*/);
    if (0 != ret) {
        set_error(EIO, "PBFile save failed, path: %s %m", meta_path.c_str());
        ret = EIO;
    }

    timer.stop();
    LOG(INFO) << "snapshot save_meta " << meta_path << " time: " << timer.u_elapsed();
    return ret;
}

std::string LocalSnapshotWriter::get_uri(const base::EndPoint& hint_addr) {
    return std::string("file://") + base::endpoint2str(hint_addr).c_str() + '/' + _path;
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

int64_t LocalSnapshotReader::snapshot_index() {
    base::FilePath path(_path);
    int64_t index = 0;
    int ret = sscanf(path.BaseName().value().c_str(), RAFT_SNAPSHOT_PATTERN, &index);
    CHECK_EQ(ret, 1);

    return index;
}

int LocalSnapshotReader::load_meta(SnapshotMeta* meta) {
    base::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/");
    meta_path.append(_s_snapshot_meta);

    SnapshotPBMeta pb_meta;
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

    timer.stop();
    RAFT_VLOG << "snapshot load_meta " << meta_path << " time: " << timer.u_elapsed();
    return ret;
}

std::string LocalSnapshotReader::get_uri(const base::EndPoint& hint_addr) {
    std::string snapshot_uri("file://");
    snapshot_uri.append(base::endpoint2str(hint_addr).c_str());
    snapshot_uri.append("/");
    snapshot_uri.append(_path);
    return snapshot_uri;
}

LocalSnapshotStorage::LocalSnapshotStorage(const std::string& path)
    : _path(path),
    _last_snapshot_index(0) {
}

LocalSnapshotStorage::~LocalSnapshotStorage() {
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

    // get last_snapshot_index
    if (snapshots.size() > 0) {
        size_t snapshot_count = snapshots.size();
        for (size_t i = 0; i < snapshot_count - 1; i++) {
            int64_t index = *snapshots.begin();
            snapshots.erase(index);

            std::string snapshot_path(_path);
            base::string_appendf(&snapshot_path, "/" RAFT_SNAPSHOT_PATTERN, index);
            LOG(INFO) << "Deleting snapshot `" << snapshot_path << "'";
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

int LocalSnapshotStorage::close(SnapshotWriter* writer_) {
    LocalSnapshotWriter* writer = dynamic_cast<LocalSnapshotWriter*>(writer_);
    int ret = writer->error_code();
    do {
        if (0 != ret) {
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
        LocalSnapshotReader* reader = new LocalSnapshotReader(snapshot_path);
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
