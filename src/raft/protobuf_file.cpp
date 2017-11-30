// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/21 16:49:14

#include <base/iobuf.h>
#include <base/sys_byteorder.h>

#include "raft/protobuf_file.h"

namespace raft {

ProtoBufFile::ProtoBufFile(const char* path, FileSystemAdaptor* fs) 
    : _path(path), _fs(fs) {
    if (_fs == NULL) {
        _fs = default_file_system();
    }
}

ProtoBufFile::ProtoBufFile(const std::string& path, FileSystemAdaptor* fs) 
    : _path(path), _fs(fs) {
    if (_fs == NULL) {
        _fs = default_file_system();
    }
}

int ProtoBufFile::save(const google::protobuf::Message* message, bool sync) {
    std::string tmp_path(_path);
    tmp_path.append(".tmp");

    base::File::Error e;
    FileAdaptor* file = _fs->open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, NULL, &e);
    if (!file) {
        LOG(WARNING) << "open file failed, path: " << _path
                     << ": " << base::File::ErrorToString(e);
        return -1;
    }
    std::unique_ptr<FileAdaptor, DestroyObj<FileAdaptor> > guard(file);

    // serialize msg
    base::IOBuf header_buf;
    base::IOBuf msg_buf;
    base::IOBufAsZeroCopyOutputStream msg_wrapper(&msg_buf);
    message->SerializeToZeroCopyStream(&msg_wrapper);

    // write len
    int32_t header_len = base::HostToNet32(msg_buf.length());
    header_buf.append(&header_len, sizeof(int32_t));
    if (sizeof(int32_t) != file->write(header_buf, 0)) {
        LOG(WARNING) << "write len failed, path: " << tmp_path;
        return -1;
    }

    ssize_t len = msg_buf.size();
    if (len != file->write(msg_buf, sizeof(int32_t))) {
        LOG(WARNING) << "write failed, path: " << tmp_path;
        return -1;
    }

    // sync
    if (sync) {
        if (!file->sync()) {
            LOG(WARNING) << "sync failed, path: " << tmp_path;
            return -1;
        }
    }

    // rename
    if (!_fs->rename(tmp_path, _path)) {
        LOG(WARNING) << "rename failed, old: " << tmp_path << " , new: " << _path;
        return -1;
    }
    return 0;
}

int ProtoBufFile::load(google::protobuf::Message* message) {
    base::File::Error e;
    FileAdaptor* file = _fs->open(_path, O_RDONLY, NULL, &e);
    if (!file) {
        LOG(WARNING) << "open file failed, path: " << _path
                     << ": " << base::File::ErrorToString(e);
        return -1;
    }

    std::unique_ptr<FileAdaptor, DestroyObj<FileAdaptor> > guard(file);

    // len
    base::IOPortal header_buf;
    if (sizeof(int32_t) != file->read(&header_buf, 0, sizeof(int32_t))) {
        LOG(WARNING) << "read len failed, path: " << _path;
        return -1;
    }
    int32_t len = 0;
    header_buf.copy_to(&len, sizeof(int32_t));
    int32_t left_len = base::NetToHost32(len);

    // read protobuf data
    base::IOPortal msg_buf;
    if (left_len != file->read(&msg_buf, sizeof(int32_t), left_len)) {
        LOG(WARNING) << "read body failed, path: " << _path;
        return -1;
    }

    // parse msg
    base::IOBufAsZeroCopyInputStream msg_wrapper(msg_buf);
    message->ParseFromZeroCopyStream(&msg_wrapper);

    return 0;
}

}
