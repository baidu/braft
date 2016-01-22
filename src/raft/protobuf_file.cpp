// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/21 16:49:14

#include <base/iobuf.h>
#include <base/fd_guard.h>
#include <base/file_util.h>
#include <base/sys_byteorder.h>

#include "raft/protobuf_file.h"

namespace raft {

int ProtoBufFile::save(google::protobuf::Message* message, bool sync) {
    std::string tmp_path(_path);
    tmp_path.append(".tmp");

    base::FilePath tmp_file_path(tmp_path);
    if (!base::CreateDirectory(tmp_file_path.DirName())) {
        PLOG(WARNING) << "create parent directory failed, path: " << tmp_file_path.DirName().value();
        return -1;
    }

    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        PLOG(WARNING) << "create tmp file failed, path: " << tmp_path;
        return -1;
    }

    base::fd_guard guard(fd);

    // serialize msg
    base::IOBuf msg_buf;
    base::IOBufAsZeroCopyOutputStream msg_wrapper(&msg_buf);
    message->SerializeToZeroCopyStream(&msg_wrapper);

    // write len
    int32_t len = base::HostToNet32(msg_buf.length());
    if (sizeof(int32_t) != write(fd, &len, sizeof(int32_t))) {
        PLOG(WARNING) << "write len failed, path: " << tmp_path;
        return -1;
    }

    // write protobuf data
    do {
        ssize_t nw = msg_buf.cut_into_file_descriptor(fd);
        if (nw < 0) {
            PLOG(WARNING) << "writev failed, path: " << tmp_path;
            return -1;
        }
    } while (msg_buf.length() > 0);

    // sync
    if (sync) {
        if (0 != fsync(fd)) {
            PLOG(WARNING) << "fsync failed, path: " << tmp_path;
            return -1;
        }
    }

    // rename
    if (0 != rename(tmp_path.c_str(), _path.c_str())) {
        PLOG(WARNING) << "rename failed, old: " << tmp_path << " , new: " << _path;
        return -1;
    }
    return 0;
}

int ProtoBufFile::load(google::protobuf::Message* message) {
    int fd = ::open(_path.c_str(), O_RDONLY);
    if (fd < 0) {
        PLOG(WARNING) << "open file failed, path: " << _path;
        return -1;
    }

    base::fd_guard guard(fd);

    // len
    int32_t len = 0;
    if (sizeof(int32_t) != read(fd, &len, sizeof(int32_t))) {
        PLOG(WARNING) << "read len failed, path: " << _path;
        return -1;
    }
    int32_t left_len = base::NetToHost32(len);

    // read protobuf data
    base::IOPortal msg_buf;
    do {
        ssize_t read_len = msg_buf.append_from_file_descriptor(fd, left_len);
        if (read_len > 0) {
            left_len -= read_len;
        } else if (read_len < 0) {
            PLOG(WARNING) << "readv failed, path: " << _path;
            break;
        }
    } while (left_len > 0);

    // parse msg
    base::IOBufAsZeroCopyInputStream msg_wrapper(msg_buf);
    message->ParseFromZeroCopyStream(&msg_wrapper);

    return 0;
}

}
