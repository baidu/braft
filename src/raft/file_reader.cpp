// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/06/16 17:38:42

#include "raft/file_reader.h"
#include <base/fd_guard.h>          // base::fd_guard
#include <base/file_util.h>
#include "raft/util.h"

namespace raft {

int LocalDirReader::read_file(base::IOBuf* out,
                              const std::string &filename,
                              off_t offset,
                              size_t max_count,
                              bool* is_eof) const {
    out->clear();
    std::string file_path = _path + "/" + filename;
    base::File::Info info;
    if (!base::GetFileInfo(base::FilePath(file_path), &info)) {
        LOG(WARNING) << "Fail to find path=" << file_path;
        return ENOENT;
    }
    if (info.is_directory) {
        LOG(WARNING) << "path=" << file_path << " is a directory";
        return EISDIR;
    }
    base::fd_guard fd(::open(file_path.c_str(), O_RDONLY));
    if (fd < 0) {
        return errno;
    }
    base::IOPortal buf;
    ssize_t nread = file_pread(&buf, fd, offset, max_count);
    if (nread < 0) {
        return errno;
    }
    *is_eof = false;
    if ((size_t)nread < max_count) {
        *is_eof = true;
    } else {
        if (lseek(fd, 0, SEEK_END) == (offset + (off_t)max_count)) {
            *is_eof = true;
        }
    }
    out->swap(buf);
    return 0;
}

}  // namespace raft
