// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/06/16 17:38:42

#include "raft/file_reader.h"
#include "raft/util.h"

namespace raft {
    
LocalDirReader::~LocalDirReader() {
    _fs->close_snapshot(_path);
}

bool LocalDirReader::open() {
    return _fs->open_snapshot(_path);
}

int LocalDirReader::read_file(base::IOBuf* out,
                              const std::string &filename,
                              off_t offset,
                              size_t max_count,
                              bool* is_eof) const {
    return read_file_with_meta(out, filename, NULL, offset, max_count, is_eof);
}

int LocalDirReader::read_file_with_meta(base::IOBuf* out,
                                        const std::string &filename,
                                        google::protobuf::Message* file_meta,
                                        off_t offset,
                                        size_t max_count,
                                        bool* is_eof) const {
    out->clear();
    std::string file_path(_path + "/" + filename);
    base::File::Error e;
    FileAdaptor* file = _fs->open(file_path, O_RDONLY | O_CLOEXEC, file_meta, &e);
    if (!file) {
        return file_error_to_os_error(e);
    }
    std::unique_ptr<FileAdaptor, DestroyObj<FileAdaptor> > guard(file);
    base::IOPortal buf;
    ssize_t nread = file->read(&buf, offset, max_count);
    if (nread < 0) {
        return EIO;
    }
    *is_eof = false;
    if ((size_t)nread < max_count) {
        *is_eof = true;
    } else {
        ssize_t size = file->size();
        if (size < 0) {
            return EIO;
        }
        if (size == ssize_t(offset + max_count)) {
            *is_eof = true;
        }
    }
    out->swap(buf);
    return 0;
}

}  // namespace raft
