// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)
//          Yang,Guodong(yangguodong01@baidu.com)

#include "braft/file_reader.h"
#include "braft/util.h"

namespace braft {
    
LocalDirReader::~LocalDirReader() {
    _fs->close_snapshot(_path);
}

bool LocalDirReader::open() {
    return _fs->open_snapshot(_path);
}

int LocalDirReader::read_file(butil::IOBuf* out,
                              const std::string &filename,
                              off_t offset,
                              size_t max_count,
                              bool read_partly,
                              size_t* read_count,
                              bool* is_eof) const {
    return read_file_with_meta(out, filename, NULL, offset, max_count, read_count, is_eof);
}

int LocalDirReader::read_file_with_meta(butil::IOBuf* out,
                                        const std::string &filename,
                                        google::protobuf::Message* file_meta,
                                        off_t offset,
                                        size_t max_count,
                                        size_t* read_count,
                                        bool* is_eof) const {
    out->clear();
    std::string file_path(_path + "/" + filename);
    butil::File::Error e;
    FileAdaptor* file = _fs->open(file_path, O_RDONLY | O_CLOEXEC, file_meta, &e);
    if (!file) {
        return file_error_to_os_error(e);
    }
    std::unique_ptr<FileAdaptor, DestroyObj<FileAdaptor> > guard(file);
    butil::IOPortal buf;
    ssize_t nread = file->read(&buf, offset, max_count);
    if (nread < 0) {
        return EIO;
    }
    *read_count = nread;
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

}  //  namespace braft
