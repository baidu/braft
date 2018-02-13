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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Wang,Yao(wangyao02@baidu.com)

#include "braft/util.h"

#include <stdlib.h>
#include <butil/macros.h>
#include <butil/raw_pack.h>                     // butil::RawPacker
#include <butil/file_util.h>

#include "braft/raft.h"

namespace braft {

static void* run_closure(void* arg) {
    ::google::protobuf::Closure *c = (google::protobuf::Closure*)arg;
    if (c) {
        c->Run();
    }
    return NULL;
}

void run_closure_in_bthread(google::protobuf::Closure* closure,
                            bool in_pthread) {
    DCHECK(closure);
    bthread_t tid;
    bthread_attr_t attr = (in_pthread) 
                          ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL;
    int ret = bthread_start_background(&tid, &attr, run_closure, closure);
    if (0 != ret) {
        PLOG(ERROR) << "Fail to start bthread";
        return closure->Run();
    }
}

void run_closure_in_bthread_nosig(google::protobuf::Closure* closure,
                                  bool in_pthread) {
    DCHECK(closure);
    bthread_t tid;
    bthread_attr_t attr = (in_pthread) 
                          ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL;
    attr =  attr | BTHREAD_NOSIGNAL;
    int ret = bthread_start_background(&tid, &attr, run_closure, closure);
    if (0 != ret) {
        PLOG(ERROR) << "Fail to start bthread";
        return closure->Run();
    }
}

ssize_t file_pread(butil::IOPortal* portal, int fd, off_t offset, size_t size) {
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t read_len = portal->pappend_from_file_descriptor(
                fd, offset, static_cast<size_t>(left));
        if (read_len > 0) {
            left -= read_len;
            offset += read_len;
        } else if (read_len == 0) {
            break;
        } else if (errno == EINTR) {
            continue;
        } else {
            LOG(WARNING) << "read failed, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            return -1;
        }
    }

    return size - left;
}

ssize_t file_pwrite(const butil::IOBuf& data, int fd, off_t offset) {
    size_t size = data.size();
    butil::IOBuf piece_data(data);
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t written = piece_data.pcut_into_file_descriptor(fd, offset, left);
        if (written >= 0) {
            offset += written;
            left -= written;
        } else if (errno == EINTR) {
            continue;
        } else {
            LOG(WARNING) << "write falied, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            return -1;
        }
    }

    return size - left;
}

void FileSegData::append(const butil::IOBuf& data, uint64_t offset) {
    uint32_t len = data.size();
    if (0 != _seg_offset && offset == (_seg_offset + _seg_len)) {
        // append to last segment
        _seg_len += len;
        _data.append(data);
    } else {
        // close last segment
        char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
        if (_seg_len > 0) {
            ::butil::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
            CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
        }

        // start new segment
        _seg_offset = offset;
        _seg_len = len;
        _seg_header = _data.reserve(sizeof(seg_header));
        CHECK(_seg_header != butil::IOBuf::INVALID_AREA);
        _data.append(data);
    }
}

void FileSegData::append(void* data, uint64_t offset, uint32_t len) {
    if (0 != _seg_offset && offset == (_seg_offset + _seg_len)) {
        // append to last segment
        _seg_len += len;
        _data.append(data, len);
    } else {
        // close last segment
        char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
        if (_seg_len > 0) {
            ::butil::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
            CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
        }

        // start new segment
        _seg_offset = offset;
        _seg_len = len;
        _seg_header = _data.reserve(sizeof(seg_header));
        CHECK(_seg_header != butil::IOBuf::INVALID_AREA);
        _data.append(data, len);
    }
}

void FileSegData::close() {
    char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
    if (_seg_len > 0) {
        ::butil::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
        CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
    }

    _seg_offset = 0;
    _seg_len = 0;
}

size_t FileSegData::next(uint64_t* offset, butil::IOBuf* data) {
    data->clear();
    if (_data.length() == 0) {
        return 0;
    }

    char header_buf[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
    size_t header_len = _data.cutn(header_buf, sizeof(header_buf));
    CHECK_EQ(header_len, sizeof(header_buf)) << "header_len: " << header_len;

    uint64_t seg_offset = 0;
    uint32_t seg_len = 0;
    ::butil::RawUnpacker(header_buf).unpack64(seg_offset).unpack32(seg_len);

    *offset = seg_offset;
    size_t body_len = _data.cutn(data, seg_len);
    CHECK_EQ(body_len, seg_len) << "seg_len: " << seg_len << " body_len: " << body_len;
    return seg_len;
}

}  //  namespace braft
