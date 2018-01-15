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

#include "raft/file_service.h"

#include <stack>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include <baidu/rpc/closure_guard.h>
#include <baidu/rpc/controller.h>
#include "raft/util.h"

namespace raft {

DEFINE_bool(raft_file_check_hole, false, "file service check hole switch, default disable");

void FileServiceImpl::get_file(::google::protobuf::RpcController* controller,
                               const ::raft::GetFileRequest* request,
                               ::raft::GetFileResponse* response,
                               ::google::protobuf::Closure* done) {
    scoped_refptr<FileReader> reader;
    baidu::rpc::ClosureGuard done_gurad(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    Map::const_iterator iter = _reader_map.find(request->reader_id());
    if (iter == _reader_map.end()) {
        lck.unlock();
        cntl->SetFailed(ENOENT, "Fail to find reader=%ld", request->reader_id());
        return;
    }
    // Don't touch iter ever after
    reader = iter->second;
    lck.unlock();
    RAFT_VLOG << "get_file from " << cntl->remote_side() << " path=" << reader->path() 
         << " filename=" << request->filename()
         << " offset=" << request->offset() << " count=" << request->count();

    if (request->count() <= 0 || request->offset() < 0) {
        cntl->SetFailed(baidu::rpc::EREQUEST, "Invalid request=%s",
                        request->ShortDebugString().c_str());
        return;
    }

    base::IOBuf buf;
    bool is_eof = false;
    const int rc = reader->read_file(
                            &buf, request->filename(),
                            request->offset(), request->count(), 
                            request->read_partly(),
                            &is_eof);
    if (rc != 0) {
        cntl->SetFailed(rc, "Fail to read from path=%s filename=%s : %s",
                        reader->path().c_str(), request->filename().c_str(), berror(rc));
        return;
    }

    response->set_eof(is_eof);
    response->set_read_size(buf.size());      
    // skip empty data
    if (buf.size() == 0) {
        return;
    }

    FileSegData seg_data;
    if (!FLAGS_raft_file_check_hole) {
        seg_data.append(buf, request->offset());
    } else {
        off_t buf_off = request->offset();
        while (!buf.empty()) {
            base::StringPiece p = buf.backing_block(0);
            if (!is_zero(p.data(), p.size())) {
                base::IOBuf piece_buf;
                buf.cutn(&piece_buf, p.size());
                seg_data.append(piece_buf, buf_off);
            } else {
                // skip zero IOBuf block
                buf.pop_front(p.size());
            }
            buf_off += p.size();
        }
    }
    cntl->response_attachment().swap(seg_data.data());
}

FileServiceImpl::FileServiceImpl() {
    _next_id = ((int64_t)getpid() << 45) | (base::gettimeofday_us() << 17 >> 17);
}

int FileServiceImpl::add_reader(FileReader* reader, int64_t* reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    *reader_id = _next_id++;
    _reader_map[*reader_id] = reader;
    return 0;
}

int FileServiceImpl::remove_reader(int64_t reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _reader_map.erase(reader_id) == 1 ? 0 : -1;
}

}  // namespace raft
