// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/04 14:37:15

#include "raft/file_service.h"

#include <stack>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include <baidu/rpc/closure_guard.h>
#include <baidu/rpc/controller.h>
#include "raft/util.h"

namespace raft {

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
    RAFT_VLOG << "get_file path=" << reader->path() 
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
                            request->offset(), request->count(), &is_eof);
    if (rc != 0) {
        cntl->SetFailed(rc, "Fail to read from path=%s filename=%s : %s",
                        reader->path(), request->filename().c_str(), berror(rc));
        return;
    }

    FileSegData seg_data;
    uint64_t nparse = 0;
    uint64_t data_len = buf.size();
    while (nparse < data_len) {
        // TODO: optimize the performance:
        char data_buf[4096];
        size_t copy_len = buf.copy_to(data_buf, sizeof(data_buf), 0);
        buf.pop_front(copy_len); // release orig iobuf as early
        if (copy_len == sizeof(data_buf)) {
            bool is_zero = true;
            uint64_t* int64_data_buf = (uint64_t*)data_buf;
            for (size_t i = 0; i < sizeof(data_buf) / sizeof(uint64_t); i++) {
                if (int64_data_buf[i] != static_cast<uint64_t>(0)) {
                    is_zero = false;
                    break;
                }
            }
            if (is_zero) {
                nparse += copy_len;
                continue;
            }
        }
        seg_data.append(data_buf, request->offset() + nparse, copy_len);
        nparse += copy_len;
    }
    cntl->response_attachment().swap(seg_data.data());

    response->set_eof(is_eof);
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
