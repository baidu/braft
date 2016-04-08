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

void FileServiceImpl::list_path(::google::protobuf::RpcController* controller,
                               const ::raft::ListPathRequest* request,
                               ::raft::ListPathResponse* response,
                               ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_gurad(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;

    //check path acl
    if (!PathACL::GetInstance()->check(request->path())) {
        cntl->SetFailed(ENOENT, "Fail to get info of path=%s",
                        request->path().c_str());
        return;
    }

    std::stack<base::FilePath> st;
    st.push(base::FilePath(request->path()));
    while (!st.empty()) {
        base::FilePath path = st.top();
        st.pop();
        base::File::Info info;
        if (!base::GetFileInfo(path, &info)) {
            cntl->SetFailed(ENOENT, "Fail to get info of path=%s",
                            path.AsUTF8Unsafe().c_str());
            return;
        }
        PathInfo* path_info = response->add_path_info();
        path_info->set_path(path.AsUTF8Unsafe());
        path_info->set_is_directory(info.is_directory);
        if (info.is_directory) {
            base::FileEnumerator dir(path, false, 
                                     base::FileEnumerator::FILES 
                                            | base::FileEnumerator::DIRECTORIES);
            for (base::FilePath sub_path = dir.Next(); !sub_path.empty();
                                                       sub_path = dir.Next()) {
                st.push(sub_path);
            }
        }
    }
    return;
}

void FileServiceImpl::get_file(::google::protobuf::RpcController* controller,
                               const ::raft::GetFileRequest* request,
                               ::raft::GetFileResponse* response,
                               ::google::protobuf::Closure* done) {

    RAFT_VLOG << "get_file path " << request->file_path()
         << " offset " << request->offset() << " count " << request->count();
    baidu::rpc::ClosureGuard done_gurad(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;

    //check path acl
    if (!PathACL::GetInstance()->check(request->file_path())) {
        cntl->SetFailed(ENOENT, "Fail to get info of path=%s",
                        request->file_path().c_str());
        return;
    }

    if (request->count() <= 0 || request->offset() < 0) {
        cntl->SetFailed(baidu::rpc::EREQUEST, "Invalid request=%s",
                        request->ShortDebugString().c_str());
        return;
    }
    base::File::Info info;
    if (!base::GetFileInfo(base::FilePath(request->file_path()), &info)) {
        cntl->SetFailed(ENOENT, "Fail to get info of path=%s",
                        request->file_path().c_str());
        return;
    }
    if (info.is_directory) {
        cntl->SetFailed(EPERM, "path=%s is a directory", 
                        request->file_path().c_str());
        return;
    }

    base::fd_guard fd(open(request->file_path().c_str(), O_RDONLY));
    if (fd < 0) {
        cntl->SetFailed(errno, "Fail to open %s, %m", 
                               request->file_path().c_str());
        return;
    }
    base::IOPortal buf;
    ssize_t nread = file_pread(&buf, fd, request->offset(), request->count());
    if (nread < 0) {
        cntl->SetFailed(errno, "Fail to read from %s, %m",
                        request->file_path().c_str());
        return;
    }

    FileSegData seg_data;
    uint64_t nparse = 0;
    uint64_t data_len = buf.size();
    while (nparse < data_len) {
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

    response->set_eof(false);
    if (nread < request->count()) {
        response->set_eof(true);
    } else {
        // if file size is offset + count, set eof reduce next request
        if (lseek(fd, 0, SEEK_END) == (request->offset() + request->count())) {
            response->set_eof(true);
        }
    }
}

}  // namespace raft
