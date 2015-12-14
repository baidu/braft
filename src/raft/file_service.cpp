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

namespace raft {

void FileServiceImpl::list_path(::google::protobuf::RpcController* controller,
                               const ::raft::ListPathRequest* request,
                               ::raft::ListPathResponse* response,
                               ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_gurad(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
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
    baidu::rpc::ClosureGuard done_gurad(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
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
    ssize_t nread = 0;
    off_t offset = request->offset();
    while (nread < request->count()) {
        const ssize_t nr = buf.pappend_from_file_descriptor(
                        fd, offset, request->count() - nread);
        if (nr > 0) {
            nread += nr;
            offset += nr;
        } else if (nr == 0) {
            break;
        } else {
            cntl->SetFailed(errno, "Fail to read from %s, %m",
                            request->file_path().c_str());
            return;
        }
    }
    cntl->response_attachment().swap(buf);
    response->set_eof(false);
    if (nread < request->count()) {
        response->set_eof(true);
    } else {
        if (lseek(fd, 0, SEEK_END) == offset) {
            response->set_eof(true);
        }
    }
}

}  // namespace raft
