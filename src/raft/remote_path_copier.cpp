// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/05 12:17:23

#include "raft/remote_path_copier.h"

#include <gflags/gflags.h>
#include <base/strings/string_piece.h>
#include <base/files/file_path.h>
#include <base/file_util.h>
#include <bthread.h>
#include <baidu/rpc/controller.h>
#include "raft/util.h"

namespace raft {

DEFINE_int32(raft_max_byte_count_per_rpc, 1024 * 128 /*128K*/,
             "Maximum of block size per RPC");

int RemotePathCopier::init(base::EndPoint remote_side) {
    if (_channel.Init(remote_side, NULL) != 0) {
        LOG(ERROR) << "Fail to init Channel to " << remote_side;
        return -1;
    }
    return 0;
}

int RemotePathCopier::_copy_file(const std::string& source, const std::string& dest,
                                 const CopyOptions& options) {
    baidu::rpc::Controller cntl;
    off_t offset = 0;
    base::fd_guard fd(::open(dest.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644));
    if (fd < 0) {
        PLOG(WARNING) << "Fail to open " << dest;
        return -1;
    }
    while (true) {
        GetFileRequest request;
        request.set_file_path(source);
        request.set_count(FLAGS_raft_max_byte_count_per_rpc);
        request.set_offset(offset);
        GetFileResponse response;
        FileService_Stub stub(&_channel);
        cntl.set_timeout_ms(options.timeout_ms);
        stub.get_file(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to issue RPC, " << cntl.ErrorText();
            return -1;
        }

        FileSegData data(cntl.response_attachment());
        uint64_t seg_offset = 0;
        base::IOBuf seg_data;
        while (0 != data.next(&seg_offset, &seg_data)) {
            ssize_t nwriten = file_pwrite(seg_data, fd, seg_offset);
            if (nwriten < 0 || static_cast<size_t>(nwriten) != seg_data.size()) {
                PLOG(WARNING) << "Fail to write into fd=" << fd;
                return -1;
            }
            seg_data.clear();
        }

        if (response.eof()) {
            return 0;
        }
        cntl.Reset();

        offset += FLAGS_raft_max_byte_count_per_rpc;
    }
}

int RemotePathCopier::copy(const std::string& source, const std::string& dest_path,
                    const CopyOptions* options) {
    CopyOptions opt;
    if (options != NULL) {
        opt = *options;
    }
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(opt.timeout_ms);
    FileService_Stub stub(&_channel);
    ListPathRequest request;
    request.set_path(source);
    ListPathResponse response;
    stub.list_path(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to list " << source << ", "
                     << cntl.ErrorText();
        return -1;
    }

    // delete dest path
    if (!base::DeleteFile(base::FilePath(dest_path), true)) {
        LOG(WARNING) << "delete path failed, path " << dest_path;
        return -1;
    }

    CHECK(response.path_info_size() > 0);
    const PathInfo& src_path_info = response.path_info(0);
    if (src_path_info.is_directory()) {
        base::FilePath source_path(source);
        std::string parent_dir = source_path.AsUTF8Unsafe();

        // create local dir
        if (!base::CreateDirectory(base::FilePath(dest_path))) {
            LOG(WARNING) << "Fail to create " << dest_path;
            return -1;
        }

        // copy files
        for (int i = 0; i < response.path_info_size(); ++i) {
            const PathInfo& path_info = response.path_info(i);
            base::StringPiece name(path_info.path());
            if (name.starts_with(parent_dir)) {
                name.remove_prefix(parent_dir.length());
            }
            base::FilePath tmp_path = base::FilePath(dest_path).Append(name.as_string());
            if (path_info.is_directory()) {
                if (!base::DirectoryExists(tmp_path) 
                    && !base::CreateDirectory(tmp_path)) {
                    LOG(WARNING) << "Fail to create " << tmp_path.AsUTF8Unsafe();
                    return -1;
                }
            } else {
                if (_copy_file(path_info.path(), tmp_path.AsUTF8Unsafe(), opt) != 0) {
                    LOG(WARNING) << "Fail to copy " << path_info.path();
                    return -1;
                }
            }
        }
    } else {
        CHECK(response.path_info_size() == 1);
        // create parent dir
        std::string parent_dir = base::FilePath(dest_path).DirName().AsUTF8Unsafe();
        if (!base::CreateDirectory(base::FilePath(parent_dir))) {
            LOG(WARNING) << "Fail to create " << parent_dir;
            return -1;
        }

        // copy file
        if (_copy_file(src_path_info.path(), dest_path, opt) != 0) {
            LOG(WARNING) << "Fail to copy " << src_path_info.path();
            return -1;
        }
    }

    return 0;
}

} // namespace raft
