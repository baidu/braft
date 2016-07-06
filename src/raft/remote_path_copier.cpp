// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/05 12:17:23

#include "raft/remote_path_copier.h"

#include <gflags/gflags.h>
#include <base/strings/string_piece.h>
#include <base/strings/string_number_conversions.h>
#include <base/files/file_path.h>
#include <base/file_util.h>
#include <bthread.h>
#include <baidu/rpc/controller.h>
#include "raft/util.h"

namespace raft {

DEFINE_int32(raft_max_byte_count_per_rpc, 1024 * 128 /*128K*/,
             "Maximum of block size per RPC");

RemotePathCopier::RemotePathCopier()
    : _reader_id(0)
{}

int RemotePathCopier::init(const std::string& uri) {
    // Parse uri format: remote://ip:port/reader_id
    static const size_t prefix_size = strlen("remote://");
    base::StringPiece uri_str(uri);
    if (!uri_str.starts_with("remote://")) {
        LOG(ERROR) << "Invalid uri=" << uri;
        return -1;
    }
    uri_str.remove_prefix(prefix_size);
    size_t slash_pos = uri_str.find('/');
    base::StringPiece ip_and_port = uri_str.substr(0, slash_pos);
    uri_str.remove_prefix(slash_pos + 1);
    if (!base::StringToInt64(uri_str, &_reader_id)) {
        LOG(ERROR) << "Invalid reader_id_format=" << uri_str
                   << " in " << uri;
        return -1;
    }
    if (_channel.Init(ip_and_port.as_string().c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init Channel to " << ip_and_port;
        return -1;
    }
    return 0;
}

int RemotePathCopier::read_piece_of_file(
            base::IOBuf* buf,
            const std::string& source,
            off_t offset,
            size_t max_count,
            long timeout_ms,
            bool* is_eof) {
    baidu::rpc::Controller cntl;
    GetFileRequest request;
    request.set_reader_id(_reader_id);
    request.set_filename(source);
    request.set_count(max_count);
    request.set_offset(offset);
    GetFileResponse response;
    FileService_Stub stub(&_channel);
    cntl.set_timeout_ms(timeout_ms);
    stub.get_file(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to issue RPC, " << cntl.ErrorText();
        return cntl.ErrorCode();
    }
    *is_eof = response.eof();
    buf->swap(cntl.response_attachment());
    return 0;
}

int RemotePathCopier::copy_to_file(
        const std::string& source, const std::string& dest_path,
        const CopyOptions* options) {
    CopyOptions opt;
    if (options != NULL) {
        opt = *options;
    }
    // Create Directory for |dest_path|
    base::File::Error e;
    if (!base::CreateDirectoryAndGetError(
                base::FilePath(dest_path).DirName(), &e)) {
        LOG(WARNING) << "Fail to create direcotry of path=" 
                     << dest_path << " : " << e;
        return -1;
    }
    base::fd_guard fd(
            ::open(dest_path.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644));
    
    if (fd < 0) {
        PLOG(WARNING) << "Fail to open " << dest_path;
        return -1;
    }
    base::make_close_on_exec(fd);
    off_t offset = 0;
    bool is_eof = false;
    while (!is_eof) {
        base::IOBuf buf;
        const size_t max_count = FLAGS_raft_max_byte_count_per_rpc;
        int rc = 0;
        for (int i = 0; i < opt.max_retry + 1; ++i) {
            buf.clear();
            rc = read_piece_of_file(&buf, source, offset, 
                                    max_count, opt.timeout_ms, &is_eof);
            if (rc == 0) {
                break;
            }
            // TODO: return according to rc
            bthread_usleep(opt.retry_interval_ms * 1000L);
        }
        if (rc != 0) {
            LOG(WARNING) << "Fail to copy " << source;
            return -1;
        }
        FileSegData data(buf);
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
        offset += max_count;
    }
    return 0;
}

int RemotePathCopier::copy_to_iobuf(const std::string& source,
                                    base::IOBuf* dest_buf, 
                                    const CopyOptions* options) {
    CopyOptions opt;
    if (options != NULL) {
        opt = *options;
    }
    base::IOBuf buf;
    bool is_eof = false;
    int rc = 0;
    for (int i = 0; i < opt.max_retry + 1; ++i) {
        buf.clear();
        rc = read_piece_of_file(&buf, source, 0, 
                                UINT_MAX/*FIXME: Not general*/,
                                opt.timeout_ms,
                                &is_eof);
        if (rc == 0) {
            break;
        }
        // TODO: return according to rc
        bthread_usleep(opt.retry_interval_ms * 1000L);
    }
    if (rc != 0) {
        LOG(WARNING) << "Fail to copy " << source;
        return -1;
    }
    CHECK(is_eof);
    FileSegData data(buf);
    uint64_t seg_offset = 0;
    base::IOBuf seg_data;
    while (0 != data.next(&seg_offset, &seg_data)) {
        CHECK_GE((size_t)seg_offset, dest_buf->length());
        dest_buf->resize(seg_offset);
        dest_buf->append(seg_data);
    }
    return 0;
}

} // namespace raft
