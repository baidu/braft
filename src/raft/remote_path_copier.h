// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/05 11:42:50

#ifndef  PUBLIC_RAFT_REMOTE_PATH_COPIER_H
#define  PUBLIC_RAFT_REMOTE_PATH_COPIER_H

#include <baidu/rpc/channel.h>
#include "raft/file_service.pb.h"
#include "raft/util.h"

namespace raft {

struct CopyOptions {
    CopyOptions();
    int max_retry;
    long retry_interval_ms;
    long timeout_ms;
};

class RemotePathCopier {
public:
    RemotePathCopier();
    int init(const std::string& uri);
    // Copy `source' from remote to dest
    int copy_to_file(const std::string& source, const std::string& dest_path,
                     const CopyOptions* options);
    int copy_to_iobuf(const std::string& source, base::IOBuf* dest_buf, 
                      const CopyOptions* options);
    // TODO: chenzhangyi01: add stop and cancel
private:
    int read_piece_of_file(base::IOBuf* buf, const std::string& source,
                           off_t offset, size_t max_count,
                           long timeout_ms, bool* is_eof);
    DISALLOW_COPY_AND_ASSIGN(RemotePathCopier);
    baidu::rpc::Channel _channel;
    int64_t _reader_id;
};

inline CopyOptions::CopyOptions()
    : max_retry(3)
    , retry_interval_ms(1000)  // 1s
    , timeout_ms(10L * 1000)   // 10s
{}

}  // namespace raft

#endif  //PUBLIC_RAFT_REMOTE_PATH_COPIER_H
