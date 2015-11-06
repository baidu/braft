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
    RemotePathCopier() {}
    int init(base::EndPoint remote_side, const std::string& tmp_dir);
    int copy(const std::string& source, const std::string& dest_dir,
             const CopyOptions* options);
private:
    int _copy_file(const std::string& source, const std::string& dest, 
                   const CopyOptions& options);
    DISALLOW_COPY_AND_ASSIGN(RemotePathCopier);
    baidu::rpc::Channel _channel;
    std::string _tmp_dir;
};

inline CopyOptions::CopyOptions()
    : max_retry(3)
    , retry_interval_ms(1000)  // 1s
    , timeout_ms(10L * 1000)   // 10s
{}

}  // namespace raft

#endif  //PUBLIC_RAFT_REMOTE_PATH_COPIER_H
