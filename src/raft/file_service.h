// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/04 14:31:45

#ifndef  PUBLIC_RAFT_FILE_SERVICE_H
#define  PUBLIC_RAFT_FILE_SERVICE_H

#include "raft/file_service.pb.h"

namespace raft {

class FileServiceImpl : public FileService {
public:
    void list_path(::google::protobuf::RpcController* controller,
                  const ::raft::ListPathRequest* request,
                  ::raft::ListPathResponse* response,
                  ::google::protobuf::Closure* done);
    void get_file(::google::protobuf::RpcController* controller,
                  const ::raft::GetFileRequest* request,
                  ::raft::GetFileResponse* response,
                  ::google::protobuf::Closure* done);
};

}  // namespace raft

#endif  //PUBLIC_RAFT_FILE_SERVICE_H
