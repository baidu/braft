// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/28 13:34:09

#ifndef PUBLIC_RAFT_RAFT_SERVICE_H
#define PUBLIC_RAFT_RAFT_SERVICE_H

#include "raft/raft.pb.h"

namespace raft {

class RaftServiceImpl : public RaftService {
public:
    RaftServiceImpl() {}
    virtual ~RaftServiceImpl() {}

    virtual void pre_vote(google::protobuf::RpcController* controller,
                              const RequestVoteRequest* request,
                              RequestVoteResponse* response,
                              google::protobuf::Closure* done);

    virtual void request_vote(google::protobuf::RpcController* controller,
                              const RequestVoteRequest* request,
                              RequestVoteResponse* response,
                              google::protobuf::Closure* done);

    virtual void append_entries(google::protobuf::RpcController* controller,
                                const AppendEntriesRequest* request,
                                AppendEntriesResponse* response,
                                google::protobuf::Closure* done);

    virtual void install_snapshot(google::protobuf::RpcController* controller,
                                  const InstallSnapshotRequest* request,
                                  InstallSnapshotResponse* response,
                                  google::protobuf::Closure* done);
};

}

#endif //~PUBLIC_RAFT_RAFT_SERVICE_H
