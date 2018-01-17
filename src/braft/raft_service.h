// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
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

// Authors: Wang,Yao(wangyao02@baidu.com)

#ifndef BRAFT_RAFT_SERVICE_H
#define BRAFT_RAFT_SERVICE_H

#include "braft/raft.pb.h"

namespace braft {

class RaftServiceImpl : public RaftService {
public:
    explicit RaftServiceImpl(butil::EndPoint addr)
        : _addr(addr) {}
    ~RaftServiceImpl();

    void pre_vote(google::protobuf::RpcController* controller,
                              const RequestVoteRequest* request,
                              RequestVoteResponse* response,
                              google::protobuf::Closure* done);

    void request_vote(google::protobuf::RpcController* controller,
                              const RequestVoteRequest* request,
                              RequestVoteResponse* response,
                              google::protobuf::Closure* done);

    void append_entries(google::protobuf::RpcController* controller,
                                const AppendEntriesRequest* request,
                                AppendEntriesResponse* response,
                                google::protobuf::Closure* done);

    void install_snapshot(google::protobuf::RpcController* controller,
                                  const InstallSnapshotRequest* request,
                                  InstallSnapshotResponse* response,
                                  google::protobuf::Closure* done);
    void timeout_now(::google::protobuf::RpcController* controller,
                     const ::braft::TimeoutNowRequest* request,
                     ::braft::TimeoutNowResponse* response,
                     ::google::protobuf::Closure* done);
private:
    butil::EndPoint _addr;
};

}

#endif //~BRAFT_RAFT_SERVICE_H
