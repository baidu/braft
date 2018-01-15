// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  PUBLIC_RAFT_CLI_SERVICE_H
#define  PUBLIC_RAFT_CLI_SERVICE_H

#include <base/status.h>
#include "raft/cli.pb.h"                // CliService
#include "raft/node.h"                  // NodeImpl

namespace raft {

class CliServiceImpl : public CliService {
public:
    virtual void add_peer(::google::protobuf::RpcController* controller,
                          const ::raft::AddPeerRequest* request,
                          ::raft::AddPeerResponse* response,
                          ::google::protobuf::Closure* done);
    virtual void remove_peer(::google::protobuf::RpcController* controller,
                             const ::raft::RemovePeerRequest* request,
                             ::raft::RemovePeerResponse* response,
                             ::google::protobuf::Closure* done);
    virtual void set_peer(::google::protobuf::RpcController* controller,
                          const ::raft::SetPeerRequest* request,
                          ::raft::SetPeerResponse* response,
                          ::google::protobuf::Closure* done);
    virtual void snapshot(::google::protobuf::RpcController* controller,
                          const ::raft::SnapshotRequest* request,
                          ::raft::SnapshotResponse* response,
                          ::google::protobuf::Closure* done);
    virtual void get_leader(::google::protobuf::RpcController* controller,
                            const ::raft::GetLeaderRequest* request,
                            ::raft::GetLeaderResponse* response,
                            ::google::protobuf::Closure* done);
private:
    base::Status get_node(scoped_refptr<NodeImpl>* node,
                          const GroupId& group_id,
                          const std::string& peer_id);
};

}

#endif  //PUBLIC_RAFT_CLI_SERVICE_H
