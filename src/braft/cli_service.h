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

#ifndef  BRAFT_CLI_SERVICE_H
#define  BRAFT_CLI_SERVICE_H

#include <butil/status.h>
#include "braft/cli.pb.h"                // CliService
#include "braft/node.h"                  // NodeImpl

namespace braft {

class CliServiceImpl : public CliService {
public:
    void add_peer(::google::protobuf::RpcController* controller,
                  const ::braft::AddPeerRequest* request,
                  ::braft::AddPeerResponse* response,
                  ::google::protobuf::Closure* done);
    void remove_peer(::google::protobuf::RpcController* controller,
                     const ::braft::RemovePeerRequest* request,
                     ::braft::RemovePeerResponse* response,
                     ::google::protobuf::Closure* done);
    void reset_peer(::google::protobuf::RpcController* controller,
                    const ::braft::ResetPeerRequest* request,
                    ::braft::ResetPeerResponse* response,
                    ::google::protobuf::Closure* done);
    void snapshot(::google::protobuf::RpcController* controller,
                  const ::braft::SnapshotRequest* request,
                  ::braft::SnapshotResponse* response,
                  ::google::protobuf::Closure* done);
    void get_leader(::google::protobuf::RpcController* controller,
                    const ::braft::GetLeaderRequest* request,
                    ::braft::GetLeaderResponse* response,
                    ::google::protobuf::Closure* done);
    void change_peers(::google::protobuf::RpcController* controller,
                      const ::braft::ChangePeersRequest* request,
                      ::braft::ChangePeersResponse* response,
                      ::google::protobuf::Closure* done);
    void transfer_leader(::google::protobuf::RpcController* controller,
                         const ::braft::TransferLeaderRequest* request,
                         ::braft::TransferLeaderResponse* response,
                         ::google::protobuf::Closure* done);
private:
    butil::Status get_node(scoped_refptr<NodeImpl>* node,
                          const GroupId& group_id,
                          const std::string& peer_id);
};

}

#endif  //BRAFT_CLI_SERVICE_H
