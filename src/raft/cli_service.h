// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2018/01/08 22:42:00

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
