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
//          Zhangyi Chen(chenzhangyi01@baidu.com)

#include <butil/logging.h>
#include <brpc/server.h>
#include "braft/raft_service.h"
#include "braft/raft.h"
#include "braft/node.h"
#include "braft/node_manager.h"

namespace braft {

RaftServiceImpl::~RaftServiceImpl() {
    NodeManager::GetInstance()->remove_address(_addr);
}

void RaftServiceImpl::pre_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    // TODO: should return butil::Status
    int rc = node->handle_pre_vote_request(request, response);
    if (rc != 0) {
        cntl->SetFailed(rc, "%s", berror(rc));
        return;
    }
}

void RaftServiceImpl::request_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->handle_request_vote_request(request, response);
    if (rc != 0) {
        cntl->SetFailed(rc, "%s", berror(rc));
        return;
    }
}

void RaftServiceImpl::append_entries(google::protobuf::RpcController* cntl_base,
                            const AppendEntriesRequest* request,
                            AppendEntriesResponse* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    return node->handle_append_entries_request(cntl, request, response, 
                                               done_guard.release());
}

void RaftServiceImpl::install_snapshot(google::protobuf::RpcController* cntl_base,
                              const InstallSnapshotRequest* request,
                              InstallSnapshotResponse* response,
                              google::protobuf::Closure* done) {
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        done->Run();
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");

        done->Run();
        return;
    }

    node->handle_install_snapshot_request(cntl, request, response, done);
}

void RaftServiceImpl::timeout_now(::google::protobuf::RpcController* controller,
                                  const ::braft::TimeoutNowRequest* request,
                                  ::braft::TimeoutNowResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        done->Run();
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        done->Run();
        return;
    }

    node->handle_timeout_now_request(cntl, request, response, done);
}

}
