// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/28 13:34:09

#include <base/logging.h>
#include <baidu/rpc/server.h>
#include "raft/raft_service.h"
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/node_manager.h"

namespace raft {

RaftServiceImpl::~RaftServiceImpl() {
    NodeManager::GetInstance()->remove_address(_addr);
}

void RaftServiceImpl::pre_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->handle_pre_vote_request(request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        char err_buf[128];
        strerror_r(rc, err_buf, sizeof(err_buf));
        cntl->SetFailed(rc, err_buf);
        return;
    }
}

void RaftServiceImpl::request_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->handle_request_vote_request(request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        char err_buf[128];
        strerror_r(rc, err_buf, sizeof(err_buf));
        cntl->SetFailed(rc, err_buf);
        return;
    }
}

void RaftServiceImpl::append_entries(google::protobuf::RpcController* cntl_base,
                            const AppendEntriesRequest* request,
                            AppendEntriesResponse* response,
                            google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    return node->handle_append_entries_request(cntl, request, response, 
                                               done_guard.release());
}

void RaftServiceImpl::install_snapshot(google::protobuf::RpcController* cntl_base,
                              const InstallSnapshotRequest* request,
                              InstallSnapshotResponse* response,
                              google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        done->Run();
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                       peer_id);
    NodeImpl* node = node_ptr.get();
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");

        done->Run();
        return;
    }

    node->handle_install_snapshot_request(cntl, request, response, done);
}

}
