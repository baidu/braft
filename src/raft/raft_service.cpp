/*
 * =====================================================================================
 *
 *       Filename:  raft_service.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/28 13:43:24
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include "raft/raft_service.h"

namespace raft {

void RaftServiceImpl::request_vote(::google::protobuf::RpcController* controller,
                          const ::raft::protocol::RequestVoteRequest* request,
                          ::raft::protocol::RequestVoteResponse* response,
                          ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        return;
    }
    Node* node = NodeManager::GetInstance()->get(request->group_id(), peer_id);
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->request_vote(request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        cntl->SetFailed(rc);
        return;
    }
}

void RaftServiceImpl::append_entries(::google::protobuf::RpcController* controller,
                            const ::raft::protocol::AppendEntriesRequest* request,
                            ::raft::protocol::AppendEntriesResponse* response,
                            ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        return;
    }
    Node* node = NodeManager::GetInstance()->get(request->group_id(), peer_id);
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->append_entries(controller->request_attachment(), request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        cntl->SetFailed(rc);
        return;
    }
}

void RaftServiceImpl::install_snapshot(::google::protobuf::RpcController* controller,
                              const ::raft::protocol::InstallSnapshotRequest* request,
                              ::raft::protocol::InstallSnapshotResponse* response,
                              ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (BAIDU_UNLIKELY(0 != peer_id.parse(request->peer_id()))) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "peer_id invalid");
        return;
    }
    Node* node = NodeManager::GetInstance()->get(request->group_id(), peer_id);
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->install_snapshot(request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        cntl->SetFailed(rc);
        return;
    }
}

}
