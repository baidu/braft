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

#include <base/logging.h>
#include <baidu/rpc/server.h>
#include "raft/raft_service.h"
#include "raft/raft.h"
#include "raft/node.h"

namespace raft {

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
    Node* node = NodeManager::GetInstance()->get(request->group_id(), peer_id);
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    NodeImpl* node_impl = node->implement();
    int rc = node_impl->handle_request_vote_request(request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        //TODO:
        cntl->SetFailed("TODO");
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
    Node* node = NodeManager::GetInstance()->get(request->group_id(), peer_id);
    if (BAIDU_UNLIKELY(!node)) {
        cntl->SetFailed(baidu::rpc::SYS_ENOENT, "peer_id not exist");
        return;
    }

    NodeImpl* node_impl = node->implement();
    int rc = node_impl->handle_append_entries_request(cntl->request_attachment(),
                                                      request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        //TODO:
        cntl->SetFailed("TODO");
        return;
    }
}

void RaftServiceImpl::install_snapshot(google::protobuf::RpcController* cntl_base,
                              const InstallSnapshotRequest* request,
                              InstallSnapshotResponse* response,
                              google::protobuf::Closure* done) {
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

    NodeImpl* node_impl = node->implement();
    int rc = node_impl->handle_install_snapshot_request(request, response);
    if (BAIDU_UNLIKELY(rc != 0)) {
        //TODO:
        cntl->SetFailed("TODO");
        return;
    }
}

}
