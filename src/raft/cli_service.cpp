// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2018/01/08 23:34:07

#include "raft/cli_service.h"

#include <baidu/rpc/controller.h>       // baidu::rpc::Controller
#include "raft/node_manager.h"          // NodeManager
#include "raft/closure_helper.h"

namespace raft {

static void add_peer_returned(baidu::rpc::Controller* cntl,
                          const AddPeerRequest* request,
                          AddPeerResponse* response,
                          std::vector<PeerId> old_peers,
                          scoped_refptr<NodeImpl> /*node*/,
                          ::google::protobuf::Closure* done,
                          const base::Status& st) {
    baidu::rpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    bool already_exists = false;
    for (size_t i = 0; i < old_peers.size(); ++i) {
        response->add_old_peers(old_peers[i].to_string());
        response->add_new_peers(old_peers[i].to_string());
        if (old_peers[i] == request->peer_id()) {
            already_exists = true;
        }
    }
    if (!already_exists) {
        response->add_new_peers(request->peer_id());
    }
}

void CliServiceImpl::add_peer(::google::protobuf::RpcController* controller,
                              const ::raft::AddPeerRequest* request,
                              ::raft::AddPeerResponse* response,
                              ::google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    baidu::rpc::ClosureGuard done_guard(done);
    scoped_refptr<NodeImpl> node;
    base::Status st = get_node(&node, request->group_id(), request->leader_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    std::vector<PeerId> peers;
    if (request->old_peers_size() > 0) {
        for (int i = 0; i < request->old_peers_size(); ++i) {
            PeerId peer_id;
            if (peer_id.parse(request->old_peers(i)) != 0) {
                cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                                        request->old_peers(i).c_str());
                return;
            }
            peers.push_back(peer_id);
        }
    } else {
        st = node->list_peers(&peers);
        if (!st.ok()) {
            cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
            return;
        }
    }
    PeerId adding_peer;
    if (adding_peer.parse(request->peer_id()) != 0) {
        cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                                request->peer_id().c_str());
        return;
    }
    LOG(WARNING) << "Receive AddPeerRequest to " << node->node_id() 
                 << " from " << cntl->remote_side()
                 << ", adding " << request->peer_id();
    Closure* add_peer_done = NewCallback(
            add_peer_returned, cntl, request, response, peers, node,
            done_guard.release());
    return node->add_peer(peers, adding_peer, add_peer_done);
}

static void remove_peer_returned(baidu::rpc::Controller* cntl,
                          const RemovePeerRequest* request,
                          RemovePeerResponse* response,
                          std::vector<PeerId> old_peers,
                          scoped_refptr<NodeImpl> /*node*/,
                          ::google::protobuf::Closure* done,
                          const base::Status& st) {
    baidu::rpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    for (size_t i = 0; i < old_peers.size(); ++i) {
        response->add_old_peers(old_peers[i].to_string());
        if (old_peers[i] != request->peer_id()) {
            response->add_new_peers(old_peers[i].to_string());
        }
    }
}
void CliServiceImpl::remove_peer(::google::protobuf::RpcController* controller,
                                 const ::raft::RemovePeerRequest* request,
                                 ::raft::RemovePeerResponse* response,
                                 ::google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    baidu::rpc::ClosureGuard done_guard(done);
    scoped_refptr<NodeImpl> node;
    base::Status st = get_node(&node, request->group_id(), request->leader_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    std::vector<PeerId> peers;
    if (request->old_peers_size() > 0) {
        for (int i = 0; i < request->old_peers_size(); ++i) {
            PeerId peer_id;
            if (peer_id.parse(request->old_peers(i)) != 0) {
                cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                                        request->old_peers(i).c_str());
                return;
            }
            peers.push_back(peer_id);
        }
    } else {
        st = node->list_peers(&peers);
        if (!st.ok()) {
            cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
            return;
        }
    }
    PeerId removing_peer;
    if (removing_peer.parse(request->peer_id()) != 0) {
        cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                                request->peer_id().c_str());
        return;
    }
    LOG(WARNING) << "Receive RemovePeerRequest to " << node->node_id() 
                 << " from " << cntl->remote_side()
                 << ", removing " << request->peer_id();
    Closure* remove_peer_done = NewCallback(
            remove_peer_returned, cntl, request, response, peers, node,
            done_guard.release());
    return node->remove_peer(peers, removing_peer, remove_peer_done);
}

void CliServiceImpl::set_peer(::google::protobuf::RpcController* controller,
                              const ::raft::SetPeerRequest* request,
                              ::raft::SetPeerResponse* response,
                              ::google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    baidu::rpc::ClosureGuard done_guard(done);
    scoped_refptr<NodeImpl> node;
    base::Status st = get_node(&node, request->group_id(), request->peer_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    std::vector<PeerId> old_peers;
    for (int i = 0; i < request->old_peers_size(); ++i) {
        old_peers.push_back(request->old_peers(i));
    }
    std::vector<PeerId> new_peers;
    for (int i = 0; i < request->new_peers_size(); ++i) {
        new_peers.push_back(request->new_peers(i));
    }
    LOG(WARNING) << "Receive set_peer to " << node->node_id()
                 << " from " << cntl->remote_side();
    st = node->set_peer2(old_peers, new_peers);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    }
}

static void snapshot_returned(baidu::rpc::Controller* cntl,
                              scoped_refptr<NodeImpl> node,
                              ::google::protobuf::Closure* done,
                              const base::Status& st) {
    baidu::rpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    }
}

void CliServiceImpl::snapshot(::google::protobuf::RpcController* controller,
                              const ::raft::SnapshotRequest* request,
                              ::raft::SnapshotResponse* response,
                              ::google::protobuf::Closure* done) {

    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    baidu::rpc::ClosureGuard done_guard(done);
    scoped_refptr<NodeImpl> node;
    base::Status st = get_node(&node, request->group_id(), request->peer_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    Closure* snapshot_done = NewCallback(snapshot_returned, cntl, node,
                                         done_guard.release());
    return node->snapshot(snapshot_done);
}

void CliServiceImpl::get_leader(::google::protobuf::RpcController* controller,
                                const ::raft::GetLeaderRequest* request,
                                ::raft::GetLeaderResponse* response,
                                ::google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    baidu::rpc::ClosureGuard done_guard(done);
    std::vector<scoped_refptr<NodeImpl> > nodes;
    NodeManager* const nm = NodeManager::GetInstance();
    nm->get_nodes_by_group_id(request->group_id(), &nodes);
    if (nodes.empty()) {
        cntl->SetFailed(ENOENT, "No nodes in group %s",
                                request->group_id().c_str());
        return;
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
        PeerId leader_id = nodes[i]->leader_id();
        if (!leader_id.is_empty()) {
            response->set_leader_id(leader_id.to_string());
            return;
        }
    }
    cntl->SetFailed(EAGAIN, "Unknown leader");
}

base::Status CliServiceImpl::get_node(scoped_refptr<NodeImpl>* node,
                                      const GroupId& group_id,
                                      const std::string& peer_id) {
    NodeManager* const nm = NodeManager::GetInstance();
    if (!peer_id.empty()) {
        *node = nm->get(group_id, peer_id);
        if (!(*node)) {
            return base::Status(ENOENT, "Fail to find node %s in group %s",
                                         peer_id.c_str(),
                                         group_id.c_str());
        }
    } else {
        std::vector<scoped_refptr<NodeImpl> > nodes;
        nm->get_nodes_by_group_id(group_id, &nodes);
        if (nodes.empty()) {
            return base::Status(ENOENT, "Fail to find node in group %s",
                                         group_id.c_str());
        }
        if (nodes.size() > 1) {
            return base::Status(EINVAL, "peer must be specified "
                                        "since there're %lu nodes in group %s",
                                         nodes.size(), group_id.c_str());
        }
        *node = nodes.front();
    }

    if ((*node)->disable_cli()) {
        return base::Status(EACCES, "CliService is not allowed to access node "
                                    "%s", (*node)->node_id().to_string().c_str());
    }

    return base::Status::OK();
}

}  // namespace raft
