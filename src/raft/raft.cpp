// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/23 15:23:00

#include <pthread.h>
#include <unistd.h>
#include <base/string_printf.h>
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/storage.h"
#include "raft/node_manager.h"

namespace raft {

#if defined(__RAFT_VERSION_ID__)
static const char* s_libraft_version = "libraft_version_" __RAFT_VERSION_ID__;
#else
static const char* s_libraft_version = "libraft_version_unknown";
#endif  // __RAFT_VERSION_ID__

static pthread_once_t global_init_once = PTHREAD_ONCE_INIT;
static void global_init_or_die_impl() {
    if (init_storage() != 0) {
        LOG(FATAL) << "Fail to init storage";
        exit(1);
    }
    LOG(NOTICE) << "init libraft ver: " << s_libraft_version;
}

int add_service(baidu::rpc::Server* server, const base::EndPoint& listen_addr) {
    if (pthread_once(&global_init_once, global_init_or_die_impl) != 0) {
        PLOG(FATAL) << "Fail to pthread_once";
        return -1;
    }
    return NodeManager::GetInstance()->add_service(server, listen_addr);
}

int add_service(baidu::rpc::Server* server, int port) {
    base::EndPoint addr(base::IP_ANY, port);
    return add_service(server, addr);
}
int add_service(baidu::rpc::Server* server, const char* listen_ip_and_port) {
    base::EndPoint addr;
    if (base::str2endpoint(listen_ip_and_port, &addr) != 0) {
        LOG(ERROR) << "Fail to parse `" << listen_ip_and_port << "'";
        return -1;
    }
    return add_service(server, addr);
}

Node::Node(const GroupId& group_id, const PeerId& peer_id) {
    _impl = new NodeImpl(group_id, peer_id);
}

Node::~Node() {
    if (_impl) {
        _impl->Release();
        _impl = NULL;
    }
}

NodeId Node::node_id() {
    return _impl->node_id();
}

PeerId Node::leader_id() {
    return _impl->leader_id();
}

bool Node::is_leader() {
    return _impl->is_leader();
}

int Node::init(const NodeOptions& options) {
    return _impl->init(options);
}

void Node::shutdown(Closure* done) {
    _impl->shutdown(done);
}

void Node::apply(const Task& task) {
    _impl->apply(task);
}

void Node::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done) {
    _impl->add_peer(old_peers, peer, done);
}

void Node::remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done) {
    _impl->remove_peer(old_peers, peer, done);
}

int Node::set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers) {
    return _impl->set_peer(old_peers, new_peers);
}

void Node::snapshot(Closure* done) {
    _impl->snapshot(done);
}

}
