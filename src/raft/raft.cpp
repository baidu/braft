/*
 * =====================================================================================
 *
 *       Filename:  raft.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/23 15:23:00
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <pthread.h>
#include <unistd.h>
#include <base/string_printf.h>
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/storage.h"

namespace raft {

#if defined(__RAFT_VERSION_ID__)
static const char* s_libraft_version = "libraft_version_" __RAFT_VERSION_ID__;
#else
static const char* s_libraft_version = "libraft_version_unknown";
#endif  // __RAFT_VERSION_ID__

void Closure::set_error(int err_code, const char* reason_fmt, ...) {
    _err_code = err_code;

    va_list ap;
    va_start(ap, reason_fmt);
    base::string_vprintf(&_err_text, reason_fmt, ap);
    va_end(ap);
}

void Closure::set_error(int err_code, const std::string& error_text) {
    _err_code = err_code;
    _err_text = error_text;
}

static pthread_once_t global_init_once = PTHREAD_ONCE_INIT;
static void global_init_or_die_impl() {
    if (init_storage() != 0) {
        LOG(FATAL) << "Fail to init storage";
        exit(1);
    }
    LOG(NOTICE) << "init libraft ver: " << s_libraft_version;
}

int init_raft(const char* server_desc,
              baidu::rpc::Server* server, baidu::rpc::ServerOptions* options) {
    if (pthread_once(&global_init_once, global_init_or_die_impl) != 0) {
        PLOG(FATAL) << "Fail to pthread_once";
        exit(1);
    }

    base::EndPoint ip_and_port;
    if (0 == base::hostname2endpoint(server_desc, &ip_and_port) ||
        0 == base::str2endpoint(server_desc, &ip_and_port)) {
    } else {
        LOG(WARNING) << "bad server description format : " << server_desc;
        return EINVAL;
    }

    if (NodeManager::GetInstance()->init(ip_and_port, server, options) != 0) {
        return -1;
    }
    return 0;
}

int StateMachine::on_snapshot_save(SnapshotWriter* writer, Closure* done) {
    LOG(WARNING) << "StateMachine: " << this << " on_snapshot_save not implement";
    return ENOSYS;
}

int StateMachine::on_snapshot_load(SnapshotReader* reader) {
    LOG(WARNING) << "StateMachine: " << this << " on_snapshot_load not implement";
    return ENOSYS;
}

void StateMachine::on_leader_start() {
    LOG(WARNING) << "StateMachine: " << this << " on_leader_start not implement";
}

void StateMachine::on_leader_stop() {
    LOG(WARNING) << "StateMachine: " << this << " on_leader_stop not implement";
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

NodeStats Node::stats() {
    return _impl->stats();
}

int Node::init(const NodeOptions& options) {
    return _impl->init(options);
}

void Node::shutdown(Closure* done) {
    _impl->shutdown(done);
}

void Node::apply(const base::IOBuf& data, Closure* done) {
    _impl->apply(data, done);
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
