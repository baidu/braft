/*
 * =====================================================================================
 *
 *       Filename:  raft.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月23日 11时49分52秒
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

DEFINE_string(raft_ip, "0.0.0.0", "raft server ip");
DEFINE_int32(raft_start_port, 8000, "raft server start port");
DEFINE_int32(raft_end_port, 9000, "raft server start port");

namespace raft {

void Closure::set_error(int err_code, const char* reason_fmt, ...) {
    _err_code = err_code;

    va_list ap;
    va_start(ap, reason_fmt);
    base::string_vappendf(&_err_text, reason_fmt, ap);
    va_end(ap);
}

static pthread_once_t register_storage_once = PTHREAD_ONCE_INIT;
int init_raft(const char* server_desc) {
    std::string ip_str(FLAGS_raft_ip);
    int start_port = FLAGS_raft_start_port;
    int end_port = FLAGS_raft_end_port;

    if (server_desc) {
        int index = 0;
        for (base::StringMultiSplitter sp(server_desc, ":-"); sp != NULL; ++sp) {
            if (index == 0) {
                ip_str = std::string(sp.field(), sp.length());
            } else if (index == 1) {
                std::string port_str(sp.field(), sp.length());
                start_port = atoi(port_str.c_str());
            } else if (index == 2) {
                std::string port_str(sp.field(), sp.length());
                end_port = atoi(port_str.c_str());
            }
            index ++;
        }
        if (index == 2) {
            end_port = start_port;
        }
        if (index > 3 || end_port < start_port) {
            LOG(WARNING) << "server description format faield: " << server_desc;
            return EINVAL;
        }
    }

    if (0 != pthread_once(&register_storage_once, init_storage)) {
        LOG(FATAL) << "pthread_once failed";
        exit(1);
    }

    return NodeManager::GetInstance()->init(ip_str.c_str(), start_port, end_port);
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

Node::Node(const GroupId& group_id, const ReplicaId& replica_id) {
    _impl = new NodeImpl(group_id, replica_id);
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

int Node::init(const NodeOptions& options) {
    return _impl->init(options);
}

void Node::shutdown(Closure* done) {
    _impl->shutdown(done);
}

int Node::apply(const base::IOBuf& data, Closure* done) {
    return _impl->apply(data, done);
}

int Node::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done) {
    return _impl->add_peer(old_peers, peer, done);
}

int Node::remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done) {
    return _impl->remove_peer(old_peers, peer, done);
}

int Node::set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers) {
    return _impl->set_peer(old_peers, new_peers);
}

void Node::snapshot(Closure* done) {
    _impl->snapshot(done);
}

}
