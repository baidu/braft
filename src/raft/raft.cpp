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

DEFINE_string(raft_ip_and_port, "0.0.0.0:8000-9000",
              "Make raft listen to the given address. "
              "format : ip:begin_port[-end_port]");

void Closure::set_error(int err_code, const char* reason_fmt, ...) {
    _err_code = err_code;

    va_list ap;
    va_start(ap, reason_fmt);
    base::string_vappendf(&_err_text, reason_fmt, ap);
    va_end(ap);
}

static pthread_once_t global_init_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool ever_initialized = false;

int init_raft(const char* server_desc,
              baidu::rpc::Server* server, baidu::rpc::ServerOptions* options) {
    BAIDU_SCOPED_LOCK(init_mutex);
    if (ever_initialized) {
        return 1;
    }
    if (init_storage() != 0) {
        LOG(FATAL) << "Fail to init storage";
        return -1;
    }
    if (server_desc != NULL) {
        FLAGS_raft_ip_and_port = server_desc;
    }
    std::string ip_str("0.0.0.0");
    int start_port = 0;
    int end_port = 0;
    int index = 0;
    for (base::StringMultiSplitter 
            sp(FLAGS_raft_ip_and_port.c_str(), ":-"); sp != NULL; ++sp) {
        if (index == 0) {
            ip_str.assign(sp.field(), sp.length());
        } else if (index == 1) {
            if (sp.to_int(&start_port) != 0) {
                index = 0;
                break;
            }
        } else if (index == 2) {
            if (sp.to_int(&end_port) != 0) {
                index = 0;
                break;
            }
        }
        index ++;
    }
    if (index == 2) {
        end_port = start_port;
    }
    if (index < 2 || index > 3 || end_port < start_port) {
        LOG(WARNING) << "bad server description format : " << server_desc;
        return EINVAL;
    }
    if (NodeManager::GetInstance()->init(ip_str.c_str(), start_port, end_port,
                                         server, options) != 0) {
        return -1;
    }
    ever_initialized = true;
    return 0;
}

void global_init_or_dir_impl() {
    if (init_raft(NULL, NULL, NULL) < 0) {
        LOG(FATAL) << "Fail to init raft";
        exit(1);
    }
}

void global_init_or_dir() {
    if (pthread_once(&global_init_once, global_init_or_dir_impl) != 0) {
        PLOG(FATAL) << "Fail to pthread_once";
        exit(1);
    }
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
