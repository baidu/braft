// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/23 15:23:00

#include <pthread.h>
#include <unistd.h>
#include <base/string_printf.h>
#include <base/class_name.h>
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/storage.h"
#include "raft/node_manager.h"
#include "raft/log.h"
#include "raft/memory_log.h"
#include "raft/stable.h"
#include "raft/snapshot.h"
#include "raft/fsm_caller.h"            // IteratorImpl

namespace raft {

#if defined(RAFT_REVISION)
static const char* s_libraft_version = "libraft_version_" RAFT_REVISION;
#else
static const char* s_libraft_version = "libraft_version_unknown";
#endif  // __RAFT_VERSION_ID__

static void print_revision(std::ostream& os, void*) {
#if defined(RAFT_REVISION)
        os << RAFT_REVISION;
#else
        os << "undefined";
#endif
}

static bvar::PassiveStatus<std::string> s_raft_revision(
        "raft_revision", print_revision, NULL);


static pthread_once_t global_init_once = PTHREAD_ONCE_INIT;

struct GlobalExtension {
    SegmentLogStorage local_log;
    MemoryLogStorage memory_log;
    LocalStableStorage local_stable;
    LocalSnapshotStorage local_snapshot;
};

int __attribute__((weak)) register_rocksdb_extension();

static void global_init_or_die_impl() {
    static GlobalExtension s_ext;

    LOG(TRACE) << "init libraft ver: " << s_libraft_version;
    log_storage_extension()->RegisterOrDie("local", &s_ext.local_log);
    log_storage_extension()->RegisterOrDie("memory", &s_ext.memory_log);
    stable_storage_extension()->RegisterOrDie("local", &s_ext.local_stable);
    snapshot_storage_extension()->RegisterOrDie("local", &s_ext.local_snapshot);

    if ((void*)register_rocksdb_extension != NULL) {
        register_rocksdb_extension();
    }
}

// Non-static for unit test
void global_init_once_or_die() {
    if (pthread_once(&global_init_once, global_init_or_die_impl) != 0) {
        PLOG(FATAL) << "Fail to pthread_once";
        exit(-1);
    }
}

int add_service(baidu::rpc::Server* server, const base::EndPoint& listen_addr) {
    global_init_once_or_die();
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

void Node::join() {
    _impl->join();
}

void Node::apply(const Task& task) {
    _impl->apply(task);
}

base::Status Node::list_peers(std::vector<PeerId>* peers) {
    return _impl->list_peers(peers);
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

int Node::set_peer(const std::vector<PeerId>& new_peers) {
    return _impl->set_peer(std::vector<PeerId>(), new_peers);
}

void Node::snapshot(Closure* done) {
    _impl->snapshot(done);
}

void Node::vote(int election_timeout) {
    _impl->vote(election_timeout);
}

int Node::transfer_leadership_to(const PeerId& peer) {
    return _impl->transfer_leadership_to(peer);
}

// ------------- Iterator
void Iterator::next() {
    if (valid()) {
        _impl->next();
    }
}

bool Iterator::valid() const {
    return _impl->is_good() && _impl->entry()->type == ENTRY_TYPE_DATA;
}

int64_t Iterator::index() const { return _impl->index(); }

int64_t Iterator::term() const { return _impl->entry()->id.term; }

const base::IOBuf& Iterator::data() const {
    return _impl->entry()->data;
}

Closure* Iterator::done() const {
    return _impl->done();
}

void Iterator::set_error_and_rollback(size_t ntail, const base::Status* st) {
    return _impl->set_error_and_rollback(ntail, st);
}

// ----------------- Default Implementation of StateMachine
StateMachine::~StateMachine() {}
void StateMachine::on_shutdown() {}

void StateMachine::on_snapshot_save(SnapshotWriter* writer, Closure* done) {
    (void)writer;
    CHECK(done);
    LOG(ERROR) << base::class_name_str(*this)
               << " didn't implement on_snapshot_save";
    done->status().set_error(-1, "%s didn't implement on_snapshot_save",
                                 base::class_name_str(*this).c_str());
    done->Run();
}

int StateMachine::on_snapshot_load(SnapshotReader* reader) {
    (void)reader;
    LOG(ERROR) << base::class_name_str(*this)
               << " didn't implement on_snapshot_load"
               << " while a snapshot is saved in " << reader->get_path();
    return -1;
}

void StateMachine::on_leader_start() {}
void StateMachine::on_leader_start(int64_t) { return on_leader_start(); }
void StateMachine::on_leader_stop() {}
void StateMachine::on_error(const Error& e) {
    LOG(ERROR) << "An error=" << e << " raised on StateMachine "
               << base::class_name_str(*this)
               << ", it's highly recommended to implement this interface"
                  " as raft stops working since some error ocurrs,"
                  " you should figure out the cause and repair or remove this node";
}

void StateMachine::on_configuration_committed(const Configuration& conf) {
    (void)conf;
    return;
}

}
