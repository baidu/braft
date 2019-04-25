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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Wang,Yao(wangyao02@baidu.com)

#include <pthread.h>
#include <unistd.h>
#include <butil/string_printf.h>
#include <butil/class_name.h>
#include "braft/raft.h"
#include "braft/node.h"
#include "braft/storage.h"
#include "braft/node_manager.h"
#include "braft/log.h"
#include "braft/memory_log.h"
#include "braft/raft_meta.h"
#include "braft/snapshot.h"
#include "braft/fsm_caller.h"            // IteratorImpl

namespace braft {

static void print_revision(std::ostream& os, void*) {
#if defined(BRAFT_REVISION)
        os << BRAFT_REVISION;
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
    LocalRaftMetaStorage local_meta;
    LocalSnapshotStorage local_snapshot;
};

static void global_init_or_die_impl() {
    static GlobalExtension s_ext;

    log_storage_extension()->RegisterOrDie("local", &s_ext.local_log);
    log_storage_extension()->RegisterOrDie("memory", &s_ext.memory_log);
    meta_storage_extension()->RegisterOrDie("local", &s_ext.local_meta);
    snapshot_storage_extension()->RegisterOrDie("local", &s_ext.local_snapshot);
}

// Non-static for unit test
void global_init_once_or_die() {
    if (pthread_once(&global_init_once, global_init_or_die_impl) != 0) {
        PLOG(FATAL) << "Fail to pthread_once";
        exit(-1);
    }
}

int add_service(brpc::Server* server, const butil::EndPoint& listen_addr) {
    global_init_once_or_die();
    return NodeManager::GetInstance()->add_service(server, listen_addr);
}

int add_service(brpc::Server* server, int port) {
    butil::EndPoint addr(butil::IP_ANY, port);
    return add_service(server, addr);
}
int add_service(brpc::Server* server, const char* listen_ip_and_port) {
    butil::EndPoint addr;
    if (butil::str2endpoint(listen_ip_and_port, &addr) != 0) {
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
        _impl->shutdown(NULL);
        _impl->join();
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

butil::Status Node::list_peers(std::vector<PeerId>* peers) {
    return _impl->list_peers(peers);
}

void Node::add_peer(const PeerId& peer, Closure* done) {
    _impl->add_peer(peer, done);
}

void Node::remove_peer(const PeerId& peer, Closure* done) {
    _impl->remove_peer(peer, done);
}

void Node::change_peers(const Configuration& new_peers, Closure* done) {
    _impl->change_peers(new_peers, done);
}

butil::Status Node::reset_peers(const Configuration& new_peers) {
    return _impl->reset_peers(new_peers);
}

void Node::snapshot(Closure* done) {
    _impl->snapshot(done);
}

void Node::vote(int election_timeout) {
    _impl->vote(election_timeout);
}

void Node::reset_election_timeout_ms(int election_timeout_ms) {
    _impl->reset_election_timeout_ms(election_timeout_ms);
}

int Node::transfer_leadership_to(const PeerId& peer) {
    return _impl->transfer_leadership_to(peer);
}

butil::Status Node::read_committed_user_log(const int64_t index, UserLog* user_log) {
    return _impl->read_committed_user_log(index, user_log);
}

void Node::get_status(NodeStatus* status) {
    return _impl->get_status(status);
}

void Node::enter_readonly_mode() {
    return _impl->enter_readonly_mode();
}

void Node::leave_readonly_mode() {
    return _impl->leave_readonly_mode();
}

bool Node::readonly() {
    return _impl->readonly();
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

const butil::IOBuf& Iterator::data() const {
    return _impl->entry()->data;
}

Closure* Iterator::done() const {
    return _impl->done();
}

void Iterator::set_error_and_rollback(size_t ntail, const butil::Status* st) {
    return _impl->set_error_and_rollback(ntail, st);
}

// ----------------- Default Implementation of StateMachine
StateMachine::~StateMachine() {}
void StateMachine::on_shutdown() {}

void StateMachine::on_snapshot_save(SnapshotWriter* writer, Closure* done) {
    (void)writer;
    CHECK(done);
    LOG(ERROR) << butil::class_name_str(*this)
               << " didn't implement on_snapshot_save";
    done->status().set_error(-1, "%s didn't implement on_snapshot_save",
                                 butil::class_name_str(*this).c_str());
    done->Run();
}

int StateMachine::on_snapshot_load(SnapshotReader* reader) {
    (void)reader;
    LOG(ERROR) << butil::class_name_str(*this)
               << " didn't implement on_snapshot_load"
               << " while a snapshot is saved in " << reader->get_path();
    return -1;
}

void StateMachine::on_leader_start(int64_t) {}
void StateMachine::on_leader_stop(const butil::Status&) {}
void StateMachine::on_error(const Error& e) {
    LOG(ERROR) << "Encountered an error=" << e << " on StateMachine "
               << butil::class_name_str(*this)
               << ", it's highly recommended to implement this interface"
                  " as raft stops working since some error ocurrs,"
                  " you should figure out the cause and repair or remove this node";
}

void StateMachine::on_configuration_committed(const Configuration& conf) {
    (void)conf;
    return;
}

void StateMachine::on_configuration_committed(const Configuration& conf, int64_t index) {
    (void)index;
    return on_configuration_committed(conf);
}

void StateMachine::on_stop_following(const LeaderChangeContext&) {}
void StateMachine::on_start_following(const LeaderChangeContext&) {}

BootstrapOptions::BootstrapOptions()
    : last_log_index(0)
    , fsm(NULL)
    , node_owns_fsm(false)
    , usercode_in_pthread(false)
{}

int bootstrap(const BootstrapOptions& options) {
    global_init_once_or_die();
    NodeImpl* node = new NodeImpl();
    const int rc = node->bootstrap(options);
    node->shutdown(NULL);
    node->join();
    node->Release();  // node acquired an additional reference in ctro.
    return rc;
}

}
