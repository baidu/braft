// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/21 14:32:24

#include "raft/replicator.h"

#include <gflags/gflags.h>
#include <base/unique_ptr.h>                // std::unique_ptr
#include <base/time.h>      
#include <bthread_unstable.h>
#include <baidu/rpc/controller.h>
#include <baidu/rpc/reloadable_flags.h>
#include "raft/raft.pb.h"
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/log_manager.h"
#include "raft/commitment_manager.h"
#include "raft/log_entry.h"

namespace raft {

DEFINE_int32(max_entries_size, 1024,
             "The max number of entries in AppendEntriesRequest");
BAIDU_RPC_VALIDATE_GFLAG(max_entries_size, ::baidu::rpc::PositiveInteger);

ReplicatorOptions::ReplicatorOptions()
    : heartbeat_timeout_ms(-1)
    , log_manager(NULL)
    , commit_manager(NULL)
    , node(NULL)
    , term(0)
{}

const int ERROR_CODE_UNSET_MAGIC = 0x1234;

OnCaughtUp::OnCaughtUp()
    : on_caught_up(NULL)
    , arg(NULL)
    , done(NULL)
    , min_margin(0)
    , pid()
    , timer(0)
    , error_code(ERROR_CODE_UNSET_MAGIC)
{}

void OnCaughtUp::_run() {
    return on_caught_up(arg, pid, error_code, done);
}

Replicator::Replicator() : _on_caught_up(NULL), _last_response_timestamp(0) {
}

Replicator::~Replicator() {
    // bind lifecycle with node, Release
    if (_options.node) {
        _options.node->Release();
        _options.node = NULL;
    }
}

int Replicator::start(const ReplicatorOptions& options, ReplicatorId *id) {
    if (options.log_manager == NULL || options.commit_manager == NULL
            || options.node == NULL) {
        LOG(ERROR) << "Invalid arguments";
        return -1;
    }
    std::unique_ptr<Replicator> r(new (std::nothrow) Replicator());
    if (!r) {
        LOG(ERROR) << "Fail to new Replicator, " << berror();
        return -1;
    }
    baidu::rpc::ChannelOptions channel_opt;
    //channel_opt.protocol = baidu::rpc::protocol;
    channel_opt.connect_timeout_ms = options.heartbeat_timeout_ms;
    channel_opt.timeout_ms = -1; // We don't need RPC timeout
    // channel_opt.pipelined_mode = true; 
    if (r->_sending_channel.Init(options.peer_id.addr, &channel_opt) != 0) {
        LOG(ERROR) << "Fail to init sending channel";
        return -1;
    }

    // bind lifecycle with node, AddRef
    options.node->AddRef();

    r->_options = options;
    r->_next_index = r->_options.log_manager->last_log_index() + 1;
    if (bthread_id_create(&r->_id, r.get(), _on_failed) != 0) {
        LOG(ERROR) << "Fail to create bthread_id";
        return -1;
    }
    bthread_id_lock(r->_id, NULL);
    // _id is unlock in _send_heartbeat
    r->_send_heartbeat();
    if (id) {
        *id = r->_id.value;
    }
    r->_on_caught_up = NULL;
    r->_last_response_timestamp = base::monotonic_time_ms();
    r.release();
    return 0;
}

int Replicator::stop(ReplicatorId id) {
    bthread_id_t dummy_id = { id };
    return bthread_id_error(dummy_id, ESTOP);
}

int Replicator::join(ReplicatorId id) {
    bthread_id_t dummy_id = { id };
    return bthread_id_join(dummy_id);
}

int64_t Replicator::last_response_timestamp(ReplicatorId id) {
    bthread_id_t dummy_id = { id };
    Replicator* r = NULL;
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return 0;
    }
    int64_t timestamp = r->_last_response_timestamp;
    CHECK_EQ(0, bthread_id_unlock(dummy_id))
        << "Fail to unlock " << dummy_id;
    return timestamp;
}

int Replicator::stop_appending_after(ReplicatorId id, int64_t log_index) {
    LOG(WARNING) << "Not implemented yet";
    return 0;
}

void Replicator::wait_for_caught_up(ReplicatorId id, 
                                    const OnCaughtUp& on_caught_up,
                                    const timespec* due_time) {
    bthread_id_t dummy_id = { id };
    OnCaughtUp *copied_on_caught_up = new OnCaughtUp(on_caught_up);
    Replicator* r = NULL;
    do {
        if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
            break;
        }
        if (r->_on_caught_up != 0) {
            LOG(ERROR) << "Previous wait_for_caught_up is not over";
            CHECK_EQ(0, bthread_id_unlock(dummy_id)) 
                    << "Fail to unlock " << dummy_id;
            break;
        }
        if (due_time != NULL) {
            copied_on_caught_up->has_timer = true;
            CHECK_EQ(0, bthread_timer_add(&copied_on_caught_up->timer,
                                        *due_time,
                                        _on_catch_up_timedout,
                                        (void*)id));
        }
        r->_on_caught_up = copied_on_caught_up;
        // success
        CHECK_EQ(0, bthread_id_unlock(dummy_id)) 
                << "Fail to unlock " << dummy_id;
        return;
    } while (0);
    copied_on_caught_up->error_code = EINVAL;
    // FAILED
    bthread_t tid;
    if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL,
                                 _run_on_caught_up, copied_on_caught_up) != 0) {
        CHECK(false) << "Fail to start bthread, " << berror();
        _run_on_caught_up(copied_on_caught_up);
    }
    
}

void Replicator::_on_rpc_returned(ReplicatorId id, baidu::rpc::Controller* cntl,
                     AppendEntriesRequest* request, 
                     AppendEntriesResponse* response) {
    std::unique_ptr<baidu::rpc::Controller> cntl_gurad(cntl);
    std::unique_ptr<AppendEntriesRequest>  req_gurad(request);
    std::unique_ptr<AppendEntriesResponse> res_gurad(response);
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    const long start_time_us = base::gettimeofday_us();
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }
    if (cntl->Failed()) {
        LOG(ERROR) << "rpc error: " << cntl->ErrorText();
        // FIXME(chenzhangyi01): if the follower crashes, any RPC to the
        // follower fails immediately, so we need to block the follower for a
        // while instead of looping until it comes back or be removed
        r->_send_heartbeat();
        return;
    }
    if (!response->success()) {
        if (response->term() > r->_options.term) {
            NodeImpl *node_impl = r->_options.node;
            // Acquire a reference of Node here incase that Node is detroyed
            // after _notify_on_caught_up. Not increase reference count in
            // start() to avoid the circular reference issue
            node_impl->AddRef();
            bthread_id_unlock_and_destroy(dummy_id);
            r->_notify_on_caught_up(EPERM, true);
            node_impl->increase_term_to(response->term());
            node_impl->Release();
            return;
        }
        // prev_log_index and prev_log_term doesn't match
        if (response->last_log_index() + 1 < r->_next_index) {
            LOG(INFO) << "last_log_index at peer=" << r->_options.peer_id 
                      << " is " << response->last_log_index();
            // the peer contains less logs than leader
            r->_next_index = response->last_log_index() + 1;
        } else {  
            // the peer contains logs from old term which should be truncated
            // decrease _last_log_at_peer by one to test the right index
            if (BAIDU_LIKELY(r->_next_index > 1)) {
                LOG(INFO) << "log_index=" << r->_next_index << " dissmatch";
                --r->_next_index;
            } else {
                LOG(ERROR) << "Peer=" << r->_options.peer_id
                           << " declares that log at index=0 doesn't match,"
                              " which is not supposed to happen";
            }
        }
        // dummy_id is unlock in _send_heartbeat
        r->_send_heartbeat();
        return;
    }
    CHECK_EQ(response->term(), r->_options.term);
    r->_last_response_timestamp = base::monotonic_time_ms();
    // FIXME: move committing out of the critical section
    for (int i = 0; i < request->entries_size(); ++i) {
        LOG(INFO) << "i=" << i;
        r->_options.commit_manager->set_stable_at_peer_reentrant(
                r->_next_index + i, r->_options.peer_id);
    }
    r->_next_index += request->entries_size();
    r->_notify_on_caught_up(0, false);
    // dummy_id is unlock in _send_entries
    r->_send_entries(start_time_us);
    return;
}

void Replicator::_fill_common_fields(AppendEntriesRequest* request) {
    request->set_term(_options.term);
    request->set_group_id(_options.group_id);
    request->set_server_id(_options.server_id.to_string());
    request->set_peer_id(_options.peer_id.to_string());
    request->set_prev_log_index(_next_index - 1);
    request->set_prev_log_term(
            _options.log_manager->get_term(_next_index - 1));
    request->set_committed_index(_options.commit_manager->last_committed_index());
}

void Replicator::_send_heartbeat() {
    baidu::rpc::Controller* cntl = new baidu::rpc::Controller;
    AppendEntriesRequest *request = new AppendEntriesRequest;
    AppendEntriesResponse *response = new AppendEntriesResponse;
    _fill_common_fields(request);
    _rpc_in_fly = cntl->call_id();
    google::protobuf::Closure* done = google::protobuf::NewCallback<
        ReplicatorId, baidu::rpc::Controller*, AppendEntriesRequest*,
        AppendEntriesResponse*>(
                _on_rpc_returned, _id.value, cntl, request, response);
    RaftService_Stub stub(&_sending_channel);
    stub.append_entries(cntl, request, response, done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

int Replicator::_prepare_entry(int offset, EntryMeta* em, base::IOBuf *data) {
    const size_t log_index = _next_index + offset;
    LogEntry *entry = _options.log_manager->get_entry(log_index);
    if (entry == NULL) {
        return -1;
    }
    em->set_term(entry->term);
    em->set_type(entry->type);
    // FIXME: why don't put peers in data?
    if (entry->peers != NULL) {
        CHECK(!entry->peers->empty()) << "log_index=" << log_index;
        for (size_t i = 0; i < entry->peers->size(); ++i) {
            em->add_peers((*entry->peers)[i].to_string());
        }
    } else {
        CHECK(entry->type != ENTRY_TYPE_ADD_PEER) << "log_index=" << log_index;
    }
    em->set_data_len(entry->data.length());
    data->append(entry->data);
    entry->Release();
    return 0;
}

void Replicator::_send_entries(long start_time_us) {
    baidu::rpc::Controller* cntl = new baidu::rpc::Controller;
    AppendEntriesRequest *request = new AppendEntriesRequest;
    AppendEntriesResponse *response = new AppendEntriesResponse;
    _fill_common_fields(request);
    EntryMeta em;
    const int max_entries_size = FLAGS_max_entries_size;
    for (int i = 0; i < max_entries_size; ++i) {
        if (_prepare_entry(i, &em, &cntl->request_attachment()) != 0) {
            break;
        }
        request->add_entries()->Swap(&em);
    }
    if (request->entries_size() == 0) {
        delete cntl;
        delete request;
        delete response;
        // _id is unlock in _wait_more
        return _wait_more_entries(start_time_us);
    }

    _rpc_in_fly = cntl->call_id();
    google::protobuf::Closure* done = google::protobuf::NewCallback<
        ReplicatorId, baidu::rpc::Controller*, AppendEntriesRequest*,
        AppendEntriesResponse*>(
                _on_rpc_returned, _id.value, cntl, request, response);
    RaftService_Stub stub(&_sending_channel);
    stub.append_entries(cntl, request, response, done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

int Replicator::_continue_sending(void* arg, int error_code) {
    LOG(ERROR) << "fuck continue sending";
    long start_time_us = base::gettimeofday_us();
    Replicator* r = NULL;
    bthread_id_t id = { (uint64_t)arg };
    if (bthread_id_lock(id, (void**)&r) != 0) {
        return -1;
    }
    // id is unlock in _send_entries or _send_heartbeat
    if (error_code == ETIMEDOUT) {
        // If timedout occurs, we don't check whether there are new entries
        // because it's urgent to send a heartbeat to maintain leadership so that
        // we don't want to waste any time in sending log entries, which migth
        // be very costly when it contains large user data
        r->_send_heartbeat();
        return 0;
    }
    r->_send_entries(start_time_us);
    return 0;
}

void Replicator::_wait_more_entries(long start_time_us) {
    const timespec due_time = base::milliseconds_from(
            base::microseconds_to_timespec(start_time_us), 
            _options.heartbeat_timeout_ms);
    _options.log_manager->wait(_next_index - 1, &due_time, 
                       _continue_sending, (void*)_id.value);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

void* Replicator::_run_on_caught_up(void* arg) {
    OnCaughtUp* on_caught_up = (OnCaughtUp*)arg;
    on_caught_up->_run();
    delete on_caught_up;
    return NULL;
}

void Replicator::_notify_on_caught_up(int error_code, bool before_destory) {
    if (_on_caught_up == NULL) {
        return;
    }
    if (error_code != ETIMEDOUT) {
        if (_next_index - 1 + _on_caught_up->min_margin
                < _options.log_manager->last_log_index()) {
            return;
        }
        if (_on_caught_up->error_code != ERROR_CODE_UNSET_MAGIC) {
            return;
        }
        _on_caught_up->pid = _options.peer_id;
        _on_caught_up->error_code = error_code;
        if (_on_caught_up->has_timer) {
            if (!before_destory && bthread_timer_del(_on_caught_up->timer) == 1) {
                // There's running timer task, let timer task trigger
                // on_caught_up to void ABA problem
                return;
            }
        }
    } else { // Timed out
        if (_on_caught_up->error_code == ERROR_CODE_UNSET_MAGIC) {
            _on_caught_up->pid = _options.peer_id;
            _on_caught_up->error_code = error_code;
        }
    }
    bthread_t tid;
    if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL,
                                 _run_on_caught_up, _on_caught_up) != 0) {
        CHECK(false) << "Fail to start bthread, " << berror();
        _run_on_caught_up(_on_caught_up);
    }
    _on_caught_up = NULL;
}

int Replicator::_on_failed(bthread_id_t id, void* arg, int error_code) {
    Replicator* r = (Replicator*)arg;
    baidu::rpc::StartCancel(r->_rpc_in_fly);
    r->_notify_on_caught_up(error_code, true);
    const int rc = bthread_id_unlock_and_destroy(id);
    CHECK_EQ(0, rc) << "Fail to unlock " << id;
    delete r;
    return rc;
}

void Replicator::_on_catch_up_timedout(void* arg) {
    bthread_id_t id = { (uint64_t)arg };
    Replicator* r = NULL;
    if (bthread_id_lock(id, (void**)&r) != 0) {
        return;
    }
    r->_notify_on_caught_up(ETIMEDOUT, false);
}

// ==================== ReplicatorGroup ==========================

ReplicatorGroupOptions::ReplicatorGroupOptions()
    : heartbeat_timeout_ms(-1)
    , log_manager(NULL)
    , commit_manager(NULL)
    , node(NULL)
    , term(0)
{}

ReplicatorGroup::ReplicatorGroup() {}

ReplicatorGroup::~ReplicatorGroup() {
    stop_all();
}

int ReplicatorGroup::init(const NodeId& node_id, const ReplicatorGroupOptions& options) {
    _common_options.heartbeat_timeout_ms = options.heartbeat_timeout_ms;
    _common_options.log_manager = options.log_manager;
    _common_options.commit_manager = options.commit_manager;
    _common_options.node = options.node;
    _common_options.term = options.term;
    _common_options.group_id = node_id.group_id;
    _common_options.server_id = node_id.peer_id;
    return 0;
}

int ReplicatorGroup::add_replicator(const PeerId& peer) {
    ReplicatorOptions options = _common_options;
    options.peer_id = peer;
    ReplicatorId rid;
    if (Replicator::start(options, &rid) != 0) {
        LOG(ERROR) << "Fail to start replicator to peer=" << peer;
        return -1;
    }
    if (!_rmap.insert(std::make_pair(peer, rid)).second) {
        LOG(ERROR) << "Duplicate peer=" << peer;
        Replicator::stop(rid);
        return -1;
    }
    return 0;
}

int ReplicatorGroup::wait_caughtup(const PeerId& peer, const OnCaughtUp& on_caught_up,
                                    const timespec* due_time) {
    std::map<PeerId, ReplicatorId>::iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    Replicator::wait_for_caught_up(rid, on_caught_up, due_time);
    return 0;
}

int64_t ReplicatorGroup::last_response_timestamp(const PeerId& peer) {
    std::map<PeerId, ReplicatorId>::iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return 0;
    }
    ReplicatorId rid = iter->second;
    return Replicator::last_response_timestamp(rid);
}

int ReplicatorGroup::stop_replicator(const PeerId &peer) {
    std::map<PeerId, ReplicatorId>::iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    // Calling ReplicatorId::stop might lead to calling stop_replicator again, 
    // erase iter first to avoid race condition
    _rmap.erase(iter);
    return Replicator::stop(rid);
}

int ReplicatorGroup::stop_all() {
    std::vector<ReplicatorId> rids;
    rids.reserve(_rmap.size());
    for (std::map<PeerId, ReplicatorId>::const_iterator 
            iter = _rmap.begin(); iter != _rmap.end(); ++iter) {
        rids.push_back(iter->second);
    }
    _rmap.clear();
    for (size_t i = 0; i < rids.size(); ++i) {
        Replicator::stop(rids[i]);
    }
    return 0;
}

int ReplicatorGroup::stop_appending_after(const PeerId &peer, int64_t log_index) {
    std::map<PeerId, ReplicatorId>::iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    return Replicator::stop_appending_after(iter->second, log_index);
}

} // namespace raft
