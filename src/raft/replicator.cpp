// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/21 14:32:24

#include "raft/replicator.h"

#include <gflags/gflags.h>                      // DEFINE_int32
#include <base/unique_ptr.h>                    // std::unique_ptr
#include <base/time.h>                          // base::gettimeofday_us
#include <baidu/rpc/controller.h>               // baidu::rpc::Controller
#include <baidu/rpc/reloadable_flags.h>         // BAIDU_RPC_VALIDATE_GFLAG

#include "raft/node.h"                          // NodeImpl
#include "raft/commitment_manager.h"            // CommitmentManager
#include "raft/log_entry.h"                     // LogEntry

namespace raft {

DEFINE_int32(raft_max_entries_size, 1024,
             "The max number of entries in AppendEntriesRequest");
BAIDU_RPC_VALIDATE_GFLAG(raft_max_entries_size, ::baidu::rpc::PositiveInteger);

DEFINE_int32(raft_max_body_size, 512 * 1024,
             "The max byte size of AppendEntriesRequest");
BAIDU_RPC_VALIDATE_GFLAG(raft_max_body_size, ::baidu::rpc::PositiveInteger);

static bvar::LatencyRecorder g_send_entries_latency("raft_send_entries");
static bvar::LatencyRecorder g_normalized_send_entries_latency("raft_send_entries_normalized");

ReplicatorOptions::ReplicatorOptions()
    : dynamic_heartbeat_timeout_ms(NULL)
    , log_manager(NULL)
    , commit_manager(NULL)
    , node(NULL)
    , term(0)
    , snapshot_storage(NULL)
{}

const int ERROR_CODE_UNSET_MAGIC = 0x1234;

Replicator::Replicator() 
    : _next_index(0)
    , _consecutive_error_times(0)
    , _has_succeeded(false)
    , _timeout_now_index(0)
    , _last_response_timestamp(0)
    , _wait_id(0)
    , _reader(NULL)
    , _catchup_closure(NULL)
{
    _rpc_in_fly.value = 0;
    _heartbeat_in_fly.value = 0;
    _timeout_now_in_fly.value = 0;
}

Replicator::~Replicator() {
    // bind lifecycle with node, Release
    // Replicator stop is async
    if (_reader) {
        _options.snapshot_storage->close(_reader);
        _reader = NULL;
    }
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
    Replicator* r = new Replicator();
    baidu::rpc::ChannelOptions channel_opt;
    //channel_opt.connect_timeout_ms = *options.heartbeat_timeout_ms;
    channel_opt.timeout_ms = -1; // We don't need RPC timeout
    if (r->_sending_channel.Init(options.peer_id.addr, &channel_opt) != 0) {
        LOG(ERROR) << "Fail to init sending channel";
        delete r;
        return -1;
    }

    // bind lifecycle with node, AddRef
    // Replicator stop is async
    options.node->AddRef();

    r->_options = options;
    r->_next_index = r->_options.log_manager->last_log_index() + 1;
    if (bthread_id_create(&r->_id, r, _on_error) != 0) {
        LOG(ERROR) << "Fail to create bthread_id";
        delete r;
        return -1;
    }
    bthread_id_lock(r->_id, NULL);
    if (id) {
        *id = r->_id.value;
    }
    LOG(INFO) << "Replicator=" << r->_id << "@" << r->_options.peer_id << " is started";
    r->_catchup_closure = NULL;
    r->_last_response_timestamp = base::monotonic_time_ms();
    r->_start_heartbeat_timer(base::gettimeofday_us());
    // Note: r->_id is unlock in _send_empty_entries, don't touch r ever after
    r->_send_empty_entries(false);
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

void Replicator::wait_for_caught_up(ReplicatorId id, 
                                    int64_t max_margin,
                                    const timespec* due_time,
                                    CatchupClosure* done) {
    bthread_id_t dummy_id = { id };
    Replicator* r = NULL;
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        done->status().set_error(EINVAL, "No such replicator");
        run_closure_in_bthread(done);
        return;
    }
    if (r->_catchup_closure != NULL) {
        CHECK_EQ(0, bthread_id_unlock(dummy_id)) 
                << "Fail to unlock " << dummy_id;
        LOG(ERROR) << "Previous wait_for_caught_up is not over";
        done->status().set_error(EINVAL, "Duplicated call");
        run_closure_in_bthread(done);
        return;
    }
    done->_max_margin = max_margin;
    if (due_time != NULL) {
        done->_has_timer = true;
        if (bthread_timer_add(&done->_timer,
                              *due_time,
                              _on_catch_up_timedout,
                              (void*)id) != 0) {
            CHECK_EQ(0, bthread_id_unlock(dummy_id));
            LOG(ERROR) << "Fail to add timer";
            done->status().set_error(EINVAL, "Duplicated call");
            run_closure_in_bthread(done);
            return;
        }
    }
    r->_catchup_closure = done;
    // success
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) 
            << "Fail to unlock " << dummy_id;
    return;
}

void* Replicator::_on_block_timedout_in_new_thread(void* arg) {
    Replicator::_continue_sending(arg, ETIMEDOUT);
    return NULL;
}

void Replicator::_on_block_timedout(void *arg) {
    bthread_t tid;
    if (bthread_start_background(
                &tid, NULL, _on_block_timedout_in_new_thread, arg) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        _on_block_timedout_in_new_thread(arg);
    }
}

void Replicator::_block(long start_time_us, int /*error_code NOTE*/) {
    // TODO: Currently we don't care about error_code which indicates why the
    // very RPC fails. To make it better there should be different timeout for
    // each individual error (e.g. we don't need check every
    // heartbeat_timeout_ms whether a dead follower has come back), but it's just
    // fine now.
    const timespec due_time = base::milliseconds_from(
            base::microseconds_to_timespec(start_time_us), 
            *_options.dynamic_heartbeat_timeout_ms);
    bthread_timer_t timer;
    const int rc = bthread_timer_add(&timer, due_time, 
                                  _on_block_timedout, (void*)_id.value);
    RAFT_VLOG << "Blocking " << _options.peer_id << " for " 
              << *_options.dynamic_heartbeat_timeout_ms << "ms";
    if (rc == 0) {
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        return;
    } else {
        LOG(ERROR) << "Fail to add timer, " << berror(rc);
        // _id is unlock in _send_empty_entries
        return _send_empty_entries(false);
    }
}

void Replicator::_on_heartbeat_returned(
        ReplicatorId id, baidu::rpc::Controller* cntl,
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

    RAFT_VLOG << "node " << r->_options.group_id << ":" << r->_options.server_id 
        << " received HeartbeatResponse from "
        << r->_options.peer_id << " prev_log_index " << request->prev_log_index()
        << " prev_log_term " << request->prev_log_term()
        << noflush;
    if (cntl->Failed()) {
        RAFT_VLOG << " fail, sleep.";

        // TODO: Should it be VLOG?
        LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0) 
                        << "Fail to issue RPC to " << r->_options.peer_id
                        << " _consecutive_error_times=" << r->_consecutive_error_times
                        << ", " << cntl->ErrorText();
        r->_start_heartbeat_timer(start_time_us);
        CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
        return;
    }
    r->_consecutive_error_times = 0;
    if (response->term() > r->_options.term) {
        RAFT_VLOG << " fail, greater term " << response->term()
            << " expect term " << r->_options.term;

        NodeImpl *node_impl = r->_options.node;
        // Acquire a reference of Node here in case that Node is detroyed
        // after _notify_on_caught_up.
        node_impl->AddRef();
        r->_notify_on_caught_up(EPERM, true);
        LOG(INFO) << "Replicator=" << dummy_id << " is going to quit";
        r->_destroy();
        node_impl->increase_term_to(response->term());
        node_impl->Release();
        return;
    }
    RAFT_VLOG;
    r->_last_response_timestamp = base::monotonic_time_ms();
    r->_start_heartbeat_timer(start_time_us);
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
    return;
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

    RAFT_VLOG << "node " << r->_options.group_id << ":" << r->_options.server_id 
        << " received AppendEntriesResponse from "
        << r->_options.peer_id << " prev_log_index " << request->prev_log_index()
        << " prev_log_term " << request->prev_log_term() << " count " << request->entries_size()
        << noflush;

    if (cntl->Failed()) {
        RAFT_VLOG << " fail, sleep.";

        // TODO: Should it be VLOG?
        LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0) 
                        << "Fail to issue RPC to " << r->_options.peer_id
                        << " _consecutive_error_times=" << r->_consecutive_error_times
                        << ", " << cntl->ErrorText();
        // If the follower crashes, any RPC to the follower fails immediately,
        // so we need to block the follower for a while instead of looping until
        // it comes back or be removed
        // dummy_id is unlock in block
        return r->_block(start_time_us, cntl->ErrorCode());
    }
    r->_consecutive_error_times = 0;
    if (!response->success()) {
        if (response->term() > r->_options.term) {
            RAFT_VLOG << " fail, greater term " << response->term()
                << " expect term " << r->_options.term;

            NodeImpl *node_impl = r->_options.node;
            // Acquire a reference of Node here in case that Node is detroyed
            // after _notify_on_caught_up.
            node_impl->AddRef();
            r->_notify_on_caught_up(EPERM, true);
            r->_destroy();
            node_impl->increase_term_to(response->term());
            node_impl->Release();
            return;
        }
        RAFT_VLOG << " fail, find next_index remote last_log_index " << response->last_log_index()
            << " local next_index " << r->_next_index;

        // prev_log_index and prev_log_term doesn't match
        if (response->last_log_index() + 1 < r->_next_index) {
            LOG(INFO) << "last_log_index at peer=" << r->_options.peer_id 
                      << " is " << response->last_log_index();
            r->_last_response_timestamp = base::monotonic_time_ms();
            // The peer contains less logs than leader
            r->_next_index = response->last_log_index() + 1;
        } else {  
            r->_last_response_timestamp = base::monotonic_time_ms();
            // The peer contains logs from old term which should be truncated,
            // decrease _last_log_at_peer by one to test the right index to keep
            if (BAIDU_LIKELY(r->_next_index > 1)) {
                LOG(INFO) << "log_index=" << r->_next_index << " dismatch";
                --r->_next_index;
            } else {
                LOG(ERROR) << "Peer=" << r->_options.peer_id
                           << " declares that log at index=0 doesn't match,"
                              " which is not supposed to happen";
            }
        }
        // dummy_id is unlock in _send_heartbeat
        r->_send_empty_entries(false);
        return;
    }

    RAFT_VLOG << " success";

    CHECK_EQ(response->term(), r->_options.term);
    r->_last_response_timestamp = base::monotonic_time_ms();
    const int entries_size = request->entries_size();
    RAFT_VLOG_IF(entries_size > 0) << "Replicated logs in [" 
                                   << r->_next_index << ", " 
                                   << r->_next_index + entries_size - 1
                                   << "] to peer " << r->_options.peer_id;
    if (entries_size > 0) {
        r->_options.commit_manager->set_stable_at_peer(
                r->_next_index, r->_next_index + entries_size - 1,
                r->_options.peer_id);
        g_send_entries_latency << cntl->latency_us();
        if (cntl->request_attachment().size() > 0) {
            g_normalized_send_entries_latency << 
                cntl->latency_us() * 1024 / cntl->request_attachment().size();
        }
    }
    r->_next_index += entries_size;
    r->_has_succeeded = true;
    r->_notify_on_caught_up(0, false);
    // dummy_id is unlock in _send_entries
    if (r->_timeout_now_index > 0 && r->_timeout_now_index < r->_next_index) {
        r->_send_timeout_now(false, false);
    }
    r->_send_entries();
    return;
}

int Replicator::_fill_common_fields(AppendEntriesRequest* request, 
                                    int64_t prev_log_index,
                                    bool is_heartbeat) {
    const int64_t prev_log_term = _options.log_manager->get_term(prev_log_index);
    if (prev_log_term == 0 && prev_log_index != 0) {
        if (!is_heartbeat) {
            CHECK_LT(prev_log_index, _options.log_manager->first_log_index());
            RAFT_VLOG << "log_index=" << prev_log_index << " was compacted";
            return -1;
        } else {
            // The log at prev_log_index has been compacted, which indicates 
            // we is or is going to install snapshot to the follower. So we let 
            // both prev_log_index and prev_log_term be 0 in the heartbeat 
            // request so that follower would do nothing besides updating its 
            // leader timestamp.
            prev_log_index = 0;
        }
    }
    request->set_term(_options.term);
    request->set_group_id(_options.group_id);
    request->set_server_id(_options.server_id.to_string());
    request->set_peer_id(_options.peer_id.to_string());
    request->set_prev_log_index(prev_log_index);
    request->set_prev_log_term(prev_log_term);
    request->set_committed_index(_options.commit_manager->last_committed_index());
    return 0;
}

void Replicator::_send_empty_entries(bool is_heartbeat) {
    std::unique_ptr<baidu::rpc::Controller> cntl(new baidu::rpc::Controller);
    std::unique_ptr<AppendEntriesRequest> request(new AppendEntriesRequest);
    std::unique_ptr<AppendEntriesResponse> response(new AppendEntriesResponse);
    if (_fill_common_fields(
                request.get(), _next_index - 1, is_heartbeat) != 0) {
        CHECK(!is_heartbeat);
        // _id is unlock in _install_snapshot
        return _install_snapshot();
    }
    if (is_heartbeat) {
        _heartbeat_in_fly = cntl->call_id();
    } else {
        _rpc_in_fly = cntl->call_id();
    }

    RAFT_VLOG << "node " << _options.group_id << ":" << _options.server_id
        << " send HeartbeatRequest to " << _options.peer_id 
        << " term " << _options.term
        << " last_committed_index " << request->committed_index();

    google::protobuf::Closure* done = baidu::rpc::NewCallback(
                is_heartbeat ? _on_heartbeat_returned :  _on_rpc_returned, 
                _id.value, cntl.get(), request.get(), response.get());

    RaftService_Stub stub(&_sending_channel);
    stub.append_entries(cntl.release(), request.release(), 
                        response.release(), done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

int Replicator::_prepare_entry(int offset, EntryMeta* em, base::IOBuf *data) {
    if (data->length() >= (size_t)FLAGS_raft_max_body_size) {
        return -1;
    }
    const size_t log_index = _next_index + offset;
    LogEntry *entry = _options.log_manager->get_entry(log_index);
    if (entry == NULL) {
        return -1;
    }
    em->set_term(entry->id.term);
    em->set_type(entry->type);
    if (entry->peers != NULL) {
        CHECK(!entry->peers->empty()) << "log_index=" << log_index;
        for (size_t i = 0; i < entry->peers->size(); ++i) {
            em->add_peers((*entry->peers)[i].to_string());
        }
    } else {
        CHECK(entry->type != ENTRY_TYPE_CONFIGURATION) << "log_index=" << log_index;
    }
    em->set_data_len(entry->data.length());
    data->append(entry->data);
    entry->Release();
    return 0;
}

void Replicator::_send_entries() {
    std::unique_ptr<baidu::rpc::Controller> cntl(new baidu::rpc::Controller);
    std::unique_ptr<AppendEntriesRequest> request(new AppendEntriesRequest);
    std::unique_ptr<AppendEntriesResponse> response(new AppendEntriesResponse);
    if (_fill_common_fields(request.get(), _next_index - 1, false) != 0) {
        return _install_snapshot();
    }
    EntryMeta em;
    const int max_entries_size = FLAGS_raft_max_entries_size;
    for (int i = 0; i < max_entries_size; ++i) {
        if (_prepare_entry(i, &em, &cntl->request_attachment()) != 0) {
            break;
        }
        request->add_entries()->Swap(&em);
    }
    if (request->entries_size() == 0) {
        // _id is unlock in _wait_more
        if (_next_index < _options.log_manager->first_log_index()) {
            return _install_snapshot();
        }
        return _wait_more_entries();
    }

    _rpc_in_fly = cntl->call_id();

    RAFT_VLOG << "node " << _options.group_id << ":" << _options.server_id
        << " send AppendEntriesRequest to " << _options.peer_id << " term " << _options.term
        << " last_committed_index " << request->committed_index()
        << " prev_log_index " << request->prev_log_index()
        << " prev_log_term " << request->prev_log_term()
        << " log_index " << _next_index << " count " << request->entries_size();

    google::protobuf::Closure* done = baidu::rpc::NewCallback<
        ReplicatorId, baidu::rpc::Controller*, AppendEntriesRequest*,
        AppendEntriesResponse*>(
                _on_rpc_returned, _id.value, cntl.get(), 
                request.get(), response.get());
    RaftService_Stub stub(&_sending_channel);
    stub.append_entries(cntl.release(), request.release(), 
                        response.release(), done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

int Replicator::_continue_sending(void* arg, int error_code) {
    Replicator* r = NULL;
    bthread_id_t id = { (uint64_t)arg };
    if (bthread_id_lock(id, (void**)&r) != 0) {
        return -1;
    }
    if (error_code == ETIMEDOUT) {
        // Send empty entries after block timeout to check the correct
        // _next_index otherwise the replictor is likely waits in
        // _wait_more_entries and no futher logs would be replicated even if the
        // last_index of this followers is less than |next_index - 1|
        r->_send_empty_entries(false);
    } else if (error_code != ESTOP) {
        // id is unlock in _send_entries
        r->_send_entries();
    } else {
        LOG(WARNING) << "Replicator=" << id << " stops sending entries";
        bthread_id_unlock(id);
    }
    return 0;
}

void Replicator::_wait_more_entries() {
    _wait_id = _options.log_manager->wait(
            _next_index - 1, _continue_sending, (void*)_id.value);
    RAFT_VLOG << "node " << _options.group_id << ":" << _options.peer_id
        << " wait more entries";
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

void Replicator::_install_snapshot() {
    CHECK(!_reader);
    _reader = _options.snapshot_storage->open();
    if (!_reader){
        NodeImpl *node_impl = _options.node;
        node_impl->AddRef();
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        raft::Error e;
        e.set_type(ERROR_TYPE_SNAPSHOT);
        e.status().set_error(EIO, "Fail to open snapshot");
        node_impl->on_error(e);
        node_impl->Release();
        return;
    } 
    std::string uri = _reader->generate_uri_for_copy();
    SnapshotMeta meta;
    // report error on failure
    if (_reader->load_meta(&meta) != 0){
        std::string snapshot_path = _reader->get_path();
        NodeImpl *node_impl = _options.node;
        node_impl->AddRef();
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        raft::Error e;
        e.set_type(ERROR_TYPE_SNAPSHOT);
        e.status().set_error(EIO, "Fail to load meta from " + snapshot_path);
        node_impl->on_error(e);
        node_impl->Release();
        return;
    } 
    baidu::rpc::Controller* cntl = new baidu::rpc::Controller;
    cntl->set_max_retry(0);
    cntl->set_timeout_ms(-1);
    InstallSnapshotRequest* request = new InstallSnapshotRequest();
    InstallSnapshotResponse* response = new InstallSnapshotResponse();
    request->set_term(_options.term);
    request->set_group_id(_options.group_id);
    request->set_server_id(_options.server_id.to_string());
    request->set_peer_id(_options.peer_id.to_string());
    request->mutable_meta()->CopyFrom(meta);
    request->set_uri(uri);

    LOG(INFO) << "node " << _options.group_id << ":" << _options.server_id
        << " send InstallSnapshotRequest to " << _options.peer_id
        << " term " << _options.term << " last_included_term " << meta.last_included_term()
        << " last_included_index " << meta.last_included_index() << " uri " << uri;

    _rpc_in_fly = cntl->call_id();
    google::protobuf::Closure* done = baidu::rpc::NewCallback<
                ReplicatorId, baidu::rpc::Controller*,
                InstallSnapshotRequest*, InstallSnapshotResponse*>(
                    _on_install_snapshot_returned, _id.value,
                    cntl, request, response);
    RaftService_Stub stub(&_sending_channel);
    stub.install_snapshot(cntl, request, response, done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

void Replicator::_on_install_snapshot_returned(
            ReplicatorId id, baidu::rpc::Controller* cntl,
            InstallSnapshotRequest* request, 
            InstallSnapshotResponse* response) {
    std::unique_ptr<baidu::rpc::Controller> cntl_guard(cntl);
    std::unique_ptr<InstallSnapshotRequest> request_guard(request);
    std::unique_ptr<InstallSnapshotResponse> response_guard(response);
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    bool succ = true;
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }
    if (r->_reader) {
        r->_options.snapshot_storage->close(r->_reader);
        r->_reader = NULL;
    }
    LOG(INFO) << "received InstallSnapshotResponse from "
        << r->_options.group_id << ":" << r->_options.peer_id
        << " last_included_index " << request->meta().last_included_index()
        << " last_included_term " << request->meta().last_included_term()
        << noflush;
    do {
        if (cntl->Failed()) {
            LOG(INFO) << " error: " << cntl->ErrorText();
            LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0) 
                            << "Fail to install snapshot at peer=" 
                            << r->_options.peer_id
                            <<", " << cntl->ErrorText();
            succ = false;
            break;
        }
        if (!response->success()) {
            succ = false;
            RAFT_VLOG << " fail.";
            // Let hearbeat do step down
            break;
        }
        // Success 
        r->_next_index = request->meta().last_included_index() + 1;
        LOG(INFO) << " success.";
    } while (0);

    // We don't retry installing the snapshot explicitly. 
    // dummy_id is unlock in _send_entries
    if (!succ) {
        return r->_block(base::gettimeofday_us(), cntl->ErrorCode());
    }
    r->_has_succeeded = true;
    r->_notify_on_caught_up(0, false);
    if (r->_timeout_now_index > 0 && r->_timeout_now_index < r->_next_index) {
        r->_send_timeout_now(false, false);
    }
    // dummy_id is unlock in _send_entries
    return r->_send_entries();
}

void Replicator::_notify_on_caught_up(int error_code, bool before_destory) {
    if (_catchup_closure == NULL) {
        return;
    }
    if (error_code != ETIMEDOUT) {
        if (_next_index - 1 + _catchup_closure->_max_margin
                < _options.log_manager->last_log_index()) {
            return;
        }
        if (_catchup_closure->_error_was_set) {
            return;
        }
        _catchup_closure->_error_was_set = true;
        if (error_code) {
            _catchup_closure->status().set_error(error_code, "%s", berror(error_code));
        }
        if (_catchup_closure->_has_timer) {
            if (!before_destory && bthread_timer_del(_catchup_closure->_timer) == 1) {
                // There's running timer task, let timer task trigger
                // on_caught_up to void ABA problem
                return;
            }
        }
    } else { // Timed out
        if (!_catchup_closure->_error_was_set) {
            _catchup_closure->status().set_error(error_code, "%s", berror(error_code));
        }
    }
    Closure* saved_catchup_closure = _catchup_closure;
    _catchup_closure = NULL;
    return run_closure_in_bthread(saved_catchup_closure);
}

void Replicator::_on_timedout(void* arg) {
    bthread_id_t id = { (uint64_t)arg };
    bthread_id_error(id, ETIMEDOUT);
}

void Replicator::_start_heartbeat_timer(long start_time_us) {
    const timespec due_time = base::milliseconds_from(
            base::microseconds_to_timespec(start_time_us), 
            *_options.dynamic_heartbeat_timeout_ms);
    if (bthread_timer_add(&_heartbeat_timer, due_time,
                       _on_timedout, (void*)_id.value) != 0) {
        _on_timedout((void*)_id.value);
    }
}

void* Replicator::_send_heartbeat(void* arg) {
    Replicator* r = NULL;
    bthread_id_t id = { (uint64_t)arg };
    if (bthread_id_lock(id, (void**)&r) != 0) {
        // This replicator is stopped
        return NULL;
    }
    // id is unlock in _send_empty_entries;
    r->_send_empty_entries(true);
    return NULL;
}

int Replicator::_on_error(bthread_id_t id, void* arg, int error_code) {
    Replicator* r = (Replicator*)arg;
    if (error_code == ESTOP) {
        baidu::rpc::StartCancel(r->_rpc_in_fly);
        baidu::rpc::StartCancel(r->_heartbeat_in_fly);
        baidu::rpc::StartCancel(r->_timeout_now_in_fly);
        bthread_timer_del(r->_heartbeat_timer);
        r->_options.log_manager->remove_waiter(r->_wait_id);
        r->_notify_on_caught_up(error_code, true);
        LOG(INFO) << "Replicator=" << id << " is going to quit";
        r->_destroy();
        return 0;
    } else if (error_code == ETIMEDOUT) {
        // This error is issued in the TimerThread, start a new bthread to avoid
        // blocking the caller.
        // Unlock id to remove the context-switch out of the critical section
        CHECK_EQ(0, bthread_id_unlock(id)) << "Fail to unlock" << id;
        bthread_t tid;
        if (bthread_start_urgent(&tid, NULL, _send_heartbeat,
                                 reinterpret_cast<void*>(id.value)) != 0) {
            PLOG(ERROR) << "Fail to start bthread";
            _send_heartbeat(reinterpret_cast<void*>(id.value));
        }
        return 0;
    } else {
        CHECK(false) << "Unknown error_code=" << error_code;
        CHECK_EQ(0, bthread_id_unlock(id)) << "Fail to unlock " << id;
        return -1;
    }
}

void Replicator::_on_catch_up_timedout(void* arg) {
    bthread_id_t id = { (uint64_t)arg };
    Replicator* r = NULL;
    if (bthread_id_lock(id, (void**)&r) != 0) {
        return;
    }
    r->_notify_on_caught_up(ETIMEDOUT, false);
    CHECK_EQ(0, bthread_id_unlock(id)) 
            << "Fail to unlock" << id;
}

int Replicator::transfer_leadership(ReplicatorId id, int64_t log_index) {
    Replicator* r = NULL;
    bthread_id_t dummy = { id };
    const int rc = bthread_id_lock(dummy, (void**)&r);
    if (rc != 0) {
        return rc;
    }
    // dummy is unlock in _transfer_leadership
    return r->_transfer_leadership(log_index);
}

int Replicator::stop_transfer_leadership(ReplicatorId id) {
    Replicator* r = NULL;
    bthread_id_t dummy = { id };
    const int rc = bthread_id_lock(dummy, (void**)&r);
    if (rc != 0) {
        return rc;
    }
    // dummy is unlock in _transfer_leadership
    r->_timeout_now_index = 0;
    CHECK_EQ(0, bthread_id_unlock(dummy)) << "Fail to unlock " << dummy;
    return 0;
}

int Replicator::_transfer_leadership(int64_t log_index) {
    if (_has_succeeded && _next_index > log_index) {
        // _id is unlock in _send_timeout_now
        _send_timeout_now(true, false);
        return 0;
    }
    // Register log_index so that _on_rpc_returned would trigger
    // _send_timeout_now if _next_index reaches log_index
    _timeout_now_index = log_index;
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
    return 0;
}

void Replicator::_send_timeout_now(bool unlock_id, bool stop_after_finish,
                                   int timeout_ms) {
    TimeoutNowRequest* request = new TimeoutNowRequest;
    TimeoutNowResponse* response = new TimeoutNowResponse;
    request->set_term(_options.term);
    request->set_group_id(_options.group_id);
    request->set_server_id(_options.server_id.to_string());
    request->set_peer_id(_options.peer_id.to_string());
    baidu::rpc::Controller* cntl = new baidu::rpc::Controller;
    if (!stop_after_finish) {
        // This RPC is issued by transfer_leadership, save this call_id so that
        // the RPC can be cancelled by stop.
        _timeout_now_in_fly = cntl->call_id();
        _timeout_now_index = 0;
    }
    if (timeout_ms > 0) {
        cntl->set_timeout_ms(timeout_ms);
    }
    RaftService_Stub stub(&_sending_channel);
    ::google::protobuf::Closure* done = baidu::rpc::NewCallback(
            _on_timeout_now_returned, _id.value, cntl, request, response,
            stop_after_finish);
    stub.timeout_now(cntl, request, response, done);
    if (unlock_id) {
        CHECK_EQ(0, bthread_id_unlock(_id));
    }
}

void Replicator::_on_timeout_now_returned(
                ReplicatorId id, baidu::rpc::Controller* cntl,
                TimeoutNowRequest* request, 
                TimeoutNowResponse* response,
                bool stop_after_finish) {
    std::unique_ptr<baidu::rpc::Controller> cntl_gurad(cntl);
    std::unique_ptr<TimeoutNowRequest>  req_gurad(request);
    std::unique_ptr<TimeoutNowResponse> res_gurad(response);
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }
    RAFT_VLOG << "node " << r->_options.group_id << ":" << r->_options.server_id 
        << " received TimeoutNowResponse from "
        << r->_options.peer_id
        << noflush;

    if (cntl->Failed()) {
        RAFT_VLOG << " fail : " << cntl->ErrorText();
        if (stop_after_finish) {
            r->_notify_on_caught_up(ESTOP, true);
            r->_destroy();
        } else {
            CHECK_EQ(0, bthread_id_unlock(dummy_id));
        }
        return;
    }
    RAFT_VLOG << (response->success() ? " success " : "fail:");
    if (response->term() > r->_options.term) {
        NodeImpl *node_impl = r->_options.node;
        // Acquire a reference of Node here in case that Node is detroyed
        // after _notify_on_caught_up.
        node_impl->AddRef();
        r->_notify_on_caught_up(EPERM, true);
        r->_destroy();
        node_impl->increase_term_to(response->term());
        node_impl->Release();
        return;
    }
    if (stop_after_finish) {
        r->_notify_on_caught_up(ESTOP, true);
        r->_destroy();
    } else {
        CHECK_EQ(0, bthread_id_unlock(dummy_id));
    }
}

int Replicator::send_timeout_now_and_stop(ReplicatorId id, int timeout_ms) {
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return -1;
    }
    // dummy_id is unlock in _send_timeout_now
    r->_send_timeout_now(true, true, timeout_ms);
    return 0;
}

int64_t Replicator::get_next_index(ReplicatorId id) {
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return 0;
    }
    int64_t next_index = 0;
    if (r->_has_succeeded) {
        next_index = r->_next_index;
    }
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
    return next_index;
}

void Replicator::_destroy() {
    bthread_id_t saved_id = _id;
    CHECK_EQ(0, bthread_id_unlock_and_destroy(saved_id));
    // TODO: Add more infomation
    LOG(INFO) << "Replicator=" << saved_id << " is going to quit";
    delete this;
}

// ==================== ReplicatorGroup ==========================

ReplicatorGroupOptions::ReplicatorGroupOptions()
    : heartbeat_timeout_ms(-1)
    , log_manager(NULL)
    , commit_manager(NULL)
    , node(NULL)
    , snapshot_storage(NULL)
{}

ReplicatorGroup::ReplicatorGroup() 
    : _dynamic_timeout_ms(-1)
{
    _common_options.dynamic_heartbeat_timeout_ms = &_dynamic_timeout_ms;
}

ReplicatorGroup::~ReplicatorGroup() {
    stop_all();
}

int ReplicatorGroup::init(const NodeId& node_id, const ReplicatorGroupOptions& options) {
    _dynamic_timeout_ms = options.heartbeat_timeout_ms;
    _common_options.log_manager = options.log_manager;
    _common_options.commit_manager = options.commit_manager;
    _common_options.node = options.node;
    _common_options.term = 0;
    _common_options.group_id = node_id.group_id;
    _common_options.server_id = node_id.peer_id;
    _common_options.snapshot_storage = options.snapshot_storage;
    return 0;
}

int ReplicatorGroup::add_replicator(const PeerId& peer) {
    CHECK_NE(0, _common_options.term);
    if (_rmap.find(peer) != _rmap.end()) {
        return 0;
    }
    ReplicatorOptions options = _common_options;
    options.peer_id = peer;
    ReplicatorId rid;
    if (Replicator::start(options, &rid) != 0) {
        LOG(ERROR) << "Fail to start replicator to peer=" << peer;
        return -1;
    }
    _rmap[peer] = rid;
    return 0;
}

int ReplicatorGroup::wait_caughtup(const PeerId& peer, 
                                   int64_t max_margin, const timespec* due_time,
                                   CatchupClosure* done) {
    std::map<PeerId, ReplicatorId>::iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    Replicator::wait_for_caught_up(rid, max_margin, due_time, done);
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

bool ReplicatorGroup::contains(const PeerId& peer) const {
    return _rmap.find(peer) != _rmap.end();
}

int ReplicatorGroup::reset_term(int64_t new_term) {
    if (new_term <= _common_options.term) {
        CHECK_GT(new_term, _common_options.term) << "term cannot be decreased";
        return -1;
    }
    _common_options.term = new_term;
    return 0;
}

int ReplicatorGroup::reset_heartbeat_interval(int new_interval_ms) {
    _dynamic_timeout_ms = new_interval_ms;
    return 0;
}

int ReplicatorGroup::transfer_leadership_to(
        const PeerId& peer, int64_t log_index) {
    std::map<PeerId, ReplicatorId>::const_iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    return Replicator::transfer_leadership(rid, log_index);
}

int ReplicatorGroup::stop_transfer_leadership(const PeerId& peer) {
    std::map<PeerId, ReplicatorId>::const_iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    return Replicator::stop_transfer_leadership(rid);
}

int ReplicatorGroup::stop_all_and_find_the_next_candidate(
                ReplicatorId* candidate, const Configuration& current_conf) {
    *candidate = INVALID_BTHREAD_ID.value;
    int64_t max_index =  0;
    for (std::map<PeerId, ReplicatorId>::const_iterator
            iter = _rmap.begin();  iter != _rmap.end(); ++iter) {
        if (!current_conf.contains(iter->first)) {
            continue;
        }
        const int64_t next_index = Replicator::get_next_index(iter->second);
        if (next_index > max_index) {
            max_index = next_index;
            *candidate = iter->second;
        }
    }
    for (std::map<PeerId, ReplicatorId>::const_iterator
            iter = _rmap.begin();  iter != _rmap.end(); ++iter) {
        if (iter->second != *candidate) {
            Replicator::stop(iter->second);
        }
    }
    _rmap.clear();
    return 0;
}

} // namespace raft
