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
//          Xiong,Kai(xiongkai@baidu.com)

#include "braft/replicator.h"

#include <gflags/gflags.h>                       // DEFINE_int32
#include <butil/unique_ptr.h>                    // std::unique_ptr
#include <butil/time.h>                          // butil::gettimeofday_us
#include <brpc/controller.h>                     // brpc::Controller
#include <brpc/reloadable_flags.h>               // BRPC_VALIDATE_GFLAG

#include "braft/node.h"                          // NodeImpl
#include "braft/ballot_box.h"                    // BallotBox 
#include "braft/log_entry.h"                     // LogEntry
#include "braft/snapshot_throttle.h"             // SnapshotThrottle

namespace braft {

DEFINE_int32(raft_max_entries_size, 1024,
             "The max number of entries in AppendEntriesRequest");
BRPC_VALIDATE_GFLAG(raft_max_entries_size, ::brpc::PositiveInteger);

DEFINE_int32(raft_max_parallel_append_entries_rpc_num, 1,
             "The max number of parallel AppendEntries requests");
BRPC_VALIDATE_GFLAG(raft_max_parallel_append_entries_rpc_num,
                    ::brpc::PositiveInteger);

DEFINE_int32(raft_max_body_size, 512 * 1024,
             "The max byte size of AppendEntriesRequest");
BRPC_VALIDATE_GFLAG(raft_max_body_size, ::brpc::PositiveInteger);

DEFINE_int32(raft_retry_replicate_interval_ms, 1000,
             "Interval of retry to append entries or install snapshot");
BRPC_VALIDATE_GFLAG(raft_retry_replicate_interval_ms,
                    brpc::PositiveInteger);

static bvar::LatencyRecorder g_send_entries_latency("raft_send_entries");
static bvar::LatencyRecorder g_normalized_send_entries_latency(
             "raft_send_entries_normalized");
static bvar::CounterRecorder g_send_entries_batch_counter(
             "raft_send_entries_batch_counter");

ReplicatorOptions::ReplicatorOptions()
    : dynamic_heartbeat_timeout_ms(NULL)
    , log_manager(NULL)
    , ballot_box(NULL)
    , node(NULL)
    , term(0)
    , snapshot_storage(NULL)
{
}

Replicator::Replicator() 
    : _next_index(0)
    , _flying_append_entries_size(0)
    , _consecutive_error_times(0)
    , _has_succeeded(false)
    , _timeout_now_index(0)
    , _last_rpc_send_timestamp(0)
    , _heartbeat_counter(0)
    , _append_entries_counter(0)
    , _install_snapshot_counter(0)
    , _readonly_index(0)
    , _wait_id(0)
    , _is_waiter_canceled(false)
    , _reader(NULL)
    , _catchup_closure(NULL)
{
    _install_snapshot_in_fly.value = 0;
    _heartbeat_in_fly.value = 0;
    _timeout_now_in_fly.value = 0;
    memset(&_st, 0, sizeof(_st));
}

Replicator::~Replicator() {
    // bind lifecycle with node, Release
    // Replicator stop is async
    _close_reader();
    if (_options.node) {
        _options.node->Release();
        _options.node = NULL;
    }
}

int Replicator::start(const ReplicatorOptions& options, ReplicatorId *id) {
    if (options.log_manager == NULL || options.ballot_box == NULL
            || options.node == NULL) {
        LOG(ERROR) << "Invalid arguments, group " << options.group_id;
        return -1;
    }
    Replicator* r = new Replicator();
    brpc::ChannelOptions channel_opt;
    //channel_opt.connect_timeout_ms = *options.heartbeat_timeout_ms;
    channel_opt.timeout_ms = -1; // We don't need RPC timeout
    if (r->_sending_channel.Init(options.peer_id.addr, &channel_opt) != 0) {
        LOG(ERROR) << "Fail to init sending channel"
                   << ", group " << options.group_id;
        delete r;
        return -1;
    }

    // bind lifecycle with node, AddRef
    // Replicator stop is async
    options.node->AddRef();

    r->_options = options;
    r->_next_index = r->_options.log_manager->last_log_index() + 1;
    if (bthread_id_create(&r->_id, r, _on_error) != 0) {
        LOG(ERROR) << "Fail to create bthread_id"
                   << ", group " << options.group_id;
        delete r;
        return -1;
    }
    bthread_id_lock(r->_id, NULL);
    if (id) {
        *id = r->_id.value;
    }
    LOG(INFO) << "Replicator=" << r->_id << "@" << r->_options.peer_id << " is started"
              << ", group " << r->_options.group_id;
    r->_catchup_closure = NULL;
    r->_last_rpc_send_timestamp = butil::monotonic_time_ms();
    r->_start_heartbeat_timer(butil::gettimeofday_us());
    // Note: r->_id is unlock in _send_empty_entries, don't touch r ever after
    r->_send_empty_entries(false);
    return 0;
}

int Replicator::stop(ReplicatorId id) {
    bthread_id_t dummy_id = { id };
    Replicator* r = NULL;
    // already stopped
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return 0;
    }
    // to run _catchup_closure if it is not NULL
    r->_notify_on_caught_up(EPERM, true);
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
    return bthread_id_error(dummy_id, ESTOP);
}

int Replicator::join(ReplicatorId id) {
    bthread_id_t dummy_id = { id };
    return bthread_id_join(dummy_id);
}

int64_t Replicator::last_rpc_send_timestamp(ReplicatorId id) {
    bthread_id_t dummy_id = { id };
    Replicator* r = NULL;
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return 0;
    }
    int64_t timestamp = r->_last_rpc_send_timestamp;
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
        LOG(ERROR) << "Previous wait_for_caught_up is not over"
                   << ", group " << r->_options.group_id;
        done->status().set_error(EINVAL, "Duplicated call");
        run_closure_in_bthread(done);
        return;
    }
    done->_max_margin = max_margin;
    if (r->_has_succeeded && r->_is_catchup(max_margin)) {
        LOG(INFO) << "Already catch up before add catch up timer"
                  << ", group " << r->_options.group_id;
        run_closure_in_bthread(done);
        CHECK_EQ(0, bthread_id_unlock(dummy_id))
                << "Fail to unlock" << dummy_id;
        return;
    }
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

void Replicator::_block(long start_time_us, int error_code) {
    // mainly for pipeline case, to avoid too many block timer when this 
    // replicator is something wrong
    if (_st.st == BLOCKING) {
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        return;
    }

    // TODO: Currently we don't care about error_code which indicates why the
    // very RPC fails. To make it better there should be different timeout for
    // each individual error (e.g. we don't need check every
    // heartbeat_timeout_ms whether a dead follower has come back), but it's just
    // fine now.
    int blocking_time = 0;
    if (error_code == EBUSY || error_code == EINTR) {
        blocking_time = FLAGS_raft_retry_replicate_interval_ms;
    } else {
        blocking_time = *_options.dynamic_heartbeat_timeout_ms;
    }
    const timespec due_time = butil::milliseconds_from(
	    butil::microseconds_to_timespec(start_time_us), blocking_time);
    bthread_timer_t timer;
    const int rc = bthread_timer_add(&timer, due_time, 
                                  _on_block_timedout, (void*)_id.value);
    if (rc == 0) {
        BRAFT_VLOG << "Blocking " << _options.peer_id << " for " 
                   << blocking_time << "ms" << ", group " << _options.group_id;
        _st.st = BLOCKING;
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        return;
    } else {
        LOG(ERROR) << "Fail to add timer, " << berror(rc);
        // _id is unlock in _send_empty_entries
        return _send_empty_entries(false);
    }
}

void Replicator::_on_heartbeat_returned(
        ReplicatorId id, brpc::Controller* cntl,
        AppendEntriesRequest* request, 
        AppendEntriesResponse* response,
        int64_t rpc_send_time) {
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<AppendEntriesRequest>  req_guard(request);
    std::unique_ptr<AppendEntriesResponse> res_guard(response);
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    const long start_time_us = butil::gettimeofday_us();
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }

    std::stringstream ss;
    ss << "node " << r->_options.group_id << ":" << r->_options.server_id 
       << " received HeartbeatResponse from "
       << r->_options.peer_id << " prev_log_index " << request->prev_log_index()
       << " prev_log_term " << request->prev_log_term();
               
    if (cntl->Failed()) {
        ss << " fail, sleep.";
        BRAFT_VLOG << ss.str();

        // TODO: Should it be VLOG?
        LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0)
                        << "Group " << r->_options.group_id
                        << " fail to issue RPC to " << r->_options.peer_id
                        << " _consecutive_error_times=" << r->_consecutive_error_times
                        << ", " << cntl->ErrorText();
        r->_start_heartbeat_timer(start_time_us);
        CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
        return;
    }
    r->_consecutive_error_times = 0;
    if (response->term() > r->_options.term) {
        ss << " fail, greater term " << response->term()
           << " expect term " << r->_options.term;
        BRAFT_VLOG << ss.str();

        NodeImpl *node_impl = r->_options.node;
        // Acquire a reference of Node here in case that Node is detroyed
        // after _notify_on_caught_up.
        node_impl->AddRef();
        r->_notify_on_caught_up(EPERM, true);
        LOG(INFO) << "Replicator=" << dummy_id << " is going to quit"
                  << ", group " << r->_options.group_id;
        butil::Status status;
        status.set_error(EHIGHERTERMRESPONSE, "Leader receives higher term "
                "heartbeat_response from peer:%s", r->_options.peer_id.to_string().c_str());
        r->_destroy();
        node_impl->increase_term_to(response->term(), status);
        node_impl->Release();
        return;
    }

    bool readonly = response->has_readonly() && response->readonly();
    BRAFT_VLOG << ss.str() << " readonly " << readonly;
    if (rpc_send_time > r->_last_rpc_send_timestamp) {
        r->_last_rpc_send_timestamp = rpc_send_time; 
    }
    r->_start_heartbeat_timer(start_time_us);
    NodeImpl* node_impl = NULL;
    // Check if readonly config changed
    if ((readonly && r->_readonly_index == 0) ||
        (!readonly && r->_readonly_index != 0)) {
        node_impl = r->_options.node;
        node_impl->AddRef();
    }
    if (!node_impl) {
        CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
        return;
    }
    const PeerId peer_id = r->_options.peer_id;
    int64_t term = r->_options.term;
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
    node_impl->change_readonly_config(term, peer_id, readonly);
    node_impl->Release();
    return;
}

void Replicator::_on_rpc_returned(ReplicatorId id, brpc::Controller* cntl,
                     AppendEntriesRequest* request, 
                     AppendEntriesResponse* response,
                     int64_t rpc_send_time) {
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<AppendEntriesRequest>  req_guard(request);
    std::unique_ptr<AppendEntriesResponse> res_guard(response);
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    const long start_time_us = butil::gettimeofday_us();
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }

    std::stringstream ss;
    ss << "node " << r->_options.group_id << ":" << r->_options.server_id 
       << " received AppendEntriesResponse from "
       << r->_options.peer_id << " prev_log_index " << request->prev_log_index()
       << " prev_log_term " << request->prev_log_term() << " count " << request->entries_size();

    bool valid_rpc = false;
    int64_t rpc_first_index = request->prev_log_index() + 1;
    int64_t min_flying_index = r->_min_flying_index();
    CHECK_GT(min_flying_index, 0);

    for (std::deque<FlyingAppendEntriesRpc>::iterator rpc_it = r->_append_entries_in_fly.begin();
        rpc_it != r->_append_entries_in_fly.end(); ++rpc_it) {
        if (rpc_it->log_index > rpc_first_index) {
            break;
        }
        if (rpc_it->call_id == cntl->call_id()) {
            valid_rpc = true;
        }
    }
    if (!valid_rpc) {
        ss << " ignore invalid rpc";
        BRAFT_VLOG << ss.str();
        CHECK_EQ(0, bthread_id_unlock(r->_id)) << "Fail to unlock " << r->_id;
        return;
    }

    if (cntl->Failed()) {
        ss << " fail, sleep.";
        BRAFT_VLOG << ss.str();

        // TODO: Should it be VLOG?
        LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0)
                        << "Group " << r->_options.group_id
                        << " fail to issue RPC to " << r->_options.peer_id
                        << " _consecutive_error_times=" << r->_consecutive_error_times
                        << ", " << cntl->ErrorText();
        // If the follower crashes, any RPC to the follower fails immediately,
        // so we need to block the follower for a while instead of looping until
        // it comes back or be removed
        // dummy_id is unlock in block
        r->_reset_next_index();
        return r->_block(start_time_us, cntl->ErrorCode());
    }
    r->_consecutive_error_times = 0;
    if (!response->success()) {
        if (response->term() > r->_options.term) {
            BRAFT_VLOG << " fail, greater term " << response->term()
                       << " expect term " << r->_options.term;
            r->_reset_next_index();

            NodeImpl *node_impl = r->_options.node;
            // Acquire a reference of Node here in case that Node is detroyed
            // after _notify_on_caught_up.
            node_impl->AddRef();
            r->_notify_on_caught_up(EPERM, true);
            butil::Status status;
            status.set_error(EHIGHERTERMRESPONSE, "Leader receives higher term "
                    "%s from peer:%s", response->GetTypeName().c_str(), r->_options.peer_id.to_string().c_str());
            r->_destroy();
            node_impl->increase_term_to(response->term(), status);
            node_impl->Release();
            return;
        }
        ss << " fail, find next_index remote last_log_index " << response->last_log_index()
           << " local next_index " << r->_next_index 
           << " rpc prev_log_index " << request->prev_log_index();
        BRAFT_VLOG << ss.str();
        if (rpc_send_time > r->_last_rpc_send_timestamp) {
            r->_last_rpc_send_timestamp = rpc_send_time; 
        }
        // prev_log_index and prev_log_term doesn't match
        r->_reset_next_index();
        if (response->last_log_index() + 1 < r->_next_index) {
            BRAFT_VLOG << "Group " << r->_options.group_id
                       << " last_log_index at peer=" << r->_options.peer_id 
                       << " is " << response->last_log_index();
            // The peer contains less logs than leader
            r->_next_index = response->last_log_index() + 1;
        } else {  
            // The peer contains logs from old term which should be truncated,
            // decrease _last_log_at_peer by one to test the right index to keep
            if (BAIDU_LIKELY(r->_next_index > 1)) {
                BRAFT_VLOG << "Group " << r->_options.group_id 
                           << " log_index=" << r->_next_index << " dismatch";
                --r->_next_index;
            } else {
                LOG(ERROR) << "Group " << r->_options.group_id 
                           << " peer=" << r->_options.peer_id
                           << " declares that log at index=0 doesn't match,"
                              " which is not supposed to happen";
            }
        }
        // dummy_id is unlock in _send_heartbeat
        r->_send_empty_entries(false);
        return;
    }

    ss << " success";
    BRAFT_VLOG << ss.str();
    
    if (response->term() != r->_options.term) {
        LOG(ERROR) << "Group " << r->_options.group_id
                   << " fail, response term " << response->term()
                   << " dismatch, expect term " << r->_options.term;
        r->_reset_next_index();
        CHECK_EQ(0, bthread_id_unlock(r->_id)) << "Fail to unlock " << r->_id;
        return;
    }
    if (rpc_send_time > r->_last_rpc_send_timestamp) {
        r->_last_rpc_send_timestamp = rpc_send_time; 
    }
    const int entries_size = request->entries_size();
    const int64_t rpc_last_log_index = request->prev_log_index() + entries_size;
    BRAFT_VLOG_IF(entries_size > 0) << "Group " << r->_options.group_id
                                    << " replicated logs in [" 
                                    << min_flying_index << ", " 
                                    << rpc_last_log_index
                                    << "] to peer " << r->_options.peer_id;
    if (entries_size > 0) {
        r->_options.ballot_box->commit_at(
                min_flying_index, rpc_last_log_index,
                r->_options.peer_id);
        g_send_entries_latency << cntl->latency_us();
        if (cntl->request_attachment().size() > 0) {
            g_normalized_send_entries_latency << 
                cntl->latency_us() * 1024 / cntl->request_attachment().size();
        }
    }
    // A rpc is marked as success, means all request before it are success,
    // erase them sequentially.
    while (!r->_append_entries_in_fly.empty() &&
           r->_append_entries_in_fly.front().log_index <= rpc_first_index) {
        r->_flying_append_entries_size -= r->_append_entries_in_fly.front().entries_size;
        r->_append_entries_in_fly.pop_front();
    }
    r->_has_succeeded = true;
    r->_notify_on_caught_up(0, false);
    // dummy_id is unlock in _send_entries
    if (r->_timeout_now_index > 0 && r->_timeout_now_index < r->_min_flying_index()) {
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
            BRAFT_VLOG << "Group " << _options.group_id
                       << " log_index=" << prev_log_index << " was compacted";
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
    request->set_committed_index(_options.ballot_box->last_committed_index());
    return 0;
}

void Replicator::_send_empty_entries(bool is_heartbeat) {
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
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
        _heartbeat_counter++;
        // set RPC timeout for heartbeat, how long should timeout be is waiting to be optimized.
        cntl->set_timeout_ms(*_options.election_timeout_ms / 2);
    } else {
        _st.st = APPENDING_ENTRIES;
        _st.first_log_index = _next_index;
        _st.last_log_index = _next_index - 1;
        CHECK(_append_entries_in_fly.empty());
        CHECK_EQ(_flying_append_entries_size, 0);
        _append_entries_in_fly.push_back(FlyingAppendEntriesRpc(_next_index, 0, cntl->call_id()));
        _append_entries_counter++;
    }

    BRAFT_VLOG << "node " << _options.group_id << ":" << _options.server_id
        << " send HeartbeatRequest to " << _options.peer_id 
        << " term " << _options.term
        << " prev_log_index " << request->prev_log_index()
        << " last_committed_index " << request->committed_index();

    google::protobuf::Closure* done = brpc::NewCallback(
                is_heartbeat ? _on_heartbeat_returned : _on_rpc_returned, 
                _id.value, cntl.get(), request.get(), response.get(),
                butil::monotonic_time_ms());

    RaftService_Stub stub(&_sending_channel);
    stub.append_entries(cntl.release(), request.release(), 
                        response.release(), done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

int Replicator::_prepare_entry(int offset, EntryMeta* em, butil::IOBuf *data) {
    if (data->length() >= (size_t)FLAGS_raft_max_body_size) {
        return ERANGE;
    }
    const int64_t log_index = _next_index + offset;
    LogEntry *entry = _options.log_manager->get_entry(log_index);
    if (entry == NULL) {
        return ENOENT;
    }
    // When leader become readonly, no new user logs can submit. On the other side,
    // if any user log are accepted after this replicator become readonly, the leader
    // still have enough followers to commit logs, we can safely stop waiting new logs
    // until the replicator leave readonly mode.
    if (_readonly_index != 0 && log_index >= _readonly_index) {
        if (entry->type != ENTRY_TYPE_CONFIGURATION) {
            return EREADONLY;
        }
        _readonly_index = log_index + 1;
    }
    em->set_term(entry->id.term);
    em->set_type(entry->type);
    if (entry->peers != NULL) {
        CHECK(!entry->peers->empty()) << "log_index=" << log_index;
        for (size_t i = 0; i < entry->peers->size(); ++i) {
            em->add_peers((*entry->peers)[i].to_string());
        }
        if (entry->old_peers != NULL) {
            for (size_t i = 0; i < entry->old_peers->size(); ++i) {
                em->add_old_peers((*entry->old_peers)[i].to_string());
            }
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
    if (_flying_append_entries_size >= FLAGS_raft_max_entries_size ||
        _append_entries_in_fly.size() >= (size_t)FLAGS_raft_max_parallel_append_entries_rpc_num ||
        _st.st == BLOCKING) {
        BRAFT_VLOG << "node " << _options.group_id << ":" << _options.server_id
            << " skip sending AppendEntriesRequest to " << _options.peer_id
            << ", too many requests in flying, or the replicator is in block,"
            << " next_index " << _next_index << " flying_size " << _flying_append_entries_size;
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        return;
    }

    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
    std::unique_ptr<AppendEntriesRequest> request(new AppendEntriesRequest);
    std::unique_ptr<AppendEntriesResponse> response(new AppendEntriesResponse);
    if (_fill_common_fields(request.get(), _next_index - 1, false) != 0) {
        _reset_next_index();
        return _install_snapshot();
    }
    EntryMeta em;
    const int max_entries_size = FLAGS_raft_max_entries_size - _flying_append_entries_size;
    int prepare_entry_rc = 0;
    CHECK_GT(max_entries_size, 0);
    for (int i = 0; i < max_entries_size; ++i) {
        prepare_entry_rc = _prepare_entry(i, &em, &cntl->request_attachment());
        if (prepare_entry_rc != 0) {
            break;
        }
        request->add_entries()->Swap(&em);
    }
    if (request->entries_size() == 0) {
        // _id is unlock in _wait_more
        if (_next_index < _options.log_manager->first_log_index()) {
            _reset_next_index();
            return _install_snapshot();
        }
        // NOTICE: a follower's readonly mode does not prevent install_snapshot
        // as we need followers to commit conf log(like add_node) when 
        // leader reaches readonly as well 
        if (prepare_entry_rc == EREADONLY) {
            if (_flying_append_entries_size == 0) {
                _st.st = IDLE;
            }
            CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
            return;
        }
        return _wait_more_entries();
    }

    _append_entries_in_fly.push_back(FlyingAppendEntriesRpc(_next_index,
                                     request->entries_size(), cntl->call_id()));
    _append_entries_counter++;
    _next_index += request->entries_size();
    _flying_append_entries_size += request->entries_size();
    
    g_send_entries_batch_counter << request->entries_size();

    BRAFT_VLOG << "node " << _options.group_id << ":" << _options.server_id
        << " send AppendEntriesRequest to " << _options.peer_id << " term " << _options.term
        << " last_committed_index " << request->committed_index()
        << " prev_log_index " << request->prev_log_index()
        << " prev_log_term " << request->prev_log_term()
        << " next_index " << _next_index << " count " << request->entries_size();
    _st.st = APPENDING_ENTRIES;
    _st.first_log_index = _min_flying_index();
    _st.last_log_index = _next_index - 1;
    google::protobuf::Closure* done = brpc::NewCallback(
                _on_rpc_returned, _id.value, cntl.get(), 
                request.get(), response.get(), butil::monotonic_time_ms());
    RaftService_Stub stub(&_sending_channel);
    stub.append_entries(cntl.release(), request.release(), 
                        response.release(), done);
    _wait_more_entries();
}

int Replicator::_continue_sending(void* arg, int error_code) {
    Replicator* r = NULL;
    bthread_id_t id = { (uint64_t)arg };
    if (bthread_id_lock(id, (void**)&r) != 0) {
        return -1;
    }
    if (error_code == ETIMEDOUT) {
        // Replication is in progress when block timedout, no need to start again
        // this case can happen when 
        //     1. pipeline is enabled and 
        //     2. disable readonly mode triggers another replication
        if (r->_wait_id != 0) {
            return 0;
        }
        
        // Send empty entries after block timeout to check the correct
        // _next_index otherwise the replictor is likely waits in
        // _wait_more_entries and no further logs would be replicated even if the
        // last_index of this followers is less than |next_index - 1|
        r->_send_empty_entries(false);
    } else if (error_code != ESTOP && !r->_is_waiter_canceled) {
        // id is unlock in _send_entries
        r->_wait_id = 0;
        r->_send_entries();
    } else if (r->_is_waiter_canceled) {
        // The replicator is checking corrent next index by sending empty entries or 
        // install snapshoting now. Althrough the resigtered waiter will be canceled
        // before the operations, there is still a little chance that LogManger already
        // waked up the waiter, and _continue_sending is waiting to execute.
        BRAFT_VLOG << "Group " << r->_options.group_id
                   << " Replicator=" << id << " canceled waiter";
        bthread_id_unlock(id);
    } else {
        LOG(WARNING) << "Group " << r->_options.group_id
                     << " Replicator=" << id << " stops sending entries";
        bthread_id_unlock(id);
    }
    return 0;
}

void Replicator::_wait_more_entries() {
    if (_wait_id == 0 && FLAGS_raft_max_entries_size > _flying_append_entries_size &&
        (size_t)FLAGS_raft_max_parallel_append_entries_rpc_num > _append_entries_in_fly.size()) {
        _wait_id = _options.log_manager->wait(
                _next_index - 1, _continue_sending, (void*)_id.value);
        _is_waiter_canceled = false;
        BRAFT_VLOG << "node " << _options.group_id << ":" << _options.peer_id
                   << " wait more entries, wait_id " << _wait_id;
    }
    if (_flying_append_entries_size == 0) {
        _st.st = IDLE;
    }
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

void Replicator::_install_snapshot() {
    if (_reader) {
        // follower's readonly mode change may cause two install_snapshot
        // one possible case is: 
        //     enable -> install_snapshot -> disable -> wait_more_entries ->
        //     install_snapshot again
        LOG(WARNING) << "node " << _options.group_id << ":" << _options.server_id
                     << " refuse to send InstallSnapshotRequest to " << _options.peer_id
                     << " because there is an running one";
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        return;
    }

    if (_options.snapshot_throttle && !_options.snapshot_throttle->
                                            add_one_more_task(true)) {
        return _block(butil::gettimeofday_us(), EBUSY);
    }
    
    // pre-set replictor state to INSTALLING_SNAPSHOT, so replicator could be
    // blocked if something is wrong, such as throttled for a period of time 
    _st.st = INSTALLING_SNAPSHOT;

    _reader = _options.snapshot_storage->open();
    if (!_reader) {
        if (_options.snapshot_throttle) {
            _options.snapshot_throttle->finish_one_task(true);
        }
        NodeImpl *node_impl = _options.node;
        node_impl->AddRef();
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        braft::Error e;
        e.set_type(ERROR_TYPE_SNAPSHOT);
        e.status().set_error(EIO, "Fail to open snapshot");
        node_impl->on_error(e);
        node_impl->Release();
        return;
    } 
    std::string uri = _reader->generate_uri_for_copy();
    // NOTICE: If uri is something wrong, retry later instead of reporting error
    // immediately(making raft Node error), as FileSystemAdaptor layer of _reader is 
    // user defined and may need some control logic when opened
    if (uri.empty()) {
        LOG(WARNING) << "node " << _options.group_id << ":" << _options.server_id
                     << " refuse to send InstallSnapshotRequest to " << _options.peer_id
                     << " because snapshot uri is empty";
        _close_reader();
        return _block(butil::gettimeofday_us(), EBUSY); 
    }
    SnapshotMeta meta;
    // report error on failure
    if (_reader->load_meta(&meta) != 0) {
        std::string snapshot_path = _reader->get_path();
        NodeImpl *node_impl = _options.node;
        node_impl->AddRef();
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        braft::Error e;
        e.set_type(ERROR_TYPE_SNAPSHOT);
        e.status().set_error(EIO, "Fail to load meta from " + snapshot_path);
        node_impl->on_error(e);
        node_impl->Release();
        return;
    } 
    brpc::Controller* cntl = new brpc::Controller;
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

    _install_snapshot_in_fly = cntl->call_id();
    _install_snapshot_counter++;
    _st.last_log_included = meta.last_included_index();
    _st.last_term_included = meta.last_included_term();
    google::protobuf::Closure* done = brpc::NewCallback<
                ReplicatorId, brpc::Controller*,
                InstallSnapshotRequest*, InstallSnapshotResponse*>(
                    _on_install_snapshot_returned, _id.value,
                    cntl, request, response);
    RaftService_Stub stub(&_sending_channel);
    stub.install_snapshot(cntl, request, response, done);
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
}

void Replicator::_on_install_snapshot_returned(
            ReplicatorId id, brpc::Controller* cntl,
            InstallSnapshotRequest* request, 
            InstallSnapshotResponse* response) {
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
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
        if (r->_options.snapshot_throttle) {
            r->_options.snapshot_throttle->finish_one_task(true);
        }
    }
    std::stringstream ss;
    ss << "received InstallSnapshotResponse from "
       << r->_options.group_id << ":" << r->_options.peer_id
       << " last_included_index " << request->meta().last_included_index()
       << " last_included_term " << request->meta().last_included_term();
    do {
        if (cntl->Failed()) {
            ss << " error: " << cntl->ErrorText();
            LOG(INFO) << ss.str();

            LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0) 
                            << "Group " << r->_options.group_id
                            << " Fail to install snapshot at peer=" 
                            << r->_options.peer_id
                            <<", " << cntl->ErrorText();
            succ = false;
            break;
        }
        if (!response->success()) {
            succ = false;
            ss << " fail.";
            LOG(INFO) << ss.str();
            // Let heartbeat do step down
            break;
        }
        // Success 
        r->_next_index = request->meta().last_included_index() + 1;
        ss << " success.";
        LOG(INFO) << ss.str();
    } while (0);

    // We don't retry installing the snapshot explicitly. 
    // dummy_id is unlock in _send_entries
    if (!succ) {
        return r->_block(butil::gettimeofday_us(), cntl->ErrorCode());
    }
    r->_has_succeeded = true;
    r->_notify_on_caught_up(0, false);
    if (r->_timeout_now_index > 0 && r->_timeout_now_index < r->_min_flying_index()) {
        r->_send_timeout_now(false, false);
    }
    // dummy_id is unlock in _send_entries
    return r->_send_entries();
}

void Replicator::_notify_on_caught_up(int error_code, bool before_destroy) {
    if (_catchup_closure == NULL) {
        return;
    }
    if (error_code != ETIMEDOUT && error_code != EPERM) {
        if (!_is_catchup(_catchup_closure->_max_margin)) {
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
            if (!before_destroy && bthread_timer_del(_catchup_closure->_timer) == 1) {
                // There's running timer task, let timer task trigger
                // on_caught_up to void ABA problem
                return;
            }
        }
    } else { // Timed out or leader step_down
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
    const timespec due_time = butil::milliseconds_from(
            butil::microseconds_to_timespec(start_time_us), 
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
        brpc::StartCancel(r->_install_snapshot_in_fly);
        brpc::StartCancel(r->_heartbeat_in_fly);
        brpc::StartCancel(r->_timeout_now_in_fly);
        r->_cancel_append_entries_rpcs();
        bthread_timer_del(r->_heartbeat_timer);
        r->_options.log_manager->remove_waiter(r->_wait_id);
        r->_notify_on_caught_up(error_code, true);
        r->_wait_id = 0;
        LOG(INFO) << "Group " << r->_options.group_id
                  << " Replicator=" << id << " is going to quit";
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
        CHECK(false) << "Group " << r->_options.group_id 
                     << " Unknown error_code=" << error_code;
        CHECK_EQ(0, bthread_id_unlock(id)) << "Fail to unlock " << id;
        return -1;
    }
}

void Replicator::_on_catch_up_timedout(void* arg) {
    bthread_id_t id = { (uint64_t)arg };
    Replicator* r = NULL;
    if (bthread_id_lock(id, (void**)&r) != 0) {
        LOG(WARNING) << "Replicator is destroyed when catch_up_timedout.";
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
    if (_has_succeeded && _min_flying_index() > log_index) {
        // _id is unlock in _send_timeout_now
        _send_timeout_now(true, false);
        return 0;
    }
    // Register log_index so that _on_rpc_returned trigger
    // _send_timeout_now if _min_flying_index reaches log_index
    _timeout_now_index = log_index;
    CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
    return 0;
}

void Replicator::_cancel_append_entries_rpcs() {
    for (std::deque<FlyingAppendEntriesRpc>::iterator rpc_it =
        _append_entries_in_fly.begin();
        rpc_it != _append_entries_in_fly.end(); ++rpc_it) {
        brpc::StartCancel(rpc_it->call_id);
    }
    _append_entries_in_fly.clear();
}

void Replicator::_reset_next_index() {
    _next_index -= _flying_append_entries_size;
    _flying_append_entries_size = 0;
    _cancel_append_entries_rpcs();
    _is_waiter_canceled = true;
    if (_wait_id != 0) {
        _options.log_manager->remove_waiter(_wait_id);
        _wait_id = 0;
    }
}

void Replicator::_send_timeout_now(bool unlock_id, bool stop_after_finish,
                                   int timeout_ms) {
    TimeoutNowRequest* request = new TimeoutNowRequest;
    TimeoutNowResponse* response = new TimeoutNowResponse;
    request->set_term(_options.term);
    request->set_group_id(_options.group_id);
    request->set_server_id(_options.server_id.to_string());
    request->set_peer_id(_options.peer_id.to_string());
    brpc::Controller* cntl = new brpc::Controller;
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
    ::google::protobuf::Closure* done = brpc::NewCallback(
            _on_timeout_now_returned, _id.value, cntl, request, response,
            stop_after_finish);
    stub.timeout_now(cntl, request, response, done);
    if (unlock_id) {
        CHECK_EQ(0, bthread_id_unlock(_id));
    }
}

void Replicator::_on_timeout_now_returned(
                ReplicatorId id, brpc::Controller* cntl,
                TimeoutNowRequest* request, 
                TimeoutNowResponse* response,
                bool stop_after_finish) {
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<TimeoutNowRequest>  req_guard(request);
    std::unique_ptr<TimeoutNowResponse> res_guard(response);
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }

    std::stringstream ss;
    ss << "node " << r->_options.group_id << ":" << r->_options.server_id 
       << " received TimeoutNowResponse from "
       << r->_options.peer_id;

    if (cntl->Failed()) {
        ss << " fail : " << cntl->ErrorText();
        BRAFT_VLOG << ss.str();

        if (stop_after_finish) {
            r->_notify_on_caught_up(ESTOP, true);
            r->_destroy();
        } else {
            CHECK_EQ(0, bthread_id_unlock(dummy_id));
        }
        return;
    }
    ss << (response->success() ? " success " : "fail:");
    BRAFT_VLOG << ss.str();

    if (response->term() > r->_options.term) {
        NodeImpl *node_impl = r->_options.node;
        // Acquire a reference of Node here in case that Node is detroyed
        // after _notify_on_caught_up.
        node_impl->AddRef();
        r->_notify_on_caught_up(EPERM, true);
        butil::Status status;
        status.set_error(EHIGHERTERMRESPONSE, "Leader receives higher term "
                "timeout_now_response from peer:%s", r->_options.peer_id.to_string().c_str());
        r->_destroy();
        node_impl->increase_term_to(response->term(), status);
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
        next_index = r->_next_index - r->_flying_append_entries_size;
    }
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
    return next_index;
}

int Replicator::change_readonly_config(ReplicatorId id, bool readonly) {
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return 0;
    }
    return r->_change_readonly_config(readonly);
}

int Replicator::_change_readonly_config(bool readonly) {
    if ((readonly && _readonly_index != 0) ||
        (!readonly && _readonly_index == 0)) {
        // Check if readonly already set
        BRAFT_VLOG << "node " << _options.group_id << ":" << _options.server_id
                   << " ignore change readonly config of " << _options.peer_id
                   << " to " << readonly << ", readonly_index " << _readonly_index;
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
        return 0;
    }
    if (readonly) {
        // Keep a readonly index here to make sure the pending logs can be committed.
        _readonly_index = _options.log_manager->last_log_index() + 1;
        LOG(INFO) << "node " << _options.group_id << ":" << _options.server_id
                  << " enable readonly for " << _options.peer_id
                  << ", readonly_index " << _readonly_index;
        CHECK_EQ(0, bthread_id_unlock(_id)) << "Fail to unlock " << _id;
    } else {
        _readonly_index = 0;
        LOG(INFO) << "node " << _options.group_id << ":" << _options.server_id
                  << " disable readonly for " << _options.peer_id;
        _wait_more_entries();
    }
    return 0;
}

bool Replicator::readonly(ReplicatorId id) {
    Replicator *r = NULL;
    bthread_id_t dummy_id = { id };
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return 0;
    }
    bool readonly = (r->_readonly_index != 0);
    CHECK_EQ(0, bthread_id_unlock(dummy_id)) << "Fail to unlock " << dummy_id;
    return readonly;
}

void Replicator::_destroy() {
    bthread_id_t saved_id = _id;
    CHECK_EQ(0, bthread_id_unlock_and_destroy(saved_id));
    // TODO: Add more information
    LOG(INFO) << "Replicator=" << saved_id << " is going to quit";
    delete this;
}

void Replicator::_describe(std::ostream& os, bool use_html) {
    const Stat st = _st;
    const PeerId peer_id = _options.peer_id;
    const int64_t next_index = _next_index;
    const int flying_append_entries_size = _flying_append_entries_size;
    const bthread_id_t id = _id;
    const int consecutive_error_times = _consecutive_error_times;
    const int64_t heartbeat_counter = _heartbeat_counter;
    const int64_t append_entries_counter = _append_entries_counter;
    const int64_t install_snapshot_counter = _install_snapshot_counter;
    const int64_t readonly_index = _readonly_index;
    CHECK_EQ(0, bthread_id_unlock(_id));
    // Don't touch *this ever after
    const char* new_line = use_html ? "<br>" : "\r\n";
    os << "replicator_" << id << '@' << peer_id << ':';
    os << " next_index=" << next_index << ' ';
    os << " flying_append_entries_size=" << flying_append_entries_size << ' ';
    if (readonly_index != 0) {
        os << " readonly_index=" << readonly_index << ' ';
    }
    switch (st.st) {
    case IDLE:
        os << "idle";
        break;
    case BLOCKING:
        os << "blocking consecutive_error_times=" << consecutive_error_times;
        break;
    case APPENDING_ENTRIES:
        os << "appending [" << st.first_log_index << ", " << st.last_log_index << ']';
        break;
    case INSTALLING_SNAPSHOT:
        os << "installing snapshot {" << st.last_log_included
           << ", " << st.last_term_included  << '}';
        break;
    }
    os << " hc=" << heartbeat_counter << " ac=" << append_entries_counter << " ic=" << install_snapshot_counter << new_line;
}

void Replicator::_get_status(PeerStatus* status) {
    status->valid = true;
    status->installing_snapshot = (_st.st == INSTALLING_SNAPSHOT);
    status->next_index = _next_index;
    status->flying_append_entries_size = _flying_append_entries_size;
    status->last_rpc_send_timestamp = _last_rpc_send_timestamp;
    status->consecutive_error_times = _consecutive_error_times;
    status->readonly_index = _readonly_index;
    CHECK_EQ(0, bthread_id_unlock(_id));
}

void Replicator::describe(ReplicatorId id, std::ostream& os, bool use_html) {
    bthread_id_t dummy_id = { id };
    Replicator* r = NULL;
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }
    // dummy_id is unlock in _describe
    return r->_describe(os, use_html);
}

void Replicator::get_status(ReplicatorId id, PeerStatus* status) {
    if (!status) {
        return;
    }
    bthread_id_t dummy_id = { id };
    Replicator* r = NULL;
    if (bthread_id_lock(dummy_id, (void**)&r) != 0) {
        return;
    }
    return r->_get_status(status);
}

void Replicator::_close_reader() {
    if (_reader) {
        _options.snapshot_storage->close(_reader);
        _reader = NULL;
        if (_options.snapshot_throttle) {
            _options.snapshot_throttle->finish_one_task(true);
        }
    }
}

// ==================== ReplicatorGroup ==========================

ReplicatorGroupOptions::ReplicatorGroupOptions()
    : heartbeat_timeout_ms(-1)
    , election_timeout_ms(-1)
    , log_manager(NULL)
    , ballot_box(NULL)
    , node(NULL)
    , snapshot_storage(NULL)
{}

ReplicatorGroup::ReplicatorGroup() 
    : _dynamic_timeout_ms(-1)
    , _election_timeout_ms(-1)
{
    _common_options.dynamic_heartbeat_timeout_ms = &_dynamic_timeout_ms;
    _common_options.election_timeout_ms = &_election_timeout_ms;
}

ReplicatorGroup::~ReplicatorGroup() {
    stop_all();
}

int ReplicatorGroup::init(const NodeId& node_id, const ReplicatorGroupOptions& options) {
    _dynamic_timeout_ms = options.heartbeat_timeout_ms;
    _election_timeout_ms = options.election_timeout_ms;
    _common_options.log_manager = options.log_manager;
    _common_options.ballot_box = options.ballot_box;
    _common_options.node = options.node;
    _common_options.term = 0;
    _common_options.group_id = node_id.group_id;
    _common_options.server_id = node_id.peer_id;
    _common_options.snapshot_storage = options.snapshot_storage;
    _common_options.snapshot_throttle = options.snapshot_throttle;
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
        LOG(ERROR) << "Group " << options.group_id
                   << " Fail to start replicator to peer=" << peer;
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

int64_t ReplicatorGroup::last_rpc_send_timestamp(const PeerId& peer) {
    std::map<PeerId, ReplicatorId>::iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return 0;
    }
    ReplicatorId rid = iter->second;
    return Replicator::last_rpc_send_timestamp(rid);
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

int ReplicatorGroup::reset_election_timeout_interval(int new_interval_ms) {
    _election_timeout_ms = new_interval_ms;
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
                ReplicatorId* candidate, const ConfigurationEntry& conf) {
    *candidate = INVALID_BTHREAD_ID.value;
    PeerId candidate_id;
    const int rc = find_the_next_candidate(&candidate_id, conf);
    if (rc == 0) {
        LOG(INFO) << "Group " << _common_options.group_id
                  << " Found " << candidate_id << " as the next candidate";
        *candidate = _rmap[candidate_id];
    } else {
        LOG(INFO) << "Group " << _common_options.group_id
                  << " Fail to find the next candidate";
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

int ReplicatorGroup::find_the_next_candidate(
        PeerId* peer_id, const ConfigurationEntry& conf) {
    int64_t max_index =  0;
    for (std::map<PeerId, ReplicatorId>::const_iterator
            iter = _rmap.begin();  iter != _rmap.end(); ++iter) {
        if (!conf.contains(iter->first)) {
            continue;
        }
        const int64_t next_index = Replicator::get_next_index(iter->second);
        if (next_index > max_index) {
            max_index = next_index;
            if (peer_id) {
                *peer_id = iter->first;
            }
        }
    }
    if (max_index == 0) {
        return -1;
    }
    return 0;
}

void ReplicatorGroup::list_replicators(std::vector<ReplicatorId>* out) const {
    out->clear();
    out->reserve(_rmap.size());
    for (std::map<PeerId, ReplicatorId>::const_iterator
            iter = _rmap.begin();  iter != _rmap.end(); ++iter) {
        out->push_back(iter->second);
    }
}

void ReplicatorGroup::list_replicators(
        std::vector<std::pair<PeerId, ReplicatorId> >* out) const {
    out->clear();
    out->reserve(_rmap.size());
    for (std::map<PeerId, ReplicatorId>::const_iterator
            iter = _rmap.begin();  iter != _rmap.end(); ++iter) {
        out->push_back(*iter);
    }
}
 
int ReplicatorGroup::change_readonly_config(const PeerId& peer, bool readonly) {
    std::map<PeerId, ReplicatorId>::const_iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    return Replicator::change_readonly_config(rid, readonly);
}

bool ReplicatorGroup::readonly(const PeerId& peer) const {
    std::map<PeerId, ReplicatorId>::const_iterator iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return false;
    }
    ReplicatorId rid = iter->second;
    return Replicator::readonly(rid);
}

} //  namespace braft
