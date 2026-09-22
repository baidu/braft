// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
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

// Authors: zhangkuo(zhangkuo@baidu.com)
//          Kai FAN(fankai@baidu.com)
//          zhangxingji(zhangxingji@baidu.com)

#include <vector>

#include "braft/learner.h"
#include "braft/raft.pb.h"
#include "braft/util.h"
#include "braft/node_manager.h"

namespace braft {

DEFINE_int32(raft_learner_req_timeout_ms, 500, "Timeout of learner rpc requests");
DEFINE_bool(raft_learner_fetch_log_if_necessary, false, 
        "fetch log if peer's first_log_index < _last_log_index");

// Timers
int LearnerTimer::init(LearnerImpl* learner, int timeout_ms) {
    BRAFT_RETURN_IF(RepeatedTimerTask::init(timeout_ms) != 0, -1);
    _learner = learner;
    _learner->AddRef();
    return 0;
}

void LearnerTimer::on_destroy() {
    if (_learner) {
        LearnerImpl* saved = _learner;
        _learner = NULL;
        saved->Release();
    }
}

void FetchLogTimer::run() {
    _learner->fetch_log();
}

void LearnerSnapshotTimer::run() {
    _learner->snapshot(NULL);
}

int LearnerSnapshotTimer::adjust_timeout_ms(int timeout_ms) {
    if (!_first_schedule) {
        return timeout_ms;
    }
    if (timeout_ms > 0) {
        timeout_ms = butil::fast_rand_less_than(timeout_ms) + 1;
    }
    _first_schedule = false;
    return timeout_ms;
}

LearnerStableClosure::LearnerStableClosure(LearnerImpl* learner, const size_t nentries) 
    : _learner(learner), _nentries(nentries) {
    _learner->AddRef();
}

void LearnerStableClosure::Run() {
    if (status().ok()) {
        CHECK(_learner);
        _learner->commit_at(_first_log_index, _first_log_index + _nentries - 1);
    } else {
        LOG(ERROR) << "node " << _learner->node_id() << " append [" << _first_log_index << ", "
                   << _first_log_index + _nentries - 1 << "] failed";
    }
    delete this;
}

LearnerStableClosure::~LearnerStableClosure() {
    if (_learner) {
        _learner->Release();
        _learner = NULL;
    }
}

class ExploreMapper : public brpc::CallMapper {
public:
    ExploreMapper(const NodeId& node_id, const std::vector<PeerId>& peer_ids)
        : _node_id(node_id)
        , _peer_ids(peer_ids) {}

    brpc::SubCall Map(int channel_index,
                            const google::protobuf::MethodDescriptor* method,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response) {
        ExploreRequest* copied_req = brpc::Clone<ExploreRequest>(request);
        copied_req->set_peer_id(_peer_ids[channel_index].to_string());
        BRAFT_VLOG << "node " << _node_id << " send explore request to " 
                  << _peer_ids[channel_index];
        return brpc::SubCall(method, copied_req, response->New(), 
                                   brpc::DELETE_REQUEST | 
                                   brpc::DELETE_RESPONSE);
    }
private:
    NodeId _node_id;
    std::vector<PeerId> _peer_ids;
};

class ExploreMerger : public brpc::ResponseMerger {   
public:
    ExploreMerger(const NodeId& node_id, int64_t last_log_index, bool skip_snapshot)
        : _node_id(node_id)
        , _last_log_index(last_log_index)
        , _skip_snapshot(skip_snapshot) {}

    Result Merge(ExploreResponse* response, const ExploreResponse* sub_response) {
        if (!sub_response->success()) {
            LOG(WARNING) << "Fail to handle ExploreResponse from " << sub_response->server_id()
                         << ", leader_id: " << sub_response->leader_id()
                         << ", node_id:" << _node_id;
            return brpc::ResponseMerger::FAIL;
        }
        BRAFT_VLOG << "receive explore response from " << sub_response->server_id()
                  << ", leader_id:" << sub_response->leader_id()
                  << ", last_committed_index:" << sub_response->last_committed_index()
                  << ", last_snapshot_index:" << sub_response->last_snapshot_index()
                  << ", node_id:" << _node_id;

        if (sub_response->last_committed_index() <= _last_log_index) {
            LOG(INFO) << "Skip handle ExploreResponse from " << sub_response->server_id()
                       << ", last_committed_index " << sub_response->last_committed_index()
                       << " is less than " << _last_log_index << ", node_id:" << _node_id;
            return brpc::ResponseMerger::MERGED;
        }
        if (_skip_snapshot && sub_response->last_snapshot_index() > 0) {
            LOG(INFO) << "Skip handle ExploreResponse from " << sub_response->server_id()
                       << ", snapshot not supported, node_id:" << _node_id;
            return brpc::ResponseMerger::MERGED;
        }
        if (PeerId(sub_response->leader_id()).is_empty()) {
            LOG(INFO) << "Skip handle ExploreResponse from " << sub_response->server_id()
                       << ", leader_id empty, node_id:" << _node_id;
            return brpc::ResponseMerger::MERGED;
        }
        if (response->has_leader_id() && sub_response->leader_id() != response->leader_id()) {
            LOG(INFO) << "Fail to handle ExploreResponse from " << sub_response->server_id()
                      << ", leader_id is changed from " << response->leader_id()
                      << " to " << sub_response->leader_id() << ", node_id:" << _node_id;
            response->set_success(false);
            return brpc::ResponseMerger::FAIL;
        }

        // node owns latest snapshot is prefer, then owns latest committed index 
        // is less preferred
        do {
            if (!response->success()) {
                break;
            }

            CHECK(response->has_last_committed_index());
            CHECK(response->has_last_snapshot_index());
            CHECK(response->has_leader_id());
            CHECK(response->has_server_id());

            if (sub_response->last_snapshot_index() > response->last_snapshot_index()) {
                break;
            } else if (sub_response->last_snapshot_index() == response->last_snapshot_index() &&
                    sub_response->server_id() != sub_response->leader_id()) {
                if (response->server_id() == sub_response->leader_id()) {
                    break;
                }
                if (sub_response->last_committed_index() > response->last_committed_index()) {
                    break;
                }
            }
            return brpc::ResponseMerger::MERGED;
        } while (false);

        response->set_success(true);
        response->set_last_snapshot_index(sub_response->last_snapshot_index());
        response->set_last_committed_index(sub_response->last_committed_index());
        response->set_first_log_index(sub_response->first_log_index());
        response->set_server_id(sub_response->server_id());
        response->set_leader_id(sub_response->leader_id());
        return brpc::ResponseMerger::MERGED;
    }

    Result Merge(google::protobuf::Message* response, const google::protobuf::Message* sub_response) {
        return Merge(static_cast<ExploreResponse*>(response), 
                static_cast<const ExploreResponse*>(sub_response));
    }

protected:
    virtual ~ExploreMerger() {}

private:
    NodeId _node_id;
    int64_t _last_log_index;
    bool _skip_snapshot;
};

LearnerImpl::LearnerImpl(const GroupId& group_id, const PeerId& server_id)
        : _group_id(group_id)
        , _server_id(server_id)
        , _config_manager(NULL)
        , _log_storage(NULL)
        , _log_manager(NULL)
        , _snapshot_executor(NULL)
        , _fsm_caller(NULL)
        , _closure_queue(false)
        , _state(STATE_UNINITIALIZED)
        , _last_committed_index(0)
        , _last_log_index(0)
        , _read_barrier(0)
        , _fetching_snapshot_index(0)
        , _learn_timer(bthread_timer_t()) {
    AddRef();
}

LearnerImpl::~LearnerImpl() {
    if (_config_manager) {
        delete _config_manager;
        _config_manager = NULL;
    }
    if (_log_manager) {
        delete _log_manager;
        _log_manager = NULL;
    }
    if (_fsm_caller) {
        delete _fsm_caller;
        _fsm_caller = NULL;
    }
    if (_options.node_owns_log_storage) {
        if (_log_storage) {
            delete _log_storage;
            _log_storage = NULL;
        }
    }
    if (_snapshot_executor) {
        delete _snapshot_executor;
        _snapshot_executor = NULL;
    }
    if (_options.node_owns_fsm) {
        _options.node_owns_fsm = false;
        delete _options.fsm;
        _options.fsm = NULL;
    }
}

int LearnerImpl::init(const NodeOptions& options) {
    _options = options;

    _config_manager = new ConfigurationManager();
    // Create _fsm_caller first as log_manager needs it to report error
    _fsm_caller = new FSMCaller();

    if (init_log_storage() != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " init_log_storage failed";
        return -1;
    }

    if (init_fsm_caller() != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " init_fsm_caller failed";
        return -1;
    }

    // snapshot storage init and load
    // NOTE: snapshot maybe discard entries when snapshot saved but not discard entries.
    //      init log storage before snapshot storage, snapshot storage will update configration
    if (init_snapshot_storage() != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " init_snapshot_storage failed";
        return -1;
    }

    if (options.learner_auto_load_applied_logs || _options.snapshot_uri.empty()) {
        butil::Status st = _log_manager->check_consistency();
        if (!st.ok()) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " is initialized with inconsitency log: "
                       << st;
            return -1;
        }
    } else if (_snapshot_executor != NULL) {
        butil::Status st = _snapshot_executor->check_snapshot_consistency();
        if (!st.ok()) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " is initialized with inconsitency log: "
                       << st;
            return -1;
        }
    } else {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " _snapshot_executor is NULL";
        return -1;

    }

    // if confs in log and options are different, 
    // using conf in options instead of conf in log  
    if (_log_manager->last_log_index() > 0) {
        _log_manager->check_and_set_configuration(&_conf);
    }
    if (!_options.initial_conf.empty() && !_conf.conf.equals(_options.initial_conf)) {
        _conf.id = LogId();
        _conf.conf = _options.initial_conf;
    }

    if (_conf.conf.empty()) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " init failed with empty conf";
        return -1;
    }

    _last_log_index = _log_manager->last_log_index();
    commit_at(_last_committed_index + 1, _last_log_index);

    _state = STATE_LEARNER;

    LOG(INFO) << "node " << _group_id << ":" << _server_id << " init,"
        << " last_log_id: " << _log_manager->last_log_id()
        << " conf: " << _conf.conf
        << " old_conf: " << _conf.old_conf;

    CHECK_EQ(0, _fetch_log_timer.init(this, options.fetch_log_interval_ms));
    CHECK_EQ(0, _snapshot_timer.init(this, options.snapshot_interval_s * 1000));
    if (_snapshot_executor && _options.snapshot_interval_s > 0) {
        BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                  << " start snapshot_timer";
        _snapshot_timer.start();
    }

    // add node to NodeManager
    if (!NodeManager::GetInstance()->add(this)) {
        LOG(ERROR) << "NodeManager add " << _group_id 
                   << ":" << _server_id << " failed";
        return -1;
    }

    AddRef();
    on_learn(this);
    return 0;
}

void LearnerImpl::join() {
    if (_fsm_caller) {
        _fsm_caller->join();
    }
    if (_snapshot_executor) {
        _snapshot_executor->join();
    }
}

void LearnerImpl::shutdown(Closure* done) {
    do {
        LOG(INFO) << "node " << _group_id << ":" << _server_id << " shutdown";

        BAIDU_SCOPED_LOCK(_mutex);
        if (_state < STATE_SHUTTING) {  // Got the right to shut
            // Remove node from NodeManager and |this| would not be accessed by
            // the coming RPCs
            NodeManager::GetInstance()->remove(this);
            _state = STATE_SHUTTING;

            unsafe_stop_learn_timer();
            _fetch_log_timer.destroy();
            _snapshot_timer.destroy();

            if (_log_manager) {
                _log_manager->shutdown();
            }
            if (_snapshot_executor) {
                _snapshot_executor->shutdown();
            }
            if (_fsm_caller) {
                _fsm_caller->shutdown();
            }
        }

        if (_state != STATE_SHUTDOWN) {
            // This node is shutting, push done into the _shutdown_continuations
            // and after_shutdown would invoked this callbacks.
            if (done) {
                _shutdown_continuations.push_back(done);
            }
            return;
        }
    } while (0);

    // This node is down, it's ok to invoke done right now. Don't inovke this
    // inplace to avoid the dead lock issue when done->Run() is going to acquire
    // a mutex which is already held by the caller
    if (done) {
        run_closure_in_bthread(done);
    }
}

void LearnerImpl::after_shutdown(LearnerImpl* node) {
    return node->after_shutdown();
}

void LearnerImpl::after_shutdown() {
    std::vector<Closure*> saved_done;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        CHECK_EQ(STATE_SHUTTING, _state);
        _state = STATE_SHUTDOWN;
        std::swap(saved_done, _shutdown_continuations);
    }
    Release();
    for (size_t i = 0; i < saved_done.size(); ++i) {
        if (NULL == saved_done[i]) {
            continue;
        }
        run_closure_in_bthread(saved_done[i]);
    }
}

int LearnerImpl::tail_snapshot(const PeerId& peer, const int64_t last_snapshot_index,
                               const int64_t peer_first_log_index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_active_state(_state)) {
        return 0;
    }
    if (_snapshot_executor == nullptr) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id 
                     << " fail to tail snapshot, snapshot not supported";
        return -1;
    }

    if (last_snapshot_index <= _last_log_index) {
        LOG(INFO) << "node " << _group_id << ":" << _server_id 
                  << " skip tail snapshot, last_log_index is higher than snapshot_index,"
                  << " last log index:" << _last_log_index 
                  << ", snapshot index:" << last_snapshot_index;
        return 0;
    }
    if (FLAGS_raft_learner_fetch_log_if_necessary 
            && peer_first_log_index != 0 
            && peer_first_log_index <= _last_log_index + 1) {
        LOG(INFO) << "node " << _group_id << ":" << _server_id 
                  << " skip tail snapshot, last_log_index is higher than peer_first_log_index,"
                  << " last log index:" << _last_log_index 
                  << ", peer first log index:" << peer_first_log_index;
        return 0;
    }
    if (_read_barrier != 0 && last_snapshot_index > _read_barrier) {
        LOG(INFO) << "node " << _group_id << ":" << _server_id 
                  << " fail to tail snapshot " << last_snapshot_index 
                  << ", encounter read barrier " << _read_barrier;
        return -1;
    }
    _fetching_snapshot_index = last_snapshot_index;
    lck.unlock();

    if (_snapshot_executor->fetch_snapshot(_group_id, peer, last_snapshot_index) != 0) {
        return -1;
    }

    lck.lock();
    _fetching_snapshot_index = 0;
    int64_t local_snapshot_index = _snapshot_executor->last_snapshot_index();
    if (_last_log_index < local_snapshot_index) {
        _last_log_index = local_snapshot_index;
    }
    if (_last_committed_index < local_snapshot_index) {
        _last_committed_index = local_snapshot_index;
    }
    if (_log_manager->check_and_set_configuration(&_conf)) {
        lck.unlock();
        LOG(INFO) << "node " << _group_id << ":" << _server_id 
                  << " configuration changed after fetch snapshot, log_id:" << _conf.id
                  << ", conf: " << _conf.conf << ", old_conf: " << _conf.old_conf;
        FetchLogClosure* done = new FetchLogClosure(this);
        run_closure_in_bthread(done);
    }
    return 0;
}

void LearnerImpl::on_learn(void* arg) {
    LearnerImpl* learner = (LearnerImpl*)arg;
    learner->stop_learn_timer();

    bthread_t tid;
    if (bthread_start_background(&tid, NULL, learn, arg) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        learn(arg);
    }
}

void* LearnerImpl::learn(void* arg) {
    LearnerImpl* learner = (LearnerImpl*)arg;

    PeerId peer;
    std::string errmsg;
    int64_t last_committed_index = 0;
    int64_t last_snapshot_index = 0;
    int64_t peer_first_log_index = 0;
    do {
        if (learner->explore_learning_info(&peer, &last_committed_index,
                &last_snapshot_index, &peer_first_log_index) != 0) {
            errmsg = "Fail to explore_learning_info";
            break;
        }
        if (learner->tail_snapshot(peer, last_snapshot_index, peer_first_log_index) != 0) {
            errmsg = "Fail to tail_snapshot";
            break;
        }
        if (learner->init_log_fetcher(peer) != 0) {
            errmsg = "Fail to init log fetcher";
            break;
        }

        learner->Release();
        return nullptr;
    } while (false);

    if (learner->start_learn_timer() != 0) {
        learner->step_down(butil::Status(-1, errmsg));
    }

    learner->Release();
    return nullptr;
}

void LearnerImpl::step_down(const butil::Status& status) {
    braft::Error e;
    e.set_type(ERROR_TYPE_LEARNER);
    e.status() = status;
    LOG(WARNING) << "node " << _group_id << ":" << _server_id
                 << " got error=" << e;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_state < STATE_ERROR) {
        _state = STATE_ERROR;
    }
    lck.unlock();
    if (_fsm_caller) {
        // on_error of _fsm_caller is guaranteed to be executed once.
        _fsm_caller->on_error(e);
    }
}

int LearnerImpl::init_log_fetcher(const PeerId& peer) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_active_state(_state)) {
        return 0;
    }

    _log_fetcher = new LogFetcher(_group_id, peer, this, _last_log_index, 
                                  _options.fetch_log_timeout_s);
    if (_log_fetcher->init() != 0) {
        _log_fetcher = NULL;
        return -1;
    }
    _fetch_log_timer.start();
    return 0;
}

scoped_refptr<LogFetcher> LearnerImpl::get_log_fetcher() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    return _log_fetcher;
}

void LearnerImpl::fetch_log() {
    scoped_refptr<LogFetcher> log_fetcher = get_log_fetcher();
    if (log_fetcher.get() == NULL) {
        return;
    }
    return log_fetcher->fetch_log();
}

void LearnerImpl::on_fetch_log_error() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_active_state(_state)) {
        return;
    }
    if (_learn_timer != bthread_timer_t()) {
        return;
    }
    _fetch_log_timer.stop();
    _log_fetcher = NULL;
    lck.unlock();

    int rc = start_learn_timer();
    if (rc != 0) {
        step_down(butil::Status(rc, "Fail to fetch log"));
    }
}

int LearnerImpl::start_learn_timer() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_active_state(_state)) {
        return 0;
    }
    if (_learn_timer != bthread_timer_t()) {
        return 0;
    }

    AddRef();
    if (bthread_timer_add(&_learn_timer,
                          butil::seconds_from_now(_options.fetch_log_timeout_s),
                          LearnerImpl::on_learn,
                          this) != 0) {
        _learn_timer = bthread_timer_t();
        LOG(FATAL) << "node " << _group_id << ":" << _server_id << " to add learner timer";
        Release();
        return -1;
    }
    return 0;
}

void LearnerImpl::stop_learn_timer() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    unsafe_stop_learn_timer();
}

void LearnerImpl::unsafe_stop_learn_timer() {
    if (_learn_timer == bthread_timer_t()) {
        return;
    }
    if (bthread_timer_del(_learn_timer) == 0) {
        Release();
    }
    _learn_timer = bthread_timer_t();
}

int LearnerImpl::init_log_storage() {
    CHECK(_fsm_caller);
    if (_options.log_storage) {
        _log_storage = _options.log_storage;
    } else {
        _log_storage = LogStorage::create(_options.log_uri);
    }
    if (!_log_storage) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " find log storage failed, uri " << _options.log_uri;
        return -1;
    }
    _log_manager = new LogManager();
    LogManagerOptions log_manager_options;
    log_manager_options.log_storage = _log_storage;
    log_manager_options.configuration_manager = _config_manager;
    log_manager_options.fsm_caller = _fsm_caller;
    return _log_manager->init(log_manager_options);
}

int LearnerImpl::init_snapshot_storage() {
    if (_options.snapshot_uri.empty()) {
        return 0;
    }
    _snapshot_executor = new SnapshotExecutor;
    SnapshotExecutorOptions opt;
    opt.uri = _options.snapshot_uri;
    opt.fsm_caller = _fsm_caller;
    opt.init_term = 0;
    opt.node = NULL;
    opt.log_manager = _log_manager;
    opt.addr = _server_id.addr;
    opt.filter_before_copy_remote = _options.filter_before_copy_remote;
    opt.usercode_in_pthread = _options.usercode_in_pthread;
    opt.learner_auto_load_applied_logs = _options.learner_auto_load_applied_logs;
    if (_options.snapshot_file_system_adaptor) {
        opt.file_system_adaptor = *_options.snapshot_file_system_adaptor;
    }
    // get snapshot_throttle
    if (_options.snapshot_throttle) {
        opt.snapshot_throttle = *_options.snapshot_throttle;
    }
    return _snapshot_executor->init(opt);
}

int LearnerImpl::explore_learning_info(PeerId* peer, int64_t* last_committed_index, 
        int64_t* last_snapshot_index, int64_t* peer_first_log_index) {
    if (!is_active_state(_state)) {
        return 0;
    }
    ExploreResponse* response = explore_raft_group();
    if (!response || !response->success()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id 
                     << " fail to explore learning info, nodes:" << _conf.conf;
        if (response) {
            delete response;
        }
        return -1;
    }

    *last_committed_index = response->last_committed_index();
    *last_snapshot_index = response->last_snapshot_index();
    *peer_first_log_index = response->first_log_index();
    peer->parse(response->server_id());
    LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " explored learning info, peer_id:" << *peer << ", last_committed_index:"
              << *last_committed_index << ", last_snapshot_index:" << *last_snapshot_index
              << " peer_first_log_index:" << *peer_first_log_index;

    delete response;
    return 0;
}

brpc::ParallelChannel* LearnerImpl::init_rpc_channel() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    std::vector<PeerId> peers;
    _conf.conf.list_peers(&peers);
    if (peers.empty()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " fail to init rpc channel with empty peers set, last_log_index:"
                     << _last_log_index;
        return NULL;
    }
    int64_t last_log_index = _last_log_index;
    lck.unlock();

    brpc::ParallelChannel* channel = new brpc::ParallelChannel;
    brpc::ParallelChannelOptions pchan_options;
    if (channel->Init(&pchan_options) != 0) {
        LOG(INFO) << "node " << _group_id << ":" << _server_id 
                    << " fail to init ParallelChannel";
        delete channel;
        return NULL;
    }

    ExploreMapper* mapper = NULL;
    ExploreMerger* merger = NULL;
    for (size_t i = 0; i < peers.size(); ++i) {
        brpc::Channel *sub_channel = new brpc::Channel;
        brpc::ChannelOptions sub_options;
        const PeerId& peer = peers[i];
        if (sub_channel->Init(peer.addr, &sub_options) != 0) {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                        << " fail to initialize sub_channel " << peer;
            delete sub_channel;
            delete channel;
            return NULL;
        }
        if (mapper == NULL || merger == NULL) {
            mapper = new ExploreMapper(NodeId(_group_id, _server_id), peers);
            merger = new ExploreMerger(NodeId(_group_id, _server_id), last_log_index,
                                       _snapshot_executor == nullptr);
        }
        if (channel->AddChannel(sub_channel, brpc::OWNS_CHANNEL,
                                mapper, merger) != 0) {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                        << " fail to AddChannel " << peer;
            delete channel;
            return NULL;
        }
    }

    return channel;
}

ExploreResponse* LearnerImpl::explore_raft_group() {
    brpc::ParallelChannel* channel = init_rpc_channel();
    if (channel == NULL) {
        return NULL;
    }

    brpc::Controller cntl;
    ExploreRequest request;
    ExploreResponse* response = new ExploreResponse;

    request.set_group_id(_group_id);
    request.set_peer_id("");
    RaftService_Stub stub(channel);
    cntl.set_timeout_ms(FLAGS_raft_learner_req_timeout_ms);
    stub.explore(&cntl, &request, response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " fail to explore_raft_group, " << cntl.ErrorText();
        delete channel;
        delete response;
        return NULL;
    }

    delete channel;
    return response;
}

int LearnerImpl::append_entries(std::vector<LogEntry*>* entries) {
    CHECK(entries->size() > 0);

    std::unique_lock<raft_mutex_t> lck(_mutex);
    int64_t first_log_index = (*entries->begin())->id.index;
    int64_t last_log_index = (*entries->rbegin())->id.index;
    if (_fetching_snapshot_index != 0) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " fail to append entries, fetching snapshot " << _fetching_snapshot_index
                     << ", first_log_index " << first_log_index;
        return -1;
    }
    if (first_log_index != _last_log_index + 1) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " fail to append entries, first_log_index " << first_log_index 
                     << " is not sequential, last_log_index:" << _last_log_index;
        return -1;
    }
    if (_read_barrier != 0) {
        for (int64_t index = last_log_index; index > _read_barrier && index >= first_log_index;
                --index) {
            entries->back()->Release();
            entries->pop_back();
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " fail to append entry " << index
                      << ", encounter read barrier " << _read_barrier;
        }
    }
    size_t entries_size = entries->size();
    if (entries->empty()) {
        return entries_size;
    }

    _last_log_index = (*entries->rbegin())->id.index;
    _log_manager->append_entries(entries, new LearnerStableClosure(this, entries_size));
    if (_log_manager->check_and_set_configuration(&_conf)) {
        lck.unlock();
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " configuration changed after fetch log, log_id:" << _conf.id
                  << ", conf: " << _conf.conf << ", old_conf: " << _conf.old_conf;
        FetchLogClosure* done = new FetchLogClosure(this);
        run_closure_in_bthread(done);
    }
    return entries_size;
}

int LearnerImpl::init_fsm_caller() {
    CHECK(_fsm_caller);
    // fsm caller init, node AddRef in init
    this->AddRef();
    FSMCallerOptions fsm_caller_options;
    fsm_caller_options.usercode_in_pthread = _options.usercode_in_pthread;
    fsm_caller_options.after_shutdown = 
        brpc::NewCallback<LearnerImpl*>(after_shutdown, this);
    fsm_caller_options.log_manager = _log_manager;
    fsm_caller_options.fsm = _options.fsm;
    fsm_caller_options.closure_queue = &_closure_queue;
    fsm_caller_options.node = NULL;
    fsm_caller_options.bootstrap_id = LogId(0, 0);
    fsm_caller_options.state_machine_running = _options.learner_auto_load_applied_logs;
    const int ret = _fsm_caller->init(fsm_caller_options);
    if (ret != 0) {
        delete fsm_caller_options.after_shutdown;
        this->Release();
    }
    return ret;
}

void LearnerImpl::snapshot(Closure* done) {
    LOG(INFO) << "node " << _group_id << ":" << _server_id 
              << " starts to do snapshot";
    if (_snapshot_executor) {
        _snapshot_executor->do_snapshot(done);
    } else {
        if (done) {
            done->status().set_error(EINVAL, "Snapshot is not supported");
            run_closure_in_bthread(done);
        }
    }
}

void LearnerImpl::handle_tail_snapshot(brpc::Controller* cntl,
                                       const TailSnapshotRequest* request,
                                       TailSnapshotResponse* response) {
    if (_snapshot_executor) {
        _snapshot_executor->handle_tail_snapshot(cntl, request, response);
    } else {
        cntl->SetFailed(EINVAL, "Snapshot is not supported");
    }
}

void LearnerImpl::get_status(NodeStatus* status) {
    if (status == NULL) {
        return;
    }

    std::unique_lock<raft_mutex_t> lck(_mutex);
    status->state = _state;
    status->term = 0;
    status->peer_id = _server_id;
    status->readonly = false;
    status->committed_index = _last_committed_index;
    lck.unlock();

    status->pending_index = 0;
    status->pending_queue_size = 0;

    LogManagerStatus log_manager_status;
    _log_manager->get_status(&log_manager_status);
    status->known_applied_index = log_manager_status.known_applied_index;
    status->first_index = log_manager_status.first_index;
    status->last_index = log_manager_status.last_index;
    status->disk_index = log_manager_status.disk_index;

    status->applying_index = _fsm_caller->applying_index();

    if (_snapshot_executor) {
        status->last_snapshot_index = _snapshot_executor->last_snapshot_index();
    }
}

void LearnerImpl::describe(std::ostream& os, bool use_html) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    const State state = _state;
    const bool readonly = false;
    const int64_t conf_index = _conf.id.index;
    const int64_t term = 0;

    std::vector<PeerId> peers;
    _conf.conf.list_peers(&peers);
    lck.unlock();

    const char *newline = use_html ? "<br>" : "\r\n";
    os << "state: " << state2str(state) << newline;
    os << "readonly: " << readonly << newline;
    os << "term: " << term << newline;
    os << "conf_index: " << conf_index << newline;
    os << "master_peers: ";
    for (size_t j = 0; j < peers.size(); ++j) {
        os << ' ';
        os << peers[j];
    }
    os << newline;

    os << "fetch_log_timer: ";
    _fetch_log_timer.describe(os, use_html);
    os << newline;
    os << "snapshot_timer: ";
    _snapshot_timer.describe(os, use_html);
    os << newline;

    _log_manager->describe(os, use_html);
    _fsm_caller->describe(os, use_html);
    if (_snapshot_executor) {
        _snapshot_executor->describe(os, use_html);
    }
}

int LearnerImpl::commit_at(int64_t first_log_index, int64_t last_log_index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_fetching_snapshot_index != 0) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " skip commit logs ["
                     << first_log_index << ", " << last_log_index << "], fetching snapshot "
                     << _fetching_snapshot_index;
        return 0;
    }
    if (first_log_index > _last_committed_index + 1) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id << " skip commit logs ["
                   << first_log_index << ", " << last_log_index << "], "
                   << "first_log_index larger than next commit index:" 
                   << _last_committed_index + 1;
        braft::Error e;
        e.set_type(ERROR_TYPE_LEARNER);
        e.status().set_error(-1, "Fail to commit logs at [%ld, %ld]", 
                first_log_index, last_log_index);
        _fsm_caller->on_error(e);
        return -1;
    }
    if (last_log_index <= _last_committed_index) {
        return 0;
    }

    _last_committed_index = last_log_index;
    _fsm_caller->on_committed(_last_committed_index);
    LOG(INFO) << "node " << _group_id << ":" << _server_id << " committed logs ["
               << first_log_index << ", " << last_log_index << "]";

    return 0;
}

butil::Status LearnerImpl::set_read_barrier(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (index != 0 && (index < _last_log_index || index < _fetching_snapshot_index)) {
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " fail to set read barrier, barrier index " << index 
                  << " is less than last_log_index(" << _last_log_index
                  << ") or fetching_snapshot_index(" << _fetching_snapshot_index << ")";
        return butil::Status(EINVAL, "last log or snapshot crosses the barrier");
    }
    _read_barrier = index;
    return butil::Status::OK();
}

void LearnerImpl::resume(Closure* done) {
    LOG(INFO) << "node " << _group_id << ":" << _server_id 
              << " starts to do resume";
    if (!_options.snapshot_uri.empty() && (_snapshot_executor == NULL || _snapshot_executor->snapshot_storage() == NULL)) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " snapshot_executor or snapshot_storage is null";
        if (done) {
            done->status().set_error(EINVAL, "snapshot_executor or snapshot_storage is null");
            run_closure_in_bthread(done);
        }
        return;
    } else if (_options.snapshot_uri.empty()) {
        if (_fsm_caller == NULL) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " fsm_caller is null";
            if (done) {
                done->status().set_error(EINVAL, "snapshot_executor or snapshot_storage is null");
                run_closure_in_bthread(done);
            }
            return;
        }
        _fsm_caller->set_state_machine_running(_last_committed_index);
        if (done) {
            done->Run();
        }
        return;
    }

    int ret = _snapshot_executor->load_snapshot(_last_committed_index, done);
    if (ret != 0) {	
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " load_snapshot failed";
        if (done) {
            done->status().set_error(EINVAL, "load_snapshot failed");
            run_closure_in_bthread(done);
        }
    }
    return;
}

void LearnerImpl::reset(Closure* done) {
    LOG(INFO) << "node " << _group_id << ":" << _server_id 
              << " starts to do reset";
    int ret = _fsm_caller->on_reset(done);
    if (ret != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << "reset error";
        if (done) {
            done->status().set_error(EINVAL, "reset error");
            run_closure_in_bthread(done);
        }
        return;
    }
    return;
}

} // namespace braft
