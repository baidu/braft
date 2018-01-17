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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Xiong,Kai(xionkai@baidu.com)

#include <bthread/unstable.h>
#include <brpc/errno.pb.h>
#include <brpc/controller.h>
#include <brpc/channel.h>

#include "braft/errno.pb.h"
#include "braft/util.h"
#include "braft/raft.h"
#include "braft/node.h"
#include "braft/log.h"
#include "braft/stable.h"
#include "braft/snapshot.h"
#include "braft/file_service.h"
#include "braft/builtin_service_impl.h"
#include "braft/node_manager.h"
#include "braft/snapshot_executor.h"
#include "braft/errno.pb.h"

namespace braft {

static bvar::Adder<int64_t> g_num_nodes("raft_node_count");

class ConfigurationChangeDone : public Closure {
public:
    void Run() {
        if (status().ok()) {
            if (_node != NULL) {
                _node->on_configuration_change_done(_term);
            }
        }
        if (_user_done) {
            _user_done->status() = status();
            _user_done->Run();
            _user_done = NULL;
        }
        delete this;
    }
private:
    ConfigurationChangeDone(
            NodeImpl* node, int64_t term, Closure* user_done)
        : _node(node)
        , _term(term)
        , _user_done(user_done)
    {
        _node->AddRef();
    }
    ~ConfigurationChangeDone() {
        CHECK(_user_done == NULL);
        if (_node != NULL) {
            _node->Release();
            _node = NULL;
        }
    }
friend class NodeImpl;
    NodeImpl* _node;
    int64_t _term;
    Closure* _user_done;
};

inline int random_timeout(int timeout_ms) {
    return butil::fast_rand_in(timeout_ms, timeout_ms << 1);
}

DEFINE_int32(raft_election_heartbeat_factor, 10, "raft election:heartbeat timeout factor");
static inline int heartbeat_timeout(int election_timeout) {
    return std::max(election_timeout / FLAGS_raft_election_heartbeat_factor, 10);
}

NodeImpl::NodeImpl(const GroupId& group_id, const PeerId& peer_id)
    : _state(STATE_UNINITIALIZED)
    , _current_term(0)
    , _last_leader_timestamp(butil::monotonic_time_ms())
    , _group_id(group_id)
    , _log_storage(NULL)
    , _stable_storage(NULL)
    , _closure_queue(NULL)
    , _config_manager(NULL)
    , _log_manager(NULL)
    , _fsm_caller(NULL)
    , _commit_manager(NULL)
    , _snapshot_executor(NULL)
    , _stop_transfer_arg(NULL)
    , _vote_triggered(false)
    , _waking_candidate(0) {
        _server_id = peer_id;
        AddRef();
    g_num_nodes << 1;
}

NodeImpl::NodeImpl()
    : _state(STATE_UNINITIALIZED)
    , _current_term(0)
    , _last_leader_timestamp(butil::monotonic_time_ms())
    , _group_id()
    , _log_storage(NULL)
    , _stable_storage(NULL)
    , _closure_queue(NULL)
    , _config_manager(NULL)
    , _log_manager(NULL)
    , _fsm_caller(NULL)
    , _commit_manager(NULL)
    , _snapshot_executor(NULL)
    , _stop_transfer_arg(NULL)
    , _vote_triggered(false)
    , _waking_candidate(0) {
        AddRef();
    g_num_nodes << 1;
}

NodeImpl::~NodeImpl() {
    if (_apply_queue) {
        // Wait until no flying task
        _apply_queue->stop();
        _apply_queue.reset();
        bthread::execution_queue_join(_apply_queue_id);
    }

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
    if (_commit_manager) {
        delete _commit_manager;
        _commit_manager = NULL;
    }

    if (_log_storage) {
        delete _log_storage;
        _log_storage = NULL;
    }
    if (_closure_queue) {
        delete _closure_queue;
        _closure_queue = NULL;
    }
    if (_stable_storage) {
        delete _stable_storage;
        _stable_storage = NULL;
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
    g_num_nodes << -1;
}

int NodeImpl::init_snapshot_storage() {
    if (_options.snapshot_uri.empty()) {
        return 0;
    }
    _snapshot_executor = new SnapshotExecutor;
    SnapshotExecutorOptions opt;
    opt.uri = _options.snapshot_uri;
    opt.fsm_caller = _fsm_caller;
    opt.node = this;
    opt.log_manager = _log_manager;
    opt.addr = _server_id.addr;
    opt.init_term = _current_term;
    opt.filter_before_copy_remote = _options.filter_before_copy_remote;
    opt.usercode_in_pthread = _options.usercode_in_pthread;
    if (_options.snapshot_file_system_adaptor) {
        opt.file_system_adaptor = *_options.snapshot_file_system_adaptor;
    }
    // get snapshot_throttle
    if (_options.snapshot_throttle) {
        opt.snapshot_throttle = *_options.snapshot_throttle;
    }
    return _snapshot_executor->init(opt);
}

int NodeImpl::init_log_storage() {
    CHECK(_fsm_caller);
    _log_storage = LogStorage::create(_options.log_uri);
    if (!_log_storage) {
        LOG(ERROR) << "Fail to find log storage of `" << _options.log_uri
            << '\'';
        return -1;
    }
    _log_manager = new LogManager();
    LogManagerOptions log_manager_options;
    log_manager_options.log_storage = _log_storage;
    log_manager_options.configuration_manager = _config_manager;
    log_manager_options.fsm_caller = _fsm_caller;
    return _log_manager->init(log_manager_options);
}

int NodeImpl::init_stable_storage() {
    int ret = 0;

    do {
        _stable_storage = StableStorage::create(_options.stable_uri);
        if (!_stable_storage) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " find stable storage failed, uri " << _options.stable_uri;
            ret = ENOENT;
            break;
        }

        ret = _stable_storage->init();
        if (ret != 0) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " int stable storage failed, uri " << _options.stable_uri
                << " ret " << ret;
            break;
        }

        _current_term = _stable_storage->get_term();
        ret = _stable_storage->get_votedfor(&_voted_id);
        if (ret != 0) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " stable storage get_votedfor failed, uri " << _options.stable_uri
                << " ret " << ret;
            break;
        }
    } while (0);

    return ret;
}

void NodeImpl::handle_snapshot_timeout() {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // check state
    if (!is_active_state(_state)) {
        return;
    }

    lck.unlock();
    // TODO: do_snapshot in another thread to avoid blocking the timer thread.
    do_snapshot(NULL);

}

int NodeImpl::init_fsm_caller(const LogId& bootstrap_id) {
    CHECK(_fsm_caller);
    _closure_queue = new ClosureQueue(_options.usercode_in_pthread);
    // fsm caller init, node AddRef in init
    FSMCallerOptions fsm_caller_options;
    fsm_caller_options.usercode_in_pthread = _options.usercode_in_pthread;
    this->AddRef();
    fsm_caller_options.after_shutdown =
        brpc::NewCallback<NodeImpl*>(after_shutdown, this);
    fsm_caller_options.log_manager = _log_manager;
    fsm_caller_options.fsm = _options.fsm;
    fsm_caller_options.closure_queue = _closure_queue;
    fsm_caller_options.node = this;
    fsm_caller_options.bootstrap_id = bootstrap_id;
    const int ret = _fsm_caller->init(fsm_caller_options);
    if (ret != 0) {
        delete fsm_caller_options.after_shutdown;
        this->Release();
    }
    return ret;
}

class BootstrapStableClosure : public LogManager::StableClosure {
public:
    void Run() { _done.Run(); }
    void wait() { _done.wait(); }
private:
    SynchronizedClosure _done;
};

int NodeImpl::bootstrap(const BootstrapOptions& options) {

    if (options.last_log_index > 0) {
        if (options.group_conf.empty() || options.fsm == NULL) {
            LOG(ERROR) << "Invalid arguments for " << __FUNCTION__
                       << "group_conf=" << options.group_conf
                       << " fsm=" << options.fsm
                       << " while last_log_index="
                       << options.last_log_index;
            return -1;
        }
    }

    if (options.group_conf.empty()) {
        LOG(ERROR) << "bootstraping an empty node makes no sense";
        return -1;
    }

    // Term is not an option since changing it is very dangerous
    const int64_t boostrap_log_term = options.last_log_index ? 1 : 0;
    const LogId boostrap_id(options.last_log_index, boostrap_log_term);

    _options.fsm = options.fsm;
    _options.node_owns_fsm = options.node_owns_fsm;
    _options.usercode_in_pthread = options.usercode_in_pthread;
    _options.log_uri = options.log_uri;
    _options.stable_uri = options.stable_uri;
    _options.snapshot_uri = options.snapshot_uri;
    _options.snapshot_file_system_adaptor = _options.snapshot_file_system_adaptor;
    _config_manager = new ConfigurationManager();

    // Create _fsm_caller first as log_manager needs it to report error
    _fsm_caller = new FSMCaller();

    if (init_log_storage() != 0) {
        LOG(ERROR) << "Fail to init log_storage";
        return -1;
    }

    if (init_stable_storage() != 0) {
        LOG(ERROR) << "Fail to init stable_storage";
        return -1;
    }
    if (_current_term == 0) {
        _current_term = 1;
        if (_stable_storage->set_term_and_votedfor(1, PeerId()) != 0) {
            LOG(ERROR) << "Fail to set term";
            return -1;
        }
        return -1;
    }

    if (options.fsm && init_fsm_caller(boostrap_id) != 0) {
        LOG(ERROR) << "Fail to init fsm_caller";
        return -1;
    }

    if (options.last_log_index > 0) {
        if (init_snapshot_storage() != 0) {
            LOG(ERROR) << "Fail to init snapshot_storage";
            return -1;
        }
        SynchronizedClosure done;
        _snapshot_executor->do_snapshot(&done);
        done.wait();
        if (!done.status().ok()) {
            LOG(ERROR) << "Fail to save snapshot " << done.status();
            return -1;
        }
    }
    CHECK_EQ(_log_manager->first_log_index(), options.last_log_index + 1);
    CHECK_EQ(_log_manager->last_log_index(), options.last_log_index);

    LogEntry* entry = new LogEntry();
    entry->AddRef();
    entry->id.term = _current_term;
    entry->type = ENTRY_TYPE_CONFIGURATION;
    entry->peers = new std::vector<PeerId>;
    options.group_conf.list_peers(entry->peers);

    std::vector<LogEntry*> entries;
    entries.push_back(entry);
    BootstrapStableClosure done;
    _log_manager->append_entries(&entries, &done);
    done.wait();
    if (!done.status().ok()) {
        LOG(ERROR) << "Fail to append configuration";
        return -1;
    }

    return 0;
}

int NodeImpl::init(const NodeOptions& options) {
    _options = options;

    // check _server_id
    if (butil::IP_ANY == _server_id.addr.ip) {
        LOG(ERROR) << "Node can't started from IP_ANY";
        return -1;
    }

    if (!NodeManager::GetInstance()->server_exists(_server_id.addr)) {
        LOG(ERROR) << "No RPC Server attached to " << _server_id.addr
                   << ", did you forget to call braft::add_service()?";
        return -1;
    }

    CHECK_EQ(0, _vote_timer.init(this, options.election_timeout_ms));
    CHECK_EQ(0, _election_timer.init(this, options.election_timeout_ms));
    CHECK_EQ(0, _stepdown_timer.init(this, options.election_timeout_ms));
    CHECK_EQ(0, _snapshot_timer.init(this, options.snapshot_interval_s * 1000));

    _config_manager = new ConfigurationManager();

    if (bthread::execution_queue_start(&_apply_queue_id, NULL,
                                       execute_applying_tasks, this) != 0) {
        LOG(ERROR) << "Fail to start execution_queue";
        return -1;
    }

    _apply_queue = execution_queue_address(_apply_queue_id);
    if (!_apply_queue) {
        LOG(ERROR) << "Fail to address execution_queue";
        return -1;
    }

    // Create _fsm_caller first as log_manager needs it to report error
    _fsm_caller = new FSMCaller();

    // log storage and log manager init
    if (init_log_storage() != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " init_log_storage failed";
        return -1;
    }

    // stable init
    if (init_stable_storage() != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " init_stable_storage failed";
        return -1;
    }

    if (init_fsm_caller(LogId(0, 0)) != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " init_fsm_caller failed";
        return -1;
    }

    // commitment manager init
    _commit_manager = new CommitmentManager();
    CommitmentManagerOptions commit_manager_options;
    commit_manager_options.waiter = _fsm_caller;
    commit_manager_options.closure_queue = _closure_queue;
    if (_commit_manager->init(commit_manager_options) != 0) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " init _commit_manager failed";
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

    butil::Status st = _log_manager->check_consistency();
    if (!st.ok()) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " is initialized with inconsitency log: "
            << st;
        return -1;
    }

    _conf.first = LogId();
    // if have log using conf in log, else using conf in options
    if (_log_manager->last_log_index() > 0) {
        _log_manager->check_and_set_configuration(&_conf);
    } else {
        _conf.second = _options.initial_conf;
    }

    // init replicator
    ReplicatorGroupOptions rg_options;
    rg_options.heartbeat_timeout_ms = heartbeat_timeout(_options.election_timeout_ms);
    rg_options.election_timeout_ms = _options.election_timeout_ms;
    rg_options.log_manager = _log_manager;
    rg_options.commit_manager = _commit_manager;
    rg_options.node = this;
    rg_options.snapshot_storage = _snapshot_executor
        ?  _snapshot_executor->snapshot_storage()
        : NULL;
    _replicator_group.init(NodeId(_group_id, _server_id), rg_options);

    // set state to follower
    _state = STATE_FOLLOWER;

    LOG(INFO) << "node " << _group_id << ":" << _server_id << " init,"
        << " term: " << _current_term
        << " last_log_id: " << _log_manager->last_log_id()
        << " conf: " << _conf.second;

    // start snapshot timer
    if (_snapshot_executor && _options.snapshot_interval_s > 0) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " start snapshot_timer";
        _snapshot_timer.start();
    }

    if (!_conf.second.empty()) {
        step_down(_current_term, false, butil::Status::OK());
    }

    // add node to NodeManager
    if (!NodeManager::GetInstance()->add(this)) {
        LOG(ERROR) << "NodeManager add " << _group_id 
                   << ":" << _server_id << " failed";
        return -1;
    }

    // Now the raft node is started , have to acquire the lock to avoid race
    // conditions
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_conf.second.size() == 1u && _conf.second.contains(_server_id)) {
        // The group contains only this server which must be the LEADER, trigger
        // the timer immediately.
        elect_self(&lck);
    }

    return 0;
}

DEFINE_int32(raft_apply_batch, 32, "Max number of tasks that can be applied "
                                   " in a single batch");
BRPC_VALIDATE_GFLAG(raft_apply_batch, ::brpc::PositiveInteger);

int NodeImpl::execute_applying_tasks(
        void* meta, bthread::TaskIterator<LogEntryAndClosure>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }
    // TODO: the batch size should limited by both task size and the total log
    // size
    const size_t batch_size = FLAGS_raft_apply_batch;
    DEFINE_SMALL_ARRAY(LogEntryAndClosure, tasks, batch_size, 256);
    size_t cur_size = 0;
    NodeImpl* m = (NodeImpl*)meta;
    for (; iter; ++iter) {
        if (cur_size == batch_size) {
            m->apply(tasks, cur_size);
            cur_size = 0;
        }
        tasks[cur_size++] = *iter;
    }
    if (cur_size > 0) {
        m->apply(tasks, cur_size);
    }
    return 0;
}

void NodeImpl::apply(const Task& task) {
    LogEntry* entry = new LogEntry;
    entry->AddRef();
    entry->data.swap(*task.data);
    LogEntryAndClosure m;
    m.entry = entry;
    m.done = task.done;
    m.expected_term = task.expected_term;
    if (_apply_queue->execute(m, &bthread::TASK_OPTIONS_INPLACE, NULL) != 0) {
        task.done->status().set_error(EPERM, "Node is down");
        entry->Release();
        return run_closure_in_bthread(task.done);
    }
}

void NodeImpl::on_configuration_change_done(int64_t term) {
    Configuration removed;
    Configuration added;
    BAIDU_SCOPED_LOCK(_mutex);
    if (_state > STATE_TRANSFERING || term != _current_term) {
        // Callback from older version
        return;
    }
    Configuration old_conf(_conf_ctx.peers);
    old_conf.diffs(_conf.second, &removed, &added);
    CHECK_GE(1u, removed.size() + added.size());

    for (Configuration::const_iterator
            iter = added.begin(); iter != added.end(); ++iter) {
        if (!_replicator_group.contains(*iter)) {
            CHECK(false)
                    << "Impossible! no replicator attached to added peer=" << *iter;
            _replicator_group.add_replicator(*iter);
        }
    }

    for (Configuration::const_iterator
            iter = removed.begin(); iter != removed.end(); ++iter) {
        _replicator_group.stop_replicator(*iter);
    }
   
    if (removed.contains(_server_id)) {
        butil::Status status;
        status.set_error(ELEADERREMOVED, "Raft leader removed, step down to wake up a new one.");
        step_down(_current_term, true, status);
    }
    _conf_ctx.reset();
}

class OnCaughtUp : public CatchupClosure {
public:
    OnCaughtUp(NodeImpl* node, int64_t term, const PeerId& peer, Closure* done)
        : _node(node)
        , _term(term)
        , _peer(peer)
        , _done(done)
    {
        _node->AddRef();
    }
    ~OnCaughtUp() {
        if (_node) {
            _node->Release();
            _node = NULL;
        }
    }
    virtual void Run() {
        _node->on_caughtup(_peer, _term, status(), _done);
        delete this;
    };
private:
    NodeImpl* _node;
    int64_t _term;
    PeerId _peer;
    Closure* _done;
};

void NodeImpl::on_caughtup(const PeerId& peer, int64_t term,
                           const butil::Status& st, Closure* done) {
    int error_code = st.error_code();
    const char* error_text = st.error_cstr();

    do {
        BAIDU_SCOPED_LOCK(_mutex);
        // CHECK _current_term and _state to avoid ABA problem
        if (term != _current_term) {
            // term has changed, we have to mark this configuration changing
            // operation as failure, otherwise there is likely an ABA problem
            if (error_code == 0) {
                error_code = EINVAL;
                error_text = "leader stepped down";
            }
            // DON'T stop replicator here.
            // Caution: Be careful if you want to add some stuff here
            break;
        }
        if (_state != STATE_LEADER) {
            if (_state == STATE_TRANSFERING) {
                // Print error to notice the users there must be something wrong
                // since configuration changing is refused during traslanting
                // leadership.
                // Note: Don't use FATAL in case the users turn
                //       `crash_on_fatal_log' on and the process is going to
                //       abort ever after.
                LOG(ERROR) << "node " << _group_id << ":" << _server_id
                           << " received on_caughtup while the state is "
                           << state2str(STATE_TRANSFERING)
                           << " which is not supposed to happend according to"
                              " the implemention(until Thu Nov 24 17:28:42 CST 2016)."
                              " There must be something wrong, please contact the raft"
                              " maintainers to checkout this issue and fix it";
                // Stop this replicator to release the resource
                _replicator_group.stop_replicator(peer);
                // Reset _conf_ctx to make configuration changing acceptable
                // after this point.
                _conf_ctx.reset();
                error_code = EINVAL;
                error_text = "Impossible condition";
            } else {
                error_code = EPERM;
                error_text = "leader stepped down";
            }
            break;
        }

        if (error_code == 0) {  // Caught up succesfully
            LOG(INFO) << "node " << _group_id << ":" << _server_id << " add_peer " << peer
                << " to " << _conf.second << ", caughtup "
                << "success, then append add_peer entry.";
            // add peer to _conf after new peer catchup
            Configuration new_conf(_conf.second);
            new_conf.add_peer(peer);
            unsafe_apply_configuration(new_conf, done);
            return;
        }

        // Retry if the node doesn't step down
        if (error_code == ETIMEDOUT &&
            (butil::monotonic_time_ms() -  _replicator_group.last_rpc_send_timestamp(peer)) <=
            _options.election_timeout_ms) {

            LOG(INFO) << "node " << _group_id << ":" << _server_id << " catching up " << peer;

            OnCaughtUp* caught_up = new OnCaughtUp(this, _current_term, peer, done);
            timespec due_time = butil::milliseconds_from_now(_options.election_timeout_ms);

            if (0 == _replicator_group.wait_caughtup(
                        peer, _options.catchup_margin, &due_time, caught_up)) {
                return;
            } else {
                LOG(ERROR) << "node " << _group_id << ":" << _server_id
                    << " wait_caughtup failed, peer " << peer;
                delete caught_up;
            }
        }

        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " add_peer " << peer
            << " to " << _conf.second << ", caughtup "
            << "failed: " << error_text << " (" << error_code << ')';

        _replicator_group.stop_replicator(peer);
        _conf_ctx.reset();
        error_code = ECATCHUP;
    } while (0);

    // call add_peer done when fail
    done->status().set_error(error_code, "caughtup failed: %s", error_text);
    done->Run();
}

void NodeImpl::handle_stepdown_timeout() {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state > STATE_TRANSFERING) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stop stepdown_timer"
            << " state is " << state2str(_state);
        return;
    }

    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    int64_t now_timestamp = butil::monotonic_time_ms();
    size_t alive_count = 0;
    Configuration dead_nodes;  // for easily print
    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            ++alive_count;
            continue;
        }

        if (now_timestamp - _replicator_group.last_rpc_send_timestamp(peers[i]) <=
            _options.election_timeout_ms) {
            ++alive_count;
            continue;
        }
        dead_nodes.add_peer(peers[i]);
    }
    if (alive_count >= peers.size() / 2 + 1) {
        return;
    }
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " stepdown when alive nodes don't satisfy quorum"
        << " dead_nodes: " << dead_nodes;
    
    butil::Status status;
    status.set_error(ERAFTTIMEDOUT, "Raft leader reaches step_down timeout.");
    step_down(_current_term, false, status);
}

bool NodeImpl::unsafe_register_conf_change(const std::vector<PeerId>& old_peers,
                                           const std::vector<PeerId>& new_peers,
                                           Closure* done) {
    if (_state != STATE_LEADER) {
        LOG(WARNING) << "[" << node_id()
                     << "] Refusing configuration changing because the state is "
                     << state2str(_state) ;
        if (done) {
            if (_state == STATE_TRANSFERING) {
                done->status().set_error(EBUSY, "Is transfering leadership");
            } else {
                done->status().set_error(EPERM, "Not leader");
            }
            run_closure_in_bthread(done);
        }
        return false;
    }

    // check concurrent conf change
    if (!_conf_ctx.empty()) {
        LOG(WARNING) << "[" << node_id()
                     << " ] Refusing concurrent configuration changing";
        if (done) {
            done->status().set_error(EBUSY, "Doing another configuration change");
            run_closure_in_bthread(done);
        }
        return false;
    }
    // Return immediately when the new peers equals to current configuration
    if (_conf.second.equals(new_peers)) {
        run_closure_in_bthread(done);
        return false;
    }
    // check not equal
    if (!_conf.second.equals(old_peers)) {
        LOG(WARNING) << "[" << node_id()
                     << "] Refusing configuration changing based on wrong configuration ,"
                     << " expect: " << _conf.second << " recv: " << Configuration(old_peers);
        if (done) {
            done->status().set_error(EINVAL, "old_peers dismatch");
            run_closure_in_bthread(done);
        }
        return false;
    }
    _conf_ctx.peers = old_peers;
    return true;
}

butil::Status NodeImpl::list_peers(std::vector<PeerId>* peers) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_state != STATE_LEADER) {
        return butil::Status(EPERM, "Not leader");
    }
    _conf.second.list_peers(peers);
    return butil::Status::OK();
}

void NodeImpl::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                        Closure* done) {
    BAIDU_SCOPED_LOCK(_mutex);
    Configuration new_conf(old_peers);
    if (old_peers.empty()) {
        new_conf = _conf.second;
    }
    new_conf.add_peer(peer);
    std::vector<PeerId> new_peers;
    new_conf.list_peers(&new_peers);

    if (!unsafe_register_conf_change(old_peers, new_peers, done)) {
        return;
    }
    LOG(INFO) << "node " << _group_id << ":" << _server_id << " add_peer " << peer
        << " to " << _conf.second << ", begin caughtup.";
    if (0 != _replicator_group.add_replicator(peer)) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " start replicator failed, peer " << peer;
        if (done) {
            _conf_ctx.reset();
            done->status().set_error(EINVAL, "Fail to start replicator");
            run_closure_in_bthread(done);
        }
        return;
    }

    // catch up new peer
    OnCaughtUp* caught_up = new OnCaughtUp(this, _current_term, peer, done);
    timespec due_time = butil::milliseconds_from_now(_options.election_timeout_ms);

    if (0 != _replicator_group.wait_caughtup(
                peer, _options.catchup_margin, &due_time, caught_up)) {
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
            << " wait_caughtup failed, peer " << peer;
        _conf_ctx.reset();
        delete caught_up;
        if (done) {
            done->status().set_error(EINVAL, "Fail to wait for caughtup");
            run_closure_in_bthread(done);
        }
        return;
    }
}

void NodeImpl::remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                           Closure* done) {
    BAIDU_SCOPED_LOCK(_mutex);
    Configuration new_conf(old_peers);
    if (old_peers.empty()) {
        new_conf = _conf.second;
    }
    new_conf.remove_peer(peer);
    std::vector<PeerId> new_peers;
    new_conf.list_peers(&new_peers);

    // Register _conf_ctx to reject configuration changing request before this
    // log was committed
    if (!unsafe_register_conf_change(old_peers, new_peers, done)) {
        // done->Run() was called in unsafe_register_conf_change on failure
        return;
    }

    LOG(INFO) << "node " << _group_id << ":" << _server_id << " remove_peer " << peer
        << " from " << _conf.second;

    // Remove peer from _conf immediately, the corresponding replicator will be
    // stopped after the log is committed
    return unsafe_apply_configuration(new_conf, done);
}

int NodeImpl::set_peer(const std::vector<PeerId>& old_peers,
                       const std::vector<PeerId>& new_peers) {
    return set_peer2(old_peers, new_peers).error_code();
}

butil::Status NodeImpl::set_peer2(const std::vector<PeerId>& old_peers,
                                 const std::vector<PeerId>& new_peers) {
    BAIDU_SCOPED_LOCK(_mutex);

    if (new_peers.empty()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set empty peers";
        return butil::Status(EINVAL, "new_peers is empty");
    }
    // check state
    if (!is_active_state(_state)) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " is in state " << state2str(_state) << ", can't set_peer";
        return butil::Status(EPERM, "Bad state %s", state2str(_state));
    }
    // check bootstrap
    if (_conf.second.empty()) {
        if (old_peers.size() == 0 && new_peers.size() > 0) {
            Configuration new_conf(new_peers);
            LOG(INFO) << "node " << _group_id << ":" << _server_id 
                << " set_peer boot from "
                << new_conf;
            _conf.second = new_conf;
            // set_peer boot and step_down
            butil::Status status;
            status.set_error(ESETPEER, "Set peer from empty configuration");
            step_down(_current_term + 1, false, status);
            return butil::Status::OK();
        } else {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " set_peer boot need old_peers empty and new_peers no_empty";
            return butil::Status(EINVAL, "old_peers must be empty "
                                        "when boostrap an empty node");
        }
    }
    // check concurrent conf change
    if (_state == STATE_LEADER && !_conf_ctx.empty()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " set_peer need wait current conf change";
        return butil::Status(EBUSY, "Changing to another configuration");
    }

    // check equal, maybe retry direct return
    if (_conf.second.equals(new_peers)) {
        return butil::Status::OK();
    }

    if (!old_peers.empty()) {
        // check not equal
        if (!_conf.second.equals(std::vector<PeerId>(old_peers))) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id 
                << " set_peer dismatch old_peers";
            return butil::Status(EINVAL, "Dons't match old_peers");
        }

        // check quorum
        if (new_peers.size() >= (old_peers.size() / 2 + 1)) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id 
                << " set_peer new_peers greater than old_peers'quorum";
            return butil::Status(EINVAL, "#new_peers is greater "
                                        "than quorum of old_peers");
        }

        // check contain
        if (!_conf.second.contains(new_peers)) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id 
                << " set_peer old_peers not contains all new_peers";
            return butil::Status(EINVAL, "old_peers not contains all new_peers");
        }
    }

    Configuration new_conf(new_peers);
    LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set_peer from "
                 << _conf.second << " to " << new_conf;
    // change conf and step_down
    _conf.second = new_conf;
    butil::Status status;
    status.set_error(ESETPEER, "Raft node set peer normally.");
    step_down(_current_term + 1, false, status);
    return butil::Status::OK();
}

void NodeImpl::snapshot(Closure* done) {
    do_snapshot(done);
}

void NodeImpl::do_snapshot(Closure* done) {
    if (_snapshot_executor) {
        _snapshot_executor->do_snapshot(done);
    } else {
        if (done) {
            done->status().set_error(EINVAL, "Snapshot is not supported");
            run_closure_in_bthread(done);
        }
    }
}

void NodeImpl::shutdown(Closure* done) {
    // Note: shutdown is probably invoked more than once, make sure this method
    // is idempotent
    {
        BAIDU_SCOPED_LOCK(_mutex);

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " shutdown,"
            " current_term " << _current_term << " state " << state2str(_state);

        if (_state < STATE_SHUTTING) {  // Got the right to shut
            // Remove node from NodeManager and |this| would not be accessed by
            // the comming RPCs
            NodeManager::GetInstance()->remove(this);
            // if it is leader, set the wakeup_a_candidate with true,
            // if it is follower, call on_stop_following in step_down
            if (_state <= STATE_FOLLOWER) {
                butil::Status status;
                status.set_error(ESHUTDOWN, "Raft node is going to quit.");
                step_down(_current_term, _state == STATE_LEADER, status);
            }

            // change state to shutdown
            _state = STATE_SHUTTING;

            // Destroy all the timer
            _election_timer.destroy();
            _vote_timer.destroy();
            _stepdown_timer.destroy();
            _snapshot_timer.destroy();

            // stop replicator and fsm_caller wait
            if (_log_manager) {
                _log_manager->shutdown();
            }

            if (_snapshot_executor) {
                _snapshot_executor->shutdown();
            }

            // step_down will call _commitment_manager->clear_pending_applications(),
            // this can avoid send LogEntry with closure to fsm_caller.
            // fsm_caller shutdown will not leak user's closure.
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
    }  // out of _mutex;

    // This node is down, it's ok to invoke done right now. Don't inovke this
    // inplace to avoid the dead lock issue when done->Run() is going to acquire
    // a mutex which is already held by the caller
    if (done) {
        run_closure_in_bthread(done);
    }
}

void NodeImpl::join() {
    if (_fsm_caller) {
        _fsm_caller->join();
    }
    if (_snapshot_executor) {
        _snapshot_executor->join();
    }
    // We have to join the _waking_candidate which is sending TimeoutNowRequest,
    // otherwise the process is likely going to quit and the RPC would be stop
    // as the working threads are stopped as well
    Replicator::join(_waking_candidate);
}

void NodeImpl::handle_election_timeout() {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // check state
    if (_state != STATE_FOLLOWER) {
        return;
    }
    // check timestamp, skip one cycle check when trigger vote
    if (!_vote_triggered &&
            (butil::monotonic_time_ms() - _last_leader_timestamp
                    < _options.election_timeout_ms)) {
        return;
    }
    _vote_triggered = false;

    // Reset leader as the leader is uncerntain on election timeout.
    PeerId empty_id;
    butil::Status status;
    status.set_error(ERAFTTIMEDOUT, "No message from leader for %d milliseconds.", _options.election_timeout_ms);
    reset_leader_id(empty_id, status);

    return pre_vote(&lck);
    // Don't touch any thing of *this ever after
}

void NodeImpl::handle_timeout_now_request(brpc::Controller* controller,
                                          const TimeoutNowRequest* request,
                                          TimeoutNowResponse* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (request->term() != _current_term) {
        const int64_t saved_current_term = _current_term;
        if (request->term() > _current_term) {
            butil::Status status;
            status.set_error(EHIGHERTERMREQUEST, "Raft node receives higher term request.");
            step_down(request->term(), false, status);
        }
        response->set_term(_current_term);
        response->set_success(false);
        lck.unlock();
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received handle_timeout_now_request "
                     "while _current_term="
                  << saved_current_term << " didn't match request_term="
                  << request->term();
        return;
    }
    if (_state != STATE_FOLLOWER) {
        const State saved_state = _state;
        const int64_t saved_term = _current_term;
        response->set_term(_current_term);
        response->set_success(false);
        lck.unlock();
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received handle_timeout_now_request "
                     "while state is " << state2str(saved_state)
                  << " at term=" << saved_term;
        return;
    }
    const butil::EndPoint remote_side = controller->remote_side();
    const int64_t saved_term = _current_term;
    // Increase term to make leader step down
    response->set_term(_current_term + 1);
    response->set_success(true);
    // Parallelize Response and election
    run_closure_in_bthread(done_guard.release());
    elect_self(&lck);
    // Don't touch any mutable field after this point, it's likely out of the
    // critical section
    if (lck.owns_lock()) {
        lck.unlock();
    }
    // Note: don't touch controller, request, response, done anymore since they
    // were dereferenced at this point
    LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " received handle_timeout_now_request from "
              << remote_side << " at term=" << saved_term;

}

class StopTransferArg {
DISALLOW_COPY_AND_ASSIGN(StopTransferArg);
public:
    StopTransferArg(NodeImpl* n, int64_t t, const PeerId& p)
        : node(n), term(t), peer(p) {
        node->AddRef();
    }
    ~StopTransferArg() {
        node->Release();
    }
    NodeImpl* node;
    int64_t term;
    PeerId peer;
};

void on_transfer_timeout(void* arg) {
    StopTransferArg* a = (StopTransferArg*)arg;
    a->node->handle_transfer_timeout(a->term, a->peer);
    delete a;
}

void NodeImpl::handle_transfer_timeout(int64_t term, const PeerId& peer) {
    LOG(INFO) << "node " << node_id()  << " failed to transfer leadership to peer="
              << peer << " : reached timeout";
    BAIDU_SCOPED_LOCK(_mutex);
    if (term == _current_term) {
        _replicator_group.stop_transfer_leadership(peer);
        if (_state == STATE_TRANSFERING) {
            _fsm_caller->on_leader_start(term);
            _state = STATE_LEADER;
            _stop_transfer_arg = NULL;
        }
    }
}

int NodeImpl::transfer_leadership_to(const PeerId& peer) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_state != STATE_LEADER) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " is in state " << state2str(_state);
        return _state == STATE_TRANSFERING ? EBUSY : EPERM;
    }
    if (!_conf_ctx.empty() /*FIXME: make this expression more readable*/) {
        // It's very messy to deal with the case when the |peer| received
        // TimeoutNowRequest and increase the term while somehow another leader
        // which was not replicated with the newest configuration has been
        // elected. If no add_peer with this very |peer| is to be invoked ever
        // after nor this peer is to be killed, this peer will spin in the voting
        // procedure and make the each new leader stepped down when the peer
        // reached vote timedoutit starts to vote (because it will increase the
        // term of the group)
        // To make things simple so just refuse the operation and force users to
        // invoke transfer_leadership_to after configuration changing is
        // completed so the peer's configuration is up-to-date when it receives
        // the TimeOutNowRequest.
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " refused to transfer leadership to peer " << peer
                     << " when the leader is changing the configuration";
        return EBUSY;
    }

    PeerId peer_id = peer;
    // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
    // last_log_id will be selected. 
    if (peer_id == ANY_PEER) {
        LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " starts to transfer leadership to any peer.";
        // find the next candidate which is the most possible to become new leader
        if (_replicator_group.find_the_next_candidate(
                        &peer_id, _conf.second) != 0) {
            return -1;    
        }
    }
    if (peer_id == _server_id) {
        LOG(INFO) << "Transfering leadership to self";
        return 0;
    }
    if (!_conf.second.contains(peer_id)) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " refused to transfer leadership to peer " << peer_id
                     << " which doesn't belong to " << _conf.second;
        return EINVAL;
    }
    const int64_t last_log_index = _log_manager->last_log_index();
    const int rc = _replicator_group.transfer_leadership_to(peer_id, last_log_index);
    if (rc != 0) {
        LOG(WARNING) << "No such peer=" << peer_id;
        return EINVAL;
    }
    _state = STATE_TRANSFERING;
    butil::Status status;
    status.set_error(ETRANSFERLEADERSHIP, "Raft leader is transfering "
            "leadership to %s", peer_id.to_string().c_str());
    _fsm_caller->on_leader_stop(status);
    LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " starts to transfer leadership to " << peer_id;
    _stop_transfer_arg = new StopTransferArg(this, _current_term, peer_id);
    if (bthread_timer_add(&_transfer_timer,
                       butil::milliseconds_from_now(_options.election_timeout_ms),
                       on_transfer_timeout, _stop_transfer_arg) != 0) {
        lck.unlock();
        LOG(ERROR) << "Fail to add timer";
        on_transfer_timeout(_stop_transfer_arg);
        return -1;
    }
    return 0;
}

void NodeImpl::vote(int election_timeout) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _options.election_timeout_ms = election_timeout;
    _replicator_group.reset_heartbeat_interval(
            heartbeat_timeout(_options.election_timeout_ms));
    _replicator_group.reset_election_timeout_interval(_options.election_timeout_ms);
    if (_state != STATE_FOLLOWER) {
        return;
    }
    _vote_triggered = true;
    LOG(INFO) << "node " << _group_id << ":" << _server_id << " trigger-vote,"
        " current_term " << _current_term << " state " << state2str(_state) <<
        " election_timeout " << election_timeout;

    _election_timer.reset(election_timeout);
}

void NodeImpl::reset_election_timeout_ms(int election_timeout_ms) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _options.election_timeout_ms = election_timeout_ms;
    _replicator_group.reset_heartbeat_interval(
            heartbeat_timeout(_options.election_timeout_ms));
    _replicator_group.reset_election_timeout_interval(_options.election_timeout_ms);
    LOG(INFO) << "node " << _group_id << ":" << _server_id << " reset_election_timeout,"
        " current_term " << _current_term << " state " << state2str(_state) <<
        " new election_timeout " << election_timeout_ms;

    _election_timer.reset(election_timeout_ms);
}

void NodeImpl::on_error(const Error& e) {
    LOG(WARNING) << "node " << _group_id << ":" << _server_id
                 << " got error=" << e;
    if (_fsm_caller) {
        // on_error of _fsm_caller is guaranteed to be executed once.
        _fsm_caller->on_error(e);
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    // if it is leader, need to wake up a new one.
    // if it is follower, also step down to call on_stop_following
    if (_state <= STATE_FOLLOWER) {
        butil::Status status;
        status.set_error(EBADNODE, "Raft node(leader or candidate) is in error.");
        step_down(_current_term, _state == STATE_LEADER, status);
    }
    if (_state < STATE_ERROR) {
        _state = STATE_ERROR;
    }
    lck.unlock();
}

void NodeImpl::handle_vote_timeout() {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // check state
    if (_state == STATE_CANDIDATE) {
        // retry vote
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " retry elect";
        elect_self(&lck);
    }
}

void NodeImpl::handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                            const RequestVoteResponse& response) {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state != STATE_CANDIDATE) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid RequestVoteResponse from " << peer_id
            << " state not in CANDIDATE but " << state2str(_state);
        return;
    }
    // check stale response
    if (term != _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received stale RequestVoteResponse from " << peer_id
            << " term " << term << " current_term " << _current_term;
        return;
    }
    // check response term
    if (response.term() > _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid RequestVoteResponse from " << peer_id
            << " term " << response.term() << " expect " << _current_term;
        butil::Status status;
        status.set_error(EHIGHERTERMRESPONSE, "Raft node receives higher term "
                "request_vote_response.");
        step_down(response.term(), false, status);
        return;
    }

    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " received RequestVoteResponse from " << peer_id
        << " term " << response.term() << " granted " << response.granted();
    // check granted quorum?
    if (response.granted()) {
        _vote_ctx.grant(peer_id);
        if (_vote_ctx.quorum()) {
            become_leader();
        }
    }
}

struct OnRequestVoteRPCDone : public google::protobuf::Closure {
    OnRequestVoteRPCDone(const PeerId& peer_id_, const int64_t term_, NodeImpl* node_)
        : peer(peer_id_), term(term_), node(node_) {
            node->AddRef();
    }
    virtual ~OnRequestVoteRPCDone() {
        node->Release();
    }

    void Run() {
        do {
            if (cntl.ErrorCode() != 0) {
                LOG(WARNING) << "node " << node->node_id()
                    << " RequestVote to " << peer << " error: " << cntl.ErrorText();
                break;
            }
            node->handle_request_vote_response(peer, term, response);
        } while (0);
        delete this;
    }

    PeerId peer;
    int64_t term;
    RequestVoteResponse response;
    brpc::Controller cntl;
    NodeImpl* node;
};

void NodeImpl::handle_pre_vote_response(const PeerId& peer_id, const int64_t term,
                                            const RequestVoteResponse& response) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // check state
    if (_state != STATE_FOLLOWER) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid PreVoteResponse from " << peer_id
            << " state not in STATE_FOLLOWER but " << state2str(_state);
        return;
    }
    // check stale response
    if (term != _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received stale PreVoteResponse from " << peer_id
            << " term " << term << " current_term " << _current_term;
        return;
    }
    // check response term
    if (response.term() > _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid PreVoteResponse from " << peer_id
            << " term " << response.term() << " expect " << _current_term;
        butil::Status status;
        status.set_error(EHIGHERTERMRESPONSE, "Raft node receives higher term "
                "pre_vote_response.");
        step_down(response.term(), false, status);
        return;
    }

    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " received PreVoteResponse from " << peer_id
        << " term " << response.term() << " granted " << response.granted();
    // check granted quorum?
    if (response.granted()) {
        _pre_vote_ctx.grant(peer_id);
        if (_pre_vote_ctx.quorum()) {
            elect_self(&lck);
        }
    }
}

struct OnPreVoteRPCDone : public google::protobuf::Closure {
    OnPreVoteRPCDone(const PeerId& peer_id_, const int64_t term_, NodeImpl* node_)
        : peer(peer_id_), term(term_), node(node_) {
            node->AddRef();
    }
    virtual ~OnPreVoteRPCDone() {
        node->Release();
    }

    void Run() {
        do {
            if (cntl.ErrorCode() != 0) {
                LOG(WARNING) << "node " << node->node_id()
                    << " request PreVote from " << peer << " error: " << cntl.ErrorText();
                break;
            }
            node->handle_pre_vote_response(peer, term, response);
        } while (0);
        delete this;
    }

    PeerId peer;
    int64_t term;
    RequestVoteResponse response;
    brpc::Controller cntl;
    NodeImpl* node;
};

void NodeImpl::pre_vote(std::unique_lock<raft_mutex_t>* lck) {
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term
        << " start pre_vote";
    if (_snapshot_executor && _snapshot_executor->is_installing_snapshot()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " term " << _current_term
            << " doesn't do pre_vote when installing snapshot as the "
                     " configuration is possibly out of date";
        return;
    }
    if (!_conf.second.contains(_server_id)) {
        LOG(WARNING) << "node " << _group_id << ':' << _server_id
                     << " can't do pre_vote as it is not in " << _conf.second;
        return;
    }

    int64_t old_term = _current_term;
    // get last_log_id outof node mutex
    lck->unlock();
    const LogId last_log_id = _log_manager->last_log_id(true);
    lck->lock();
    // pre_vote need defense ABA after unlock&lock
    if (old_term != _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " raise term " << _current_term << " when get last_log_id";
        return;
    }

    _pre_vote_ctx.reset();
    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    _pre_vote_ctx.set(peers);
    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            continue;
        }
        brpc::ChannelOptions options;
        options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0;
        brpc::Channel channel;
        if (0 != channel.Init(peers[i].addr, &options)) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " channel init failed, addr " << peers[i].addr;
            continue;
        }

        RequestVoteRequest request;
        request.set_group_id(_group_id);
        request.set_server_id(_server_id.to_string());
        request.set_peer_id(peers[i].to_string());
        request.set_term(_current_term + 1); // next term
        request.set_last_log_index(last_log_id.index);
        request.set_last_log_term(last_log_id.term);

        OnPreVoteRPCDone* done = new OnPreVoteRPCDone(peers[i], _current_term, this);
        RaftService_Stub stub(&channel);
        stub.pre_vote(&done->cntl, &request, &done->response, done);
    }
    _pre_vote_ctx.grant(_server_id);

    if (_pre_vote_ctx.quorum()) {
        elect_self(lck);
    }
}

// in lock
void NodeImpl::elect_self(std::unique_lock<raft_mutex_t>* lck) {
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term
        << " start vote and grant vote self";
    if (!_conf.second.contains(_server_id)) {
        LOG(WARNING) << "node " << _group_id << ':' << _server_id
                     << " can't do elect_self as it is not in " << _conf.second;
        return;
    }
    // cancel follower election timer
    if (_state == STATE_FOLLOWER) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stop election_timer";
        _election_timer.stop();
    }
    // reset leader_id before vote
    PeerId empty_id;
    butil::Status status;
    status.set_error(ERAFTTIMEDOUT, "A follower's leader_id is reset to NULL "
            "as it begins to request_vote.");
    reset_leader_id(empty_id, status);

    _state = STATE_CANDIDATE;
    _current_term++;
    _voted_id = _server_id;
    _vote_ctx.reset();

    RAFT_VLOG << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " start vote_timer";
    _vote_timer.start();

    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    _vote_ctx.set(peers);

    int64_t old_term = _current_term;
    // get last_log_id outof node mutex
    lck->unlock();
    const LogId last_log_id = _log_manager->last_log_id(true);
    lck->lock();
    // vote need defense ABA after unlock&lock
    if (old_term != _current_term) {
        // term changed casue by step_down
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " raise term " << _current_term << " when get last_log_id";
        return;
    }

    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            continue;
        }
        brpc::ChannelOptions options;
        options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0;
        brpc::Channel channel;
        if (0 != channel.Init(peers[i].addr, &options)) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " channel init failed, addr " << peers[i].addr;
            continue;
        }

        RequestVoteRequest request;
        request.set_group_id(_group_id);
        request.set_server_id(_server_id.to_string());
        request.set_peer_id(peers[i].to_string());
        request.set_term(_current_term);
        request.set_last_log_index(last_log_id.index);
        request.set_last_log_term(last_log_id.term);

        OnRequestVoteRPCDone* done = new OnRequestVoteRPCDone(peers[i], _current_term, this);
        RaftService_Stub stub(&channel);
        stub.request_vote(&done->cntl, &request, &done->response, done);
    }

    _vote_ctx.grant(_server_id);
    //TODO: outof lock
    _stable_storage->set_term_and_votedfor(_current_term, _server_id);
    if (_vote_ctx.quorum()) {
        become_leader();
    }
}

// in lock
void NodeImpl::step_down(const int64_t term, bool wakeup_a_candidate, 
        const butil::Status& status) {
    RAFT_VLOG << "node " << _group_id << ":" << _server_id
              << " term " << _current_term 
              << " stepdown from " << state2str(_state)
              << " new_term " << term <<
              " wakeup_a_candidate=" << wakeup_a_candidate;

    if (!is_active_state(_state)) {
        return;
    }
    // delete timer and something else
    if (_state == STATE_CANDIDATE) {
        _vote_timer.stop();
    } else if (_state <= STATE_TRANSFERING) {
        _stepdown_timer.stop();

        _commit_manager->clear_pending_tasks();

        // signal fsm leader stop immediately
        if (_state  == STATE_LEADER) {
            _fsm_caller->on_leader_stop(status);
        }
    }
    
    // reset leader_id 
    PeerId empty_id;
    reset_leader_id(empty_id, status);

    // soft state in memory
    _state = STATE_FOLLOWER;
    _pre_vote_ctx.reset();
    _vote_ctx.reset();
    _conf_ctx.reset();
    _last_leader_timestamp = butil::monotonic_time_ms();

    if (_snapshot_executor) {
        _snapshot_executor->interrupt_downloading_snapshot(term);
    }

    // stable state
    if (term > _current_term) {
        _current_term = term;
        _voted_id.reset();
        //TODO: outof lock
        _stable_storage->set_term_and_votedfor(term, _voted_id);
    }

    // stop stagging new node
    if (wakeup_a_candidate) {
        _replicator_group.stop_all_and_find_the_next_candidate(
                                            &_waking_candidate, _conf.second);
        // FIXME: We issue the RPC in the critical section, which is fine now
        // since the Node is going to quit when reaching the branch
        Replicator::send_timeout_now_and_stop(
                _waking_candidate, _options.election_timeout_ms);
    } else {
        _replicator_group.stop_all();
    }
    if (_stop_transfer_arg != NULL) {
        const int rc = bthread_timer_del(_transfer_timer);
        if (rc == 0) {
            // Get the right to delete _stop_transfer_arg.
            delete _stop_transfer_arg;
        }  // else on_transfer_timeout will delete _stop_transfer_arg

        // There is at most one StopTransferTimer at the same term, it's safe to
        // mark _stop_transfer_arg to NULL
        _stop_transfer_arg = NULL;
    }
    _election_timer.start();
}
// in lock
void NodeImpl::reset_leader_id(const PeerId& new_leader_id, 
        const butil::Status& status) {
    if (new_leader_id.is_empty()) {
        if (!_leader_id.is_empty() && _state > STATE_TRANSFERING) {
            LeaderChangeContext stop_following_context(_leader_id, 
                    _current_term, status);
            _fsm_caller->on_stop_following(stop_following_context);
        }
        _leader_id.reset();
    } else {
        if (_leader_id.is_empty()) {
            LeaderChangeContext start_following_context(new_leader_id, 
                    _current_term, status);
            _fsm_caller->on_start_following(start_following_context);
        }
        _leader_id = new_leader_id;
    }
}

// in lock
void NodeImpl::check_step_down(const int64_t request_term, const PeerId& server_id) {
    butil::Status status;
    if (request_term > _current_term) {
        status.set_error(ENEWLEADER, "Raft node receives message from "
                "new leader with higher term."); 
        step_down(request_term, false, status);
    } else if (_state != STATE_FOLLOWER) { 
        status.set_error(ENEWLEADER, "Candidate receives message "
                "from new leader with the same term.");
        step_down(request_term, false, status);
    } else if (_leader_id.is_empty()) {
        status.set_error(ENEWLEADER, "Follower receives message "
                "from new leader with the same term.");
        step_down(request_term, false, status); 
    }
    // save current leader
    if (_leader_id.is_empty()) { 
        reset_leader_id(server_id, status);
    }
}

class LeaderStartClosure : public Closure {
public:
    LeaderStartClosure(StateMachine* fsm, int64_t term) : _fsm(fsm), _term(term) {}
    ~LeaderStartClosure() {}
    void Run() {
        if (status().ok()) {
            _fsm->on_leader_start(_term);
        }
        delete this;
    }
private:
    StateMachine* _fsm;
    int64_t _term;
};

// in lock
void NodeImpl::become_leader() {
    CHECK(_state == STATE_CANDIDATE);
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " become leader, and stop vote_timer";
    // cancel candidate vote timer
    _vote_timer.stop();

    _state = STATE_LEADER;
    _leader_id = _server_id;

    _replicator_group.reset_term(_current_term);

    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            continue;
        }

        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term
            << " add replicator " << peers[i];
        //TODO: check return code
        _replicator_group.add_replicator(peers[i]);
    }

    // init commit manager
    _commit_manager->reset_pending_index(_log_manager->last_log_index() + 1);

    // Register _conf_ctx to reject configuration changing before the first log
    // is committed.
    CHECK(_conf_ctx.empty());
    _conf.second.list_peers(&_conf_ctx.peers);
    unsafe_apply_configuration(_conf.second,
                               new LeaderStartClosure(_options.fsm, _current_term));
    _stepdown_timer.start();
}

class LeaderStableClosure : public LogManager::StableClosure {
public:
    void Run();
private:
    LeaderStableClosure(const NodeId& node_id,
                        size_t nentries,
                        CommitmentManager* commit_manager);
    ~LeaderStableClosure() {}
friend class NodeImpl;
    NodeId _node_id;
    size_t _nentries;
    CommitmentManager* _commit_manager;
};

LeaderStableClosure::LeaderStableClosure(const NodeId& node_id,
                                         size_t nentries,
                                         CommitmentManager* commit_manager)
    : _node_id(node_id), _nentries(nentries), _commit_manager(commit_manager)
{
}

void LeaderStableClosure::Run() {
    if (status().ok()) {
        if (_commit_manager) {
            // commit_manager check quorum ok, will call fsm_caller
            _commit_manager->set_stable_at_peer(
                    _first_log_index, _first_log_index + _nentries - 1, _node_id.peer_id);
        }
    } else {
        LOG(ERROR) << "node " << _node_id << " append [" << _first_log_index << ", "
                   << _first_log_index + _nentries - 1 << "] failed";
    }
    delete this;
}

void NodeImpl::apply(LogEntryAndClosure tasks[], size_t size) {
    std::vector<LogEntry*> entries;
    entries.reserve(size);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_state != STATE_LEADER) {
        butil::Status st;
        if (_state != STATE_TRANSFERING) {
            st.set_error(EPERM, "is not leader");
        } else {
            st.set_error(EBUSY, "is transfering leadership");
        }
        lck.unlock();
        RAFT_VLOG << "node " << _group_id << ":" << _server_id << " can't apply : " << st;
        for (size_t i = 0; i < size; ++i) {
            tasks[i].entry->Release();
            if (tasks[i].done) {
                tasks[i].done->status() = st;
                run_closure_in_bthread(tasks[i].done);
            }
        }
        return;
    }
    for (size_t i = 0; i < size; ++i) {
        if (tasks[i].expected_term != -1 && tasks[i].expected_term != _current_term) {
            RAFT_VLOG << "node " << _group_id << ":" << _server_id
                      << " can't apply taks whose expected_term=" << tasks[i].expected_term
                      << " doesn't match current_term=" << _current_term;
            if (tasks[i].done) {
                tasks[i].done->status().set_error(
                        EPERM, "expected_term=%ld doesn't match current_term=%ld",
                        tasks[i].expected_term, _current_term);
                run_closure_in_bthread(tasks[i].done);
            }
            tasks[i].entry->Release();
            continue;
        }
        entries.push_back(tasks[i].entry);
        entries.back()->id.term = _current_term;
        entries.back()->type = ENTRY_TYPE_DATA;
        _commit_manager->append_pending_task(_conf.second, tasks[i].done);
    }
    _log_manager->append_entries(&entries,
                               new LeaderStableClosure(
                                        NodeId(_group_id, _server_id),
                                        entries.size(),
                                        _commit_manager));
    // update _conf.first
    _log_manager->check_and_set_configuration(&_conf);
}

void NodeImpl::unsafe_apply_configuration(const Configuration& new_conf,
                                          Closure* done) {
    CHECK(!_conf_ctx.empty());
    LogEntry* entry = new LogEntry();
    entry->AddRef();
    entry->id.term = _current_term;
    entry->type = ENTRY_TYPE_CONFIGURATION;
    entry->peers = new std::vector<PeerId>;
    new_conf.list_peers(entry->peers);
    ConfigurationChangeDone* configuration_change_done =
            new ConfigurationChangeDone(this, _current_term, done);
    // Use the new_conf to deal the quorum of this very log
    std::vector<PeerId> old_peers;
    _conf.second.list_peers(&old_peers);
    _commit_manager->append_pending_task(new_conf, configuration_change_done);

    std::vector<LogEntry*> entries;
    entries.push_back(entry);
    _log_manager->append_entries(&entries,
                                 new LeaderStableClosure(
                                        NodeId(_group_id, _server_id),
                                        1u,
                                        _commit_manager));
    // update _conf.first
    _log_manager->check_and_set_configuration(&_conf);
}

int NodeImpl::handle_pre_vote_request(const RequestVoteRequest* request,
                                      RequestVoteResponse* response) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    
    if (!is_active_state(_state)) {
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id 
            << " is not in active state " << "current_term " << saved_current_term 
            << " state " << state2str(saved_state);
        return EINVAL;
    }

    PeerId candidate_id;
    if (0 != candidate_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received PreVote from " << request->server_id()
            << " server_id bad format";
        return EINVAL;
    }

    bool granted = false;
    do {
        if (request->term() < _current_term) {
            // ignore older term
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                << " ignore PreVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            break;
        }

        // get last_log_id outof node mutex
        lck.unlock();
        LogId last_log_id = _log_manager->last_log_id(true);
        lck.lock();
        // pre_vote not need ABA check after unlock&lock

        granted = (LogId(request->last_log_index(), request->last_log_term())
                        >= last_log_id);

        LOG(INFO) << "node " << _group_id << ":" << _server_id
            << " received PreVote from " << request->server_id()
            << " in term " << request->term()
            << " current_term " << _current_term
            << " granted " << granted;

    } while (0);

    response->set_term(_current_term);
    response->set_granted(granted);
    return 0;
}

int NodeImpl::handle_request_vote_request(const RequestVoteRequest* request,
                                          RequestVoteResponse* response) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    if (!is_active_state(_state)) {
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id 
            << " is not in active state " << "current_term " << saved_current_term 
            << " state " << state2str(saved_state);
        return EINVAL;
    }

    PeerId candidate_id;
    if (0 != candidate_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received RequestVote from " << request->server_id()
            << " server_id bad format";
        return EINVAL;
    }

    do {
        // check term
        if (request->term() >= _current_term) {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                << " received RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            // incress current term, change state to follower
            if (request->term() > _current_term) {
                butil::Status status;
                status.set_error(EHIGHERTERMREQUEST, "Raft node receives higher term "
                        "request_vote_request.");
                step_down(request->term(), false, status);
            }
        } else {
            // ignore older term
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                << " ignore RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            break;
        }

        // get last_log_id outof node mutex
        lck.unlock();
        LogId last_log_id = _log_manager->last_log_id(true);
        lck.lock();
        // vote need ABA check after unlock&lock
        if (request->term() != _current_term) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " raise term " << _current_term << " when get last_log_id";
            break;
        }

        bool log_is_ok = (LogId(request->last_log_index(), request->last_log_term())
                          >= last_log_id);
        // save
        if (log_is_ok && _voted_id.is_empty()) {
            butil::Status status;
            status.set_error(EVOTEFORCANDIDATE, "Raft node votes for some candidate, "
                    "step down to restart election_timer.");
            step_down(request->term(), false, status);
            _voted_id = candidate_id;
            //TODO: outof lock
            _stable_storage->set_votedfor(candidate_id);
        }
    } while (0);

    response->set_term(_current_term);
    response->set_granted(request->term() == _current_term && _voted_id == candidate_id);
    return 0;
}

class FollowerStableClosure : public LogManager::StableClosure {
public:
    FollowerStableClosure(
            brpc::Controller* cntl,
            const AppendEntriesRequest* request,
            AppendEntriesResponse* response,
            google::protobuf::Closure* done,
            NodeImpl* node,
            int64_t term)
        : _cntl(cntl)
        , _request(request)
        , _response(response)
        , _done(done)
        , _node(node)
        , _term(term)
    {
        _node->AddRef();
    }
    void Run() {
        run();
        delete this;
    }
private:
    ~FollowerStableClosure() {
        if (_node) {
            _node->Release();
        }
    }
    void run() {
        brpc::ClosureGuard done_guard(_done);
        if (!status().ok()) {
            _cntl->SetFailed(status().error_code(), "%s",
                             status().error_cstr());
            return;
        }
        std::unique_lock<raft_mutex_t> lck(_node->_mutex);
        if (_term != _node->_current_term) {
            // The change of term indicates that leader has been changed during
            // appending entries, so we can't respond ok to the old leader
            // because we are not sure if the appended logs would be truncated
            // by the new leader:
            //  - If they won't be truncated and we respond failure to the old
            //    leader, the new leader would know that they are stored in this
            //    peer and they will be eventually committed when the new leader
            //    found that quorum of the cluster have stored.
            //  - If they will be truncated and we responded success to the old
            //    leader, the old leader would possibly regard those entries as
            //    committed (very likely in a 3-nodes cluster) and respond
            //    success to the clients, which would break the rule that
            //    committed entries would never be truncated.
            // So we have to respond failure to the old leader and set the new
            // term to make it stepped down if it didn't.
            _response->set_success(false);
            _response->set_term(_node->_current_term);
            return;
        }
        // It's safe to release lck as we know everything is ok at this point.
        lck.unlock();

        // DON'T touch _node any more
        _response->set_success(true);
        _response->set_term(_term);

        const int64_t committed_index =
                std::min(_request->committed_index(),
                         // ^^^ committed_index is likely less than the
                         // last_log_index
                         _request->prev_log_index() + _request->entries_size()
                         // ^^^ The logs after the appended entries are
                         // untrustable so we can't commit them even if their
                         // indexes are less than request->committed_index()
                        );
        //_commit_manager is thread safe and tolerats disorder.
        _node->_commit_manager->set_last_committed_index(committed_index);
    }

    brpc::Controller* _cntl;
    const AppendEntriesRequest* _request;
    AppendEntriesResponse* _response;
    google::protobuf::Closure* _done;
    NodeImpl* _node;
    int64_t _term;
};

void NodeImpl::handle_append_entries_request(brpc::Controller* cntl,
                                             const AppendEntriesRequest* request,
                                             AppendEntriesResponse* response,
                                             google::protobuf::Closure* done) {
    butil::IOBuf data_buf;
    data_buf.swap(cntl->request_attachment());
    std::vector<LogEntry*> entries;
    entries.reserve(request->entries_size());
    brpc::ClosureGuard done_guard(done);
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // pre set term, to avoid get term in lock
    response->set_term(_current_term);

    if (!is_active_state(_state)) {
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id 
            << " is not in active state " << "current_term " << saved_current_term 
            << " state " << state2str(saved_state);
        cntl->SetFailed(EINVAL, "node %s:%s is not in active state, state %s", 
                _group_id.c_str(), _server_id.to_string().c_str(), state2str(saved_state));
        return;
    }

    PeerId server_id;
    if (0 != server_id.parse(request->server_id())) {
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received AppendEntries from " << request->server_id()
            << " server_id bad format";
        cntl->SetFailed(brpc::EREQUEST,
                        "Fail to parse server_id `%s'",
                        request->server_id().c_str());
        return;
    }

    // check stale term
    if (request->term() < _current_term) {
        const int64_t saved_current_term = _current_term;
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " ignore stale AppendEntries from " << request->server_id()
            << " in term " << request->term()
            << " current_term " << saved_current_term;
        response->set_success(false);
        response->set_term(saved_current_term);
        return;
    }

    // check term and state to step down
    check_step_down(request->term(), server_id);   
     
    if (server_id != _leader_id) {
        LOG(ERROR) << "Another peer=" << server_id
                   << " declares that it is the leader at term="
                   << _current_term << " which was occupied by leader="
                   << _leader_id;
        // Increase the term by 1 and make both leaders step down to minimize the
        // loss of split brain
        butil::Status status;
        status.set_error(ELEADERCONFLICT, "More than one leader in the same term."); 
        step_down(request->term() + 1, false, status);
        response->set_success(false);
        response->set_term(request->term() + 1);
        return;
    }

    _last_leader_timestamp = butil::monotonic_time_ms();

    if (request->entries_size() > 0 &&
            (_snapshot_executor
                && _snapshot_executor->is_installing_snapshot())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received append entries while installing snapshot";
        cntl->SetFailed(EBUSY, "Is installing snapshot");
        return;
    }

    const int64_t prev_log_index = request->prev_log_index();
    const int64_t prev_log_term = request->prev_log_term();
    const int64_t local_prev_log_term = _log_manager->get_term(prev_log_index);
    if (local_prev_log_term != prev_log_term) {
        int64_t last_index = _log_manager->last_log_index();
        response->set_success(false);
        response->set_term(_current_term);
        response->set_last_log_index(last_index);
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " reject term_unmatched AppendEntries from " << request->server_id()
            << " in term " << request->term()
            << " prev_log_index " << request->prev_log_index()
            << " prev_log_term " << request->prev_log_term()
            << " local_prev_log_term " << local_prev_log_term
            << " last_log_index " << last_index
            << " entries_size " << request->entries_size();
        return;
    }

    if (request->entries_size() == 0) {
        response->set_success(true);
        response->set_term(_current_term);
        response->set_last_log_index(_log_manager->last_log_index());
        lck.unlock();
        // see the comments at FollowerStableClosure::run()
        _commit_manager->set_last_committed_index(
                std::min(request->committed_index(),
                         prev_log_index));
        return;
    }

    // Parse request
    int64_t index = prev_log_index;
    for (int i = 0; i < request->entries_size(); i++) {
        index++;
        const EntryMeta& entry = request->entries(i);
        if (entry.type() != ENTRY_TYPE_UNKNOWN) {
            LogEntry* log_entry = new LogEntry();
            log_entry->AddRef();
            log_entry->id.term = entry.term();
            log_entry->id.index = index;
            log_entry->type = (EntryType)entry.type();
            if (entry.peers_size() > 0) {
                log_entry->peers = new std::vector<PeerId>;
                for (int i = 0; i < entry.peers_size(); i++) {
                    log_entry->peers->push_back(entry.peers(i));
                }
                CHECK_EQ(log_entry->type, ENTRY_TYPE_CONFIGURATION);
            } else {
                CHECK_NE(entry.type(), ENTRY_TYPE_CONFIGURATION);
            }
            if (entry.has_data_len()) {
                int len = entry.data_len();
                data_buf.cutn(&log_entry->data, len);
            }
            entries.push_back(log_entry);
        }
    }
    FollowerStableClosure* c = new FollowerStableClosure(
            cntl, request, response, done_guard.release(),
            this, _current_term);
    _log_manager->append_entries(&entries, c);

    // update configuration after _log_manager updated its memory status
    _log_manager->check_and_set_configuration(&_conf);
}

int NodeImpl::increase_term_to(int64_t new_term, const butil::Status& status) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (new_term <= _current_term) {
        return EINVAL;
    }
    step_down(new_term, false, status);
    return 0;
}

void NodeImpl::after_shutdown(NodeImpl* node) {
    return node->after_shutdown();
}

void NodeImpl::after_shutdown() {
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

void NodeImpl::handle_install_snapshot_request(brpc::Controller* controller,
                                    const InstallSnapshotRequest* request,
                                    InstallSnapshotResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;

    if (_snapshot_executor == NULL) {
        cntl->SetFailed(EINVAL, "Not support snapshot");
        return;
    }
    PeerId server_id;
    if (0 != server_id.parse(request->server_id())) {
        cntl->SetFailed(brpc::EREQUEST, "Fail to parse server_id=`%s'",
                        request->server_id().c_str());
        return;
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    
    if (!is_active_state(_state)) {
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id 
            << " is not in active state " << "current_term " << saved_current_term 
            << " state " << state2str(saved_state);
        cntl->SetFailed(EINVAL, "node %s:%s is not in active state, state %s", 
                _group_id.c_str(), _server_id.to_string().c_str(), state2str(saved_state));
        return;
    }

    // check stale term
    if (request->term() < _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " ignore stale AppendEntries from " << request->server_id()
            << " in term " << request->term()
            << " current_term " << _current_term;
        response->set_term(_current_term);
        response->set_success(false);
        return;
    }
    
    check_step_down(request->term(), server_id);

    if (server_id != _leader_id) {
        LOG(ERROR) << "Another peer=" << server_id
                   << " declares that it is the leader at term="
                   << _current_term << " which was occupied by leader="
                   << _leader_id;
        // Increase the term by 1 and make both leaders step down to minimize the
        // loss of split brain
        butil::Status status;
        status.set_error(ELEADERCONFLICT, "More than one leader in the same term."); 
        step_down(request->term() + 1, false, status);
        response->set_success(false);
        response->set_term(request->term() + 1);
        return;
    }
    lck.unlock();
    LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " received InstallSnapshotRequest"
              << " last_included_log_index="
              << request->meta().last_included_index()
              << " last_include_log_term="
              << request->meta().last_included_term()
              << " from " << server_id
              << " when last_log_id=" << _log_manager->last_log_id();
    return _snapshot_executor->install_snapshot(
            cntl, request, response, done_guard.release());
}

void NodeImpl::update_configuration_after_installing_snapshot() {
    BAIDU_SCOPED_LOCK(_mutex);
    _log_manager->check_and_set_configuration(&_conf);
}

butil::Status NodeImpl::read_committed_user_log(const int64_t index, UserLog* user_log) {
    if (index <= 0) {
        return butil::Status(EINVAL, "request index:%ld is invalid.", index);
    }
    const int64_t saved_last_applied_index = _fsm_caller->last_applied_index();
    if (index > saved_last_applied_index) {
        return butil::Status(ENOMOREUSERLOG, "request index:%ld is greater"
                " than last_applied_index:%ld.", index, saved_last_applied_index);
    }
    int64_t cur_index = index;
    LogEntry* entry = _log_manager->get_entry(cur_index);
    if (entry == NULL){
        return butil::Status(ELOGDELETED, "user log is deleted at index:%ld.", index);
    }
    do {
        if (entry->type == ENTRY_TYPE_DATA){
            user_log->set_log_index(cur_index);
            user_log->set_log_data(entry->data);
            entry->Release();
            return butil::Status();
        } else {
            entry->Release();
            ++cur_index;
        }
        if (cur_index > saved_last_applied_index) {
            return butil::Status(ENOMOREUSERLOG, "no user log between index:%ld"
                    " and last_applied_index:%ld.", index, saved_last_applied_index);
        }
        entry = _log_manager->get_entry(cur_index);
    } while (entry != NULL);
    // entry is likely to be NULL because snapshot is done after 
    // getting saved_last_applied_index.
    return butil::Status(ELOGDELETED, "user log is deleted at index:%ld.", cur_index);
}

void NodeImpl::describe(std::ostream& os, bool use_html) {
    PeerId leader;
    std::vector<ReplicatorId> replicators;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    const State st = _state;
    if (st == STATE_FOLLOWER) {
        leader = _leader_id;
    }
    const int64_t term = _current_term;
    const int64_t conf_index = _conf.first.index;
    //const int ref_count = ref_count_;
    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    // No replicator attached to nodes that are not leader;
    _replicator_group.list_replicators(&replicators);
    const int64_t leader_timestamp = _last_leader_timestamp;
    lck.unlock();
    const char *newline = use_html ? "<br>" : "\r\n";
    os << "state: " << state2str(st) << newline;
    os << "term: " << term << newline;
    os << "conf_index: " << conf_index << newline;
    os << "peers:";
    for (size_t j = 0; j < peers.size(); ++j) {
        os << ' ';
        if (use_html && peers[j] != _server_id) {
            os << "<a href=\"http://" << peers[j].addr
               << "/raft_stat/" << _group_id << "\">";
        }
        os << peers[j];
        if (use_html && peers[j] != _server_id) {
            os << "</a>";
        }
    }
    os << newline;  // newline for peers

    if (st == STATE_FOLLOWER) {
        os << "leader: ";
        if (use_html) {
            os << "<a href=\"http://" << leader.addr
               << "/raft_stat/" << _group_id << "\">"
               << leader << "</a>";
        } else {
            os << leader;
        }
        os << newline;
        os << "last_msg_to_now: " << butil::monotonic_time_ms() - leader_timestamp 
           << newline;
    }

    // Show timers
    os << "election_timer: ";
    _election_timer.describe(os, use_html);
    os << newline;
    os << "vote_timer: ";
    _vote_timer.describe(os, use_html);
    os << newline;
    os << "stepdown_timer: ";
    _stepdown_timer.describe(os, use_html);
    os << newline;
    os << "snapshot_timer: ";
    _snapshot_timer.describe(os, use_html);
    os << newline;

    _log_manager->describe(os, use_html);
    _fsm_caller->describe(os, use_html);
    _commit_manager->describe(os, use_html);
    if (_snapshot_executor) {
        _snapshot_executor->describe(os, use_html);
    }
    for (size_t i = 0; i < replicators.size(); ++i) {
        Replicator::describe(replicators[i], os, use_html);
    }
}

// Timers
int NodeTimer::init(NodeImpl* node, int timeout_ms) {
    RAFT_RETURN_IF(RepeatedTimerTask::init(timeout_ms) != 0, -1);
    _node = node;
    node->AddRef();
    return 0;
}

void NodeTimer::on_destroy() {
    if (_node) {
        _node->Release();
        _node = NULL;
    }
}

void ElectionTimer::run() {
    _node->handle_election_timeout();
}

int ElectionTimer::adjust_timeout_ms(int timeout_ms) {
    return random_timeout(timeout_ms);
}

void VoteTimer::run() {
    _node->handle_vote_timeout();
}

int VoteTimer::adjust_timeout_ms(int timeout_ms) {
    return random_timeout(timeout_ms);
}

void StepdownTimer::run() {
    _node->handle_stepdown_timeout();
}

void SnapshotTimer::run() {
    _node->handle_snapshot_timeout();
}

}  //  namespace braft
