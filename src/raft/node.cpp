// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/08 17:00:05

#include <bthread_unstable.h>
#include <baidu/rpc/errno.pb.h>
#include <baidu/rpc/controller.h>
#include <baidu/rpc/channel.h>

#include "raft/util.h"
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/log.h"
#include "raft/stable.h"
#include "raft/snapshot.h"
#include "raft/file_service.h"
#include "raft/builtin_service_impl.h"
#include "raft/node_manager.h"
#include "raft/snapshot_executor.h"

namespace raft {

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

static ::bvar::LatencyRecorder g_node_contention("raft_node_contention");

static inline int random_timeout(int timeout_ms) {
    return base::fast_rand_in(timeout_ms, timeout_ms << 1);
}

DEFINE_int32(raft_election_heartbeat_factor, 10, "raft election:heartbeat timeout factor");
static inline int heartbeat_timeout(int election_timeout) {
    return std::max(election_timeout / FLAGS_raft_election_heartbeat_factor, 10);
}

static inline int vote_timeout(int election_timeout) {
    return random_timeout(std::max(election_timeout / FLAGS_raft_election_heartbeat_factor, 1));
}

NodeImpl::NodeImpl(const GroupId& group_id, const PeerId& peer_id)
    : _state(SHUTDOWN)
    , _current_term(0)
    , _last_leader_timestamp(base::monotonic_time_ms())
    , _group_id(group_id)
    , _log_storage(NULL)
    , _stable_storage(NULL)
    , _closure_queue(NULL)
    , _config_manager(NULL)
    , _log_manager(NULL)
    , _fsm_caller(NULL)
    , _commit_manager(NULL)
    , _snapshot_executor(NULL)
    , _vote_triggered(false) {
        _server_id = peer_id;
        AddRef();
        _mutex.set_recorder(g_node_contention);
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
    return _snapshot_executor->init(opt);
}

int NodeImpl::init_log_storage() {
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

static void on_snapshot_timer(void* arg) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);

    node->handle_snapshot_timeout();
    node->Release();
}

void NodeImpl::handle_snapshot_timeout() {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (!is_active_state(_state)) {
        return;
    }

    do_snapshot(NULL);

    AddRef();
    raft_timer_add(&_snapshot_timer,
                      base::milliseconds_from_now(_options.snapshot_interval * 1000),
                      on_snapshot_timer, this);
    RAFT_VLOG << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " restart snapshot_timer";
}

int NodeImpl::init(const NodeOptions& options) {
    int ret = 0;

    // check _server_id
    if (base::IP_ANY == _server_id.addr.ip) {
        LOG(ERROR) << "Node can't started from IP_ANY";
        return -1;
    }

    if (!NodeManager::GetInstance()->server_exists(_server_id.addr)) {
        LOG(ERROR) << "No RPC Server attached to " << _server_id.addr
                   << ", did you forget to call raft::add_service()?";
        return -1;
    }

    _options = options;

    _config_manager = new ConfigurationManager();
    
    bthread::ExecutionQueueOptions opt;
    opt.max_tasks_size = 256;
    if (bthread::execution_queue_start(&_apply_queue_id, &opt, 
                                       execute_applying_tasks, this) != 0) {
        LOG(ERROR) << "Fail to start execution_queue";
        return -1;
    }

    _apply_queue = execution_queue_address(_apply_queue_id);
    if (!_apply_queue) {
        LOG(ERROR) << "Fail to address execution_queue";
        return -1;
    }

    do {
        // log storage and log manager init
        ret = init_log_storage();
        if (0 != ret) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                << " init_log_storage failed";
            break;
        }

        // stable init
        ret = init_stable_storage();
        if (0 != ret) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                << " init_stable_storage failed";
            break;
        }

        _closure_queue = new ClosureQueue();

        // fsm caller init, node AddRef in init
        _fsm_caller = new FSMCaller();
        FSMCallerOptions fsm_caller_options;
        this->AddRef();
        fsm_caller_options.after_shutdown = 
            baidu::rpc::NewCallback<NodeImpl*>(after_shutdown, this);
        fsm_caller_options.log_manager = _log_manager;
        fsm_caller_options.fsm = _options.fsm;
        fsm_caller_options.closure_queue = _closure_queue;
        ret = _fsm_caller->init(fsm_caller_options);
        if (ret != 0) {
            delete fsm_caller_options.after_shutdown;
            this->Release();
            break;
        }

        // commitment manager init
        _commit_manager = new CommitmentManager();
        CommitmentManagerOptions commit_manager_options;
        commit_manager_options.waiter = _fsm_caller;
        commit_manager_options.closure_queue = _closure_queue;
        //commit_manager_options.last_committed_index = _last_snapshot_index;
        ret = _commit_manager->init(commit_manager_options);
        if (ret != 0) {
            // fsm caller init AddRef, here Release
            Release();
            break;
        }

        // snapshot storage init and load
        // NOTE: snapshot maybe discard entries when snapshot saved but not discard entries.
        //      init log storage before snapshot storage, snapshot storage will update configration
        ret = init_snapshot_storage();
        if (0 != ret) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                << " init_snapshot_storage failed";
            break;
        }

        _conf.first = LogId();
        // if have log using conf in log, else using conf in options
        if (_log_manager->last_log_index() > 0) {
            _log_manager->check_and_set_configuration(&_conf);
        } else {
            _conf.second = _options.conf;
        }

        //TODO: call fsm on_apply (_last_snapshot_index + 1, last_committed_index]
        // init replicator
        ReplicatorGroupOptions options;
        options.heartbeat_timeout_ms = heartbeat_timeout(_options.election_timeout);
        options.log_manager = _log_manager;
        options.commit_manager = _commit_manager;
        options.node = this;
        options.snapshot_storage = _snapshot_executor
                                   ?  _snapshot_executor->snapshot_storage()
                                   : NULL;
        _replicator_group.init(NodeId(_group_id, _server_id), options);

        // set state to follower
        _state = FOLLOWER;

        // add node to NodeManager
        if (!NodeManager::GetInstance()->add(this)) {
            LOG(WARNING) << "NodeManager add " << _group_id << ":" << _server_id << "failed";
            ret = EINVAL;
            break;
        }
        LOG(INFO) << "node " << _group_id << ":" << _server_id << " init,"
            << " term: " << _current_term
            << " last_log_id: " << _log_manager->last_log_id()
            << " conf: " << _conf.second;
        if (!_conf.second.empty()) {
            step_down(_current_term);
        }

        // start snapshot timer
        if (_snapshot_executor && _options.snapshot_interval > 0) {
            AddRef();
            raft_timer_add(&_snapshot_timer,
                              base::milliseconds_from_now(_options.snapshot_interval * 1000),
                              on_snapshot_timer, this);
            RAFT_VLOG << "node " << _group_id << ":" << _server_id
                << " term " << _current_term << " start snapshot_timer";
        }
    } while (0);

    return ret;
}

int NodeImpl::execute_applying_tasks(
        void* meta, LogEntryAndClosure* const tasks[], size_t size) {
    if (size == 0) {
        return 0;
    }
    ((NodeImpl*)meta)->apply(tasks, size);
    return 0;
}

void NodeImpl::apply(const Task& task) {
    LogEntry* entry = new LogEntry;
    entry->data.swap(*task.data);
    LogEntryAndClosure m;
    m.entry = entry;
    m.done = task.done;
    if (_apply_queue->execute(m, &bthread::TASK_OPTIONS_INPLACE) != 0) {
        task.done->status().set_error(EINVAL, "Node is down");
        entry->Release();
        return run_closure_in_bthread(task.done);
    }
}

void NodeImpl::on_configuration_change_done(int64_t term) {
    Configuration removed;
    Configuration added;
    BAIDU_SCOPED_LOCK(_mutex);
    if (_state != LEADER || term != _current_term) {
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
        step_down(_current_term);
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
                           const base::Status& st, Closure* done) {
    int error_code = st.error_code();
    const char* error_text = st.error_cstr();

    do {
        BAIDU_SCOPED_LOCK(_mutex);
        // CHECK term and status to avoid ABA problem
        if (_state != LEADER || term != _current_term) {
            // Reset error code if this was a successful callback.
            if (error_code == 0) {
                error_code = EINVAL;
                error_text = "leader stepped down";
            }
            // DON'T stop replicator here.
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
            (base::monotonic_time_ms() -  _replicator_group.last_response_timestamp(peer)) <=
            _options.election_timeout) {

            LOG(INFO) << "node " << _group_id << ":" << _server_id << " catching up " << peer;

            OnCaughtUp* caught_up = new OnCaughtUp(this, _current_term, peer, done);
            timespec due_time = base::milliseconds_from_now(_options.election_timeout);

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
            << "failed: " << error_code;

        _replicator_group.stop_replicator(peer);
        _conf_ctx.reset();
    } while (0);

    // call add_peer done when fail
    CHECK(_conf_ctx.empty());
    done->status().set_error(error_code, "caughtup failed: %s", error_text);
    done->Run();
}

static void on_stepdown_timer(void* arg) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);

    node->handle_stepdown_timeout();
    node->Release();
}

void NodeImpl::handle_stepdown_timeout() {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state != LEADER) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stop stepdown_timer"
            << " state not in LEADER but " << state2str(_state);
        return;
    }

    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    int64_t now_timestamp = base::monotonic_time_ms();
    size_t dead_count = 0;
    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            continue;
        }

        if (now_timestamp - _replicator_group.last_response_timestamp(peers[i]) >
            _options.election_timeout) {
            dead_count++;
        }
    }
    if (dead_count < (peers.size() / 2 + 1)) {
        AddRef();
        int64_t stepdown_timeout = _options.election_timeout;
        raft_timer_add(&_stepdown_timer, base::milliseconds_from_now(stepdown_timeout),
                          on_stepdown_timer, this);
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " restart stepdown_timer";
    } else {
        LOG(INFO) << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stepdown when quorum node dead";
        step_down(_current_term);
    }
}

bool NodeImpl::unsafe_register_conf_change(const std::vector<PeerId>& old_peers,
                                           const std::vector<PeerId>& new_peers,
                                           Closure* done) {
    if (_state != LEADER) {
        LOG(WARNING) << "[" << node_id()
                     << "] Refusing configuration changing because the state is "
                     << state2str(_state);
        if (done) {
            done->status().set_error(EPERM, "Not leader");
            run_closure_in_bthread(done);
        }
        return false;
    }

    // check concurrent conf change
    if (!_conf_ctx.empty()) {
        LOG(WARNING) << "[" << node_id() 
                     << " ] Refusing concurrent configuration changing";
        if (done) {
            done->status().set_error(EINVAL, "Doing another configuration change");
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
                     << "] Refusing configuration changing based on"
                        " wrong configuraiont";
        if (done) {
            done->status().set_error(EINVAL, "old_peers dismatch");
            run_closure_in_bthread(done);
        }
        return false;
    }
    _conf_ctx.peers = old_peers;
    return true;
}

void NodeImpl::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                        Closure* done) {
    Configuration new_conf(old_peers);
    new_conf.add_peer(peer);
    std::vector<PeerId> new_peers;
    new_conf.list_peers(&new_peers);
    BAIDU_SCOPED_LOCK(_mutex);
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
    timespec due_time = base::milliseconds_from_now(_options.election_timeout);

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
    Configuration new_conf(old_peers);
    new_conf.remove_peer(peer);
    std::vector<PeerId> new_peers;
    new_conf.list_peers(&new_peers);
    BAIDU_SCOPED_LOCK(_mutex);

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

int NodeImpl::set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers) {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    CHECK(is_active_state(_state))
        << "node " << _group_id << ":" << _server_id << " shutdown, can't set_peer";
    // check bootstrap
    if (_conf.second.empty()) {
        if (old_peers.size() == 0 && new_peers.size() > 0) {
            Configuration new_conf(new_peers);
            LOG(INFO) << "node " << _group_id << ":" << _server_id << " set_peer boot from "
                << new_conf;
            _conf.second = new_conf;
            step_down(1);
            return 0;
        } else {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " set_peer boot need old_peers empty and new_peers no_empty";
            return EINVAL;
        }
    }
    // check concurrent conf change
    if (_state == LEADER && !_conf_ctx.empty()) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set_peer need wait "
            "current conf change";
        return EINVAL;
    }
    // check equal, maybe retry direct return
    if (_conf.second.equals(new_peers)) {
        return 0;
    }
    // check not equal
    if (!_conf.second.equals(std::vector<PeerId>(old_peers))) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set_peer dismatch old_peers";
        return EINVAL;
    }
    // check quorum
    if (new_peers.size() >= (old_peers.size() / 2 + 1)) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set_peer new_peers greater "
            "than old_peers'quorum";
        return EINVAL;
    }
    // check contain
    if (!_conf.second.contains(new_peers)) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set_peer old_peers not "
            "contains all new_peers";
        return EINVAL;
    }

    Configuration new_conf(new_peers);
    LOG(INFO) << "node " << _group_id << ":" << _server_id << " set_peer from " << _conf.second
        << " to " << new_conf;
    // step down and change conf
    step_down(_current_term + 1);
    _conf.second = new_conf;
    return 0;
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
    // remove node from NodeManager, rpc will not touch Node
    NodeManager::GetInstance()->remove(this);

    {
        BAIDU_SCOPED_LOCK(_mutex);

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " shutdown,"
            " current_term " << _current_term << " state " << state2str(_state);

        if (is_active_state(_state)) {
            // leader stop disk thread and replicator, stop stepdown timer, change state to FOLLOWER
            // candidate stop vote timer, change state to FOLLOWER
            if (_state != FOLLOWER) {
                step_down(_current_term);
            }
            // change state to shutdown
            _state = SHUTTING;

            // follower stop election timer
            RAFT_VLOG << "node " << _group_id << ":" << _server_id
                << " term " << _current_term << " stop election_timer";
            int ret = raft_timer_del(_election_timer);
            if (ret == 0) {
                Release();
            }

            // all stop snapshot timer
            RAFT_VLOG << "node " << _group_id << ":" << _server_id
                << " term " << _current_term << " stop snapshot_timer";
            ret = raft_timer_del(_snapshot_timer);
            if (ret == 0) {
                Release();
            }

            // stop replicator and fsm_caller wait
            _log_manager->shutdown();

            // step_down will call _commitment_manager->clear_pending_applications(),
            // this can avoid send LogEntry with closure to fsm_caller.
            // fsm_caller shutdown will not leak user's closure.
            _fsm_caller->shutdown();//done);
        }
        if (_state != SHUTDOWN) {
            _shutdown_continuations.push_back(done);
            return;
        }
    }  // out of _mutex;

    run_closure_in_bthread(done);
}

static void on_election_timer(void* arg) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);

    node->handle_election_timeout();
    node->Release();
}

void NodeImpl::handle_election_timeout() {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state != FOLLOWER) {
        return;
    }
    // check timestamp, skip one cycle check when trigger vote
    if (!_vote_triggered &&
        base::monotonic_time_ms() - _last_leader_timestamp < _options.election_timeout) {
        AddRef();
        int64_t election_timeout = random_timeout(_options.election_timeout);
        raft_timer_add(&_election_timer, base::milliseconds_from_now(election_timeout),
                          on_election_timer, this);
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " restart election_timer";
        return;
    }

    _vote_triggered = false;

    // start pre_vote, need restart election_timer
    AddRef();
    int64_t election_timeout = random_timeout(_options.election_timeout);
    raft_timer_add(&_election_timer, base::milliseconds_from_now(election_timeout),
                      on_election_timer, this);
    RAFT_VLOG << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " restart election_timer";

    pre_vote();

    // first vote
    //elect_self();
}

void NodeImpl::vote(int election_timeout) {
    BAIDU_SCOPED_LOCK(_mutex);

    LOG(INFO) << "node " << _group_id << ":" << _server_id << " trigger-vote,"
        " current_term " << _current_term << " state " << state2str(_state) <<
        " election_timeout " << election_timeout;

    _vote_triggered = true;
    _options.election_timeout = election_timeout;
    //if (_state == LEADER) {
    //    step_down(_current_term);
    //} else {
    //    _leader_id.reset();
    //}

    int ret = raft_timer_del(_election_timer);
    if (ret == 0) {
        int64_t new_election_timeout = random_timeout(_options.election_timeout);
        raft_timer_add(&_election_timer, base::milliseconds_from_now(new_election_timeout),
                          on_election_timer, this);
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " restart election_timer";
    }
}

static void on_vote_timer(void* arg) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);

    node->handle_vote_timeout();
    node->Release();
}

void NodeImpl::handle_vote_timeout() {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state == CANDIDATE) {
        // retry vote
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " retry elect";
        elect_self();
    }
}

void NodeImpl::handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                            const RequestVoteResponse& response) {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state != CANDIDATE) {
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
        step_down(response.term());
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
    baidu::rpc::Controller cntl;
    NodeImpl* node;
};

void NodeImpl::handle_pre_vote_response(const PeerId& peer_id, const int64_t term,
                                            const RequestVoteResponse& response) {
    BAIDU_SCOPED_LOCK(_mutex);

    // check state
    if (_state != FOLLOWER) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid PreVoteResponse from " << peer_id
            << " state not in FOLLOWER but " << state2str(_state);
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
        step_down(response.term());
        return;
    }

    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " received PreVoteResponse from " << peer_id
        << " term " << response.term() << " granted " << response.granted();
    // check granted quorum?
    if (response.granted()) {
        _pre_vote_ctx.grant(peer_id);
        if (_pre_vote_ctx.quorum()) {
            elect_self();
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
                    << " PreVote to " << peer << " error: " << cntl.ErrorText();
                break;
            }
            node->handle_pre_vote_response(peer, term, response);
        } while (0);
        delete this;
    }

    PeerId peer;
    int64_t term;
    RequestVoteResponse response;
    baidu::rpc::Controller cntl;
    NodeImpl* node;
};

void NodeImpl::pre_vote() {
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term
        << " start pre_vote";
    if (_snapshot_executor && _snapshot_executor->is_installing_snapshot()) {
        LOG(WARNING) << "We don't do pre_vote when installing snapshot as the "
                     " configuration is possibly out of date";
        return;
    }
    if (!_conf.second.contains(_server_id)) {
        LOG(WARNING) << "node " << _group_id << ':' << _server_id
                     << " can't do pre_vote as it is not in " << _conf.second;
        return;
    }
    _pre_vote_ctx.reset();
    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    _pre_vote_ctx.set(peers.size());
    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            continue;
        }
        baidu::rpc::ChannelOptions options;
        options.connection_type = baidu::rpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0;
        baidu::rpc::Channel channel;
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
        const LogId last_log_id = _log_manager->last_log_id();
        request.set_last_log_index(last_log_id.index);
        request.set_last_log_term(last_log_id.term);

        OnPreVoteRPCDone* done = new OnPreVoteRPCDone(peers[i], _current_term, this);
        RaftService_Stub stub(&channel);
        stub.pre_vote(&done->cntl, &request, &done->response, done);
    }
    _pre_vote_ctx.grant(_server_id);

    if (_pre_vote_ctx.quorum()) {
        elect_self();
    }
}

// in lock
void NodeImpl::elect_self() {
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term
        << " start vote and grant vote self";
    // cancel follower election timer
    if (_state == FOLLOWER) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stop election_timer";
        int ret = raft_timer_del(_election_timer);
        if (ret == 0) {
            Release();
        } else {
            CHECK(ret == 1);
        }
    }

    // reset leader_id before vote
    _leader_id.reset();

    _state = CANDIDATE;
    _current_term++;
    _voted_id = _server_id;
    _vote_ctx.reset();

    AddRef();
    raft_timer_add(&_vote_timer,
                      base::milliseconds_from_now(vote_timeout(_options.election_timeout)),
                      on_vote_timer, this);
    RAFT_VLOG << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " start vote_timer";

    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    _vote_ctx.set(peers.size());
    for (size_t i = 0; i < peers.size(); i++) {
        if (peers[i] == _server_id) {
            continue;
        }
        baidu::rpc::ChannelOptions options;
        options.connection_type = baidu::rpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0;
        baidu::rpc::Channel channel;
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
        const LogId last_log_id = _log_manager->last_log_id();
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
void NodeImpl::step_down(const int64_t term) {
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " stepdown from " << state2str(_state)
        << " new_term " << term;

    if (_state == CANDIDATE) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stop vote_timer";
        int ret = raft_timer_del(_vote_timer);
        if (0 == ret) {
            Release();
        } else {
            CHECK(ret == 1);
        }
    } else if (_state == LEADER) {
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " stop stepdown_timer";
        int ret = raft_timer_del(_stepdown_timer);
        if (0 == ret) {
            Release();
        } else {
            CHECK(ret == 1);
        }

        _commit_manager->clear_pending_tasks();

        // signal fsm leader stop immediately
        _fsm_caller->on_leader_stop();
    }

    if (_snapshot_executor) {
        _snapshot_executor->interrupt_downloading_snapshot(term);
    }

    _state = FOLLOWER;
    _leader_id.reset();
    _current_term = term;
    _voted_id.reset();
    _conf_ctx.reset();
    //TODO: outof lock
    _stable_storage->set_term_and_votedfor(term, _voted_id);

    // no empty configuration, start election timer
    if (!_conf.second.empty() && _conf.second.contains(_server_id)) {
        // maybe called from follower, try del timer
        int ret = raft_timer_del(_election_timer);
        if (ret == 0) {
            Release();
        }
        AddRef();
        int64_t election_timeout = random_timeout(_options.election_timeout);
        raft_timer_add(&_election_timer, base::milliseconds_from_now(election_timeout),
                          on_election_timer, this);
        RAFT_VLOG << "node " << _group_id << ":" << _server_id
            << " term " << _current_term << " start election_timer";
    }

    // stop stagging new node
    _replicator_group.stop_all();
}

class LeaderStartClosure : public Closure {
public:
    LeaderStartClosure(StateMachine* fsm) : _fsm(fsm) {}
    ~LeaderStartClosure() {}
    void Run() {
        if (status().ok()) {
            _fsm->on_leader_start();
        }
        delete this;
    }
private:
    StateMachine* _fsm;
};

// in lock
void NodeImpl::become_leader() {
    CHECK(_state == CANDIDATE);
    LOG(INFO) << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " become leader, and stop vote_timer";
    // cancel candidate vote timer
    int ret = raft_timer_del(_vote_timer);
    if (0 == ret) {
        Release();
    } else {
        CHECK(ret == 1);
    }

    _state = LEADER;
    _leader_id = _server_id;

    _replicator_group.reset_term(_current_term);
    _replicator_group.reset_heartbeat_interval(heartbeat_timeout(_options.election_timeout));

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
                               new LeaderStartClosure(_options.fsm));

    AddRef();
    int64_t stepdown_timeout = _options.election_timeout;
    raft_timer_add(&_stepdown_timer, base::milliseconds_from_now(stepdown_timeout),
                      on_stepdown_timer, this);
    RAFT_VLOG << "node " << _group_id << ":" << _server_id
        << " term " << _current_term << " start stepdown_timer";
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
        // commit_manager check quorum ok, will call fsm_caller
        _commit_manager->set_stable_at_peer(
                _first_log_index, _first_log_index + _nentries - 1, _node_id.peer_id);
    } else {
        LOG(ERROR) << "node " << _node_id << " append [" << _first_log_index << ", "
                   << _first_log_index + _nentries - 1 << "] failed";
    }
    delete this;
}

void NodeImpl::apply(LogEntryAndClosure* const tasks[], size_t size) {
    std::vector<LogEntry*> entries;
    entries.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        entries.push_back(tasks[i]->entry);
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_state != LEADER) {
        lck.unlock();
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " can't apply not in LEADER";
        for (size_t i = 0; i < entries.size(); ++i) {
            entries[i]->Release();
            if (tasks[i]->done) {
                tasks[i]->done->status().set_error(EPERM, "Not leader");
                run_closure_in_bthread(tasks[i]->done);
            }
        }
        return;
    }
    for (size_t i = 0; i < size; ++i) {
        entries[i]->id.term = _current_term;
        entries[i]->type = ENTRY_TYPE_DATA;
        _commit_manager->append_pending_task(_conf.second, tasks[i]->done);
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
    BAIDU_SCOPED_LOCK(_mutex);

    PeerId candidate_id;
    if (0 != candidate_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received PreVote from " << request->server_id()
            << " server_id bad format";
        return EINVAL;
    }

    bool granted = false;
    do {
        // check leader to tolerate network partitions:
        //     1. leader always reject RequstVote
        //     2. follower reject RequestVote before change to candidate
        //if (!_leader_id.is_empty()) {
        //    LOG(WARNING) << "node " << _group_id << ":" << _server_id
        //        << " reject PreVote from " << request->server_id()
        //        << " in term " << request->term()
        //        << " current_term " << _current_term
        //        << " current_leader " << _leader_id;
        //    break;
        //}

        // check next term
        if (request->term() < _current_term) {
            // ignore older term
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                << " ignore PreVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            break;
        }

        LogId last_log_id = _log_manager->last_log_id();
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
    BAIDU_SCOPED_LOCK(_mutex);

    LogId last_log_id = _log_manager->last_log_id();
    bool log_is_ok = (LogId(request->last_log_index(), request->last_log_term()) 
                        >= last_log_id);
    PeerId candidate_id;
    if (0 != candidate_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received RequestVote from " << request->server_id()
            << " server_id bad format";
        return EINVAL;
    }

    do {
        // check leader to tolerate network partitioning:
        //     1. leader always reject RequstVote
        //     2. follower reject RequestVote before change to candidate
        //if (!_leader_id.is_empty()) {
        //    LOG(WARNING) << "node " << _group_id << ":" << _server_id
        //        << " reject RequestVote from " << request->server_id()
        //        << " in term " << request->term()
        //        << " current_term " << _current_term
        //        << " current_leader " << _leader_id;
        //    break;
        //}

        // check term
        if (request->term() >= _current_term) {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                << " received RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            // incress current term, change state to follower
            if (request->term() > _current_term) {
                step_down(request->term());
            }
        } else {
            // ignore older term
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                << " ignore RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            break;
        }

        // save
        if (log_is_ok && _voted_id.is_empty()) {
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
            baidu::rpc::Controller* cntl,
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
        baidu::rpc::ClosureGuard done_guard(_done);
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
            //    leader: the new leader would know that they are stored in this
            //    peer and they will be eventually committed when the new leader
            //    found that quorum of the cluster have stored.
            //  - If they will be truncated and we responed success to the old
            //    leader: the old leader would possibly regard thos entries as
            //    committed (very likely in a 3-nodes cluster) and respond
            //    success to the clients, which would break the rule that
            //    committed entries would never be truncated.
            // So we have to respond failure to the old leader and set the new
            // term to make it stepped down if it did't.
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

    baidu::rpc::Controller* _cntl;
    const AppendEntriesRequest* _request;
    AppendEntriesResponse* _response;
    google::protobuf::Closure* _done;
    NodeImpl* _node;
    int64_t _term;
};

void NodeImpl::handle_append_entries_request(baidu::rpc::Controller* cntl,
                                             const AppendEntriesRequest* request,
                                             AppendEntriesResponse* response,
                                             google::protobuf::Closure* done) {
    base::IOBuf data_buf;
    data_buf.swap(cntl->request_attachment());
    std::vector<LogEntry*> entries;
    entries.reserve(request->entries_size());
    baidu::rpc::ClosureGuard done_guard(done);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    
    // pre set term, to avoid get term in lock
    response->set_term(_current_term);

    PeerId server_id;
    if (0 != server_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received AppendEntries from " << request->server_id()
            << " server_id bad format";
        cntl->SetFailed(baidu::rpc::EREQUEST,
                        "Fail to parse server_id `%s'", 
                        request->server_id().c_str());
        return;
    }

    // check stale term
    if (request->term() < _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " ignore stale AppendEntries from " << request->server_id()
            << " in term " << request->term()
            << " current_term " << _current_term;
        response->set_success(false);
        response->set_term(_current_term);
        return;
    }

    // check term and state to step down
    if (request->term() > _current_term || _state != FOLLOWER) {
        step_down(request->term());
    }

    // save current leader
    if (_leader_id.is_empty()) {
        _leader_id = server_id;
    }

    // FIXME: is it conflicted with set_peer?
    CHECK_EQ(server_id, _leader_id) 
            << "Another peer=" << server_id 
            << " declares that it is the leader at term="
            << _current_term << " which is occupied by leader="
            << _leader_id;

    _last_leader_timestamp = base::monotonic_time_ms();
    if (request->entries_size() > 0 && 
            (_snapshot_executor 
                && _snapshot_executor->is_installing_snapshot())) {
        LOG(WARNING) << "Received append entries while installing snapshot";
        cntl->SetFailed(EBUSY, "Is installing snapshot");
        return;
    }

    const int64_t prev_log_index = request->prev_log_index();
    const int64_t prev_log_term = request->prev_log_term();
    if (_log_manager->get_term(prev_log_index) != prev_log_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " reject term_unmatched AppendEntries from " << request->server_id()
            << " in term " << request->term()
            << " prev_log_index " << request->prev_log_index()
            << " last_log_index " << _log_manager->last_log_index();
        response->set_success(false);
        response->set_term(_current_term);
        response->set_last_log_index(_log_manager->last_log_index());
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

int NodeImpl::increase_term_to(int64_t new_term) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (new_term <= _current_term) {
        return EINVAL;
    }
    step_down(new_term);
    return 0;
}

void NodeImpl::after_shutdown(NodeImpl* node) {
    return node->after_shutdown();
}

void NodeImpl::after_shutdown() {
    std::vector<Closure*> saved_done;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        CHECK_EQ(SHUTTING, _state);
        _state = SHUTDOWN;
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

void NodeImpl::handle_install_snapshot_request(baidu::rpc::Controller* controller,
                                    const InstallSnapshotRequest* request,
                                    InstallSnapshotResponse* response,
                                    google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    if (_snapshot_executor == NULL) {
        cntl->SetFailed(EINVAL, "Not support snapshot");
        return;
    }
    PeerId server_id;
    if (0 != server_id.parse(request->server_id())) {
        cntl->SetFailed(baidu::rpc::EREQUEST, "Fail to parse server_id=`%s'",
                        request->server_id().c_str());
        return;
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
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
    if (request->term() > _current_term || _state != FOLLOWER) {
        step_down(request->term());
        response->set_term(request->term());
    }

    // save current leader
    if (_leader_id.is_empty()) {
        _leader_id = server_id;
    }

    // FIXME: is it conflicted with set_peer?
    CHECK_EQ(server_id, _leader_id) 
            << "Another peer=" << server_id 
            << " declares that it is the leader at term="
            << _current_term << " which is occupied by leader="
            << _leader_id;
    lck.unlock();
    return _snapshot_executor->install_snapshot(
            cntl, request, response, done_guard.release());
}

void NodeImpl::update_configuration_after_installing_snapshot() {
    BAIDU_SCOPED_LOCK(_mutex);
    _log_manager->check_and_set_configuration(&_conf);
}

void NodeImpl::describe(std::ostream& os, bool use_html) {
    PeerId leader;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    const State st = _state;
    if (st == FOLLOWER) {
        leader = _leader_id;
    }
    const int64_t term = _current_term;
    const int64_t conf_index = _conf.first.index;
    //const int ref_count = ref_count_;
    std::vector<PeerId> peers;
    _conf.second.list_peers(&peers);
    lck.unlock();
    const char *newline = use_html ? "<br>" : "\r\n";
    os << "state: " << state2str(st) << newline;
    os << "term: " << term << newline;
    os << "conf_index: " << conf_index << newline;
    os << "peers:";
    for (size_t j = 0; j < peers.size(); ++j) {
        if (peers[j] != _server_id) {  // Excludes self 
            os << ' ';
            if (use_html) {
                os << "<a href=\"http://" << peers[j].addr 
                   << "/raft_stat/" << _group_id << "\">";
            }
            os << peers[j];
            if (use_html) {
                os << "</a>";
            }
        }
    }
    os << newline;  // newline for peers

    if (st == FOLLOWER) {
        os << "leader: ";
        if (use_html) {
            os << "<a href=\"http://" << leader.addr
                << "/raft_stat/" << _group_id << "\">"
                << leader << "</a>";
        } else {
            os << leader;
        }
        os << newline;
    }
    _log_manager->describe(os, use_html);
    _fsm_caller->describe(os, use_html);
    _commit_manager->describe(os, use_html);
    if (_snapshot_executor) {
        _snapshot_executor->describe(os, use_html);
    }
}

}  // namespace raft
