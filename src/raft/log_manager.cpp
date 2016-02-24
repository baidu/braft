// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/14 11:45:23

#include "raft/log_manager.h"

#include <base/logging.h>
#include <bthread.h>
#include <bthread_unstable.h>
#include <baidu/rpc/reloadable_flags.h>
#include "raft/log_entry.h"
#include "raft/util.h"
#include "raft/storage.h"
#include "raft/node.h"
#include "raft/util.h"

namespace raft {

DEFINE_int32(raft_leader_batch, 256, "max leader io batch");
BAIDU_RPC_VALIDATE_GFLAG(raft_leader_batch, ::baidu::rpc::PositiveInteger);

static bvar::LatencyRecorder g_log_manager_contention_recorder
            ("raft_log_manager_contention");
static bvar::LatencyRecorder g_log_manager_modify_storage_contention_recorder
            ("raft_log_manager_modify_storage_contention");
static bvar::Adder<int64_t> g_read_entry_from_storage
            ("raft_read_entry_from_storage_count");
static bvar::PerSecond<bvar::Adder<int64_t> > g_read_entry_from_storage_second
            ("raft_read_entry_from_storage_second", &g_read_entry_from_storage);

static bvar::Adder<int64_t> g_read_term_from_storage
            ("raft_read_term_from_storage_count");
static bvar::PerSecond<bvar::Adder<int64_t> > g_read_term_from_storage_second
            ("raft_read_term_from_storage_second", &g_read_term_from_storage);

LogManagerOptions::LogManagerOptions()
    : log_storage(NULL), configuration_manager(NULL)
{}

LogManager::LogManager()
    : _log_storage(NULL)
    , _config_manager(NULL)
    , _disk_index(0)
    , _applied_index(0)
    , _first_log_index(0)
    , _last_log_index(0)
    , _last_snapshot_index(0)
    , _last_snapshot_term(0)
    , _leader_disk_thread_running(false)
    , _stopped(false)
{
    CHECK_EQ(0, bthread_id_list_init(&_wait_list, 16/*FIXME*/, 16));
    _mutex.set_recorder(g_log_manager_contention_recorder);
    _modify_storage_mutex.set_recorder(
            g_log_manager_modify_storage_contention_recorder);
}

int LogManager::init(const LogManagerOptions &options) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (options.log_storage == NULL) {
        return EINVAL;
    }
    _log_storage = options.log_storage;
    _config_manager = options.configuration_manager;
    int ret = _log_storage->init(_config_manager);
    if (ret != 0) {
        return ret;
    }
    _first_log_index = _log_storage->first_log_index();
    _last_log_index = _log_storage->last_log_index();
    _disk_index.store(_last_log_index, boost::memory_order_relaxed);
    return 0;
}

LogManager::~LogManager() {
    bthread_id_list_destroy(&_wait_list);
}

int LogManager::start_disk_thread() {
    RAFT_VLOG << "disk_thread start";
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(!_leader_disk_thread_running);
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    queue_options.max_tasks_size = FLAGS_raft_leader_batch;
    int ret = bthread::execution_queue_start(&_leader_disk_queue,
                                   &queue_options,
                                   leader_disk_run,
                                   this);
    if (ret == 0) {
        _leader_disk_thread_running = true;
    } else {
        LOG(FATAL) << "start_disk_thread failed";
    }
    return ret;
}

int LogManager::stop_disk_thread() {
    RAFT_VLOG << "disk_thread stop";
    std::unique_lock<raft_mutex_t> lck(_mutex);
    bthread::ExecutionQueueId<StableClosure*> saved_queue = _leader_disk_queue;
    // FIXME: see the comments in truncate_prefix
    lck.unlock();
    bthread::execution_queue_stop(saved_queue);
    int ret = bthread::execution_queue_join(saved_queue);
    if (0 == ret) {
        lck.lock();
        _leader_disk_thread_running = false;
    }
    return ret;
    // _mutex is unlock by guard
}

void LogManager::clear_memory_logs(const int64_t index) {
    LogEntry* entries_to_clear[32];
    size_t nentries = 0;
    do {
        nentries = 0;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            while (!_logs_in_memory.empty() 
                    && nentries < ARRAY_SIZE(entries_to_clear)) {
                LogEntry* entry = _logs_in_memory.front();
                if (entry->index > index) {
                    break;
                }
                entries_to_clear[nentries++] = entry;
                _logs_in_memory.pop_front();
            }
        }  // out of _mutex
        for (size_t i = 0; i < nentries; ++i) {
            entries_to_clear[i]->Release();
        }
    } while (nentries == ARRAY_SIZE(entries_to_clear));
}

int64_t LogManager::first_log_index() {
    BAIDU_SCOPED_LOCK(_mutex);
    return _first_log_index;
}

int64_t LogManager::last_log_index() {
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_last_log_index != 0) {
            return _last_log_index;
        }
    }
    return _log_storage->last_log_index();
}

class TruncatePrefixClosure : public LogManager::StableClosure {
public:
    explicit TruncatePrefixClosure(int64_t first_index_kept)
        : _first_index_kept(first_index_kept)
    {}
    void Run() {
        delete this;
    }
    int64_t first_index_kept() const { return _first_index_kept; }
private:
    int64_t _first_index_kept;
};

class ResetClosure : public LogManager::StableClosure {
public:
    explicit ResetClosure(int64_t next_log_index)
        : _next_log_index(next_log_index)
    {}
    void Run() {
        delete this;
    }
    int64_t next_log_index() const { return _next_log_index; }
private:
    int64_t _next_log_index;
};

int LogManager::truncate_prefix(const int64_t first_index_kept,
                                std::unique_lock<raft_mutex_t>& lck) {
    base::Timer timer;
    timer.start();
    CHECK(lck.owns_lock());
    if (first_index_kept < _first_log_index) {
        return EINVAL;
    }
    // As the duration between two snapshot (which leads to truncate_prefix at
    // last) is likely to be a long period, _logs_in_memory is likely to
    // contain a large amount of logs to release, which holds the mutex so that
    // all the replicator/application are blocked.
    // FIXME(chenzhangyi01): to resolve this issue, we have to build a data
    // structure which is able to pop_front/pop_back N elements into another
    // container in O(1) time, one solution is a segmented double-linked list
    // along with a bounded queue as the indexer, of which the payoff is that
    // _logs_in_memory has to be bounded.
    while (!_logs_in_memory.empty()) {
        LogEntry* entry = _logs_in_memory.front();
        if (entry->index < first_index_kept) {
            entry->Release();
            _logs_in_memory.pop_front();
        } else {
            break;
        }
    }
    _first_log_index  = first_index_kept;
    if (first_index_kept > _last_log_index) {
        // The entrie log is dropped
        _last_log_index = first_index_kept - 1;
    }
    _config_manager->truncate_prefix(first_index_kept);
    if (_leader_disk_thread_running &&
        _disk_index.load(boost::memory_order_relaxed) < first_index_kept) {
        TruncatePrefixClosure* c = new TruncatePrefixClosure(first_index_kept);
        const int ret = bthread::execution_queue_execute(_leader_disk_queue, c);
        CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
    } else {
        // Acquire _modify_storage_mutex first before unlock lck to make sure
        // there's not the out-of-order issue
        BAIDU_SCOPED_LOCK(_modify_storage_mutex);
        lck.unlock();
        _log_storage->truncate_prefix(first_index_kept);
    }
    timer.stop();
    RAFT_VLOG << "truncate_prefix " << first_index_kept << " time: " << timer.u_elapsed();

    return 0;
}

int LogManager::reset(const int64_t next_log_index,
                      std::unique_lock<raft_mutex_t>& lck) {
    CHECK(lck.owns_lock());
    std::deque<LogEntry*> saved_logs_in_memory;
    saved_logs_in_memory.swap(_logs_in_memory);
    _first_log_index = next_log_index;
    _last_log_index = next_log_index - 1;
    _config_manager->truncate_prefix(_first_log_index);
    _config_manager->truncate_suffix(_last_log_index);
    if (_leader_disk_thread_running) {
        TruncatePrefixClosure* c = new TruncatePrefixClosure(next_log_index);
        const int ret = bthread::execution_queue_execute(_leader_disk_queue, c);
        lck.unlock();
        CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
    } else {
        BAIDU_SCOPED_LOCK(_modify_storage_mutex);
        lck.unlock();
        _log_storage->reset(next_log_index);
    }
    CHECK(!lck.owns_lock());
    for (size_t i = 0; i < saved_logs_in_memory.size(); ++i) {
        saved_logs_in_memory[i]->Release();
    }
    return 0;
}

int LogManager::truncate_suffix(const int64_t last_index_kept) {
    base::Timer timer;
    timer.start();

    {
        BAIDU_SCOPED_LOCK(_mutex);

        while (!_logs_in_memory.empty()) {
            LogEntry* entry = _logs_in_memory.back();
            if (entry->index > last_index_kept) {
                entry->Release();
                _logs_in_memory.pop_back();
            } else {
                break;
            }
        }
        // not need flush queue, because only leader has queue, leader never call truncate_suffix
        _last_log_index = last_index_kept;
        _config_manager->truncate_suffix(last_index_kept);
        _disk_index.store(last_index_kept, boost::memory_order_release);
    }
    int ret = _log_storage->truncate_suffix(last_index_kept);

    timer.stop();
    RAFT_VLOG << "truncate_suffix " << last_index_kept << " time: " << timer.u_elapsed();

    return ret;
}

int LogManager::append_entries(const std::vector<LogEntry*>& entries) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    //TODO: move index setting to LogStorage
    for (size_t i = 0; i < entries.size(); i++) {
        // follower append has index, not need set
        if (entries[i]->index != 0) {
            break;
        }
        entries[i]->index = _last_log_index + 1 + i;
    }
    lck.unlock();

    RAFT_VLOG << "follower append " << entries[0]->index
        << "-" << entries[0]->index + entries.size() - 1 << noflush;

    base::Timer timer;
    timer.start();
    int ret = _log_storage->append_entries(entries);
    if (static_cast<size_t>(ret) == entries.size()) {
        ret = 0;
        lck.lock();
        int64_t last_index = 0;
        for (size_t i = 0; i < entries.size(); i++) {
            last_index = entries[i]->index;
            // skip duplicated follower append logs
            if (_logs_in_memory.size() == 0 ||
                (_logs_in_memory.size() > 0 &&
                 _logs_in_memory.back()->index + 1 == entries[i]->index)) {

                // new logs append to memory
                _logs_in_memory.push_back(entries[i]);

                // update configuration
                if (entries[i]->type == ENTRY_TYPE_CONFIGURATION) {
                    _config_manager->add(entries[i]->index, Configuration(*(entries[i]->peers)));
                }
            } else {
                entries[i]->Release();
            }
        }
        _last_log_index = last_index;
        _disk_index.store(_last_log_index);
    } else {
        // Remove partially appended logs which would make later appending
        // undefined
        _log_storage->truncate_suffix(_last_log_index);
        ret = EIO;
    }
    timer.stop();

    RAFT_VLOG << " time: " << timer.u_elapsed();
    return ret;
}

void LogManager::append_entry(
            LogEntry* log_entry, StableClosure* done) {
    BAIDU_SCOPED_LOCK(_mutex);
    log_entry->index = ++_last_log_index;
    done->_first_log_index = log_entry->index;
    // Add ref for disk thread, release in 
    log_entry->AddRef();
    done->_entries.push_back(log_entry);
    _logs_in_memory.push_back(log_entry);
    if (log_entry->type == ENTRY_TYPE_CONFIGURATION) {
        _config_manager->add(log_entry->index, Configuration(*(log_entry->peers)));
    }

    RAFT_VLOG << "leader append " << log_entry->index;

    CHECK(_leader_disk_thread_running);
    // signal leader disk
    int ret = bthread::execution_queue_execute(_leader_disk_queue, done);
    CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();

    // signal replicator
    //int64_t last_log_index = log_entry->index;
    bthread_id_list_reset(&_wait_list, 0);
}

void LogManager::append_entries(
            std::vector<LogEntry*> *entries, StableClosure* done) {
    CHECK(!entries->empty());
    BAIDU_SCOPED_LOCK(_mutex);
    for (size_t i = 0; i < entries->size(); ++i) {
        (*entries)[i]->index = ++_last_log_index;
        // Add ref for disk thread, release in leader_disk_run
        (*entries)[i]->AddRef();
        if ((*entries)[i]->type == ENTRY_TYPE_CONFIGURATION) {
            _config_manager->add((*entries)[i]->index, 
                                 Configuration(*((*entries)[i]->peers)));
        }
    }
    done->_first_log_index = (*entries)[0]->index;
    _logs_in_memory.insert(_logs_in_memory.end(), entries->begin(), entries->end());
    done->_entries.swap(*entries);
    //RAFT_VLOG << "leader append " << log_entry->index;
    CHECK(_leader_disk_thread_running);
    // signal leader disk
    int ret = bthread::execution_queue_execute(_leader_disk_queue, done);
    CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();

    // signal replicator
    //int64_t last_log_index = log_entry->index;
    bthread_id_list_reset(&_wait_list, 0);
}

int LogManager::leader_disk_run(void* meta,
                                StableClosure** const tasks[], size_t tasks_size) {
    if (tasks_size == 0) {
        return 0;
    }

    LogManager* log_manager = static_cast<LogManager*>(meta);
    int64_t last_index = 0;
    std::vector<LogEntry*> to_append;
    to_append.reserve(1024);
    for (size_t i = 0; i < tasks_size; i++) {
        StableClosure* done = *tasks[i];
        // Skip TruncatePrefixClosure
        // FIXME: Currrently only the leader starts disk thread so that there's
        // no gap around the TruncatePrefixClosure. If followers also start disk
        // thread, it's buggy after install snapshot.
        if (!done->_entries.empty()) {
            to_append.insert(to_append.end(), 
                             done->_entries.begin(), done->_entries.end());
        }
    }
    
    int ret = 0;
    if (to_append.size() > 0) {
        base::Timer timer;
        timer.start();
        RAFT_VLOG << "leader_disk_thread append " << to_append.front()->index
            << "-" << to_append.back()->index << noflush;

        ret = log_manager->_log_storage->append_entries(to_append);
        if (to_append.size() == static_cast<size_t>(ret)) {
            ret = 0;
            last_index = to_append.back()->index;
        } else {
            // TDOO: make sure the actions on EIO are supposed to be.
            CHECK(false) << to_append.size() << "!=" << ret;
            ret = EIO;
        }
        timer.stop();
        RAFT_VLOG << " time: " << timer.u_elapsed();
        for (size_t i = 0; i < to_append.size(); ++i) {
            to_append[i]->Release();
        }
        to_append.clear();
    }

    for (size_t i = 0; i < tasks_size; i++) {
        StableClosure* done = *tasks[i];
        // skip barrier
        baidu::rpc::ClosureGuard done_guard(done);
        if (!done->_entries.empty()) {
            //done->_entry->Release();
            if (ret != 0) {
                done->status().set_error(EIO, "append entry failed");
            }
            done->_entries.clear();
            continue;
        }
        TruncatePrefixClosure* tpc = dynamic_cast<TruncatePrefixClosure*>(done);
        if (tpc) {
            LOG(INFO) << "Truncating storage to first_index_kept="
                << tpc->first_index_kept();
            log_manager->_log_storage->truncate_prefix(
                            tpc->first_index_kept());
            continue;
        }
        ResetClosure* rc = dynamic_cast<ResetClosure*>(done);
        if (rc) {
            LOG(INFO) << "Reseting storage to next_log_index="
                      << rc->next_log_index();
            log_manager->_log_storage->reset(rc->next_log_index());
            continue;
        }

        CHECK(false) << "Cannot reach here";
    }

    if (last_index != 0) {
        log_manager->set_disk_index(last_index);
    }

    return 0;
}

void LogManager::set_snapshot(const SnapshotMeta* meta) {
    LOG(INFO) << "Set snapshot last_included_index="
              << meta->last_included_index
              << " last_included_term=" <<  meta->last_included_term;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (meta->last_included_index <= _last_snapshot_index) {
        return;
    }
    _last_snapshot_index = meta->last_included_index;
    _last_snapshot_term = meta->last_included_term;
    _config_manager->set_snapshot(meta->last_included_index, meta->last_configuration);
    int64_t term = 0;
    LogEntry* entry = get_entry_from_memory(meta->last_included_index);
    if (entry) {
        term = entry->term;
    } else {
        term = _log_storage->get_term(meta->last_included_index);
        g_read_term_from_storage << 1;
    }
    if (term == 0 || term == meta->last_included_term) {
        truncate_prefix(meta->last_included_index + 1, lck);
        return;
    } else {
        // TODO: check the result of reset.
        reset(meta->last_included_index + 1, lck);
        return;
    }
    CHECK(false) << "Cannot reach here";
}

LogEntry* LogManager::get_entry_from_memory(const int64_t index) {
    LogEntry* entry = NULL;
    if (!_logs_in_memory.empty()) {
        int64_t first_index = _logs_in_memory.front()->index;
        int64_t last_index = _logs_in_memory.back()->index;
        CHECK_EQ(last_index - first_index + 1, static_cast<int64_t>(_logs_in_memory.size()));
        if (index >= first_index && index <= last_index) {
            entry = _logs_in_memory[index - first_index];
        }
    }
    return entry;
}

int64_t LogManager::get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }

    std::unique_lock<raft_mutex_t> lck(_mutex);
    // check index equal snapshot_index, return snapshot_term
    if (index == _last_snapshot_index) {
        return _last_snapshot_term;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        return entry->term;
    }
    lck.unlock();
    g_read_term_from_storage << 1;
    return _log_storage->get_term(index);
}

LogEntry* LogManager::get_entry(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // out of range, direct return NULL
    if (index > _last_log_index) {
        return NULL;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        entry->AddRef();
        return entry;
    }
    lck.unlock();
    g_read_entry_from_storage << 1;
    return _log_storage->get_entry(index);
}

ConfigurationPair LogManager::get_configuration(const int64_t index) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _config_manager->get_configuration(index);
}

bool LogManager::check_and_set_configuration(ConfigurationPair* current) {
    if (current == NULL) {
        CHECK(false) << "current should not be NULL";
        return false;
    }
    BAIDU_SCOPED_LOCK(_mutex);

    int64_t last_config_index = _config_manager->last_configuration_index();
    if (current->first != last_config_index) {
        *current = _config_manager->last_configuration();
        return true;
    }
    return false;
}

void LogManager::set_disk_index(int64_t index) {
    CHECK(_leader_disk_thread_running) << "Must be called in the leader disk thread";
    int64_t old_disk_index = _disk_index.load(boost::memory_order_relaxed);
    do {
        if (old_disk_index >= index) {
            return;
        }
    } while (_disk_index.compare_exchange_weak(old_disk_index, index, 
            boost::memory_order_release, boost::memory_order_relaxed));

    int64_t clear_index = std::min(
            index, _applied_index.load(boost::memory_order_acquire));
    return clear_memory_logs(clear_index);
}

void LogManager::set_applied_index(int64_t index) {
    int64_t old_applied_index = _applied_index.load(boost::memory_order_relaxed);
    do {
        if (old_applied_index >= index) {
            return;
        }
    } while (!_applied_index.compare_exchange_weak(old_applied_index, index,
                boost::memory_order_release, boost::memory_order_relaxed));
    int64_t clear_index = std::min(
            _disk_index.load(boost::memory_order_acquire), index);
    return clear_memory_logs(clear_index);
}

//////////////////////////////////////////
//
void on_timed_out(void *arg) {
    bthread_id_t id;
    id.value = reinterpret_cast<int64_t>(arg);
    bthread_id_error(id, ETIMEDOUT);
}

int on_notified(bthread_id_t id, void *arg, int rc) {
    *(int*)arg = rc;
    return bthread_id_unlock_and_destroy(id);
}

void LogManager::shutdown() {
    BAIDU_SCOPED_LOCK(_mutex);
    _stopped = true;
    bthread_id_list_reset(&_wait_list, ESTOP);
}

int LogManager::wait(int64_t expected_last_log_index,
                     const timespec *due_time) {
    int return_code = 0;
    bthread_id_t wait_id;
    int rc = bthread_id_create(&wait_id, &return_code, on_notified);
    if (rc != 0) {
        return -1;
    }
    bthread_timer_t timer_id;
    if (due_time) {
        CHECK_EQ(0, bthread_timer_add(&timer_id, *due_time, on_timed_out,
                                      reinterpret_cast<void*>(wait_id.value)));
    }
    notify_on_new_log(expected_last_log_index, wait_id);
    bthread_id_join(wait_id);
    if (due_time) {
        bthread_timer_del(timer_id);
    }
    return return_code;
}

struct WaitMeta {
    WaitMeta()
        : on_new_log(NULL)
        , arg(NULL)
        , timer_id()
        , has_timer(false)
        , error_code(0)
    {}
    int (*on_new_log)(void *arg, int error_code);
    void *arg;
    bthread_timer_t timer_id;
    bool has_timer;
    int error_code;
};

void* run_on_new_log(void *arg) {
    WaitMeta* wm = (WaitMeta*)arg;
    if (wm->has_timer) {
        bthread_timer_del(wm->timer_id);
    }
    wm->on_new_log(wm->arg, wm->error_code);
    delete wm;
    return NULL;
}

int on_wait_notified(bthread_id_t id, void *arg, int error_code) {
    WaitMeta* wm = (WaitMeta*)arg;
    wm->error_code = error_code;
    bthread_t tid;
    if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, run_on_new_log, wm) != 0) {
        run_on_new_log(wm);
    }
    return bthread_id_unlock_and_destroy(id);
}

void LogManager::wait(int64_t expected_last_log_index, 
                      const timespec *due_time,
                      int (*on_new_log)(void *arg, int error_code), void *arg) {
    WaitMeta *wm = new WaitMeta();
    wm->on_new_log = on_new_log;
    wm->arg = arg;
    bthread_id_t wait_id;
    int rc = bthread_id_create(&wait_id, wm, on_wait_notified);
    if (rc != 0) {
        on_new_log(arg, rc);
        return;
    }
    CHECK_EQ(0, bthread_id_lock(wait_id, NULL));
    bthread_timer_t timer_id;
    if (due_time) {
        CHECK_EQ(0, bthread_timer_add(&timer_id, *due_time, on_timed_out,
                                      reinterpret_cast<void*>(wait_id.value)));
        wm->timer_id = timer_id;
        wm->has_timer = true;
    }
    notify_on_new_log(expected_last_log_index, wait_id);
    CHECK_EQ(0, bthread_id_unlock(wait_id));
}

void LogManager::notify_on_new_log(int64_t expected_last_log_index,
                                   bthread_id_t wait_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (expected_last_log_index != _last_log_index && _stopped) {
        bthread_id_error(wait_id, 0);
        return;
    }
    if (bthread_id_list_add(&_wait_list, wait_id) != 0) {
        bthread_id_error(wait_id, EAGAIN);
        return;
    }
}

void LogManager::describe(std::ostream& os, bool use_html) {
    const char* newline = use_html ? "<br>" : "\n";
    int64_t first_index = _log_storage->first_log_index();
    int64_t last_index = _log_storage->last_log_index();
    os << "storage: [" << first_index << ", " << last_index << ']' << newline;
    os << "disk_index: " << _disk_index.load(boost::memory_order_relaxed) << newline;
    os << " known_applied_index: " << _applied_index.load(boost::memory_order_relaxed) << newline;
    const int64_t cur_last_log_index = last_log_index();
    os << " last_log_index: " << cur_last_log_index << newline;
    os << " last_log_term: " << get_term(cur_last_log_index) << newline; 
}

}  // namespace raft
