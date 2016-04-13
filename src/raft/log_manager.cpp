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
#include "raft/bthread_support.h"

namespace raft {

DEFINE_int32(raft_leader_batch, 256, "max leader io batch");
BAIDU_RPC_VALIDATE_GFLAG(raft_leader_batch, ::baidu::rpc::PositiveInteger);

static bvar::Adder<int64_t> g_read_entry_from_storage
            ("raft_read_entry_from_storage_count");
static bvar::PerSecond<bvar::Adder<int64_t> > g_read_entry_from_storage_second
            ("raft_read_entry_from_storage_second", &g_read_entry_from_storage);

static bvar::Adder<int64_t> g_read_term_from_storage
            ("raft_read_term_from_storage_count");
static bvar::PerSecond<bvar::Adder<int64_t> > g_read_term_from_storage_second
            ("raft_read_term_from_storage_second", &g_read_term_from_storage);

static bvar::LatencyRecorder g_storage_append_entries_latency("raft_storage_append_entries");
static bvar::LatencyRecorder g_nomralized_append_entries_latency("raft_storage_append_entries_normalized");

LogManagerOptions::LogManagerOptions()
    : log_storage(NULL), configuration_manager(NULL)
{}

LogManager::LogManager()
    : _log_storage(NULL)
    , _config_manager(NULL)
    , _first_log_index(0)
    , _last_log_index(0)
    , _stopped(false)
{
    CHECK_EQ(0, bthread_id_list_init(&_wait_list, 16/*FIXME*/, 16));
    CHECK_EQ(0, start_disk_thread());
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
    _disk_id.index = _last_log_index;
    _disk_id.term = _log_storage->get_term(_last_log_index);
    return 0;
}

LogManager::~LogManager() {
    stop_disk_thread();
    for (size_t i = 0; i < _logs_in_memory.size(); ++i) {
        _logs_in_memory[i]->Release();
    }
    _logs_in_memory.clear();
    bthread_id_list_destroy(&_wait_list);
}

int LogManager::start_disk_thread() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    queue_options.max_tasks_size = FLAGS_raft_leader_batch;
    return bthread::execution_queue_start(&_disk_queue,
                                   &queue_options,
                                   disk_thread,
                                   this);
}

int LogManager::stop_disk_thread() {
    bthread::execution_queue_stop(_disk_queue);
    return bthread::execution_queue_join(_disk_queue);
}

void LogManager::clear_memory_logs(const LogId& id) {
    LogEntry* entries_to_clear[256];
    size_t nentries = 0;
    do {
        nentries = 0;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            while (!_logs_in_memory.empty() 
                    && nentries < ARRAY_SIZE(entries_to_clear)) {
                LogEntry* entry = _logs_in_memory.front();
                if (entry->id > id) {
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

class LastLogIdClosure : public LogManager::StableClosure {
public:
    explicit LastLogIdClosure(BthreadCond* cond, LogId* log_id)
        : _cond(cond), _last_log_id(log_id)
    {}
    void Run() {
        delete this;
    }
    void set_last_log_id(const LogId& log_id) {
        *_last_log_id = log_id;
        _cond->signal();
    }
private:
    BthreadCond* _cond;
    LogId* _last_log_id;
};

int64_t LogManager::last_log_index(bool is_flush) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_flush) {
        return _last_log_index;
    } else {
        BthreadCond cond;
        LogId last_id;
        LastLogIdClosure* c = new LastLogIdClosure(&cond, &last_id);
        const int rc = bthread::execution_queue_execute(_disk_queue, c);
        lck.unlock();

        if (rc != 0) {
            return 0;
        } else {
            cond.wait();
            return last_id.index;
        }
    }
}

LogId LogManager::last_log_id(bool is_flush) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_flush) {
        if (_last_log_index >= _first_log_index) {
            return LogId(_last_log_index, unsafe_get_term(_last_log_index));
        }
        return _last_snapshot_id;
    } else {
        BthreadCond cond;
        LogId last_id;
        LastLogIdClosure* c = new LastLogIdClosure(&cond, &last_id);
        const int rc = bthread::execution_queue_execute(_disk_queue, c);
        lck.unlock();

        if (rc != 0) {
            return LogId();
        } else {
            cond.wait();
            return last_id;
        }
    }
}

class TruncatePrefixClosure : public LogManager::StableClosure {
public:
    explicit TruncatePrefixClosure(const int64_t first_index_kept)
        : _first_index_kept(first_index_kept)
    {}
    void Run() {
        delete this;
    }
    int64_t first_index_kept() const { return _first_index_kept; }
private:
    int64_t _first_index_kept;
};

class TruncateSuffixClosure : public LogManager::StableClosure {
public:
    explicit TruncateSuffixClosure(const int64_t last_index_kept)
        : _last_index_kept(last_index_kept)
    {}
    void Run() {
        delete this;
    }
    int64_t last_index_kept() const { return _last_index_kept; }
private:
    int64_t _last_index_kept;
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
    std::deque<LogEntry*> saved_logs_in_memory;
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
        if (entry->id.index < first_index_kept) {
            saved_logs_in_memory.push_back(entry);
            _logs_in_memory.pop_front();
        } else {
            break;
        }
    }
    _first_log_index = first_index_kept;
    if (first_index_kept > _last_log_index) {
        // The entrie log is dropped
        _last_log_index = first_index_kept - 1;
    }
    _config_manager->truncate_prefix(first_index_kept);
    TruncatePrefixClosure* c = new TruncatePrefixClosure(first_index_kept);
    const int rc = bthread::execution_queue_execute(_disk_queue, c);
    lck.unlock();
    for (size_t i = 0; i < saved_logs_in_memory.size(); ++i) {
        saved_logs_in_memory[i]->Release();
    }
    return rc;
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
    ResetClosure* c = new ResetClosure(next_log_index);
    const int ret = bthread::execution_queue_execute(_disk_queue, c);
    lck.unlock();
    CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
    for (size_t i = 0; i < saved_logs_in_memory.size(); ++i) {
        saved_logs_in_memory[i]->Release();
    }
    return 0;
}

void LogManager::unsafe_truncate_suffix(const int64_t last_index_kept) {

    if (last_index_kept < _applied_id.index) {
        LOG(FATAL) << "Can't truncate logs before _applied_id=" <<_applied_id.index
                   << ", last_log_kept=" << last_index_kept;
        return;
    }

    while (!_logs_in_memory.empty()) {
        LogEntry* entry = _logs_in_memory.back();
        if (entry->id.index > last_index_kept) {
            entry->Release();
            _logs_in_memory.pop_back();
        } else {
            break;
        }
    }
    _last_log_index = last_index_kept;
    _config_manager->truncate_suffix(last_index_kept);
    TruncateSuffixClosure* tsc = new TruncateSuffixClosure(last_index_kept);
    CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, tsc));
}

int LogManager::check_and_resolve_confliction(
            std::vector<LogEntry*> *entries, StableClosure* done) {
    AsyncClosureGuard done_guard(done);   
    if (entries->front()->id.index == 0) {
        // Node is currently the leader and |entries| are from the user who 
        // don't know the exact index the logs should be. So we should assign 
        // index to the appending entries
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->id.index = ++_last_log_index;
        }
        done_guard.release();
        return 0;
    } else {
        // Node is currently a follower and |entries| are from the leader. We 
        // should check and resolve the confliction between the local logs and
        // |entries|
        if (entries->front()->id.index > _last_log_index + 1) {
            done->status().set_error(EINVAL, "There's gap between first_index=%ld "
                                     "and last_log_index=%ld", 
                                     entries->front()->id.index, _last_log_index);
            return -1;
        }
        const int64_t applied_index = _applied_id.index;
        if (entries->back()->id.index <= applied_index) {
            LOG(WARNING) << "Received entries of which the last_log="
                         << entries->back()->id.index
                         << " is not greater than _applied_index=" << applied_index
                         << ", return immediately with nothing changed";
            return 1;
        }

        if (entries->front()->id.index == _last_log_index + 1) {
            // Fast path
            _last_log_index = entries->back()->id.index;
        } else {
            // Appending entries overlap the local ones. We should find if there
            // is a conflicting index from which we should truncate the local
            // ones.
            size_t conflicting_index = 0;
            for (; conflicting_index < entries->size(); ++conflicting_index) {
                if (unsafe_get_term((*entries)[conflicting_index]->id.index)
                        != (*entries)[conflicting_index]->id.term) {
                    break;
                }
            }
            if (conflicting_index != entries->size()) {
                if ((*entries)[conflicting_index]->id.index <= _last_log_index) {
                    // Truncate all the conflicting entries to make local logs
                    // consensus with the leader.
                    unsafe_truncate_suffix(
                            (*entries)[conflicting_index]->id.index - 1);
                }
                _last_log_index = entries->back()->id.index;
            }  // else this is a duplicated AppendEntriesRequest, we have 
               // nothing to do besides releasing all the entries
            
            // Release all the entries before the conflicting_index and the rest
            // would be append to _logs_in_memory and _log_storage after this
            // function returns
            for (size_t i = 0; i < conflicting_index; ++i) {
                (*entries)[i]->Release();
            }
            entries->erase(entries->begin(), 
                           entries->begin() + conflicting_index);
        }
        done_guard.release();
        return 0;
    }
    CHECK(false) << "Can't reach here";
    done->status().set_error(EIO, "Impossible");
    return -1;
}

void LogManager::append_entries(
            std::vector<LogEntry*> *entries, StableClosure* done) {
    CHECK(done);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!entries->empty() && check_and_resolve_confliction(entries, done) != 0) {
        lck.unlock();
        // release entries
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->Release();
        }
        entries->clear();
        return;
    }

    for (size_t i = 0; i < entries->size(); ++i) {
        // Add ref for disk_thread
        (*entries)[i]->AddRef();
        if ((*entries)[i]->type == ENTRY_TYPE_CONFIGURATION) {
            _config_manager->add((*entries)[i]->id,
                                 Configuration(*((*entries)[i]->peers)));
        }
    }

    if (!entries->empty()) {
        done->_first_log_index = entries->front()->id.index;
        _logs_in_memory.insert(_logs_in_memory.end(), entries->begin(), entries->end());
    }

    done->_entries.swap(*entries);
    int ret = bthread::execution_queue_execute(_disk_queue, done);
    CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();

    bthread_id_list_reset(&_wait_list, 0);
}

void LogManager::append_to_storage(std::vector<LogEntry*>* to_append, 
                                   LogId* last_id) {
    size_t written_size = 0;
    for (size_t i = 0; i < to_append->size(); ++i) {
        written_size += (*to_append)[i]->data.size();
    }
    base::Timer timer;
    timer.start();
    int nappent = _log_storage->append_entries(*to_append);
    timer.stop();
    if (nappent != (int)to_append->size()) {
        // FIXME
        LOG(FATAL) << "We cannot tolerate the fault caused by log_storage, "
            << "nappent=" << nappent 
            << ", to_append=" << to_append->size()
            << ", abort";
        abort();
    }
    *last_id = to_append->back()->id;
    for (size_t j = 0; j < to_append->size(); ++j) {
        (*to_append)[j]->Release();
    }
    to_append->clear();
    g_storage_append_entries_latency << timer.u_elapsed();
    if (written_size) {
        g_nomralized_append_entries_latency << timer.u_elapsed() * 1024 / written_size;
    }
}

int LogManager::disk_thread(void* meta,
                            StableClosure** const tasks[], size_t tasks_size) {
    if (tasks_size == 0) {
        return 0;
    }

    LogManager* log_manager = static_cast<LogManager*>(meta);
    LogId last_id;
    std::vector<LogEntry*> to_append;
    to_append.reserve(1024);
    size_t first_index = 0;
    for (size_t i = 0; i < tasks_size; i++) {
        StableClosure* done = *tasks[i];
        if (!done->_entries.empty()) {
            to_append.insert(to_append.end(), 
                             done->_entries.begin(), done->_entries.end());
        } else {
            // Flush to_append before execute barrier task
            if (!to_append.empty()) {
                log_manager->append_to_storage(&to_append, &last_id);
            }
            for (size_t j = first_index; j < i; ++j) {
                (*tasks[j])->_entries.clear();
                (*tasks[j])->Run();
            }
            int ret = 0;
            do {
                LastLogIdClosure* llic =
                        dynamic_cast<LastLogIdClosure*>(done);
                if (llic) {
                    llic->set_last_log_id(log_manager->get_disk_id());
                    break;
                }
                TruncatePrefixClosure* tpc = 
                        dynamic_cast<TruncatePrefixClosure*>(done);
                if (tpc) {
                    RAFT_VLOG << "Truncating storage to first_index_kept="
                        << tpc->first_index_kept();
                    ret = log_manager->_log_storage->truncate_prefix(
                                    tpc->first_index_kept());
                    break;
                }
                TruncateSuffixClosure* tsc = 
                        dynamic_cast<TruncateSuffixClosure*>(done);
                if (tsc) {
                    LOG(WARNING) << "Truncating storage to last_index_kept="
                        << tsc->last_index_kept();
                    ret = log_manager->_log_storage->truncate_suffix(
                                    tsc->last_index_kept());
                    if (ret == 0) {
                        // update last_id after truncate_suffix
                        last_id.index = tsc->last_index_kept();
                        last_id.term = log_manager->get_term(last_id.index);
                    }
                    break;
                }
                ResetClosure* rc = dynamic_cast<ResetClosure*>(done);
                if (rc) {
                    LOG(INFO) << "Reseting storage to next_log_index="
                              << rc->next_log_index();
                    ret = log_manager->_log_storage->reset(rc->next_log_index());
                    break;
                }
            } while (0);
            if (ret != 0) {
                // FIXME
                LOG(FATAL) << "We cannot tolerate the fault caused by log_storage, "
                    << "ret=" << ret << ", abort";
                abort();
            }
            done->Run();
            first_index = i + 1;
        }
    }
    if (!to_append.empty()) {
        log_manager->append_to_storage(&to_append, &last_id);
    }
    for (size_t j = first_index; j < tasks_size; ++j) {
        (*tasks[j])->_entries.clear();
        (*tasks[j])->Run();
    }
    log_manager->set_disk_id(last_id);
    return 0;
}

void LogManager::set_snapshot(const SnapshotMeta* meta) {
    RAFT_VLOG << "Set snapshot last_included_index="
              << meta->last_included_index
              << " last_included_term=" <<  meta->last_included_term;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (meta->last_included_index <= _last_snapshot_id.index) {
        return;
    }

    _config_manager->set_snapshot(
            LogId(meta->last_included_index, meta->last_included_term), 
            meta->last_configuration);
    int64_t term = unsafe_get_term(meta->last_included_index);

    const int64_t saved_last_snapshot_index = _last_snapshot_id.index;
    _last_snapshot_id.index = meta->last_included_index;
    _last_snapshot_id.term = meta->last_included_term;
    if (_last_snapshot_id > _applied_id) {
        _applied_id = _last_snapshot_id;
    }
    if (term == 0) {
        // last_included_index is larger than last_index
        truncate_prefix(meta->last_included_index + 1, lck);
        return;
    } else if (term == meta->last_included_term) {
        // Truncating log to the index of the last snapshot.
        truncate_prefix(saved_last_snapshot_index + 1, lck);
        return;
    } else {
        // TODO: check the result of reset.
        reset(meta->last_included_index + 1, lck);
        return;
    }
    CHECK(false) << "Cannot reach here";
}

void LogManager::clear_bufferred_logs() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_last_snapshot_id.index != 0) {
        truncate_prefix(_last_snapshot_id.index + 1, lck);
    }
}

LogEntry* LogManager::get_entry_from_memory(const int64_t index) {
    LogEntry* entry = NULL;
    if (!_logs_in_memory.empty()) {
        int64_t first_index = _logs_in_memory.front()->id.index;
        int64_t last_index = _logs_in_memory.back()->id.index;
        CHECK_EQ(last_index - first_index + 1, static_cast<int64_t>(_logs_in_memory.size()));
        if (index >= first_index && index <= last_index) {
            entry = _logs_in_memory[index - first_index];
        }
    }
    return entry;
}

int64_t LogManager::unsafe_get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }

    if (index > _last_log_index) {
        return 0;
    }

    // check index equal snapshot_index, return snapshot_term
    if (index == _last_snapshot_id.index) {
        return _last_snapshot_id.term;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        return entry->id.term;
    }
    g_read_term_from_storage << 1;
    return _log_storage->get_term(index);
}

int64_t LogManager::get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }

    std::unique_lock<raft_mutex_t> lck(_mutex);
    // out of range, direct return NULL
    if (index > _last_log_index) {
        return 0;
    }

    // check index equal snapshot_index, return snapshot_term
    if (index == _last_snapshot_id.index) {
        return _last_snapshot_id.term;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        return entry->id.term;
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

void LogManager::get_configuration(const int64_t index, ConfigurationPair* conf) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _config_manager->get_configuration(index, conf);
}

bool LogManager::check_and_set_configuration(ConfigurationPair* current) {
    if (current == NULL) {
        CHECK(false) << "current should not be NULL";
        return false;
    }
    BAIDU_SCOPED_LOCK(_mutex);

    const ConfigurationPair& last_conf = _config_manager->last_configuration();
    if (current->first != last_conf.first) {
        *current = last_conf;
        return true;
    }
    return false;
}

LogId LogManager::get_disk_id() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    return _disk_id;
}

void LogManager::set_disk_id(const LogId& disk_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (disk_id < _disk_id) {
        return;
    }
    _disk_id = disk_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    lck.unlock();
    return clear_memory_logs(clear_id);
}

void LogManager::set_applied_id(const LogId& applied_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (applied_id < _applied_id) {
        return;
    }
    _applied_id = applied_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    lck.unlock();
    return clear_memory_logs(clear_id);
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
    raft_timer_t timer_id;
    if (due_time) {
        CHECK_EQ(0, raft_timer_add(&timer_id, *due_time, on_timed_out,
                                      reinterpret_cast<void*>(wait_id.value)));
    }
    notify_on_new_log(expected_last_log_index, wait_id);
    bthread_id_join(wait_id);
    if (due_time) {
        raft_timer_del(timer_id);
    }
    return return_code;
}

struct WaitMeta {
    WaitMeta()
        : on_new_log(NULL)
        , arg(NULL)
        , has_timer(false)
        , error_code(0)
    {}
    int (*on_new_log)(void *arg, int error_code);
    void *arg;
    raft_timer_t timer_id;
    bool has_timer;
    int error_code;
};

void* run_on_new_log(void *arg) {
    WaitMeta* wm = (WaitMeta*)arg;
    if (wm->has_timer) {
        raft_timer_del(wm->timer_id);
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
    raft_timer_t timer_id;
    if (due_time) {
        CHECK_EQ(0, raft_timer_add(&timer_id, *due_time, on_timed_out,
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
    os << "disk_index: " << _disk_id.index << newline;
    os << "known_applied_index: " << _applied_id.index << newline;
    os << "last_log_id: " << last_log_id() << newline;
}

}  // namespace raft
