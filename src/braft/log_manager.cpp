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

#include "braft/log_manager.h"

#include <butil/logging.h>                       // LOG
#include <butil/object_pool.h>                   // butil::get_object
#include <bthread/unstable.h>                   // bthread_flush
#include <bthread/countdown_event.h>            // bthread::CountdownEvent
#include <brpc/reloadable_flags.h>         // BRPC_VALIDATE_GFLAG
#include "braft/storage.h"                       // LogStorage
#include "braft/fsm_caller.h"                    // FSMCaller

namespace braft {

DEFINE_int32(raft_leader_batch, 256, "max leader io batch");
BRPC_VALIDATE_GFLAG(raft_leader_batch, ::brpc::PositiveInteger);

static bvar::Adder<int64_t> g_read_entry_from_storage
            ("raft_read_entry_from_storage_count");
static bvar::PerSecond<bvar::Adder<int64_t> > g_read_entry_from_storage_second
            ("raft_read_entry_from_storage_second", &g_read_entry_from_storage);

static bvar::Adder<int64_t> g_read_term_from_storage
            ("raft_read_term_from_storage_count");
static bvar::PerSecond<bvar::Adder<int64_t> > g_read_term_from_storage_second
            ("raft_read_term_from_storage_second", &g_read_term_from_storage);

static bvar::LatencyRecorder g_storage_append_entries_latency(
                                        "raft_storage_append_entries");
static bvar::LatencyRecorder g_nomralized_append_entries_latency(
                                        "raft_storage_append_entries_normalized");

static bvar::CounterRecorder g_storage_flush_batch_counter(
                                        "raft_storage_flush_batch_counter");

void LogManager::StableClosure::update_metric(IOMetric* m) {
    metric.open_segment_time_us = m->open_segment_time_us;
    metric.append_entry_time_us = m->append_entry_time_us;
    metric.sync_segment_time_us = m->sync_segment_time_us;
}

LogManagerOptions::LogManagerOptions()
    : log_storage(NULL)
    , configuration_manager(NULL)
    , fsm_caller(NULL)
{}

LogManager::LogManager()
    : _log_storage(NULL)
    , _config_manager(NULL)
    , _stopped(false)
    , _has_error(false)
    , _next_wait_id(0)
    , _first_log_index(0)
    , _last_log_index(0)
{
    CHECK_EQ(0, start_disk_thread());
}

int LogManager::init(const LogManagerOptions &options) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (options.log_storage == NULL) {
        return EINVAL;
    }
    if (_wait_map.init(16) != 0) {
        PLOG(ERROR) << "Fail to init _wait_map";
        return ENOMEM;
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
    // Term will be 0 if the node has no logs, and we will correct the value
    // after snapshot load finish.
    _disk_id.term = _log_storage->get_term(_last_log_index);
    _fsm_caller = options.fsm_caller;
    return 0;
}

LogManager::~LogManager() {
    stop_disk_thread();
    for (size_t i = 0; i < _logs_in_memory.size(); ++i) {
        _logs_in_memory[i]->Release();
    }
    _logs_in_memory.clear();
}

int LogManager::start_disk_thread() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
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
    LastLogIdClosure() {
    }
    void Run() {
        _event.signal();
    }
    void set_last_log_id(const LogId& log_id) {
        CHECK(log_id.index == 0 || log_id.term != 0) << "Invalid log_id=" << log_id;
        _last_log_id = log_id;
    }
    LogId last_log_id() const { return _last_log_id; }

    void wait() {
        _event.wait();
    }
private:
    bthread::CountdownEvent _event;
    LogId _last_log_id;
};

int64_t LogManager::last_log_index(bool is_flush) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_flush) {
        return _last_log_index;
    } else {
        if (_last_log_index == _last_snapshot_id.index) {
            return _last_log_index;
        }
        LastLogIdClosure c;
        CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, &c));
        lck.unlock();
        c.wait();
        return c.last_log_id().index;
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
        if (_last_log_index == _last_snapshot_id.index) {
            return _last_snapshot_id;
        }
        LastLogIdClosure c;
        CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, &c));
        lck.unlock();
        c.wait();
        return c.last_log_id();
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
    TruncateSuffixClosure(int64_t last_index_kept, int64_t last_term_kept)
        : _last_index_kept(last_index_kept)
        , _last_term_kept(last_term_kept)
    {}
    void Run() {
        delete this;
    }
    int64_t last_index_kept() const { return _last_index_kept; }
    int64_t last_term_kept() const { return _last_term_kept; }
private:
    int64_t _last_index_kept;
    int64_t _last_term_kept;
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
    CHECK_GE(first_index_kept, _first_log_index);
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
    const int64_t last_term_kept = unsafe_get_term(last_index_kept);
    CHECK(last_index_kept == 0 || last_term_kept != 0)
        << "last_index_kept=" << last_index_kept;
    _config_manager->truncate_suffix(last_index_kept);
    TruncateSuffixClosure* tsc = new
            TruncateSuffixClosure(last_index_kept, last_term_kept);
    CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, tsc));
}

int LogManager::check_and_resolve_conflict(
            std::vector<LogEntry*> *entries, StableClosure* done) {
    AsyncClosureGuard done_guard(done);   
    if (entries->front()->id.index == 0) {
        // Node is currently the leader and |entries| are from the user who 
        // don't know the correct indexes the logs should assign to. So we have
        // to assign indexes to the appending entries
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
            done->status().set_error(EINVAL, "There's gap between first_index=%" PRId64
                                     " and last_log_index=%" PRId64,
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
    if (_has_error.load(butil::memory_order_relaxed)) {
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->Release();
        }
        entries->clear();
        done->status().set_error(EIO, "Corrupted LogStorage");
        return run_closure_in_bthread(done);
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!entries->empty() && check_and_resolve_conflict(entries, done) != 0) {
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
            ConfigurationEntry conf_entry(*((*entries)[i]));
            _config_manager->add(conf_entry);
        }
    }

    if (!entries->empty()) {
        done->_first_log_index = entries->front()->id.index;
        _logs_in_memory.insert(_logs_in_memory.end(), entries->begin(), entries->end());
    }

    done->_entries.swap(*entries);
    int ret = bthread::execution_queue_execute(_disk_queue, done);
    CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
    wakeup_all_waiter(lck);
}

void LogManager::append_to_storage(std::vector<LogEntry*>* to_append, 
                                   LogId* last_id, IOMetric* metric) {
    if (!_has_error.load(butil::memory_order_relaxed)) {
        size_t written_size = 0;
        for (size_t i = 0; i < to_append->size(); ++i) {
            written_size += (*to_append)[i]->data.size();
        }
        butil::Timer timer;
        timer.start();
        int nappent = _log_storage->append_entries(*to_append, metric);
        timer.stop();
        if (nappent != (int)to_append->size()) {
            // FIXME
            LOG(ERROR) << "Fail to append_entries, "
                       << "nappent=" << nappent 
                       << ", to_append=" << to_append->size();
            report_error(EIO, "Fail to append entries");
        }
        if (nappent > 0) { 
            *last_id = (*to_append)[nappent - 1]->id;
        }
        g_storage_append_entries_latency << timer.u_elapsed();
        if (written_size) {
            g_nomralized_append_entries_latency << timer.u_elapsed() * 1024 / written_size;
        }
    }
    for (size_t j = 0; j < to_append->size(); ++j) {
        (*to_append)[j]->Release();
    }
    to_append->clear();
}

DEFINE_int32(raft_max_append_buffer_size, 256 * 1024, 
             "Flush buffer to LogStorage if the buffer size reaches the limit");

class AppendBatcher {
public:
    AppendBatcher(LogManager::StableClosure* storage[], size_t cap, LogId* last_id, 
                 LogManager* lm)
        : _storage(storage)
        , _cap(cap)
        , _size(0)
        , _buffer_size(0)
        , _last_id(last_id)
        , _lm(lm)
    {
        _to_append.reserve(1024);
    }
    ~AppendBatcher() { flush(); }

    void flush() {
        if (_size > 0) {
            IOMetric metric;
            _lm->append_to_storage(&_to_append, _last_id, &metric);
            g_storage_flush_batch_counter << _size;
            for (size_t i = 0; i < _size; ++i) {
                _storage[i]->_entries.clear();
                if (_lm->_has_error.load(butil::memory_order_relaxed)) {
                    _storage[i]->status().set_error(
                            EIO, "Corrupted LogStorage");
                }
                _storage[i]->update_metric(&metric);
                _storage[i]->Run();
            }
            _to_append.clear();
        }
        _size = 0;
        _buffer_size = 0;
    }
    void append(LogManager::StableClosure* done) {
        if (_size == _cap || 
                _buffer_size >= (size_t)FLAGS_raft_max_append_buffer_size) {
            flush();
        }
        _storage[_size++] = done;
        _to_append.insert(_to_append.end(), 
                         done->_entries.begin(), done->_entries.end());
        for (size_t i = 0; i < done->_entries.size(); ++i) {
            _buffer_size += done->_entries[i]->data.length();
        }
    }

private:
    LogManager::StableClosure** _storage;
    size_t _cap;
    size_t _size;
    size_t _buffer_size;
    std::vector<LogEntry*> _to_append;
    LogId *_last_id;
    LogManager* _lm;
};

int LogManager::disk_thread(void* meta,
                            bthread::TaskIterator<StableClosure*>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    LogManager* log_manager = static_cast<LogManager*>(meta);
    // FIXME(chenzhangyi01): it's buggy
    LogId last_id = log_manager->_disk_id;
    StableClosure* storage[256];
    AppendBatcher ab(storage, ARRAY_SIZE(storage), &last_id, log_manager);
    
    for (; iter; ++iter) {
                // ^^^ Must iterate to the end to release to corresponding
                //     even if some error has occurred
        StableClosure* done = *iter;
        done->metric.bthread_queue_time_us = butil::cpuwide_time_us() - 
                                            done->metric.start_time_us;
        if (!done->_entries.empty()) {
            ab.append(done);
        } else {
            ab.flush();
            int ret = 0;
            do {
                LastLogIdClosure* llic =
                        dynamic_cast<LastLogIdClosure*>(done);
                if (llic) {
                    // Not used log_manager->get_disk_id() as it might be out of
                    // date
                    // FIXME: it's buggy
                    llic->set_last_log_id(last_id);
                    break;
                }
                TruncatePrefixClosure* tpc = 
                        dynamic_cast<TruncatePrefixClosure*>(done);
                if (tpc) {
                    BRAFT_VLOG << "Truncating storage to first_index_kept="
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
                        last_id.term = tsc->last_term_kept();
                        CHECK(last_id.index == 0 || last_id.term != 0)
                                << "last_id=" << last_id;
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
                log_manager->report_error(ret, "Failed operation on LogStorage");
            }
            done->Run();
        }
    }
    CHECK(!iter) << "Must iterate to the end";
    ab.flush();
    log_manager->set_disk_id(last_id);
    return 0;
}

void LogManager::set_snapshot(const SnapshotMeta* meta) {
    BRAFT_VLOG << "Set snapshot last_included_index="
              << meta->last_included_index()
              << " last_included_term=" <<  meta->last_included_term();
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (meta->last_included_index() <= _last_snapshot_id.index) {
        return;
    }
    Configuration conf;
    for (int i = 0; i < meta->peers_size(); ++i) {
        conf.add_peer(meta->peers(i));
    }
    Configuration old_conf;
    for (int i = 0; i < meta->old_peers_size(); ++i) {
        old_conf.add_peer(meta->old_peers(i));
    }
    ConfigurationEntry entry;
    entry.id = LogId(meta->last_included_index(), meta->last_included_term());
    entry.conf = conf;
    entry.old_conf = old_conf;
    _config_manager->set_snapshot(entry);
    int64_t term = unsafe_get_term(meta->last_included_index());

    const LogId last_but_one_snapshot_id = _last_snapshot_id;
    _last_snapshot_id.index = meta->last_included_index();
    _last_snapshot_id.term = meta->last_included_term();
    if (_last_snapshot_id > _applied_id) {
        _applied_id = _last_snapshot_id;
    }
    if (_last_snapshot_id > _disk_id) {
        _disk_id = _last_snapshot_id;
    }
    if (term == 0) {
        // last_included_index is larger than last_index
        // FIXME: what if last_included_index is less than first_index?
        _virtual_first_log_id = _last_snapshot_id;
        truncate_prefix(meta->last_included_index() + 1, lck);
        return;
    } else if (term == meta->last_included_term()) {
        // Truncating log to the index of the last snapshot.
        // We don't truncate log before the latest snapshot immediately since
        // some log around last_snapshot_index is probably needed by some
        // followers
        if (last_but_one_snapshot_id.index > 0) {
            // We have last snapshot index
            _virtual_first_log_id = last_but_one_snapshot_id;
            truncate_prefix(last_but_one_snapshot_id.index + 1, lck);
        }
        return;
    } else {
        // TODO: check the result of reset.
        _virtual_first_log_id = _last_snapshot_id;
        reset(meta->last_included_index() + 1, lck);
        return;
    }
    CHECK(false) << "Cannot reach here";
}

void LogManager::clear_bufferred_logs() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_last_snapshot_id.index != 0) {
        _virtual_first_log_id = _last_snapshot_id;
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
    // check virtual first log
    if (index == _virtual_first_log_id.index) {
        return _virtual_first_log_id.term;
    }
    // check last_snapshot_id
    if (index == _last_snapshot_id.index) {
        return _last_snapshot_id.term;
    }
    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) {
        return 0;
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
    // check virtual first log
    if (index == _virtual_first_log_id.index) {
        return _virtual_first_log_id.term;
    }
    // check last_snapshot_id
    if (index == _last_snapshot_id.index) {
        return _last_snapshot_id.term;
    }
    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) {
        return 0;
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
    if (index > _last_log_index || index < _first_log_index) {
        return NULL;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        entry->AddRef();
        return entry;
    }
    lck.unlock();
    g_read_entry_from_storage << 1;
    entry = _log_storage->get_entry(index);
    if (!entry) {
        report_error(EIO, "Corrupted entry at index=%" PRId64, index);
    }
    return entry;
}

void LogManager::get_configuration(const int64_t index, ConfigurationEntry* conf) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _config_manager->get(index, conf);
}

bool LogManager::check_and_set_configuration(ConfigurationEntry* current) {
    if (current == NULL) {
        CHECK(false) << "current should not be NULL";
        return false;
    }
    BAIDU_SCOPED_LOCK(_mutex);

    const ConfigurationEntry& last_conf = _config_manager->last_configuration();
    if (current->id != last_conf.id) {
        *current = last_conf;
        return true;
    }
    return false;
}

void LogManager::set_disk_id(const LogId& disk_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);  // Race with set_applied_id
    if (disk_id < _disk_id) {
        return;
    }
    _disk_id = disk_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    lck.unlock();
    return clear_memory_logs(clear_id);
}

void LogManager::set_applied_id(const LogId& applied_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);  // Race with set_disk_id
    if (applied_id < _applied_id) {
        return;
    }
    _applied_id = applied_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    lck.unlock();
    return clear_memory_logs(clear_id);
}

void LogManager::shutdown() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _stopped = true;
    wakeup_all_waiter(lck);
}

void* LogManager::run_on_new_log(void *arg) {
    WaitMeta* wm = (WaitMeta*)arg;
    wm->on_new_log(wm->arg, wm->error_code);
    butil::return_object(wm);
    return NULL;
}

LogManager::WaitId LogManager::wait(
        int64_t expected_last_log_index, 
        int (*on_new_log)(void *arg, int error_code), void *arg) {
    WaitMeta* wm = butil::get_object<WaitMeta>();
    if (BAIDU_UNLIKELY(wm == NULL)) {
        PLOG(FATAL) << "Fail to new WaitMeta";
        abort();
        return -1;
    }
    wm->on_new_log = on_new_log;
    wm->arg = arg;
    wm->error_code = 0;
    return notify_on_new_log(expected_last_log_index, wm);
}

LogManager::WaitId LogManager::notify_on_new_log(
        int64_t expected_last_log_index, WaitMeta* wm) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (expected_last_log_index != _last_log_index || _stopped) {
        wm->error_code = _stopped ? ESTOP : 0;
        lck.unlock();
        bthread_t tid;
        if (bthread_start_urgent(&tid, NULL, run_on_new_log, wm) != 0) {
            PLOG(ERROR) << "Fail to start bthread";
            run_on_new_log(wm);
        }
        return 0;  // Not pushed into _wait_map
    }
    if (_next_wait_id == 0) {  // skip 0
        ++_next_wait_id;
    }
    const int wait_id = _next_wait_id++;
    _wait_map[wait_id] = wm;
    return wait_id;
}

int LogManager::remove_waiter(WaitId id) {
    WaitMeta* wm = NULL;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        WaitMeta** pwm = _wait_map.seek(id);
        if (pwm) {
            wm = *pwm;
            _wait_map.erase(id);
        }
    }
    if (wm) {
        butil::return_object(wm);
    }
    return wm ? 0 : -1;
}

void LogManager::wakeup_all_waiter(std::unique_lock<raft_mutex_t>& lck) {
    if (_wait_map.empty()) {
        return;
    }
    WaitMeta* wm[_wait_map.size()];
    size_t nwm = 0;
    for (butil::FlatMap<int64_t, WaitMeta*>::const_iterator
            iter = _wait_map.begin(); iter != _wait_map.end(); ++iter) {
        wm[nwm++] = iter->second;
    }
    _wait_map.clear();
    const int error_code = _stopped ? ESTOP : 0;
    lck.unlock();
    for (size_t i = 0; i < nwm; ++i) {
        wm[i]->error_code = error_code;
        bthread_t tid;
        bthread_attr_t attr = BTHREAD_ATTR_NORMAL | BTHREAD_NOSIGNAL;
        if (bthread_start_background(
                    &tid, &attr,
                    run_on_new_log, wm[i]) != 0) {
            PLOG(ERROR) << "Fail to start bthread";
            run_on_new_log(wm[i]);
        }
    }
    bthread_flush();
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

void LogManager::get_status(LogManagerStatus* status) {
    if (!status) {
        return;
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    status->first_index = _log_storage->first_log_index();
    status->last_index = _log_storage->last_log_index();
    status->disk_index = _disk_id.index;
    status->known_applied_index = _applied_id.index;
}

void LogManager::report_error(int error_code, const char* fmt, ...) {
    _has_error.store(true, butil::memory_order_relaxed);
    va_list ap;
    va_start(ap, fmt);
    Error e;
    e.set_type(ERROR_TYPE_LOG);
    e.status().set_error(error_code, fmt, ap);
    va_end(ap);
    _fsm_caller->on_error(e);
}

butil::Status LogManager::check_consistency() {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK_GT(_first_log_index, 0);
    CHECK_GE(_last_log_index, 0);
    if (_last_snapshot_id == LogId(0, 0)) {
        if (_first_log_index == 1) {
            return butil::Status::OK();
        }
        return butil::Status(EIO, "Missing logs in (0, %" PRId64 ")", _first_log_index);
    } else {
        if (_last_snapshot_id.index >= _first_log_index - 1
                && _last_snapshot_id.index <= _last_log_index) {
            return butil::Status::OK();
        }
        return butil::Status(EIO, "There's a gap between snapshot={%" PRId64 ", %" PRId64 "}"
                                 " and log=[%" PRId64 ", %" PRId64 "] ",
                            _last_snapshot_id.index, _last_snapshot_id.term,
                            _first_log_index, _last_log_index);
    }
    CHECK(false) << "Can't reach here";
    return butil::Status(-1, "Impossible condition");
}

}  //  namespace braft
