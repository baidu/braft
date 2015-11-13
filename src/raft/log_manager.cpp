// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/14 11:45:23

#include "raft/log_manager.h"

#include <base/logging.h>
#include <bthread.h>
#include <bthread_unstable.h>
#include "raft/log_entry.h"
#include "raft/util.h"
#include "raft/storage.h"
#include "raft/node.h"

namespace raft {

LogManagerOptions::LogManagerOptions()
    : log_storage(NULL), configuration_manager(NULL)
{}

LogManager::LogManager()
    : _log_storage(NULL), _config_manager(NULL),
    _last_log_index(0),
    _leader_disk_thread_running(false),
    _stopped(false)
{
    CHECK_EQ(0, bthread_id_list_init(&_wait_list, 16/*FIXME*/, 16));
    CHECK_EQ(0, bthread_mutex_init(&_mutex, NULL));
}

int LogManager::init(const LogManagerOptions &options) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    if (options.log_storage == NULL) {
        return EINVAL;
    }
    _log_storage = options.log_storage;
    _config_manager = options.configuration_manager;
    int ret = _log_storage->init(_config_manager);
    if (ret != 0) {
        return ret;
    }
    _last_log_index = _log_storage->last_log_index();
    return 0;
}

LogManager::~LogManager() {
    bthread_id_list_destroy(&_wait_list);
    bthread_mutex_destroy(&_mutex);
}

int LogManager::start_disk_thread() {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    CHECK(!_leader_disk_thread_running);
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    queue_options.max_tasks_size = 5;
    int ret = bthread::execution_queue_start(&_leader_disk_queue,
                                   &queue_options,
                                   LogManager::leader_disk_run,
                                   this);
    if (ret == 0) {
        _leader_disk_thread_running = true;
    } else {
        LOG(FATAL) << "start_disk_thread failed";
    }
    return ret;
}

int LogManager::stop_disk_thread() {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    bthread::execution_queue_stop(_leader_disk_queue);
    int ret = bthread::execution_queue_join(_leader_disk_queue);
    if (0 == ret) {
        _leader_disk_thread_running = false;
    }
    return ret;
}

void LogManager::clear_memory_logs(const int64_t index) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    while (!_logs_in_memory.empty()) {
        LogEntry* entry = _logs_in_memory.front();
        if (entry->index <= index) {
            entry->Release();
            _logs_in_memory.pop_front();
        } else {
            break;
        }
    }
}

int64_t LogManager::first_log_index() {
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    return _log_storage->first_log_index();
}

int64_t LogManager::last_log_index() {
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    if (_last_log_index != 0) {
        return _last_log_index;
    }
    return _log_storage->last_log_index();
}

int LogManager::truncate_prefix(const int64_t first_index_kept) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    int ret = 0;
    //OPTIMIZE: flush leader disk thread sync, can be optimize in async
    //some snapshot included logs not write to storage when leader snapshot finished
    if (_leader_disk_thread_running) {
        ret = bthread::execution_queue_stop(_leader_disk_queue);
        if (ret != 0) {
            LOG(FATAL) << "leader disk thread stop failed";
            return ret;
        }
        ret = bthread::execution_queue_join(_leader_disk_queue);
        if (ret != 0) {
            LOG(FATAL) << "leader disk thread join failed";
            return ret;
        }

        bthread::ExecutionQueueOptions queue_options;
        queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
        queue_options.max_tasks_size = 5;
        ret = bthread::execution_queue_start(&_leader_disk_queue,
                                                 &queue_options,
                                                 LogManager::leader_disk_run,
                                                 this);
        if (ret != 0) {
            LOG(FATAL) << "leader disk thread start failed";
            return ret;
        }
    }

    while (!_logs_in_memory.empty()) {
        LogEntry* entry = _logs_in_memory.front();
        if (entry->index < first_index_kept) {
            entry->Release();
            _logs_in_memory.pop_front();
        } else {
            break;
        }
    }

    _config_manager->truncate_prefix(first_index_kept);
    return _log_storage->truncate_prefix(first_index_kept);
}

int LogManager::truncate_suffix(const int64_t last_index_kept) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

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
    return _log_storage->truncate_suffix(last_index_kept);
}

int LogManager::append_entries(const std::vector<LogEntry*>& entries) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    //TODO: move index setting to LogStorage
    for (size_t i = 0; i < entries.size(); i++) {
        entries[i]->index = _last_log_index + 1 + i;
    }

    int ret = _log_storage->append_entries(entries);
    if (static_cast<size_t>(ret) == entries.size()) {
        ret = 0;
        for (size_t i = 0; i < entries.size(); i++) {
            _logs_in_memory.push_back(entries[i]);
            if (entries[i]->type == ENTRY_TYPE_ADD_PEER || entries[i]->type == ENTRY_TYPE_REMOVE_PEER) {
                _config_manager->add(entries[i]->index, Configuration(*(entries[i]->peers)));
            }
        }
        _last_log_index += entries.size();
    } else {
        ret = EIO;
    }

    return ret;
}

void LogManager::append_entry(
            LogEntry* log_entry, LeaderStableClosure* done) {
    bthread_mutex_lock(&_mutex);

    log_entry->index = ++_last_log_index;
    _logs_in_memory.push_back(log_entry);
    if (log_entry->type == ENTRY_TYPE_ADD_PEER || log_entry->type == ENTRY_TYPE_REMOVE_PEER) {
        _config_manager->add(log_entry->index, Configuration(*(log_entry->peers)));
    }

    CHECK(_leader_disk_thread_running);
    // signal leader disk
    int ret = bthread::execution_queue_execute(_leader_disk_queue, done);
    CHECK(ret == 0);

    // signal replicator
    //int64_t last_log_index = log_entry->index;
    bthread_id_list_reset(&_wait_list, 0);
    bthread_mutex_unlock(&_mutex);
}

int LogManager::leader_disk_run(void* meta,
                                LeaderStableClosure** const tasks[], size_t tasks_size) {
    LogManager* log_manager = static_cast<LogManager*>(meta);
    std::vector<LogEntry*> entries;
    for (size_t i = 0; i < tasks_size; i++) {
        LeaderStableClosure* done = *tasks[i];
        entries.push_back(done->_entry);
    }

    int ret = 0;
    if (entries.size() > 0) {
        // mutex protect, log_storage not thread-safe
        std::lock_guard<bthread_mutex_t> guard(log_manager->_mutex);
        ret = log_manager->_log_storage->append_entries(entries);
        if (entries.size() != static_cast<size_t>(ret)) {
            ret = EIO;
        }
    }

    for (size_t i = 0; i < tasks_size; i++) {
        LeaderStableClosure* done = *tasks[i];
        if (ret != 0) {
            done->set_error(EIO, "append entry failed");
        }
        done->Run();
    }

    return 0;
}

LogEntry* LogManager::get_entry_from_memory(const int64_t index) {
    LogEntry* entry = NULL;
    if (!_logs_in_memory.empty()) {
        int64_t first_index = _logs_in_memory.front()->index;
        int64_t last_index = _logs_in_memory.back()->index;
        if (index >= first_index && index <= last_index) {
            entry = _logs_in_memory.at(index - first_index);
        }
    }
    return entry;
}

int64_t LogManager::get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        return entry->term;
    }

    return _log_storage->get_term(index);
}

LogEntry* LogManager::get_entry(const int64_t index) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        entry->AddRef();
    } else {
        entry = _log_storage->get_entry(index);
    }
    return entry;
}

ConfigurationPair LogManager::get_configuration(const int64_t index) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    return _config_manager->get_configuration(index);
}

bool LogManager::check_and_set_configuration(ConfigurationPair& current) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    int64_t last_config_index = _config_manager->last_configuration_index();
    if (BAIDU_UNLIKELY(current.first < last_config_index)) {
        current = _config_manager->last_configuration();
        return true;
    }
    /*
    std::pair<int64_t, Configuration> last = _config_manager->last_configuration();
    if (BAIDU_UNLIKELY(current.first < last.first)) {
        current = _config_manager->last_configuration();
        return true;
    }
    assert(current.first == last.first);
    //*/
    return false;
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
    bthread_mutex_lock(&_mutex);
    bthread_id_list_reset(&_wait_list, ESTOP);
    _stopped = true;
    bthread_mutex_unlock(&_mutex);
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
    bthread_mutex_lock(&_mutex);
    if (expected_last_log_index != _last_log_index && _stopped) {
        bthread_mutex_unlock(&_mutex);
        bthread_id_error(wait_id, 0);
        return;
    }
    if (bthread_id_list_add(&_wait_list, wait_id) != 0) {
        bthread_mutex_unlock(&_mutex);
        bthread_id_error(wait_id, EAGAIN);
        return;
    }
    bthread_mutex_unlock(&_mutex);
}

}  // namespace raft
