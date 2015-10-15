// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/14 11:45:23

#include "raft/log_manager.h"

#include <base/loggings.h>
#include <bthread_unstable.h>

namespace raft {

LogManagerOptions::LogManagerOptions() 
    : log_storage(NULL)
    , initial_term(0)
{}

LogManager::LogManager() 
    : _log_storage(NULL)
{
    CHECK_EQ(0, bthread_id_list_init(&_wait_list, 16/*FIXEM*/, 16));
    CHECK_EQ(0, bthread_mutex_init(&_mutex));
}

int LogManager::init(const LogManagerOptions &options) {
    if (options.log_storage == NULL) {
        return EINVAL;
    }
    _log_storage = options.log_storage;
    _first_log_index = _log_storage->first_index_index();
    _last_log_index = _log_storage->last_log_index();
    return 0;
}

LogManager::~LogManager() {
    bthread_id_list_destroy(&_wait_list);
    bthread_mutex_destroy(&_mutex);
}

int LogManager::start_disk_thread() {
    LOG(WARNING) << "Not implement yet";
    return 0;
}

int LogManager::stop_dist_thread() {
    return 0;
}

int LogManager::truncate_prefix(const int64_t first_index_kept) {
    CHECK(false) << "Not implement yet";
    return -1;
}

int LogManager::truncate_suffix(const int64_t last_index_kept) {
    bthread_mutex_lock(&_mutex);
    if (last_index_kept > _last_log_index || last_index_kept < _first_log_index) {
        int64_t saved_first_log_index = _first_log_index;
        int64_t saved_last_log_index = _last_log_index;
        bthread_mutex_unlock(&_mutex);
        LOG(ERROR) << "last_index_kept=" << last_index_kept << " is out of range=["
                   << saved_first_log_index << ", " << saved_last_log_index << ']';
        return EINVAL;
    }
    for (int64_t i = last_index_kept + 1; 
            i <= _last_log_index && !_logs_in_memory.empty(); ++i) {
        _logs_in_memory.pop_back();
    }
    _first_log_index_in_memory = std::min(_memory_log_offset, last_index_kept);
    _last_log_index = last_index_kept;
    bthread_mutex_unlock(&_mutex);
    // TODO: handle the failure of truncate_suffix
    return _log_storage->truncate_suffix(last_index_kept);
}

int LogManager::append(const LogEntry& log_entry) {
    if (_log_storage->append_log(&log_entry) != 0) {
        return -1;
    }
    bthread_mutex_lock(&_mutex);
    _logs_in_memory.push_back(log_entry);
    _last_log_index++;
    bthread_id_list_reset(_wait_list, 0);
    bthread_mutex_unlock(&_mutex);
    return 0;
}

size LogManager::append_in_batch(const *LogEntry[] entries, size_t size) {
    std::vector<LogEntry*> entry_vector;
    entry_vector.reserve(size);
    for (size_t i = 0; < size; ++i) {
        entry_vector->push_back(entries[i]);
    }
    int ret = _log_storage->append_logs(entry_vector);
    if (ret <= 0) {
        return 0;
    }
    bthread_mutex_lock(&_mutex);
    _last_log_index += ret;
    for (int i = 0; i < ret; ++i) {
        _logs_in_memory.push_back(*entries[i]);
    }
    bthread_id_list_reset(_wait_list, 0);
    bthread_mutex_unlock(&_mutex);
}

void LogManager::append(
            const LogEntry& log_entry,
            int (*on_stable)(void* arg, int64_t log_index, int error_code),
            void* arg) {
    // Fast implementation
    if (_log_storage->append_log(&log_entry) != 0) {
        on_stable(arg, -1, EPIPE/*FIXME*/);
        return;
    }
    bthread_mutex_lock(&_mutex);
    _logs_in_memory.push_back(log_entry);
    int last_log_index = ++_last_log_index;
    bthread_id_list_reset(_wait_list, 0);
    bthread_mutex_unlock(&_mutex);
    on_stable(arg, last_log_index, 0);
}

int LogManager::get_log(int64_t index, LogEntry *entry) {
    bthread_mutex_lock(&_mutex);
    if (index < first_index_index) {
        bthread_mutex_unlock(&_mutex);
        return ECOMPACTED;
    }
    if (index > last_log_index) {
        bthread_mutex_unlock(&_mutex);
        return ENODATA;
    }
    if (index < _first_log_index_in_memory) {
        bthread_mutex_unlock();
        // FIXME: there's a race condtion with truncate_prefix
        LogEntry *tmp = _log_storage->get_log(index);
        if (tmp == NULL) {
            return ECOMPACTED;  // FIXME
        }
        if (entry) {
            *entry = *tmp;
        }
        delete tmp; // FIXME
        return 0;
    }
    if (entry) {
        *entry = _logs_in_memory[index - _first_log_index_in_memory];
    }
    bthread_mutex_unlock(&_mutex);
    return 0;
}

int LogManager::get_term_of_log(int64_t index, int64_t *term) {
    // Fast implementation
    LogEntry entry;
    const int rc = get_log(index, &entry);
    if (rc != 0) {
        return rc;
    }
    if (term) {
        *term = entry.term
    }
    return 0;
}

int on_timed_out(void *arg) {
    bthread_id_t id;
    id.value = reinterpret_cast<int64_t>(arg);
    bthread_id_error(id, ETIMEDOUT);
}

int on_notified(bthread_id_t id, void *arg, int rc) {
    *(int*)arg = rc;
    return bthread_id_unlock_and_destroy(id);
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
        CHECK_EQ(0, bthread_timer_add(&timer_id, on_timed_out,
                                      reinterpret_cast<void*>(wait_id)));
    }
    notify_on_new_log(expected_last_log_index, wait_id);
    bthread_id_join(id);
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
        bthread_timer_del(wm->timer_id());
    }
    wm->on_new_log(wm->arg, wm->error_code);
    delete wm;
    return NULL;
}

int on_wait_notified(bthread_id_t id, void *arg, int error_code) {
    WaitMeta* wm = (WaitMeta*)arg;
    wm->error_code = error_code;
    bthread_t tid;
    if (bthread_start(&tid, &BTHRED_ATTR_NORMAL, run_on_new_log, wm) != 0) {
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
    bthread_id_lock(wait_id);
    bthread_timer_t timer_id;
    if (due_time) {
        CHECK_EQ(0, bthread_timer_add(&timer_id, *due_time, on_timed_out,
                                      reinterpret_cast<void*>(wait_id)));
        wm->timer_id = timer_id;
        wm->has_timer = true;
    }
    notify_on_new_log(expected_last_log_index, wait_id);
    CHECK_EQ(0, bthread_id_unlock(wait_id));
}

void LogManager::notify_on_new_log(int64_t expected_last_log_index,
                                  bthread_id_t wait_id) {
    bthread_mutex_lock(&_mutex);
    if (expected_last_log_index != _last_log_index) {
        bthread_mutex_unlock(&_mutex);
        bthread_id_error(wait_id);
        return;
    }
    if (bthread_id_list_add(&_wait_list, wait_id) != 0) {
        bthread_mutex_unlock(&_mutex);
        bthread_id_error(wait_id);
        return;
    }
    bthread_mutex_unlock(&_mutex);
}

}  // namespace raft
