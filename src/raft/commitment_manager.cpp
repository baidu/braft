// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/16 10:44:05

#include <base/scoped_lock.h>
#include <base/unique_ptr.h>
#include "raft/commitment_manager.h"
#include "raft/util.h"

namespace raft {

CommitmentManager::CommitmentManager()
    : _waiter(NULL)
    , _last_commited_index(0)
{
    CHECK_EQ(0, bthread_mutex_init(&_mutex, NULL));
}

CommitmentManager::~CommitmentManager() {
    clear_pending_applications();
    bthread_mutex_destroy(&_mutex);
}

int CommitmentManager::init(const CommitmentManagerOptions &options) {
    if (options.waiter == NULL) {
        LOG(ERROR) << "waiter is NULL";
        return EINVAL;
    }
    const size_t spaces_size = sizeof(PendingMeta*) * options.max_pending_size;
    void *queue_spaces = malloc(spaces_size);
    if (queue_spaces == NULL) {
        LOG(ERROR) << "Fail to malloc spaces_size=" << spaces_size;
        return errno;
    }
    base::BoundedQueue<PendingMeta*> tmp_queue(queue_spaces, spaces_size, base::OWNS_STORAGE);
    _pending_apps.swap(tmp_queue);
    _last_commited_index = options.last_committed_index;
    _waiter = options.waiter;
    _pending_index = 0;
    return 0;
}

int CommitmentManager::set_stable_at_peer_reentrant(
        int64_t log_index, const PeerId& peer) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index > 0);
    if (log_index < _pending_index) {
        return 0;
    }
    if (log_index >= _pending_index + (int64_t)_pending_apps.size()) {
        CHECK(false);
        return ERANGE;
    }
    PendingMeta *pm = *_pending_apps.top(log_index - _pending_index);
    std::set<PeerId>::iterator iter = pm->peers.find(peer);
    if (iter == pm->peers.end()) {
        return 0;
    }
    pm->peers.erase(iter); // DON'T touch iter after this point
    if (--pm->quorum > 0) {
        return 0;
    }
    void* saved_context = pm->context;
    delete pm;
    _pending_apps.pop();
    CHECK(_pending_index == log_index);  // FIXME:
    ++_pending_index;
    _last_commited_index = log_index;
    _waiter->on_committed(log_index, saved_context); // FIXME
    return 0;
}

int CommitmentManager::clear_pending_applications() {
    BAIDU_SCOPED_LOCK(_mutex);
    // FIXME: should on_cleared be called out of the critical section?
    for (size_t i = 0; i < _pending_apps.size(); ++i) {
        PendingMeta *pm = *_pending_apps.top();
        _pending_apps.pop();
        _waiter->on_cleared(_pending_index + i, pm->context, -1/*FIXME*/);
        delete pm;
    }
    _pending_index = 0;
    return 0;
}

int CommitmentManager::reset_pending_index(int64_t new_pending_index) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index == 0 && _pending_apps.empty());
    CHECK(new_pending_index > _last_commited_index);
    _pending_index = new_pending_index;
    return 0;
}

int CommitmentManager::append_pending_application(const Configuration& conf, void* context) {
    std::unique_ptr<PendingMeta> pm(new PendingMeta);
    conf.peer_set(&pm->peers);
    pm->quorum = pm->peers.size() / 2 + 1;
    pm->context = context;
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index > 0);
    if (!_pending_apps.push_top(pm.get())) {
        LOG(WARNING) << "_pending_apps is full";
        return EAGAIN;
    }
    pm.release();
    return 0;
}

int CommitmentManager::set_last_committed_index(int64_t last_committed_index) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index == 0 && _pending_apps.empty()) << "Must be called by follower";
    if (last_committed_index < _last_commited_index) {
        return -1;
    }
    if (last_committed_index > _last_commited_index) {
        _last_commited_index = last_committed_index;
        _waiter->on_committed(last_committed_index, NULL);
    }
    return 0;
}

}  // namespace raft
