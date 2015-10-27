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
    , _last_committed_index(0)
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
    _last_committed_index.store(
            options.last_committed_index, boost::memory_order_relaxed);
    _waiter = options.waiter;
    _pending_index = 0;
    return 0;
}

int CommitmentManager::set_stable_at_peer_reentrant(
        int64_t log_index, const PeerId& peer) {
    // FIXME(chenzhangyi01): The cricital section is unacceptable because it 
    // blocks all the other Replicators and LogManagers
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
    if (pm->peers.erase(peer) == 0) {
        return 0;
    }
    if (--pm->quorum > 0) {
        return 0;
    }
    // When removing a peer off the raft group which contains even number of
    // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
    // this case, the log after removal may be committed before some previous
    // logs, since we use the new configuration to deal the quorum of the
    // removal request, we think it's safe the commit all the uncommitted 
    // previous logs which is not well proved right now
    // TODO: add vlog when committing previous logs
    for (int64_t index = _pending_index; index <= log_index; ++log_index) {
        PendingMeta *tmp = *_pending_apps.top();
        _pending_apps.pop();
        void *saved_context = tmp->context;
        delete tmp;
        // FIXME: remove this off the critical section
        _waiter->on_committed(index, saved_context);
    }
   
    _pending_index = log_index + 1;
    _last_committed_index.store(log_index, boost::memory_order_release);

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
    CHECK(new_pending_index > _last_committed_index.load(
                                    boost::memory_order_relaxed));
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
    // FIXME: it seems that lock is not necessary here
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index == 0 && _pending_apps.empty()) << "Must be called by follower";
    if (last_committed_index < _last_committed_index.load(boost::memory_order_relaxed)) {
        return -1;
    }
    if (last_committed_index > _last_committed_index.load(boost::memory_order_relaxed)) {
        _last_committed_index.store(last_committed_index, boost::memory_order_release);
        _waiter->on_committed(last_committed_index, NULL);
    }
    return 0;
}

}  // namespace raft
