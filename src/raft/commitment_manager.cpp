// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/16 10:44:05

#include <base/scoped_lock.h>
#include "raft/commitment_manager.h"
#include "raft/util.h"
#include "raft/fsm_caller.h"

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
    RAFT_VLOG << "_pending_index=" << _pending_index << " log_index=" << log_index
        << " peer=" << peer;
    if (_pending_index == 0) {
        return EINVAL;
    }
    if (log_index < _pending_index) {
        return 0;
    }
    if (log_index >= _pending_index + (int64_t)_pending_apps.size()) {
        return ERANGE;
    }
    PendingMeta *pm = _pending_apps.at(log_index - _pending_index);
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
    for (int64_t index = _pending_index; index <= log_index; ++index) {
        PendingMeta *tmp = _pending_apps.front();
        _pending_apps.pop_front();
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
    size_t pending_size = _pending_apps.size();
    for (size_t i = 0; i < pending_size; ++i) {
        PendingMeta *pm = _pending_apps.front();
        _pending_apps.pop_front();

        _waiter->on_cleared(_pending_index + i, pm->context, -1/*FIXME*/);
        delete pm;
    }
    _pending_index = 0;
    return 0;
}

int CommitmentManager::reset_pending_index(int64_t new_pending_index) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index == 0 && _pending_apps.empty())
        << "pending_index " << _pending_index << " pending_apps " << _pending_apps.size();
    CHECK(new_pending_index > _last_committed_index.load(
                                    boost::memory_order_relaxed));
    _pending_index = new_pending_index;
    return 0;
}

int CommitmentManager::append_pending_application(const Configuration& conf, void* context) {
    PendingMeta* pm = new PendingMeta;
    conf.peer_set(&pm->peers);
    pm->quorum = pm->peers.size() / 2 + 1;
    pm->context = context;

    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index > 0);
    _pending_apps.push_back(pm);
    return 0;
}

int CommitmentManager::set_last_committed_index(int64_t last_committed_index) {
    // FIXME: it seems that lock is not necessary here
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_index == 0 && _pending_apps.empty()) << "Must be called by follower";
    if (last_committed_index < _last_committed_index.load(boost::memory_order_relaxed)) {
        return EINVAL;
    }
    if (last_committed_index > _last_committed_index.load(boost::memory_order_relaxed)) {
        _last_committed_index.store(last_committed_index, boost::memory_order_relaxed);
        _waiter->on_committed(last_committed_index, NULL);
    }
    return 0;
}

}  // namespace raft
