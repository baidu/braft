/*
 * =====================================================================================
 *
 *       Filename:  counter.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月23日 16时34分18秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_COUNTER_H
#define PUBLIC_RAFT_EXAMPLE_COUNTER_H

#include <string>
#include <bthread.h>
#include <base/callback.h> //Closure
#include "raft/raft.h"

DECLARE_bool(enable_verify);

namespace counter {

class Counter : public raft::NodeUser {
public:
    Counter();
    virtual ~Counter();

    int init(const raft::GroupId& group_id, const raft::PeerId& peer_id,
             raft::NodeOptions* options);

    int shutdown();

    int add(int64_t value, raft::NodeCtx* ctx);

    int get(int64_t* value_ptr);

    std::string leader();

    // NodeUser method
    virtual void apply(const void* data, const int len, raft::NodeCtx* ctx);

    virtual int snapshot_save(base::Closure* done);

    virtual int snapshot_load(base::Closure* done);

private:
    raft::Node* _node;
    bthread_mutex_t _mutex;
    int64_t _value;
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_COUNTER_H
