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
#include <boost/atomic.hpp>
#include <baidu/rpc/controller.h>
#include "raft/util.h"
#include "raft/raft.h"
#include "client_req_id.h"
#include "state_machine.h"

namespace counter {

class Counter;
class FetchAndAddDone : public raft::Closure {
public:
    FetchAndAddDone(Counter* counter, baidu::rpc::Controller* controller,
            const FetchAndAddRequest* request, FetchAndAddResponse* response,
            google::protobuf::Closure* done)
        : _counter(counter), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~FetchAndAddDone() {}

    void set_result(const int64_t value, const int64_t index) {
        _response->set_value(value);
        _response->set_index(index);
    }

    virtual void Run();
private:
    Counter* _counter;
    baidu::rpc::Controller* _controller;
    const FetchAndAddRequest* _request;
    FetchAndAddResponse* _response;
    google::protobuf::Closure* _done;
};

class Counter : public example::CommonStateMachine {
public:
    Counter(const raft::GroupId& group_id, const raft::PeerId& peer_id);

    // FSM method
    virtual void on_apply(const base::IOBuf& buf,
                          const int64_t index, raft::Closure* done);
    virtual void on_shutdown();
    virtual int on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done);
    virtual int on_snapshot_load(raft::SnapshotReader* reader);
    virtual void on_leader_start();
    virtual void on_leader_stop();

    // user logic method
    void fetch_and_add(int32_t ip, int32_t pid, int64_t req_id,
                       int64_t value, FetchAndAddDone* done);
    int get(int64_t* value_ptr, const int64_t index);

private:
    virtual ~Counter();

    boost::atomic<int64_t> _value;
    boost::atomic<int64_t> _applied_index;
    boost::atomic<bool> _is_leader;

    CounterDuplicatedRequestCache _duplicated_request_cache;
};

}


#endif //~PUBLIC_RAFT_EXAMPLE_COUNTER_H
