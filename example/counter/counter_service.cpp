/*
 * =====================================================================================
 *
 *       Filename:  counter_service.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月23日 14时07分17秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <base/logging.h>
#include "counter_service.h"
#include "counter.h"

namespace counter {

CounterServiceImpl::CounterServiceImpl(Counter* counter)
    : _counter(counter) {
}

CounterServiceImpl::~CounterServiceImpl() {
}

void CounterServiceImpl::add(google::protobuf::RpcController* controller,
                             const AddRequest* request,
                             AddResponse* response,
                             google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received add " << request->value() << " from " << cntl->remote_side();

    // node apply
    raft::NodeCtx* ctx = new raft::NodeCtx(controller, request, response, done);
    if (0 != _counter->add(request->value(), ctx)) {
        baidu::rpc::ClosureGuard done_guard(done);
        response->set_success(false);
        response->set_leader(_counter->leader());

        delete ctx;
    }
}

void CounterServiceImpl::get(google::protobuf::RpcController* controller,
                             const GetRequest* request,
                             GetResponse* response,
                             google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received get from " << cntl->remote_side();

    // get and response
    int64_t value = 0;
    if (0 == _counter->get(&value)) {
        response->set_success(true);
        response->set_value(value);
    } else {
        response->set_success(false);
        response->set_leader(_counter->leader());
    }
}

}
