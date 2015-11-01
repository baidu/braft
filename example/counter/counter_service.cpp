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

class Closure : public raft::Closure {
public:
    Closure(Counter* counter, baidu::rpc::Controller* controller,
            const AddRequest* request, AddResponse* response, google::protobuf::Closure* done)
        : _counter(counter), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~Closure() {}

    virtual void Run() {
        LOG(NOTICE) << "rpc return";
        if (_err_code == 0) {
            _response->set_success(true);
        } else {
            LOG(WARNING) << "counter: " << _counter << " add failed: "
                << _err_code << noflush;
            if (!_err_text.empty()) {
                LOG(WARNING) << "(" << _err_text << ")";
            }
            LOG(WARNING);

            _response->set_success(false);
            _response->set_leader(base::endpoint2str(_counter->leader()).c_str());
        }
        _done->Run();
    }
private:
    Counter* _counter;
    baidu::rpc::Controller* _controller;
    const AddRequest* _request;
    AddResponse* _response;
    google::protobuf::Closure* _done;
};

void CounterServiceImpl::add(google::protobuf::RpcController* controller,
                             const AddRequest* request,
                             AddResponse* response,
                             google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received add " << request->value() << " from " << cntl->remote_side();

    // node apply
    Closure* cb = new Closure(_counter, cntl, request, response, done);
    if (0 != _counter->add(request->value(), cb)) {
        LOG(WARNING) << "add failed, redirect to leader: " << _counter->leader();
        baidu::rpc::ClosureGuard done_guard(done);
        response->set_success(false);
        response->set_leader(base::endpoint2str(_counter->leader()).c_str());
        delete cb;
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
        response->set_leader(base::endpoint2str(_counter->leader()).c_str());
    }
}

}
