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

void FetchAndAddDone::Run() {
    if (_err_code == 0) {
        VLOG(9) << "counter: " << _counter << " fetch_and_add success";
        _response->set_success(true);
    } else {
        LOG(WARNING) << "counter: " << _counter << " fetch_and_add failed: "
            << _err_code << noflush;
        if (!_err_text.empty()) {
            LOG(WARNING) << "(" << _err_text << ")" << noflush;
        }
        LOG(WARNING);

        _response->set_success(false);
        _response->set_leader(base::endpoint2str(_counter->leader()).c_str());
    }
    _done->Run();
    delete this;
}

void CounterServiceImpl::fetch_and_add(google::protobuf::RpcController* controller,
                             const FetchAndAddRequest* request,
                             FetchAndAddResponse* response,
                             google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    VLOG(9) << "received fetch_and_add " << request->value() << " from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        done->Run();
        return;
    }

    // node apply
    FetchAndAddDone* fetch_and_add_done = new FetchAndAddDone(_counter, cntl,
                                                              request, response, done);
    _counter->fetch_and_add(cntl->remote_side().ip.s_addr, request->pid(), request->req_id(),
                            request->value(), fetch_and_add_done);
}

void CounterServiceImpl::get(google::protobuf::RpcController* controller,
                             const GetRequest* request,
                             GetResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    VLOG(9) << "received get from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        return;
    }

    int64_t index = 0;
    if (request->has_index()) {
        index = request->index();
    }
    // get and response
    int64_t value = 0;
    if (0 == _counter->get(&value, index)) {
        response->set_success(true);
        response->set_value(value);
    } else {
        response->set_success(false);
        response->set_leader(base::endpoint2str(_counter->leader()).c_str());
    }
}

}
