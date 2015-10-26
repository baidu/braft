/*
 * =====================================================================================
 *
 *       Filename:  counter.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月23日 16时40分11秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gflags/gflags.h>
#include "counter.pb.h"
#include "counter.h"

DEFINE_bool(enable_verify, false, "verify when get");

namespace counter {

Counter::Counter() : _node(NULL), _value(0) {
    bthread_mutex_init(&_mutex, NULL);
}

Counter::~Counter() {
    bthread_mutex_destroy(&_mutex);
}

int Counter::init(const raft::GroupId& group_id, const raft::PeerId& peer_id,
             raft::NodeOptions* options) {
    _node = raft::NodeManager::GetInstance()->create(group_id, peer_id, options);
    return _node->init();
}

int Counter::shutdown() {
    return _node->shutdown();
}

std::string Counter::leader() {
    return _node->leader().to_string();
}

int Counter::add(int64_t value, raft::NodeCtx* ctx) {
    std::string request_str;
    AddRequest request;
    request.set_value(value);
    request.SerializeToString(&request_str);

    return _node->apply(request_str.data(), request_str.size(), ctx);
}

int Counter::get(int64_t* value_ptr) {
    if (FLAGS_enable_verify) {
        if (0 != _node->verify()) {
            return -1;
        }
    }
    {
        bthread_mutex_lock(&_mutex);
        *value_ptr = _value;
        bthread_mutex_unlock(&_mutex);
    }
    return 0;
}

void Counter::apply(const void* data, const int len, raft::NodeCtx* ctx) {
    AddRequest request;
    if (ctx) {
        request.Swap(static_cast<AddRequest*>(ctx->request));
    } else {
        request.ParseFromArray(data, len);
    }

    //TODO: move err to error()
    bool success = false;
    if (!ctx || (ctx && 0 == ctx->err_no)) {
        bthread_mutex_lock(&_mutex);

        _value += request.value();
        success = true;

        bthread_mutex_unlock(&_mutex);
    }

    if (ctx) {
        google::protobuf::Closure* done = ctx->done;
        AddRequest* response = static_cast<AddResponse*>(ctx->response);
        response->set_success(success);
        if (!success) {
            response->set_leader(_node.leader().to_string());
        }
        done->Run();
        delete ctx;
    }
}

int Counter::snapshot_save(base::Closure* done) {
    //TODO:
    return 0;
}

int Counter::snapshot_load(base::Closure* done) {
    //TODO:
    return 0;
}

}
