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

namespace counter {

Counter::Counter(const raft::GroupId& group_id, const raft::ReplicaId& replica_id)
    : StateMachine(), _node(group_id, replica_id), _value(0) {
    bthread_mutex_init(&_mutex, NULL);
}

Counter::~Counter() {
    bthread_mutex_destroy(&_mutex);
}

int Counter::init(const raft::NodeOptions& options) {
    return _node.init(options);
}

void Counter::shutdown(raft::Closure* done) {
    _node.shutdown(done);
}

base::EndPoint Counter::leader() {
    return _node.leader_id().addr;
}

base::EndPoint Counter::self() {
    return _node.node_id().peer_id.addr;
}

int Counter::add(int64_t value, raft::Closure* done) {
    std::string request_str;
    AddRequest request;
    request.set_value(value);
    request.SerializeToString(&request_str);

    return _node.apply(request_str.data(), request_str.size(), done);
}

int Counter::get(int64_t* value_ptr) {
    bthread_mutex_lock(&_mutex);
    *value_ptr = _value;
    bthread_mutex_unlock(&_mutex);
    return 0;
}

void Counter::on_apply(const void* data, const int len, const int64_t index, raft::Closure* done) {
    AddRequest request;
    request.ParseFromArray(data, len);

    bthread_mutex_lock(&_mutex);
    _value += request.value();
    bthread_mutex_unlock(&_mutex);
}

void Counter::on_shutdown() {
    //TODO:
    LOG(ERROR) << "Not Implement";
    delete this;
}

int Counter::on_snapshot_save() {
    //TODO:
    LOG(ERROR) << "Not Implement";
    return ENOSYS;
}

int Counter::on_snapshot_load() {
    //TODO:
    LOG(ERROR) << "Not Implement";
    return ENOSYS;
}

void Counter::on_leader_start() {
    //TODO
    LOG(ERROR) << "Not Implement";
}

void Counter::on_leader_stop() {
    //TODO
    LOG(ERROR) << "Not Implement";
}

}
