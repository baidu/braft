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
#include "baidu/rpc/closure_guard.h"
#include "counter.pb.h"
#include "counter.h"
#include "raft/util.h"
#include "raft/protobuf_file.h"
#include "raft/storage.h"

namespace counter {

Counter::Counter(const raft::GroupId& group_id, const raft::ReplicaId& replica_id)
    : StateMachine(), _node(group_id, replica_id), _value(0) {
}

Counter::~Counter() {
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

void Counter::add(int64_t value, raft::Closure* done) {
    AddRequest request;
    request.set_value(value);
    base::IOBuf data;
    base::IOBufAsZeroCopyOutputStream wrapper(&data);
    request.SerializeToZeroCopyStream(&wrapper);

    _node.apply(data, done);
}

int Counter::get(int64_t* value_ptr) {
    *value_ptr = _value;
    return 0;
}

void Counter::on_apply(const base::IOBuf &data, const int64_t index, raft::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);

    LOG(NOTICE) << "apply " << index;
    AddRequest request;
    base::IOBufAsZeroCopyInputStream wrapper(data);
    request.ParseFromZeroCopyStream(&wrapper);

    _value += request.value();
}

void Counter::on_shutdown() {
    //TODO:
    LOG(ERROR) << "Not Implement";
    delete this;
}

int Counter::on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);

    std::string snapshot_path = raft::fileuri2path(writer->get_uri());
    snapshot_path.append("/data");

    LOG(INFO) << "snapshot_save to " << snapshot_path;

    SnapshotInfo info;
    info.set_value(_value);
    raft::ProtoBufFile pb_file(snapshot_path);
    return pb_file.save(&info, true);
}

int Counter::on_snapshot_load(raft::SnapshotReader* reader) {
    std::string snapshot_path = raft::fileuri2path(reader->get_uri());
    LOG(INFO) << "uri: " << reader->get_uri() << " snapshot path: " << snapshot_path;
    snapshot_path.append("/data");

    LOG(INFO) << "snapshot_load from " << snapshot_path;
    raft::ProtoBufFile pb_file(snapshot_path);
    SnapshotInfo info;
    int ret = pb_file.load(&info);
    if (0 == ret) {
        _value = info.value();
    }
    return ret;
}

void Counter::on_leader_start() {
    //TODO
    LOG(ERROR) << "on_leader_start Not Implement";
}

void Counter::on_leader_stop() {
    //TODO
    LOG(ERROR) << "on_leader_stop Not Implement";
}

}
