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
#include "raft/protobuf_file.h"
#include "raft/storage.h"

DEFINE_int64(max_duplicated_request_cache, 300000, "duplicated request cache maxsize");

namespace counter {

Counter::Counter(const raft::GroupId& group_id, const raft::ReplicaId& replica_id)
    : StateMachine(), _node(group_id, replica_id), _value(0), _is_leader(false),
    _duplicated_request_cache(FLAGS_max_duplicated_request_cache) {
    bthread_mutex_init(&_mutex, NULL);
}

Counter::~Counter() {
    bthread_mutex_destroy(&_mutex);
}

int Counter::init(const raft::NodeOptions& options) {
    return _node.init(options);
}

raft::NodeStats Counter::stats() {
    return _node.stats();
}

static int diff_peers(const std::vector<raft::PeerId>& old_peers,
                  const std::vector<raft::PeerId>& new_peers, raft::PeerId* peer) {
    raft::Configuration old_conf(old_peers);
    raft::Configuration new_conf(new_peers);
    LOG(TRACE) << "diff conf, old: " << old_conf << " new: " << new_conf;
    if (old_peers.size() == new_peers.size() - 1 && new_conf.contain(old_peers)) {
        // add peer
        for (size_t i = 0; i < old_peers.size(); i++) {
            new_conf.remove_peer(old_peers[i]);
        }
        std::vector<raft::PeerId> peers;
        new_conf.peer_vector(&peers);
        CHECK(1 == peers.size());
        *peer = peers[0];
        return 0;
    } else if (old_peers.size() == new_peers.size() + 1 && old_conf.contain(new_peers)) {
        // remove peer
        for (size_t i = 0; i < new_peers.size(); i++) {
            old_conf.remove_peer(new_peers[i]);
        }
        std::vector<raft::PeerId> peers;
        old_conf.peer_vector(&peers);
        CHECK(1 == peers.size());
        *peer = peers[0];
        return 0;
    } else {
        return -1;
    }
}

void Counter::set_peer(const std::vector<raft::PeerId>& old_peers,
                  const std::vector<raft::PeerId>& new_peers, raft::Closure* done) {
    raft::PeerId peer;
    if (new_peers.size() == old_peers.size() + 1) {
        if (0 == diff_peers(old_peers, new_peers, &peer)) {
            LOG(TRACE) << "add peer " << peer;
            _node.add_peer(old_peers, peer, done);
        } else {
            done->set_error(EINVAL, "add_peer invalid peers");
            done->Run();
        }
    } else if (old_peers.size() == new_peers.size() + 1) {
        if (0 == diff_peers(old_peers, new_peers, &peer)) {
            LOG(TRACE) << "remove peer " << peer;
            _node.remove_peer(old_peers, peer, done);
        } else {
            done->set_error(EINVAL, "remove_peer invalid peers");
            done->Run();
        }
    } else {
        int ret = _node.set_peer(old_peers, new_peers);
        if (ret != 0) {
            done->set_error(ret, "set_peer failed");
        }
        done->Run();
    }
}

void Counter::snapshot(raft::Closure* done) {
    _node.snapshot(done);
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

void Counter::fetch_and_add(int32_t ip, int32_t pid, int64_t req_id,
                            int64_t value, FetchAndAddDone* done) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    ClientRequestId client_req_id(ip, pid, req_id);
    CounterDuplicatedRequestCache::iterator iter = _duplicated_request_cache.Get(client_req_id);
    if (iter != _duplicated_request_cache.end()) {
        FetchAndAddResult* result = iter->second;
        LOG(WARNING) << "find duplicated request from cache, ip: " << base::EndPoint(base::int2ip(ip), 0)
            << " pid: " << pid << " req_id: " << req_id << " value: " << value
            << " return " << result->value << " at index: " << result->index;
        done->set_result(result->value, result->index);
        done->Run();
    } else {
        FetchAndAddRequest request;
        request.set_ip(ip);
        request.set_pid(pid);
        request.set_req_id(req_id);
        request.set_value(value);
        base::IOBuf data;
        base::IOBufAsZeroCopyOutputStream wrapper(&data);
        request.SerializeToZeroCopyStream(&wrapper);

        _node.apply(data, done);
    }
}

int Counter::get(int64_t* value_ptr, const int64_t index) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    if (_is_leader) {
        *value_ptr = _value;
        return 0;
    } else if (index >= _applied_index) {
        *value_ptr = _value;
        return 0;
    } else {
        return EPERM;
    }
}

void Counter::on_apply(const base::IOBuf &data, const int64_t index, raft::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);

    FetchAndAddRequest request;
    base::IOBufAsZeroCopyInputStream wrapper(data);
    request.ParseFromZeroCopyStream(&wrapper);

    std::lock_guard<bthread_mutex_t> guard(_mutex);

    LOG(NOTICE) << "fetch_and_add, index: " << index
        << " val: " << request.value() << " ret: " << _value;

    // add to duplicated request cache
    ClientRequestId client_req_id(request.ip(), request.pid(), request.req_id());
    _duplicated_request_cache.Put(client_req_id, new FetchAndAddResult(_value, index));

    // fetch
    if (done) {
        //FetchAndAddDone* fetch_and_add_done = dynamic_cast<FetchAndAddDone*>(done);
        FetchAndAddDone* fetch_and_add_done = (FetchAndAddDone*)done;
        fetch_and_add_done->set_result(_value, index);
    }
    // add
    _value += request.value();

    _applied_index = index;
}

void Counter::on_shutdown() {
    //TODO:
    LOG(ERROR) << "on_shutdown";
    exit(1);
    //delete this;
}

int Counter::on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);

    std::string snapshot_path = raft::fileuri2path(writer->get_uri());
    snapshot_path.append("/data");

    LOG(INFO) << "on_snapshot_save to " << snapshot_path;

    SnapshotInfo info;
    info.set_value(_value);
    raft::ProtoBufFile pb_file(snapshot_path);
    return pb_file.save(&info, true);
}

int Counter::on_snapshot_load(raft::SnapshotReader* reader) {
    std::string snapshot_path = raft::fileuri2path(reader->get_uri());
    snapshot_path.append("/data");

    LOG(INFO) << "on_snapshot_load from " << snapshot_path;
    raft::ProtoBufFile pb_file(snapshot_path);
    SnapshotInfo info;
    int ret = pb_file.load(&info);
    if (0 == ret) {
        _value = info.value();
    }
    return ret;
}

void Counter::on_leader_start() {
    LOG(INFO) << "on_leader_start, can accept get";
    _is_leader = true;
}

void Counter::on_leader_stop() {
    LOG(INFO) << "on_leader_stop, can't accept get";
    _is_leader = false;
}

}
