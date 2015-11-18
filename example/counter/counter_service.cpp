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
        LOG(NOTICE) << "counter: " << _counter << " fetch_and_add success";
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

    LOG(TRACE) << "received fetch_and_add " << request->value() << " from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        done->Run();
        return;
    }

    // node apply
    FetchAndAddDone* fetch_and_add_done = new FetchAndAddDone(_counter, cntl,
                                                              request, response, done);
    _counter->fetch_and_add(request->value(), fetch_and_add_done);
}

void CounterServiceImpl::get(google::protobuf::RpcController* controller,
                             const GetRequest* request,
                             GetResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received get from " << cntl->remote_side();

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

void CounterServiceImpl::stats(google::protobuf::RpcController* controller,
                             const StatsRequest* request,
                             StatsResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received stats from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        return;
    }

    raft::NodeStats stats = _counter->stats();
    response->set_state(raft::state2str(stats.state));
    response->set_term(stats.term);
    response->set_last_log_index(stats.last_log_index);
    response->set_last_log_term(stats.last_log_term);
    response->set_committed_index(stats.committed_index);
    response->set_applied_index(stats.applied_index);
    response->set_last_snapshot_index(stats.last_snapshot_index);
    response->set_last_snapshot_term(stats.last_snapshot_term);
    std::vector<raft::PeerId> peers;
    stats.configuration.peer_vector(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        response->add_peers(peers[i].to_string());
    }
}

class SetPeerDone : public raft::Closure {
public:
    SetPeerDone(Counter* counter, baidu::rpc::Controller* controller,
            const SetPeerRequest* request, SetPeerResponse* response,
            google::protobuf::Closure* done)
        : _counter(counter), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~SetPeerDone() {}

    virtual void Run() {
        if (_err_code == 0) {
            LOG(NOTICE) << "counter: " << _counter << " set_peer success";
            _response->set_success(true);
        } else {
            LOG(WARNING) << "counter: " << _counter << " set_peer failed: "
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
private:
    Counter* _counter;
    baidu::rpc::Controller* _controller;
    const SetPeerRequest* _request;
    SetPeerResponse* _response;
    google::protobuf::Closure* _done;
};

void CounterServiceImpl::set_peer(google::protobuf::RpcController* controller,
                             const SetPeerRequest* request,
                             SetPeerResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received set_peer from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        done->Run();
        return;
    }

    std::vector<raft::PeerId> old_peers;
    std::vector<raft::PeerId> new_peers;

    for (int i = 0; i < request->old_peers_size(); i++) {
        raft::PeerId peer;
        CHECK_EQ(0, peer.parse(request->old_peers(i)));

        old_peers.push_back(peer);
    }
    for (int i = 0; i < request->new_peers_size(); i++) {
        raft::PeerId peer;
        CHECK_EQ(0, peer.parse(request->new_peers(i)));

        new_peers.push_back(peer);
    }

    SetPeerDone* set_peer_done = new SetPeerDone(_counter, cntl, request, response, done);
    _counter->set_peer(old_peers, new_peers, set_peer_done);
}

class SnapshotDone : public raft::Closure {
public:
    SnapshotDone(Counter* counter, baidu::rpc::Controller* controller,
            const SnapshotRequest* request, SnapshotResponse* response,
            google::protobuf::Closure* done)
        : _counter(counter), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~SnapshotDone() {}

    virtual void Run() {
        if (_err_code == 0) {
            LOG(NOTICE) << "counter: " << _counter << " snapshot success";
            _response->set_success(true);
        } else {
            LOG(WARNING) << "counter: " << _counter << " snapshot failed: "
                << _err_code << noflush;
            if (!_err_text.empty()) {
                LOG(WARNING) << "(" << _err_text << ")";
            }
            LOG(WARNING);

            _response->set_success(false);
        }
        _done->Run();
        delete this;
    }
private:
    Counter* _counter;
    baidu::rpc::Controller* _controller;
    const SnapshotRequest* _request;
    SnapshotResponse* _response;
    google::protobuf::Closure* _done;
};

void CounterServiceImpl::snapshot(google::protobuf::RpcController* controller,
                             const SnapshotRequest* request,
                             SnapshotResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received snapshot from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        done->Run();
        return;
    }

    SnapshotDone* snapshot_done = new SnapshotDone(_counter, cntl, request, response, done);
    _counter->snapshot(snapshot_done);
}

class ShutdownDone : public raft::Closure {
public:
    ShutdownDone(Counter* counter, baidu::rpc::Controller* controller,
            const ShutdownRequest* request, ShutdownResponse* response,
            google::protobuf::Closure* done)
        : _counter(counter), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~ShutdownDone() {}

    virtual void Run() {
        if (_err_code == 0) {
            LOG(NOTICE) << "counter: " << _counter << " shutdown success";
            _response->set_success(true);
        } else {
            LOG(WARNING) << "counter: " << _counter << " shutdown failed: "
                << _err_code << noflush;
            if (!_err_text.empty()) {
                LOG(WARNING) << "(" << _err_text << ")";
            }
            LOG(WARNING);

            _response->set_success(false);
        }
        _done->Run();
        delete this;
    }
private:
    Counter* _counter;
    baidu::rpc::Controller* _controller;
    const ShutdownRequest* _request;
    ShutdownResponse* _response;
    google::protobuf::Closure* _done;
};

void CounterServiceImpl::shutdown(google::protobuf::RpcController* controller,
                             const ShutdownRequest* request,
                             ShutdownResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received shutdown from " << cntl->remote_side();

    // check counter
    if (!_counter) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "counter not set");
        done->Run();
        return;
    }

    ShutdownDone* shutdown_done = new ShutdownDone(_counter, cntl, request, response, done);
    _counter->shutdown(shutdown_done);
}

}
