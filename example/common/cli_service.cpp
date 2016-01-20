/*
 * =====================================================================================
 *
 *       Filename:  cli_service.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/23 14:07:17
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <base/logging.h>
#include <baidu/rpc/closure_guard.h>
#include "cli_service.h"
#include "state_machine.h"

namespace example {

class SetPeerDone : public raft::Closure {
public:
    SetPeerDone(CommonStateMachine* state_machine, baidu::rpc::Controller* controller,
            const SetPeerRequest* request, SetPeerResponse* response,
            google::protobuf::Closure* done)
        : _state_machine(state_machine), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~SetPeerDone() {}

    virtual void Run() {
        if (_err_code == 0) {
            LOG(NOTICE) << "state_machine: " << _state_machine << " set_peer success";
            _response->set_success(true);
        } else {
            LOG(WARNING) << "state_machine: " << _state_machine << " set_peer failed: "
                << _err_code << noflush;
            if (!_err_text.empty()) {
                LOG(WARNING) << "(" << _err_text << ")" << noflush;
            }
            LOG(WARNING);

            _response->set_success(false);
            _response->set_leader(base::endpoint2str(_state_machine->leader()).c_str());
        }
        _done->Run();
        delete this;
    }
private:
    CommonStateMachine* _state_machine;
    baidu::rpc::Controller* _controller;
    const SetPeerRequest* _request;
    SetPeerResponse* _response;
    google::protobuf::Closure* _done;
};

void CliServiceImpl::set_peer(google::protobuf::RpcController* controller,
                             const SetPeerRequest* request,
                             SetPeerResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received set_peer from " << cntl->remote_side();

    // check state_machine
    if (!_state_machine) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "state_machine not set");
        done->Run();
        return;
    }

    bool is_force = request->has_force() ? request->force() : false;
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

    SetPeerDone* set_peer_done = new SetPeerDone(_state_machine, cntl, request, response, done);
    _state_machine.load()->set_peer(old_peers, new_peers, is_force, set_peer_done);
}

class SnapshotDone : public raft::Closure {
public:
    SnapshotDone(CommonStateMachine* state_machine, baidu::rpc::Controller* controller,
            const SnapshotRequest* request, SnapshotResponse* response,
            google::protobuf::Closure* done)
        : _state_machine(state_machine), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~SnapshotDone() {}

    virtual void Run() {
        if (_err_code == 0) {
            LOG(NOTICE) << "state_machine: " << _state_machine << " snapshot success";
            _response->set_success(true);
        } else {
            LOG(WARNING) << "state_machine: " << _state_machine << " snapshot failed: "
                << _err_code << noflush;
            if (!_err_text.empty()) {
                LOG(WARNING) << "(" << _err_text << ")" << noflush;
            }
            LOG(WARNING);

            _response->set_success(false);
        }
        _done->Run();
        delete this;
    }
private:
    CommonStateMachine* _state_machine;
    baidu::rpc::Controller* _controller;
    const SnapshotRequest* _request;
    SnapshotResponse* _response;
    google::protobuf::Closure* _done;
};

void CliServiceImpl::snapshot(google::protobuf::RpcController* controller,
                             const SnapshotRequest* request,
                             SnapshotResponse* response,
                             google::protobuf::Closure* done) {
    request = request; // no used
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received snapshot from " << cntl->remote_side();

    // check state_machine
    if (!_state_machine) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "state_machine not set");
        done->Run();
        return;
    }

    SnapshotDone* snapshot_done = new SnapshotDone(_state_machine, cntl, request, response, done);
    _state_machine.load()->snapshot(snapshot_done);
}

class ShutdownDone : public raft::Closure {
public:
    ShutdownDone(CommonStateMachine* state_machine, baidu::rpc::Controller* controller,
            const ShutdownRequest* request, ShutdownResponse* response,
            google::protobuf::Closure* done)
        : _state_machine(state_machine), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~ShutdownDone() {}

    virtual void Run() {
        if (_err_code == 0) {
            LOG(NOTICE) << "state_machine: " << _state_machine << " shutdown success";
            _response->set_success(true);
        } else {
            LOG(WARNING) << "state_machine: " << _state_machine << " shutdown failed: "
                << _err_code << noflush;
            if (!_err_text.empty()) {
                LOG(WARNING) << "(" << _err_text << ")" << noflush;
            }
            LOG(WARNING);

            _response->set_success(false);
        }
        _done->Run();
        delete this;
    }
private:
    CommonStateMachine* _state_machine;
    baidu::rpc::Controller* _controller;
    const ShutdownRequest* _request;
    ShutdownResponse* _response;
    google::protobuf::Closure* _done;
};

void CliServiceImpl::shutdown(google::protobuf::RpcController* controller,
                             const ShutdownRequest* request,
                             ShutdownResponse* response,
                             google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    LOG(TRACE) << "received shutdown from " << cntl->remote_side();

    // check state_machine
    if (!_state_machine) {
        cntl->SetFailed(EINVAL, "state_machine not set");
        done->Run();
        return;
    }

    ShutdownDone* shutdown_done = new ShutdownDone(_state_machine, cntl, request, response, done);
    _state_machine.load()->shutdown(shutdown_done);
}

void CliServiceImpl::leader(::google::protobuf::RpcController* controller,
                            const ::example::GetLeaderRequest* /*request*/,
                            ::example::GetLeaderResponse* response,
                            ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
            static_cast<baidu::rpc::Controller*>(controller);
    if (!_state_machine) {
        cntl->SetFailed(EINVAL, "state_machine not set");
        return;
    }
    base::EndPoint leader_addr = _state_machine.load()->leader();
    if (leader_addr != base::EndPoint()) {
        response->set_success(true);
        response->set_leader_addr(base::endpoint2str(leader_addr).c_str());
    } else {
        response->set_success(false);
    }
}

}  // namespace example
