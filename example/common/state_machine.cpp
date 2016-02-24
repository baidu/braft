/*
 * =====================================================================================
 *
 *       Filename:  common_state_machine.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/23 16:40:11
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <errno.h>
#include <base/logging.h>
#include <baidu/rpc/closure_guard.h>
#include "cli.pb.h"
#include "state_machine.h"

namespace example {

CommonStateMachine::CommonStateMachine(const raft::GroupId& group_id,
                                       const raft::PeerId& peer_id)
    : StateMachine(), _node(group_id, peer_id) {
}

CommonStateMachine::~CommonStateMachine() {
}

int CommonStateMachine::init(const raft::NodeOptions& options) {
    return _node.init(options);
}

int CommonStateMachine::diff_peers(const std::vector<raft::PeerId>& old_peers,
                  const std::vector<raft::PeerId>& new_peers, raft::PeerId* peer) {
    raft::Configuration old_conf(old_peers);
    raft::Configuration new_conf(new_peers);
    if (old_peers.size() == new_peers.size() - 1 && new_conf.contains(old_peers)) {
        // add peer
        for (size_t i = 0; i < old_peers.size(); i++) {
            new_conf.remove_peer(old_peers[i]);
        }
        std::vector<raft::PeerId> peers;
        new_conf.list_peers(&peers);
        CHECK(1 == peers.size());
        *peer = peers[0];
        return 0;
    } else if (old_peers.size() == new_peers.size() + 1 && old_conf.contains(new_peers)) {
        // remove peer
        for (size_t i = 0; i < new_peers.size(); i++) {
            old_conf.remove_peer(new_peers[i]);
        }
        std::vector<raft::PeerId> peers;
        old_conf.list_peers(&peers);
        CHECK(1 == peers.size());
        *peer = peers[0];
        return 0;
    } else {
        return -1;
    }
}

void CommonStateMachine::set_peer(const std::vector<raft::PeerId>& old_peers,
                  const std::vector<raft::PeerId>& new_peers, bool is_force, raft::Closure* done) {
    raft::PeerId peer;
    if (is_force) {
        int ret = _node.set_peer(old_peers, new_peers);
        if (ret != 0) {
            done->status().set_error(ret, "set_peer failed");
        }
        done->Run();
    } else if (new_peers.size() == old_peers.size() + 1) {
        if (0 == diff_peers(old_peers, new_peers, &peer)) {
            LOG(TRACE) << "add peer " << peer;
            _node.add_peer(old_peers, peer, done);
        } else {
            done->status().set_error(EINVAL, "add_peer invalid peers");
            done->Run();
        }
    } else if (old_peers.size() == new_peers.size() + 1) {
        if (0 == diff_peers(old_peers, new_peers, &peer)) {
            LOG(TRACE) << "remove peer " << peer;
            _node.remove_peer(old_peers, peer, done);
        } else {
            done->status().set_error(EINVAL, "remove_peer invalid peers");
            done->Run();
        }
    } else {
        LOG(ERROR) << "set_peer argument failed";
        done->status().set_error(EINVAL, "set_peer bad argument");
        done->Run();
    }
}

void CommonStateMachine::snapshot(raft::Closure* done) {
    _node.snapshot(done);
}

void CommonStateMachine::shutdown(raft::Closure* done) {
    _node.shutdown(done);
}

base::EndPoint CommonStateMachine::leader() {
    return _node.leader_id().addr;
}

}
