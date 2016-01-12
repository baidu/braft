/*
 * =====================================================================================
 *
 *       Filename:  common_state_machine.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/23 16:34:18
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_COMMON_STATE_MACHINE_H
#define PUBLIC_RAFT_EXAMPLE_COMMON_STATE_MACHINE_H

#include "raft/raft.h"

namespace example {

class CommonStateMachine : public raft::StateMachine {
public:
    CommonStateMachine(const raft::GroupId& group_id, const raft::PeerId& peer_id);

    // user operation method
    int init(const raft::NodeOptions& options);
    void set_peer(const std::vector<raft::PeerId>& old_peers,
                  const std::vector<raft::PeerId>& new_peers, bool is_force, raft::Closure* done);
    void snapshot(raft::Closure* done);
    void shutdown(raft::Closure* done);

    // geter
    base::EndPoint leader();

    // FSM method
    virtual void on_apply(const base::IOBuf& buf,
                          const int64_t index, raft::Closure* done) = 0;
    virtual void on_shutdown() = 0;
    virtual int on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) = 0;
    virtual int on_snapshot_load(raft::SnapshotReader* reader) = 0;
    virtual void on_leader_start() = 0;
    virtual void on_leader_stop() = 0;

protected:
    virtual ~CommonStateMachine();
    raft::Node _node;

private:
    int diff_peers(const std::vector<raft::PeerId>& old_peers,
                   const std::vector<raft::PeerId>& new_peers, raft::PeerId* peer);
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_COMMON_STATE_MACHINE_H
