// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2018/01/09 09:32:47

#ifndef  PUBLIC_RAFT_CLI_H
#define  PUBLIC_RAFT_CLI_H

#include "raft/raft.h"

namespace raft {
namespace cli {

struct CliOptions {
    int timeout_ms;
    int max_retry;
    CliOptions() : timeout_ms(-1), max_retry(3) {}
};

// Add a new peer into the replicating group which consists of |conf|.
// Returns OK on success, error infomation otherwise.
base::Status add_peer(const GroupId& group_id, const Configuration& conf,
                      const PeerId& peer_id, const CliOptions& options);

base::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                         const PeerId& peer_id, const CliOptions& options);
base::Status set_peer(const GroupId& group_id, const PeerId& peer_id,
                      const Configuration& new_conf, const CliOptions& options);

base::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                      const CliOptions& options);

}  // namespace cli
}  // namespace raft

#endif  //PUBLIC_RAFT_CLI_H
