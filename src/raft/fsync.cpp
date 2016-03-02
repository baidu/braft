// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/02/23 22:59:28

#include "raft/fsync.h"
#include <baidu/rpc/reloadable_flags.h>  //BAIDU_RPC_VALIDATE_GFLAG

namespace raft {

DEFINE_bool(raft_use_fsync_rather_than_fdatasync,
            true,
            "Use fsync rather than fdatasync to flush page cache");

BAIDU_RPC_VALIDATE_GFLAG(raft_use_fsync_rather_than_fdatasync,
                         baidu::rpc::PassValidate);


}  // namespace raft
