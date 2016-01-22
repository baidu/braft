// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/11 18:58:19

#include <bvar/bvar.h>

namespace raft {

bvar::Adder<int64_t> g_nentries("raft_num_log_entries");

}
