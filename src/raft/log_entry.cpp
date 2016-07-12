// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/11 18:58:19

#include <bvar/bvar.h>
#include "raft/log_entry.h"

namespace raft {

bvar::Adder<int64_t> g_nentries("raft_num_log_entries");

LogEntry::LogEntry(): type(ENTRY_TYPE_UNKNOWN), peers(NULL) {
    // FIXME: Use log entry in the RAII way
    g_nentries << 1;
    AddRef();
}

LogEntry::~LogEntry() {
    g_nentries << -1;
    if (peers) {
        delete peers;
        peers = NULL;
    }
}
void LogEntry::set_data(const base::IOBuf &buf) {
    if (!data.empty()) {
        data.clear();
        data.append(buf);
    }
}

}
