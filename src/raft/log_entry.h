// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/29 18:18:50

#ifndef  PUBLIC_RAFT_LOG_ENTRY_H
#define  PUBLIC_RAFT_LOG_ENTRY_H

#include <base/iobuf.h>                         // base::IOBuf
#include <base/memory/ref_counted.h>            // base::RefCountedThreadSafe
#include "raft/configuration.h"
#include "raft/raft.pb.h"

namespace raft {

// term start from 1, log index start from 1
struct LogEntry : public base::RefCountedThreadSafe<LogEntry> {
public:
    EntryType type; // log type
    int64_t index; // log index
    int64_t term; // leader term
    std::vector<PeerId>* peers; // peers
    base::IOBuf data;

    LogEntry(): type(ENTRY_TYPE_UNKNOWN), index(0), term(0), peers(NULL) {
        // FIXME: Use log entry in the RAII way
        AddRef();
    }

    void add_peer(const std::vector<PeerId>& peers_) {
        peers = new std::vector<PeerId>(peers_);
    }
    void set_data(const base::IOBuf &buf) {
        if (!data.empty()) {
            data.clear();
            data.append(buf);
        }
    }
private:
    friend class base::RefCountedThreadSafe<LogEntry>;
    // FIXME: Temporarily make dctor public to make it compilied
    virtual ~LogEntry() {
        if (peers) {
            delete peers;
            peers = NULL;
        }
    }
};

}

#endif  //PUBLIC_RAFT_LOG_ENTRY_H
