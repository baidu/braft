// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/10/29 18:18:50

#ifndef  PUBLIC_RAFT_LOG_ENTRY_H
#define  PUBLIC_RAFT_LOG_ENTRY_H

#include <base/iobuf.h>                         // base::IOBuf
#include <base/memory/ref_counted.h>            // base::RefCountedThreadSafe
#include <bvar/bvar.h>
#include <base/third_party/murmurhash3/murmurhash3.h>  // fmix64
#include "raft/configuration.h"
#include "raft/raft.pb.h"

namespace raft {

// Log indentifier
struct LogId {
    LogId() : index(0), term(0) {}
    LogId(int64_t index_, int64_t term_) : index(index_), term(term_) {}
    int64_t index;
    int64_t term;
};

// term start from 1, log index start from 1
struct LogEntry : public base::RefCountedThreadSafe<LogEntry> {
public:
    EntryType type; // log type
    LogId id;
    std::vector<PeerId>* peers; // peers
    base::IOBuf data;

    LogEntry();

private:
    DISALLOW_COPY_AND_ASSIGN(LogEntry);
    friend class base::RefCountedThreadSafe<LogEntry>;
    virtual ~LogEntry();
};

// Comparators

inline bool operator==(const LogId& lhs, const LogId& rhs) {
    return lhs.index == rhs.index && lhs.term == rhs.term;
}

inline bool operator!=(const LogId& lhs, const LogId& rhs) {
    return !(lhs == rhs);
}

inline bool operator<(const LogId& lhs, const LogId& rhs) {
    if (lhs.term == rhs.term) {
        return lhs.index < rhs.index;
    }
    return lhs.term < rhs.term;
}

inline bool operator>(const LogId& lhs, const LogId& rhs) {
    if (lhs.term == rhs.term) {
        return lhs.index > rhs.index;
    }
    return lhs.term > rhs.term;
}

inline bool operator<=(const LogId& lhs, const LogId& rhs) {
    return !(lhs > rhs);
}

inline bool operator>=(const LogId& lhs, const LogId& rhs) {
    return !(lhs < rhs);
}

struct LogIdHasher {
    size_t operator()(const LogId& id) const {
        return base::fmix64(id.index) ^ base::fmix64(id.term);
    }
};

inline std::ostream& operator<<(std::ostream& os, const LogId& id) {
    os << "(index=" << id.index << ",term=" << id.term << ')';
    return os;
}

}  // namespace raft

#endif  //PUBLIC_RAFT_LOG_ENTRY_H
