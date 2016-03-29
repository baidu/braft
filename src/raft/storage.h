// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/11/05 11:34:03

#ifndef PUBLIC_RAFT_RAFT_STORAGE_H
#define PUBLIC_RAFT_RAFT_STORAGE_H

#include <string>
#include <vector>
#include <gflags/gflags.h>
#include <base/status.h>
#include <baidu/rpc/extension.h>

#include "raft/configuration.h"
#include "raft/configuration_manager.h"

namespace raft {

DECLARE_bool(raft_sync);

class LogEntry;

class LogStorage {
public:
    virtual ~LogStorage() {}

    // init logstorage, check consistency and integrity
    virtual int init(ConfigurationManager* configuration_manager) = 0;

    // first log index in log
    virtual int64_t first_log_index() = 0;

    // last log index in log
    virtual int64_t last_log_index() = 0;

    // get logentry by index
    virtual LogEntry* get_entry(const int64_t index) = 0;

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index) = 0;

    // append entries to log
    virtual int append_entry(const LogEntry* entry) = 0;

    // append entries to log, return append success number
    virtual int append_entries(const std::vector<LogEntry*>& entries) = 0;

    // delete logs from storage's head, [first_log_index, first_index_kept) will be discarded
    virtual int truncate_prefix(const int64_t first_index_kept) = 0;

    // delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded
    virtual int truncate_suffix(const int64_t last_index_kept) = 0;

    // Drop all the existing logs and reset next log index to |next_log_index|.
    // This function is called after installing snapshot from leader
    virtual int reset(const int64_t next_log_index) = 0;

    // Create an instance of this kind of LogStorage with the parameters encoded 
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.
    virtual LogStorage* new_instance(const std::string& uri) const = 0;

    static LogStorage* create(const std::string& uri);
};

class StableStorage {
public:
    virtual ~StableStorage() {}

    // init stable storage, check consistency and integrity
    virtual int init() = 0;

    // set current term
    virtual int set_term(const int64_t term) = 0;

    // get current term
    virtual int64_t get_term() = 0;

    // set votefor information
    virtual int set_votedfor(const PeerId& peer_id) = 0;

    // get votefor information
    virtual int get_votedfor(PeerId* peer_id) = 0;

    // set term and votedfor information
    virtual int set_term_and_votedfor(const int64_t term, const PeerId& peer_id) = 0;

    // Create an instance of this kind of LogStorage with the parameters encoded 
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.
    virtual StableStorage* new_instance(const std::string& uri) const = 0;

    static StableStorage* create(const std::string& uri);
};

struct SnapshotMeta {
    int64_t last_included_index;
    int64_t last_included_term;
    Configuration last_configuration;
};

class SnapshotWriter : public base::Status {
public:
    SnapshotWriter() {}
    virtual ~SnapshotWriter() {}

    virtual int copy(const std::string& uri) = 0;
    virtual int save_meta(const SnapshotMeta& meta) = 0;
    virtual std::string get_uri(const base::EndPoint& hint_addr) = 0;
};

class SnapshotReader : public base::Status {
public:
    SnapshotReader() {}
    virtual ~SnapshotReader() {}

    virtual int load_meta(SnapshotMeta* meta) = 0;
    virtual std::string get_uri(const base::EndPoint& hint_addr) = 0;
};

class SnapshotStorage {
public:
    virtual ~SnapshotStorage() {}

    // init
    virtual int init() = 0;

    // create new snapshot writer
    virtual SnapshotWriter* create() = 0;

    // close snapshot writer
    virtual int close(SnapshotWriter* writer) = 0;

    // get lastest snapshot reader
    virtual SnapshotReader* open() = 0;

    // close snapshot reader
    virtual int close(SnapshotReader* reader) = 0;

    virtual SnapshotStorage* new_instance(const std::string& uri) const = 0;

    static SnapshotStorage* create(const std::string& uri);
};

inline baidu::rpc::Extension<const LogStorage>* log_storage_extension() {
    return baidu::rpc::Extension<const LogStorage>::instance();
}

inline baidu::rpc::Extension<const StableStorage>* stable_storage_extension() {
    return baidu::rpc::Extension<const StableStorage>::instance();
}

inline baidu::rpc::Extension<const SnapshotStorage>* snapshot_storage_extension() {
    return baidu::rpc::Extension<const SnapshotStorage>::instance();
}

}

#endif //~PUBLIC_RAFT_RAFT_STORAGE_H
