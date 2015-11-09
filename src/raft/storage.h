/*
 * =====================================================================================
 *
 *       Filename:  storage.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/05 11:34:03
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_RAFT_STORAGE_H
#define PUBLIC_RAFT_RAFT_STORAGE_H

#include <string>
#include <vector>

#include "raft/configuration.h"

namespace raft {

class LogEntry;
//class ConfigurationManager;

class LogStorage {
public:
    LogStorage(const std::string& /*uri*/) {}
    virtual ~LogStorage() {}

    // init log storage, check consistency and integrity
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
};

class StableStorage {
public:
    StableStorage(const std::string& /*uri*/) {}
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

    virtual int set_term_and_votedfor(const int64_t term, const PeerId& peer_id) = 0;
};

struct SnapshotMeta {
    int64_t last_included_index;
    int64_t last_included_term;
    Configuration last_configuration;
};

class SnapshotWriter {
public:
    SnapshotWriter(const SnapshotMeta& meta) : _err_code(0), _meta(meta) {}
    virtual ~SnapshotWriter() {}

    virtual int init() = 0;
    virtual int copy(const std::string& uri) = 0;
    virtual int save_meta() = 0;

    // error func
    int error_code();
    std::string error_text();
    void set_error(int err_code, const char* reason_fmt, ...);
protected:
    int _err_code;
    std::string _err_text;
    SnapshotMeta _meta;
};

class SnapshotReader {
public:
    SnapshotReader() {}
    virtual ~SnapshotReader() {}

    virtual int init() = 0;
    virtual int load_meta(SnapshotMeta* meta) = 0;
    virtual std::string get_uri() = 0;

    // error func
    int error_code();
    std::string error_text();
    void set_error(int err_code, const char* reason_fmt, ...);
protected:
    int _err_code;
    std::string _err_text;
};

class SnapshotStorage {
public:
    SnapshotStorage(const std::string& /*uri*/) {}
    virtual ~SnapshotStorage() {}

    // init
    virtual int init() = 0;

    // create new snapshot writer
    virtual SnapshotWriter* create(const SnapshotMeta& meta) = 0;

    // close snapshot writer
    virtual int close(SnapshotWriter* writer) = 0;

    // get lastest snapshot reader
    virtual SnapshotReader* open() = 0;

    // close snapshot reader
    virtual int close(SnapshotReader* reader) = 0;
};

struct Storage {
    std::string name;

    typedef LogStorage* (*CreateLogStorage)(const std::string& uri);
    CreateLogStorage create_log_storage;

    typedef StableStorage* (*CreateStableStorage)(const std::string& uri);
    CreateStableStorage create_stable_storage;

    typedef SnapshotStorage* (*CreateSnapshotStorage)(const std::string& uri);
    CreateSnapshotStorage create_snapshot_storage;
};

// init storage
void init_storage();

// register storage
int register_storage(const std::string& uri, const Storage& storage);

// find storage by uri
Storage* find_storage(const std::string& uri);

}

#endif //~PUBLIC_RAFT_RAFT_STORAGE_H
