/*
 * =====================================================================================
 *
 *       Filename:  raft.h
 *
 *    Description:  raft consensus replicate library
 *
 *        Version:  1.0
 *        Created:  2015/09/16 16:54:30
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_RAFT_H
#define PUBLIC_RAFT_RAFT_H

#include <string>
#include <base/endpoint.h>
#include <base/callback.h>

#include <raft/configuration.h>
#include <raft/log.h>
#include <raft/manifest.h>

namespace raft {

typedef std::string GroupId; // user defined id, translate to string
struct PeerId {
    base::EndPoint addr; // addr
    int idx; // idx in same addr, default 0

    PeerId(base::EndPoint addr_) : addr(addr_), idx(0) {}
    PeerId(base::EndPoint addr_, int idx_) : addr(addr_), idx(idx_) {}
};

struct NodeOptions {
    PeerId peer_id; // peer id
    int election_timeout; //ms, follower to candidate timeout
    int heartbeat_period; //ms, leader to other heartbeat period
    int rpc_timeout; //ms, rpc retry timeout
    int max_append_entries; // max entries in per AppendEntries RPC
    int snapshot_interval; // s, snapshot interval
    int snapshot_lowlevel_threshold; // at least logs not in snapshot
    int snapshot_highlevel_threshold; // at most log not in snapshot
    bool enable_pipeline; // pipeline switch
    NodeUser* user; // user defined function
    LogStorage* log_storage; // user defined log storage
    ManifestStorage* manifest_storage; // user defined manifest storage
    ConfigurationStorage* configuration_storage; // user defined configuration storage

    NodeOptions()
        : election_timeout(1000), heartbeat_period(100),
        rpc_timeout(1000), max_append_entries(100),
        snapshot_interval(86400), snapshot_lowlevel_threshold(100000),
        snapshot_highlevel_threshold(10000000), enable_pipeline(false),
        user(NULL), log_storage(NULL), manifest_storage(NULL), configuration_storage(NULL) {}
};

class NodeUser {
public:
    NodeUser() {}
    virtual ~NodeUser() {}

    // user defined logentry proc function
    // done is transformed by apply(), leader is valid, follower is NULL
    virtual int apply(const LogEntry& entry, base::Closure* done);

    // user defined snapshot generate function
    // done can't be defined by user
    virtual int snapshot_save(base::Closure* done);

    // user defined snapshot load function
    // done can't be defined by user
    virtual int snapshot_load(base::Closure* done);
};

class Node {
public:
    Node() {};
    virtual ~Node() {};

    // create raft node, group_id is user defined id's string format
    // set NodeUser ptr in option to proc apply and snapshot
    // set LogStorage/ManifestStorage/ConfigurationStorage ptr in option to define special storage
    static Node* create(const GroupId& group_id, const NodeOptions* option);

    // destroy raft node
    static int destroy(Node* node);

    // apply data to replicated-state-machine
    // done is user defined function, maybe response to client, transform to on_applied
    int apply(const void* data, const int len, base::Closure* done);

    // add peer to replicated-state-machine
    // done is user defined function, maybe response to client
    int add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, base::Closure* done);

    // remove peer from replicated-state-machine
    // done is user defined function, maybe response to client
    int remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, base::Closure* done);

    // set peer to local replica
    // done is user defined function, maybe response to client
    int set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers,
                 base::Closure* done);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    int shutdown(base::Closure* done);
};

enum LogType {
    NO_OP = 0, // no operatue, used by new leader fence and log barrier
    DATA = 1, // normal data
    ADD_PEER = 2, // add peer
    REMOVE_PEER = 3, // remove peer
};

struct LogEntry {
    LogType type; // log type
    int64_t index; // log index
    int64_t term; // leader term
    int len; // data len
    void* data; // data ptr
};

class LogStorage {
public:
    LogStorage(const std::string& uri) {}
    virtual ~LogStorage() {}

    // first log index in log
    virtual int64_t first_log_index();

    // last log index in log
    virtual int64_t last_log_index();

    // get logentry by index
    virtual LogEntry* get_log(const int64_t index);

    // append entries to log
    virtual int append_logs(const int64_t index, const std::vector<LogEntry*>& entries);

    // delete logs in range, used by log compaction and trucate uncommitted logs
    virtual int delete_logs(const int64_t begin_index, const int64_t end_index);
};

class StableStorage {
public:
    StableStorage(const std::string& uri) {}
    virtual ~StableStorage() {}

    // set current term
    virtual int set_term(const int64_t term);

    // get current term
    virtual int64_t get_term();

    // set votefor information
    virtual int set_votefor(const int64_t term, const PeerId& peer_id);

    // get votefor information
    virtual int get_votefor(int64_t* term, PeerId* peer_id);
};

struct Configuration {
    std::vector<PeerId> peer_set;
};

class ConfigurationStorage {
public:
    ConfigurationStorage(const std::string& uri) {}
    virtual ~ConfigurationStorage() {}

    // append new configuration to storage
    virtual int append_configuration(const int64_t index, const Configuration* config);

    // delete staled configuration from storage
    virtual int delete_configuration(const int64_t begin_index, const int64_t end_index);

    // load all configuration from stroage
    virtual int load_configuration(std::vector<Configuration*>* configs);
};

// SnapshotStore implement in on_snapshot_save() and on_snapshot_load()

};

#endif //~PUBLIC_RAFT_RAFT_H
