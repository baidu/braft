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
#include <cassert>
#include <base/endpoint.h>
#include <base/callback.h>
#include <base/memory/singleton.h>

#include "bthread.h"
#include "raft/raft.pb.h"
#include "raft/configuration.h"

//#include <raft/configuration.h>
//#include <raft/log.h>
//#include <raft/manifest.h>

namespace raft {

// term start from 1, log index start from 1
struct LogEntry : public base::RefCountedThreadSafe<LogEntry> {
public:
    EntryType type; // log type
    int64_t index; // log index
    int64_t term; // leader term
    std::vector<PeerId>* peers; // peers
    int len; // data len
    void* data; // data ptr

    LogEntry(): type(ENTRY_TYPE_UNKNOWN), index(0), term(0), peers(NULL), len(0), data(NULL) {}

    void add_peer(const std::vector<PeerId>& peers_) {
        peers = new std::vector<PeerId>(peers_);
    }
    void set_data(void* data_, int len_) {
        len = len_;
        data = data_;
    }
    // FIXME: Temporarily make dctor public to make it compilied
    virtual ~LogEntry() {
        if (peers) {
            delete peers;
            peers = NULL;
        }
        if (data) {
            free(data);
            data = NULL;
        }
    }
private:
    friend class base::RefCountedThreadSafe<LogEntry>;
};

class LogStorage {
public:
    LogStorage(const std::string& uri) {}
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
    StableStorage(const std::string& uri) {}
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

// SnapshotStore implement in on_snapshot_save() and on_snapshot_load()

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

struct NodeOptions {
    int election_timeout; //ms, follower to candidate timeout
    int heartbeat_period; //ms, leader to other heartbeat period
    int rpc_timeout; //ms, rpc retry timeout
    int max_append_entries; // max entries in per AppendEntries RPC
    int snapshot_interval; // s, snapshot interval
    int snapshot_lowlevel_threshold; // at least logs not in snapshot
    int snapshot_highlevel_threshold; // at most log not in snapshot
    bool enable_pipeline; // pipeline switch
    Configuration conf; // peer conf
    NodeUser* user; // user defined function
    LogStorage* log_storage; // user defined log storage
    StableStorage* stable_storage; // user defined manifest storage

    NodeOptions()
        : election_timeout(1000), heartbeat_period(100),
        rpc_timeout(1000), max_append_entries(100),
        snapshot_interval(86400), snapshot_lowlevel_threshold(100000),
        snapshot_highlevel_threshold(10000000), enable_pipeline(false),
        user(NULL), log_storage(NULL), stable_storage(NULL) {}
};

class Node {
public:
    Node(const GroupId& group_id, const PeerId& peer_id, const NodeOptions* option) {}
    virtual ~Node() {}

    virtual NodeId node_id();

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
    // only used in major node is down, reduce peerset to make group available
    int set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers,
                 base::Closure* done);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    int shutdown(base::Closure* done);
};

class NodeManager {
public:
    static NodeManager* GetInstance() {
        return Singleton<NodeManager>::get();
    }

    // create raft node, group_id is user defined id's string format
    // set NodeUser ptr in option to proc apply and snapshot
    // set LogStorage/StableStorage ptr in option to define special storage
    Node* create(const GroupId& group_id, const PeerId& server_id, const NodeOptions* option);

    // destroy raft node
    int destroy(Node* node);

    // get Node by group_id
    Node* get(const GroupId& group_id, const PeerId& peer_id);

private:
    NodeManager();
    ~NodeManager();
    DISALLOW_COPY_AND_ASSIGN(NodeManager);
    friend struct DefaultSingletonTraits<NodeManager>;

    bthread_mutex_t _mutex;
    typedef std::map<NodeId, Node*> NodeMap;
    NodeMap _nodes;
};

};

#endif //~PUBLIC_RAFT_RAFT_H
