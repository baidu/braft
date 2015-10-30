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
#include <gflags/gflags.h>

#include "raft/raft.pb.h"
#include "raft/configuration.h"

DECLARE_string(raft_ip);
DECLARE_int32(raft_start_port);
DECLARE_int32(raft_end_port);

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

class Closure: public google::protobuf::Closure {
public:
    Closure() : _err_code(0) {}
    virtual ~Closure() {}

    void set_error(int err_code, const char* reason_fmt, ...);

    virtual void Run() = 0;
protected:
    int _err_code;
    std::string _err_text;
};

class StateMachine {
public:
    StateMachine() {}

    // user defined logentry proc function
    // [OPTIMIZE] add Closure argument to avoid parse data
    // [NOTE] index: realize follower read strong consistency
    // [NOTE] done: on_apply return some result to done
    virtual void on_apply(const void* data, const int len, const int64_t index, Closure* done) = 0;

    // user define shutdown function
    virtual void on_shutdown() = 0;

    // user defined snapshot generate function
    virtual int on_snapshot_save();

    // user defined snapshot load function
    virtual int on_snapshot_load();

    // user defined leader start function
    // [NOTE] user can direct append to node ignore this callback.
    //        this callback can sure read consistency, after leader's first NO_OP committed
    virtual void on_leader_start();

    // user defined leader start function
    // [NOTE] this method called immediately when leader stepdown,
    //        maybe before some method: apply success on_apply or fail done.
    //        user sure resource available.
    virtual void on_leader_stop();
protected:
    virtual ~StateMachine() {}
};

struct NodeOptions {
    int election_timeout; //ms, follower to candidate timeout
    int snapshot_interval; // s, snapshot interval
    int snapshot_lowlevel_threshold; // at least logs not in snapshot
    int snapshot_highlevel_threshold; // at most log not in snapshot
    bool enable_pipeline; // pipeline switch
    Configuration conf; // peer conf
    StateMachine* fsm; // user defined function [MUST]
    LogStorage* log_storage; // user defined log storage
    StableStorage* stable_storage; // user defined manifest storage

    NodeOptions()
        : election_timeout(1000),
        snapshot_interval(86400), snapshot_lowlevel_threshold(100000),
        snapshot_highlevel_threshold(10000000), enable_pipeline(false),
        fsm(NULL), log_storage(NULL), stable_storage(NULL) {}
};

class NodeImpl;
class Node {
public:
    Node(const GroupId& group_id, const ReplicaId& replica_id);
    virtual ~Node();

    // get node id
    NodeId node_id();

    // get leader PeerId, for redirect
    PeerId leader_id();

    // init node
    int init(const NodeOptions& options);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    // [NOTE] code after apply can't access resource in done
    void shutdown(Closure* done);

    // apply data to replicated-state-machine [thread-safe]
    // done is user defined function, maybe response to client, transform to on_applied
    // [NOTE] code after apply can't access resource in done
    int apply(const void* data, const int len, Closure* done);

    // add peer to replicated-state-machine [thread-safe]
    // done is user defined function, maybe response to client
    // [NOTE] code after apply can't access resource in done
    int add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done);

    // remove peer from replicated-state-machine [thread-safe]
    // done is user defined function, maybe response to client
    // [NOTE] code after apply can't access resource in done
    int remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done);

    // set peer to local replica [thread-safe]
    // done is user defined function, maybe response to client
    // only used in major node is down, reduce peerset to make group available
    int set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers);

private:
    NodeImpl* _impl;
};

int init_raft(const char* server_desc);

};

#endif //~PUBLIC_RAFT_RAFT_H
