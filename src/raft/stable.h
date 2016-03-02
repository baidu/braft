// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 09/17/2015 14:49:51

#ifndef PUBLIC_RAFT_STABLE_H
#define PUBLIC_RAFT_STABLE_H

#include "raft/storage.h"

namespace raft {

class LocalStableStorage : public StableStorage {
public:
    LocalStableStorage(const std::string& path)
        : StableStorage(path), _is_inited(false), _path(path), _term(1) {}
    virtual ~LocalStableStorage() {}

    // init stable storage, check consistency and integrity
    virtual int init();

    // set current term
    virtual int set_term(const int64_t term);

    // get current term
    virtual int64_t get_term();

    // set votefor information
    virtual int set_votedfor(const PeerId& peer_id);

    // get votefor information
    virtual int get_votedfor(PeerId* peer_id);

    // set term and peer_id
    virtual int set_term_and_votedfor(const int64_t term, const PeerId& peer_id);
private:
    static const char* _s_stable_meta;
    int load();
    int save();

    bool _is_inited;
    std::string _path;
    int64_t _term;
    PeerId _votedfor;
};

StableStorage* create_local_stable_storage(const std::string& uri);

}

#endif //~PUBLIC_RAFT_STABLE_H
