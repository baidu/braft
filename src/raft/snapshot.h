// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/11/04 14:00:08

#ifndef PUBLIC_RAFT_RAFT_SNAPSHOT_H
#define PUBLIC_RAFT_RAFT_SNAPSHOT_H

#include <string>
#include "raft/storage.h"
#include "raft/macros.h"

namespace raft {

class LocalSnapshotWriter : public SnapshotWriter {
public:
    LocalSnapshotWriter(const std::string& path);
    virtual ~LocalSnapshotWriter();

    int64_t snapshot_index();
    virtual int init();
    virtual int copy(const std::string& uri);
    virtual int save_meta(const SnapshotMeta& meta);
    virtual std::string get_uri(const base::EndPoint& hint_addr);
protected:
    static const char* _s_snapshot_meta;
    std::string _path;
    SnapshotMeta _meta;
};

class LocalSnapshotReader: public SnapshotReader {
public:
    LocalSnapshotReader(const std::string& path);
    virtual ~LocalSnapshotReader();

    int64_t snapshot_index();
    virtual int init();
    virtual int load_meta(SnapshotMeta* meta);
    virtual std::string get_uri(const base::EndPoint& hint_addr);
protected:
    static const char* _s_snapshot_meta;
    std::string _path;
};

class LocalSnapshotStorage : public SnapshotStorage {
public:
    LocalSnapshotStorage(const std::string& path);
    virtual ~LocalSnapshotStorage();

    static const char* _s_temp_path;

    virtual int init();
    virtual SnapshotWriter* create();
    virtual int close(SnapshotWriter* writer);

    virtual SnapshotReader* open();
    virtual int close(SnapshotReader* reader);
protected:
    void ref(const int64_t index);
    void unref(const int64_t index);

    raft_mutex_t _mutex;
    std::string _path;
    int64_t _last_snapshot_index;
    std::map<int64_t, int> _ref_map;
};

SnapshotStorage* create_local_snapshot_storage(const std::string& uri);

}

#endif //~PUBLIC_RAFT_RAFT_SNAPSHOT_H
