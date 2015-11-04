/*
 * =====================================================================================
 *
 *       Filename:  snapshot.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/04 14:00:08
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_RAFT_SNAPSHOT_H
#define PUBLIC_RAFT_RAFT_SNAPSHOT_H

#include <string>
#include "raft/raft.h"

namespace raft {

class LocalSnapshotWriter : public SnapshotWriter {
public:
    LocalSnapshotWriter(const std::string& path, const SnapshotMeta& meta);
    virtual ~LocalSnapshotWriter();

    int64_t snapshot_index();
    virtual int init();
    virtual int save_meta(const SnapshotMeta& meta);
    virtual int err_code();
protected:
    static const char* _s_snapshot_meta;
    std::string _path;
    SnapshotMeta _meta;
    int _err_code;
};

class LocalSnapshotReader: public SnapshotReader {
public:
    LocalSnapshotReader(const std::string& path);
    virtual ~LocalSnapshotReader();

    virtual int init();
    virtual int load_meta(SnapshotMeta* meta);
    virtual int err_code();
protected:
    static const char* _s_snapshot_meta;
    std::string _path;
    int _err_code;
};

class LocalSnapshotStorage : public SnapshotStorage {
public:
    LocalSnapshotStorage(const std::string& path);
    virtual ~LocalSnapshotStorage();

    virtual int init();
    virtual SnapshotWriter* create(const SnapshotMeta& meta);
    virtual int close(SnapshotWriter* writer);

    virtual SnapshotReader* open();
    virtual int close(SnapshotReader* reader);
protected:
    static const char* _s_temp_path;
    static const char* _s_lock_path;
    std::string _path;
    int _lock_fd;
    int64_t _last_snapshot_index;
};

}

#endif //~PUBLIC_RAFT_RAFT_SNAPSHOT_H
