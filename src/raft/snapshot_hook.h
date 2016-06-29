// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/06/21 14:31:52

#ifndef  PUBLIC_RAFT_SNAPSHOT_HOOK_H
#define  PUBLIC_RAFT_SNAPSHOT_HOOK_H

#include <base/memory/ref_counted.h>        // base::RefCountedThreadSafe
#include <raft/storage.h>                   // Snapshot
#include <raft/file_reader.h>               // FileReader

namespace raft {
class LocalFileMeta;

// Base class of Hook
class SnapshotHook : public base::RefCountedThreadSafe<SnapshotHook> {
public:
    virtual ~SnapshotHook() {}
};

// Hook actions of LocalSnapshotStorage
class LocalSnapshotHook : public SnapshotHook {
    DISALLOW_COPY_AND_ASSIGN(LocalSnapshotHook);
public:
    LocalSnapshotHook() {}
    virtual ~LocalSnapshotHook() {}
    // Get the reference_reader which is used to read the files referenced by
    // the files in a very Snapshot
    virtual int read_file(base::IOBuf* out,
                  const std::string& snapshot_path,
                  const std::string& filename,
                  const ::raft::LocalFileMeta& file_meta,
                  off_t offset,
                  size_t max_count,
                  bool* is_eof) {
        (void)file_meta;
        return LocalDirReader(snapshot_path).read_file(
                out, filename, offset, max_count, is_eof);
    }
    
    // This method is invoked when the SnapshotStorage found a 
    virtual int on_snapshot_found(::raft::Snapshot* snapshot) {
        // Suppress warning
        (void)snapshot;
        return 0;
    }

    virtual int on_snapshot_destroyed(::raft::Snapshot* snapshot) {
        // Suppress warning
        (void)snapshot;
        return 0;
    }
    
    // Filter writer with temporary files and the last snapshot before copying
    // from remote_snapshot
    // Returns 0 on success, -1 otherwise and all data in writer would be wiped
    // out.
    virtual int filter_before_copy(::raft::SnapshotWriter* writer, 
                           ::raft::Snapshot* last_snapshot,
                           ::raft::Snapshot* remote_snapshot) {
        (void)writer;
        (void)remote_snapshot;
        (void)last_snapshot;
        return -1;
    }
};

}

#endif  //PUBLIC_RAFT_SNAPSHOT_HOOK_H
