// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef BRAFT_RAFT_STORAGE_H
#define BRAFT_RAFT_STORAGE_H

#include <string>
#include <vector>
#include <gflags/gflags.h>
#include <butil/status.h>
#include <butil/class_name.h>
#include <brpc/extension.h>
#include <butil/strings/string_piece.h>
#include "braft/configuration.h"
#include "braft/configuration_manager.h"

namespace google {
namespace protobuf {
class Message;
}  // namespace protobuf
}  // namespace google

namespace braft {

DECLARE_bool(raft_sync);
DECLARE_bool(raft_sync_meta);
DECLARE_bool(raft_create_parent_directories);

struct LogEntry;

struct IOMetric {
public:
    IOMetric() 
        : start_time_us(butil::cpuwide_time_us())
        , bthread_queue_time_us(0)
        , open_segment_time_us(0)
        , append_entry_time_us(0)
        , sync_segment_time_us(0) {}

    int64_t start_time_us;
    int64_t bthread_queue_time_us;
    int64_t open_segment_time_us;
    int64_t append_entry_time_us;
    int64_t sync_segment_time_us;
};

inline std::ostream& operator<<(std::ostream& os, const IOMetric& m) {
    return os << " bthread_queue_time_us: " << m.bthread_queue_time_us
              << " open_segment_time_us: " << m.open_segment_time_us 
              << " append_entry_time_us: " << m.append_entry_time_us
              << " sync_segment_time_us: " << m.sync_segment_time_us;
}

inline butil::StringPiece parse_uri(butil::StringPiece* uri, std::string* parameter) {
    // ${protocol}://${parameters}
    size_t pos = uri->find("://");
    if (pos == butil::StringPiece::npos) {
        return butil::StringPiece();
    }
    butil::StringPiece protocol = uri->substr(0, pos);
    uri->remove_prefix(pos + 3/* length of '://' */);
    protocol.trim_spaces();
    parameter->reserve(uri->size());
    parameter->clear();
    size_t removed_spaces = 0;
    for (butil::StringPiece::const_iterator 
            iter = uri->begin(); iter != uri->end(); ++iter) {
        if (!isspace(*iter)) {
            parameter->push_back(*iter);
        } else {
            ++removed_spaces;
        }
    }
    LOG_IF(WARNING, removed_spaces) << "Removed " << removed_spaces 
            << " spaces from `" << *uri << '\'';
    return protocol;
}

inline int gc_dir(const std::string& path) {
    butil::File::Error e;
    butil::FilePath target_path(path);
    butil::FilePath tmp_path(path + ".tmp");
    // delete tmp path firstly in case there is garbage
    if (!butil::DeleteFile(tmp_path, true)) {
        LOG(ERROR) << "Fail to delete tmp file, path: " << tmp_path.value();
        return -1;
    }

    if (butil::PathExists(target_path)) {
        const bool rc = butil::ReplaceFile(butil::FilePath(target_path),
                                          butil::FilePath(tmp_path), &e);
        if (!rc) {
            LOG(ERROR) << "Fail to rename `" << target_path.value()
                       << " to `" << tmp_path.value() << "' : " << e;
            return -1;
        }
        if (!butil::DeleteFile(tmp_path, true)) {
            LOG(ERROR) << "Fail to delete tmp file, path: " << tmp_path.value();
            return -1;
        }
    } else {
        LOG(NOTICE) << "Target path not exist, so no need to gc, path: " 
                    << target_path.value();
    }
    return 0; 
}

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

    // append entries to log and update IOMetric, return append success number 
    virtual int append_entries(const std::vector<LogEntry*>& entries, IOMetric* metric) = 0;

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

    // GC an instance of this kind of LogStorage with the parameters encoded 
    // in |uri|
    virtual butil::Status gc_instance(const std::string& uri) const {
        CHECK(false) << butil::class_name_str(*this)
                     << " didn't implement gc_instance interface while deleting"
                        " raft log in " << uri;
        butil::Status status;
        status.set_error(ENOSYS, "gc_instance interface is not implemented");
        return status;
    }
    
    static butil::Status destroy(const std::string& uri);
};

class RaftMetaStorage {
public:
    virtual ~RaftMetaStorage() {}

    // init stable storage
    virtual butil::Status init() = 0;

    // set term and votedfor information
    virtual butil::Status set_term_and_votedfor(const int64_t term, 
                    const PeerId& peer_id, const VersionedGroupId& group) = 0;

    // get term and votedfor information
    virtual butil::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                const VersionedGroupId& group) = 0;

    // Create an instance of this kind of RaftMetaStorage with the parameters encoded 
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.
    virtual RaftMetaStorage* new_instance(const std::string& uri) const = 0;

    static RaftMetaStorage* create(const std::string& uri);
    
    // GC an instance of this kind of StableStorage with the parameters encoded 
    // in |uri|
    virtual butil::Status gc_instance(const std::string& uri, 
                                     const VersionedGroupId& vgid) const {
        CHECK(false) << butil::class_name_str(*this)
                     << " didn't implement gc_instance interface while deleting"
                        " raft stable meta in " << uri;
        butil::Status status;
        status.set_error(ENOSYS, "gc_instance interface is not implemented");
        return status;
    }
    
    static butil::Status destroy(const std::string& uri, 
                                const VersionedGroupId& vgid);

};

// Snapshot 
class Snapshot : public butil::Status {
public:
    Snapshot() {}
    virtual ~Snapshot() {}

    // Get the path of the Snapshot
    virtual std::string get_path() = 0;

    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files) = 0;

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename, 
                              ::google::protobuf::Message* file_meta) {
        (void)filename;
        file_meta->Clear();
        return 0;
    }
};

class SnapshotWriter : public Snapshot {
public:
    SnapshotWriter() {}
    virtual ~SnapshotWriter() {}

    // Save the meta information of the snapshot which is used by the raft
    // framework.
    virtual int save_meta(const SnapshotMeta& meta) = 0;

    // Add a file to the snapshot.
    // |file_meta| is an implmentation-defined protobuf message 
    // All the implementation must handle the case that |file_meta| is NULL and
    // no error can be raised.
    // Note that whether the file will be created onto the backing storage is
    // implementation-defined.
    virtual int add_file(const std::string& filename) { 
        return add_file(filename, NULL);
    }

    virtual int add_file(const std::string& filename, 
                         const ::google::protobuf::Message* file_meta) = 0;

    // Remove a file from the snapshot
    // Note that whether the file will be removed from the backing storage is
    // implementation-defined.
    virtual int remove_file(const std::string& filename) = 0;
};

class SnapshotReader : public Snapshot {
public:
    SnapshotReader() {}
    virtual ~SnapshotReader() {}

    // Load meta from 
    virtual int load_meta(SnapshotMeta* meta) = 0;

    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy() = 0;
};

// Copy Snapshot from the given resource
class SnapshotCopier : public butil::Status {
public:
    virtual ~SnapshotCopier() {}
    // Cancel the copy job
    virtual void cancel() = 0;
    // Block the thread until this copy job finishes, or some error occurs.
    virtual void join() = 0;
    // Get the the SnapshotReader which represents the copied Snapshot
    virtual SnapshotReader* get_reader() = 0;
};

class SnapshotHook;
class FileSystemAdaptor;
class SnapshotThrottle;

class SnapshotStorage {
public:
    virtual ~SnapshotStorage() {}

    virtual int set_filter_before_copy_remote() {
        CHECK(false) << butil::class_name_str(*this) 
                     << " doesn't support filter before copy remote";
        return -1;
    }

    virtual int set_file_system_adaptor(FileSystemAdaptor* fs) {
        (void)fs;
        CHECK(false) << butil::class_name_str(*this) 
                     << " doesn't support file system adaptor";
        return -1;
    }

    virtual int set_snapshot_throttle(SnapshotThrottle* st) {
        (void)st;
        CHECK(false) << butil::class_name_str(*this) 
                     << " doesn't support snapshot throttle";
        return -1;
    }

    // Initialize
    virtual int init() = 0;

    // create new snapshot writer
    virtual SnapshotWriter* create() = 0;

    // close snapshot writer
    virtual int close(SnapshotWriter* writer) = 0;

    // get lastest snapshot reader
    virtual SnapshotReader* open() = 0;

    // close snapshot reader
    virtual int close(SnapshotReader* reader) = 0;

    // Copy snapshot from uri and open it as a SnapshotReader
    virtual SnapshotReader* copy_from(const std::string& uri) WARN_UNUSED_RESULT = 0;
    virtual SnapshotCopier* start_to_copy_from(const std::string& uri) = 0;
    virtual int close(SnapshotCopier* copier) = 0;

    // Create an instance of this kind of SnapshotStorage with the parameters encoded 
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.
    virtual SnapshotStorage* new_instance(const std::string& uri) const WARN_UNUSED_RESULT = 0;
    static SnapshotStorage* create(const std::string& uri);
    
    // GC an instance of this kind of SnapshotStorage with the parameters encoded 
    // in |uri|
    virtual butil::Status gc_instance(const std::string& uri) const {
        CHECK(false) << butil::class_name_str(*this)
                     << " didn't implement gc_instance interface while deleting"
                        " raft snapshot in " << uri;
        butil::Status status;
        status.set_error(ENOSYS, "gc_instance interface is not implemented");
        return status;
    }

    static butil::Status destroy(const std::string& uri);
};

inline brpc::Extension<const LogStorage>* log_storage_extension() {
    return brpc::Extension<const LogStorage>::instance();
}

inline brpc::Extension<const RaftMetaStorage>* meta_storage_extension() {
    return brpc::Extension<const RaftMetaStorage>::instance();
}

inline brpc::Extension<const SnapshotStorage>* snapshot_storage_extension() {
    return brpc::Extension<const SnapshotStorage>::instance();
}

}

#endif //~BRAFT_RAFT_STORAGE_H
