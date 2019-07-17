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
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef BRAFT_RAFT_META_H
#define BRAFT_RAFT_META_H

#include <butil/memory/ref_counted.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <bthread/execution_queue.h>
#include "braft/storage.h"

namespace braft {

class FileBasedSingleMetaStorage;
class KVBasedMergedMetaStorageImpl;

class MixedMetaStorage : public RaftMetaStorage { 
public:
    explicit MixedMetaStorage(const std::string& path);
    MixedMetaStorage() {}
    virtual ~MixedMetaStorage();

    // init meta storage
    virtual butil::Status init();

    // set term and votedfor information
    virtual butil::Status set_term_and_votedfor(const int64_t term, 
                const PeerId& peer_id, const VersionedGroupId& group);

    // get term and votedfor information
    virtual butil::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                const VersionedGroupId& group);

    RaftMetaStorage* new_instance(const std::string& uri) const;
    
    butil::Status gc_instance(const std::string& uri, 
                              const VersionedGroupId& vgid) const;
    
    bool is_bad() { return _is_bad; }
 
private:

    static int parse_mixed_path(const std::string& uri, std::string& merged_path, 
                                std::string& single_path); 

    bool _is_inited;
    bool _is_bad;
    std::string _path;
    // Origin stable storage for each raft node 
    FileBasedSingleMetaStorage* _single_impl;
    // Merged stable storage for raft nodes on the same disk
    scoped_refptr<KVBasedMergedMetaStorageImpl> _merged_impl;
};

// Manage meta info of ONLY ONE raft instance
class FileBasedSingleMetaStorage : public RaftMetaStorage { 
public:
    explicit FileBasedSingleMetaStorage(const std::string& path)
        : _is_inited(false), _path(path), _term(1) {}
    FileBasedSingleMetaStorage() {}
    virtual ~FileBasedSingleMetaStorage() {}

    // init stable storage
    virtual butil::Status init();
    
    // set term and votedfor information
    virtual butil::Status set_term_and_votedfor(const int64_t term, const PeerId& peer_id, 
                                       const VersionedGroupId& group);
    
    // get term and votedfor information
    virtual butil::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                const VersionedGroupId& group);

    RaftMetaStorage* new_instance(const std::string& uri) const;

    butil::Status gc_instance(const std::string& uri,
                              const VersionedGroupId& vgid) const;

private:
    static const char* _s_raft_meta;
    int load();
    int save();

    bool _is_inited;
    std::string _path;
    int64_t _term;
    PeerId _votedfor;
};

// Manage meta info of A BATCH of raft instances who share the same disk_path prefix 
class KVBasedMergedMetaStorage : public RaftMetaStorage { 

public:
    explicit KVBasedMergedMetaStorage(const std::string& path);
    KVBasedMergedMetaStorage() {}

    virtual ~KVBasedMergedMetaStorage();

    // init stable storage
    virtual butil::Status init();
    
    // set term and votedfor information
    virtual butil::Status set_term_and_votedfor(const int64_t term, 
                                                const PeerId& peer_id, 
                                                const VersionedGroupId& group);

    // get term and votedfor information
    virtual butil::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                const VersionedGroupId& group);

    RaftMetaStorage* new_instance(const std::string& uri) const;
    
    butil::Status gc_instance(const std::string& uri,
                              const VersionedGroupId& vgid) const;
    
    // GC meta info of a raft instance indicated by |group|
    virtual butil::Status delete_meta(const VersionedGroupId& group);

private:

    scoped_refptr<KVBasedMergedMetaStorageImpl> _merged_impl;
};

// Inner class of KVBasedMergedMetaStorage
class KVBasedMergedMetaStorageImpl : 
            public butil::RefCountedThreadSafe<KVBasedMergedMetaStorageImpl> {
friend class scoped_refptr<KVBasedMergedMetaStorageImpl>;

public:
    explicit KVBasedMergedMetaStorageImpl(const std::string& path)
        : _is_inited(false), _path(path) {}
    KVBasedMergedMetaStorageImpl() {}
    virtual ~KVBasedMergedMetaStorageImpl() {
        if (_db) {
            delete _db;
        }
    }
    
    struct WriteTask {
        //TaskType type;
        int64_t term;
        PeerId votedfor;
        VersionedGroupId vgid;
        Closure* done;
    };

    // init stable storage
    virtual butil::Status init();
    
    // set term and votedfor information
    virtual void set_term_and_votedfor(const int64_t term, const PeerId& peer_id, 
                                const VersionedGroupId& group, Closure* done);
    
    // get term and votedfor information
    // [NOTICE] If some new instance init stable storage for the first time,
    // no record would be found from db, in which case initial term and votedfor
    // will be set.
    // Initial term: 1   Initial votedfor: ANY_PEER
    virtual butil::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                const VersionedGroupId& group);

    // GC meta info of a raft instance indicated by |group|
    virtual butil::Status delete_meta(const VersionedGroupId& group);

private:
    friend class butil::RefCountedThreadSafe<KVBasedMergedMetaStorageImpl>;
   
    static int run(void* meta, bthread::TaskIterator<WriteTask>& iter);

    void run_tasks(leveldb::WriteBatch& updates, Closure* dones[], size_t size);

    bthread::ExecutionQueueId<WriteTask> _queue_id;

    raft_mutex_t _mutex;
    bool _is_inited;
    std::string _path;
    leveldb::DB* _db;
};

}

#endif //~BRAFT_RAFT_META_H
