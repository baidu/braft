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

#include <errno.h>
#include <butil/time.h>
#include <butil/logging.h>
#include <butil/file_util.h>                         // butil::CreateDirectory
#include <gflags/gflags.h>
#include <brpc/reloadable_flags.h>
#include "braft/util.h"
#include "braft/protobuf_file.h"
#include "braft/local_storage.pb.h"
#include "braft/raft_meta.h"

namespace braft {

static bvar::LatencyRecorder g_load_pb_raft_meta("raft_load_pb_raft_meta");
static bvar::LatencyRecorder g_save_pb_raft_meta("raft_save_pb_raft_meta");
static bvar::LatencyRecorder g_load_kv_raft_meta("raft_load_kv_raft_meta");
static bvar::LatencyRecorder g_save_kv_raft_meta("raft_save_kv_raft_meta");
static bvar::LatencyRecorder g_delete_kv_raft_meta("raft_delete_kv_raft_meta");

const char* FileBasedSingleMetaStorage::_s_raft_meta = "stable_meta";

// MetaStorageManager
//
// To manage all KVBasedMergedMetaStorageImpl of all the raft instances.
// Typically nodes on the same disk will share a KVBasedMergedMetaStorageImpl, 
// so we use disk_path as the KEY to manage all the instances.
class MetaStorageManager {
public:
    static MetaStorageManager* GetInstance() {
        return Singleton<MetaStorageManager>::get();
    }

    scoped_refptr<KVBasedMergedMetaStorageImpl> 
    register_meta_storage(const std::string& path) {
        scoped_refptr<KVBasedMergedMetaStorageImpl> mss = get_meta_storage(path);
        if (mss != NULL) {
            return mss;
        }
        
        mss = new KVBasedMergedMetaStorageImpl(path);
        {
            _ss_map.Modify(_add, path, mss);
        }
        return get_meta_storage(path); 
    }
    
    scoped_refptr<KVBasedMergedMetaStorageImpl> 
    get_meta_storage(const std::string& path) {
        DoublyBufferedMetaStorageMap::ScopedPtr ptr;
        CHECK_EQ(0, _ss_map.Read(&ptr));
        MetaStorageMap::const_iterator it = ptr->find(path);
        if (it != ptr->end()) {
            return it->second;
        }
        return NULL;
    }
    
    // GC an invalid item in KVBasedMergedMetaStorageImpl when destroying 
    // an raft instance on the disk for some reason, such as IO error.
    int remove_instance_from_meta_storage(const std::string& path, 
                                            const VersionedGroupId& v_group_id) {
        scoped_refptr<KVBasedMergedMetaStorageImpl> mss = 
                                                get_meta_storage(path);
        if (mss == NULL) {
            return 0;
        }
        butil::Status status = mss->delete_meta(v_group_id);
        if (!status.ok()) {
            return -1;
        }
        return 0;
    }

private:
    MetaStorageManager() {};
    ~MetaStorageManager() {};
    DISALLOW_COPY_AND_ASSIGN(MetaStorageManager);
    friend struct DefaultSingletonTraits<MetaStorageManager>;
    
    typedef std::map<std::string, scoped_refptr<KVBasedMergedMetaStorageImpl> > 
                                                                MetaStorageMap;
    typedef butil::DoublyBufferedData<MetaStorageMap> DoublyBufferedMetaStorageMap;
    
    static size_t _add(MetaStorageMap& m, const std::string& path, 
                       const scoped_refptr<KVBasedMergedMetaStorageImpl> mss) {
        std::pair<MetaStorageMap::const_iterator, bool> iter = 
                                        m.insert(std::make_pair(path, mss));
        if (iter.second) {
            return 1lu;
        }
        return 0lu;
    }
 
    static size_t _remove(MetaStorageMap& m, const std::string& path) {
        return m.erase(path);
    }

    DoublyBufferedMetaStorageMap _ss_map;
};

#define global_mss_manager MetaStorageManager::GetInstance()

// MixedMetaStorage
//
// Uri of Multi-raft using mixed stable storage is: 
//     local-mixed://merged_path={merged_path}&&single_path={single_path}
int MixedMetaStorage::parse_mixed_path(const std::string& uri, 
                                         std::string& merged_path, 
                                         std::string& single_path) {
    // here uri has removed protocol already, check just for safety
    butil::StringPiece copied_uri(uri);
    size_t pos = copied_uri.find("://");
    if (pos != butil::StringPiece::npos) {
        copied_uri.remove_prefix(pos + 3/* length of '://' */);
    }
    
    pos = copied_uri.find("merged_path=");
    if (pos == butil::StringPiece::npos) {
        return -1;
    }
    copied_uri.remove_prefix(pos + 12/* length of 'merged_path=' */);
    
    pos = copied_uri.find("&&single_path=");
    if (pos == butil::StringPiece::npos) {
        return -1;
    }
    merged_path = copied_uri.substr(0, pos).as_string();
    copied_uri.remove_prefix(pos + 14/* length of '&&single_path=' */);
    single_path = copied_uri.as_string();
    
    return 0;
}

MixedMetaStorage::MixedMetaStorage(const std::string& uri) {
    _is_inited = false;
    _is_bad = false;

    std::string merged_path;
    std::string single_path;

    int ret = parse_mixed_path(uri, merged_path, single_path);
    if (ret != 0) {
        LOG(ERROR) << "node parse mixed path failed, uri " << uri;
        _is_bad = true; 
    } else {
        // Use single_path as the path of MixedMetaStorage as it usually 
        // contains group_id
        _path = single_path;

        _single_impl = new FileBasedSingleMetaStorage(single_path);
        _merged_impl = global_mss_manager->register_meta_storage(merged_path);

        if (!_single_impl || !_merged_impl) {
            // Both _single_impl and _merged_impl are needed in MixedMetaStorage
            LOG(ERROR) << "MixedMetaStorage failed to create both"
                            " sub stable storage, uri " << uri;
            _is_bad = true;
        }
    }
}

MixedMetaStorage::~MixedMetaStorage() {
    if (_single_impl) {
        delete _single_impl;
        _single_impl = NULL;
    } 
    if (_merged_impl) {
        _merged_impl = NULL;
    }
}

butil::Status MixedMetaStorage::init() {
    butil::Status status;
     if (_is_inited) {
        return status;
    }
    // check bad
    if (_is_bad) {
        status.set_error(EINVAL, "MixedMetaStorage is bad, path %s", 
                         _path.c_str());
        return status;
    }
    
    // both _single_impl and _merged_impl are valid since _is_bad is false
    status = _single_impl->init();
    if (!status.ok()) {
        LOG(ERROR) << "Init Mixed stable storage failed because init Single"
                      " stable storage failed, path " << _path;
        return status;
    }

    status = _merged_impl->init();
    if (!status.ok()) {
        LOG(ERROR) << "Init Mixed stable storage failed because init merged"
                      " stable storage failed, path " << _path;
        return status;
    }

    _is_inited = true;
    LOG(NOTICE) << "Succeed to init MixedMetaStorage, path: " << _path;
    return status;
}

butil::Status MixedMetaStorage::set_term_and_votedfor(const int64_t term, 
                        const PeerId& peer_id, const VersionedGroupId& group) {
    butil::Status status;
    if (!_is_inited) {
        LOG(WARNING) << "MixedMetaStorage not init, path: " << _path;
        status.set_error(EINVAL, "MixedMetaStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }

    status = _single_impl->set_term_and_votedfor(term, peer_id, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group 
            << " single stable storage failed to set_term_and_votedfor, path: "
            << _path;
        return status;
    }

    status = _merged_impl->set_term_and_votedfor(term, peer_id, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group 
            << " merged stable storage failed to set_term_and_votedfor, path: "
            << _path;
        return status;
    }
    return status;
}

// [NOTICE] Conflict cases may occur in this mode, it's important to ensure consistency
// 1. Single is newer than Merged:
//      case 1: upgrade storage from Single to Mixed, data in Merged is stale
//      case 2: last set_term_and_votedfor succeeded in Single but failed in Merged
// 2. Merged is newer than Single:
//      case: downgrade storage from Merged to Mixed, data in Single is stale
butil::Status MixedMetaStorage::get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                       const VersionedGroupId& group) {
    butil::Status status;
    if (!_is_inited) {
        LOG(WARNING) << "MixedMetaStorage not init, path: " << _path;
        status.set_error(EINVAL, "MixedMetaStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }
    
    // If data from single stable storage is newer than that from merged stable storage,
    // merged stable storage should catch up the newer data to ensure safety; Vice versa.
    bool single_newer_than_merged = false;
    
    int64_t term_1;
    PeerId peer_id_1;
    status = _single_impl->get_term_and_votedfor(&term_1, &peer_id_1, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group 
            << " single stable storage failed to get_term_and_votedfor, path: "
            << _path << ", error: " << status.error_cstr();
        return status;
    }
    
    int64_t term_2;
    PeerId peer_id_2;
    status = _merged_impl->get_term_and_votedfor(&term_2, 
                                                 &peer_id_2, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group
            << " merged stable storage failed to get_term_and_votdfor,"
            << " path: " << _path << ", error: " << status.error_cstr();
        return status;
    // check consistency of two stable storage
    } else if (term_1 == term_2 && peer_id_1 == peer_id_2) {
        // if two results are consistent, just return success
        *term = term_1;
        *peer_id = peer_id_1;
        return status;
    }

    // this case is theoretically impossible, pay much attention to it if happens
    if (term_1 == term_2 && peer_id_1 != ANY_PEER 
                         && peer_id_2 != ANY_PEER) {
        CHECK(false) << "Unexpected conflict when mixed stable storage of " 
            << group << " get_term_and_votedfor, the same term " << term_1
            << ", but different non-empty votdfor(" << peer_id_1 
            << " from single stable storage and " << peer_id_2
            << " from merged stable storage), path: " << _path;
        status.set_error(EINVAL, "Unexpected conflict");
        return status;
    }

    // if two results are not consistent, check out which is newer and catch up
    // data for the stale one 
    single_newer_than_merged = term_1 > term_2 || 
        (term_1 == term_2 && peer_id_1 != ANY_PEER && peer_id_2 == ANY_PEER);
    
    if (single_newer_than_merged) {
        *term = term_1;
        *peer_id = peer_id_1;
        status = _merged_impl->set_term_and_votedfor(*term, *peer_id, group);
        if (!status.ok()) {
            LOG(WARNING) << "node " << group 
                << " merged stable storage failed to set term " << *term
                << " and vote for peer " << *peer_id
                << " when catch up data, path " << _path
                << ", error: " << status.error_cstr();
            return status;
        }
        LOG(NOTICE) << "node " << group 
            << " merged stable storage succeed to set term " << *term
            << " and vote for peer " << *peer_id
            << " when catch up data, path " << _path;
    } else {
        LOG(WARNING) << "LocalMetaStorage not init(), path: " << _path;
        *term = term_2;
        *peer_id = peer_id_2;
        status = _single_impl->set_term_and_votedfor(*term, *peer_id, group);
        if (!status.ok()) {
            LOG(WARNING) << "node " << group 
                << " single stable storage failed to set term " << *term
                << " and vote for peer " << *peer_id
                << " when catch up data, path " << _path
                << ", error: " << status.error_cstr();
            return status;
        } 
        LOG(NOTICE) << "node " << group 
            << " single stable storage succeed to set term " << *term
            << " and vote for peer " << *peer_id
            << " when catch up data, path " << _path;
    }  

    return status;
}

RaftMetaStorage* MixedMetaStorage::new_instance(const std::string& uri) const {
    return new MixedMetaStorage(uri);
}

butil::Status MixedMetaStorage::gc_instance(const std::string& uri, 
                                             const VersionedGroupId& vgid) const {
    butil::Status status;
    std::string merged_path;
    std::string single_path;

    int ret = parse_mixed_path(uri, merged_path, single_path);
    if (ret != 0) {
        LOG(WARNING) << "node parse mixed path failed, uri " << uri;
        status.set_error(EINVAL, "Group %s failed to parse mixed path, uri %s", 
                         vgid.c_str(), uri.c_str());
        return status;
    }
    if (0 != gc_dir(single_path)) {
        LOG(WARNING) << "Group " << vgid << " failed to gc path " << single_path;
        status.set_error(EIO, "Group %s failed to gc path %s", 
                         vgid.c_str(), single_path.c_str());
        return status;
    }
    if (0 != global_mss_manager->
                    remove_instance_from_meta_storage(merged_path, vgid)) {
        LOG(ERROR) << "Group " << vgid << " failed to gc kv from path: " 
                   << merged_path;
        status.set_error(EIO, "Group %s failed to gc kv from path %s", 
                         vgid.c_str(), merged_path.c_str());
        return status;
    }
    LOG(INFO) << "Group " << vgid << " succeed to gc from single path: " 
              << single_path << " and merged path: " << merged_path;
    return status; 
 }
 
// FileBasedSingleMetaStorage
butil::Status FileBasedSingleMetaStorage::init() {
    butil::Status status;
    if (_is_inited) {
        return status;
    }

    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        status.set_error(e, "Fail to create dir when init SingleMetaStorage, "
                         "path: %s", _path.c_str());
        return status;
    }

    int ret = load();
    if (ret != 0) {
        LOG(ERROR) << "Fail to load pb meta when init single stable storage"
                      ", path: " << _path;
        status.set_error(EIO, "Fail to load pb meta when init stabel storage"
                         ", path: %s", _path.c_str());
        return status;
    }

    _is_inited = true;
    return status;
}

butil::Status FileBasedSingleMetaStorage::set_term_and_votedfor(const int64_t term, 
            const PeerId& peer_id, const VersionedGroupId&) {
    butil::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "SingleMetaStorage not init, path: %s", 
                         _path.c_str());
        return status;
    }   
    _term = term;
    _votedfor = peer_id;
    if (save() != 0) {
        status.set_error(EIO, "SingleMetaStorage failed to save pb meta, path: %s", 
                         _path.c_str());
        return status;
    }
    return status;
}
 
butil::Status FileBasedSingleMetaStorage::get_term_and_votedfor(int64_t* term, 
                                PeerId* peer_id, const VersionedGroupId& group) {
    butil::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "SingleMetaStorage not init, path: %s", 
                         _path.c_str());
        return status;
    }   
    *term = _term;
    *peer_id = _votedfor;
    return status;
}

int FileBasedSingleMetaStorage::load() {
    butil::Timer timer;
    timer.start();
 
    std::string path(_path);
    path.append("/");
    path.append(_s_raft_meta);

    ProtoBufFile pb_file(path);

    StablePBMeta meta;
    int ret = pb_file.load(&meta);
    if (ret == 0) {
        _term = meta.term();
        ret = _votedfor.parse(meta.votedfor());
    } else if (errno == ENOENT) {
        ret = 0;
    } else {
        PLOG(ERROR) << "Fail to load meta from " << path;
    }
    
    timer.stop();
    // Only reload process will load stable meta of raft instances,
    // reading just go through memory
    g_load_pb_raft_meta << timer.u_elapsed();
    LOG(INFO) << "Loaded single stable meta, path " << _path
              << " term " << _term 
              << " votedfor " << _votedfor.to_string() 
              << " time: " << timer.u_elapsed();
    return ret;
}

int FileBasedSingleMetaStorage::save() {
    butil::Timer timer;
    timer.start();

    StablePBMeta meta;
    meta.set_term(_term);
    meta.set_votedfor(_votedfor.to_string());

    std::string path(_path);
    path.append("/");
    path.append(_s_raft_meta);

    ProtoBufFile pb_file(path);
    int ret = pb_file.save(&meta, raft_sync_meta());
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << path;

    timer.stop();
    g_save_pb_raft_meta << timer.u_elapsed();
    LOG(INFO) << "Saved single stable meta, path " << _path
              << " term " << _term 
              << " votedfor " << _votedfor.to_string() 
              << " time: " << timer.u_elapsed();
    return ret;
}

RaftMetaStorage* FileBasedSingleMetaStorage::new_instance(
                                        const std::string& uri) const {
    return new FileBasedSingleMetaStorage(uri);
}

butil::Status FileBasedSingleMetaStorage::gc_instance(const std::string& uri, 
                                        const VersionedGroupId& vgid) const {
    butil::Status status;
    if (0 != gc_dir(uri)) {
        LOG(WARNING) << "Group " << vgid << " failed to gc single stable storage"
                        ", path: " << uri;
        status.set_error(EIO, "Group %s failed to gc single stable storage"
                         ", path: %s", vgid.c_str(), uri.c_str());
        return status;
    }
    LOG(INFO) << "Group " << vgid << " succeed to gc single stable storage"
                 ", path: " << uri;
    return status;
}

// KVBasedMergedMetaStorage
KVBasedMergedMetaStorage::KVBasedMergedMetaStorage(const std::string& path) {
    _merged_impl = global_mss_manager->register_meta_storage(path);
}

KVBasedMergedMetaStorage::~KVBasedMergedMetaStorage() {
    if (_merged_impl) {
        _merged_impl = NULL;
    }
}

butil::Status KVBasedMergedMetaStorage::init() {
    return _merged_impl->init();
};

butil::Status KVBasedMergedMetaStorage::set_term_and_votedfor(const int64_t term, 
            const PeerId& peer_id, const VersionedGroupId& group) {
    return _merged_impl->set_term_and_votedfor(term, peer_id, group);
};

butil::Status KVBasedMergedMetaStorage::get_term_and_votedfor(int64_t* term, 
            PeerId* peer_id, const VersionedGroupId& group) {
    return _merged_impl->get_term_and_votedfor(term, peer_id, group);
};

RaftMetaStorage* KVBasedMergedMetaStorage::new_instance(
                                    const std::string& uri) const {
    return new KVBasedMergedMetaStorage(uri);
}

butil::Status KVBasedMergedMetaStorage::gc_instance(const std::string& uri,
            const VersionedGroupId& vgid) const {
    butil::Status status;
    if (0 != global_mss_manager->
                remove_instance_from_meta_storage(uri, vgid)) {
        LOG(WARNING) << "Group " << vgid << " failed to gc kv, path: " << uri;
        status.set_error(EIO, "Group %s failed to gc kv in path: %s", 
                         vgid.c_str(), uri.c_str());
        return status;
    }
    LOG(INFO) << "Group " << vgid << " succeed to gc kv, path: " << uri;
    return status;
};

butil::Status KVBasedMergedMetaStorage::delete_meta(
                                    const VersionedGroupId& group) {
    return _merged_impl->delete_meta(group);
};

// KVBasedMergedMetaStorageImpl
butil::Status KVBasedMergedMetaStorageImpl::init() {
    std::unique_lock<raft_mutex_t> lck(_mutex); 

    butil::Status status;
    if (_is_inited) {
        return status;
    }
    
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        lck.unlock();
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        status.set_error(e, "Fail to create dir when init MergedMetaStorage, "
                         "path: %s", _path.c_str());
        return status;
    }

    leveldb::Options options;
    options.create_if_missing = true;
    //options.error_if_exists = true;   

    leveldb::Status st;
    st = leveldb::DB::Open(options, _path.c_str(), &_db);
    if (!st.ok()) {
        lck.unlock();
        LOG(ERROR) << "Fail to open db: " << st.ToString() << " path: " << _path;
        status.set_error(EIO, "Fail to open db, path: %s, error: %s", 
                         _path.c_str(), st.ToString().c_str());
        return status;
    }   

    _is_inited = true;
    return status;
}

butil::Status KVBasedMergedMetaStorageImpl::set_term_and_votedfor(
        const int64_t term, const PeerId& peer_id, const VersionedGroupId& group) {
    butil::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "MergedMetaStorage of group %s not init"
                         ", path: %s", group.c_str(), _path.c_str());
        return status;
    }
    
    butil::Timer timer;
    timer.start();
    leveldb::WriteOptions options;
    options.sync = raft_sync_meta();
    
    // TODO replace pb
    StablePBMeta meta;
    meta.set_term(term);
    meta.set_votedfor(peer_id.to_string());
    std::string meta_string;
    meta.SerializeToString(&meta_string);
    leveldb::Slice value(meta_string.data(), meta_string.size());
    
    leveldb::Slice key(group.data(), group.size()); 
    leveldb::Status st = _db->Put(options, key, value);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to put vote info into db, group " << group
                   << " term " << term << " vote for " << peer_id
                   << ", path: " << _path << ", error: " << st.ToString();
        status.set_error(EIO, "MergedMetaStorage of group %s failed"
                "to put value(term %ld, votedfor %s), path: %s, error: %s", 
                group.c_str(), term, peer_id.to_string().c_str(), 
                _path.c_str(), st.ToString().c_str());
        return status;
    }

    timer.stop();
    g_save_kv_raft_meta << timer.u_elapsed();
    LOG(INFO) << "Saved merged stable meta, path " << _path
              << " group " << group
              << " term " << term
              << " votedfor " << peer_id.to_string() 
              << " time: " << timer.u_elapsed();
    return status;
}

butil::Status KVBasedMergedMetaStorageImpl::get_term_and_votedfor(int64_t* term, 
                                PeerId* peer_id, const VersionedGroupId& group) {
    butil::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "MergedMetaStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }
    
    butil::Timer timer;
    timer.start();
    
    leveldb::Slice key(group.data(), group.size());
    std::string value;
    leveldb::Status st = _db->Get(leveldb::ReadOptions(), key, &value);
    if (st.IsNotFound()) {
        // Not exist in db, set initial term 1 and votedfor 0.0.0.0:0:0
        *term = 1;
        *peer_id = ANY_PEER;
        status = set_term_and_votedfor(*term, *peer_id, group);
        if (!status.ok()) {
            LOG(ERROR) << "node " << group
                     << " failed to set initial term and votedfor when first time init"
                     << ", path " << _path
                     << ", error " << status.error_cstr();
            return status;
        }
        LOG(NOTICE) << "node " << group
                 << " succeed to set initial term and votedfor when first time init"
                 << ", path " << _path;
        return status;
    } else if (!st.ok()) {
        LOG(ERROR) << "node " << group
                << " failed to get value from db, path " << _path
                << ", error " << st.ToString().c_str();
        status.set_error(EIO, "MergedMetaStorage of group %s failed to"
                         "get value from db, path: %s, error: %s", 
                         group.c_str(), _path.c_str(), st.ToString().c_str());
        return status;
    }
   
    // TODO replace pb
    StablePBMeta meta;
    meta.ParseFromString(value);
    *term = meta.term();
    if (0 != peer_id->parse(meta.votedfor())) {
        LOG(ERROR) << "node " << group 
                   << " failed to parse votedfor when loading meta from db, path " 
                   << _path;
        status.set_error(EINVAL, "MergedMetaStorage of group %s failed to"
                         " parse votedfor when loading meta from db, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }

    timer.stop();
    g_load_kv_raft_meta << timer.u_elapsed(); 
    LOG(INFO) << "Loaded merged stable meta, path " << _path
              << " group " << group
              << " term " << *term
              << " votedfor " << *peer_id
              << " time: " << timer.u_elapsed();
    return status;
}

butil::Status KVBasedMergedMetaStorageImpl::delete_meta(
                                        const VersionedGroupId& group) { 
    butil::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "MergedMetaStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }
    
    butil::Timer timer;
    timer.start();
    leveldb::WriteOptions options;
    options.sync = raft_sync_meta();
    
    leveldb::Slice key(group.data(), group.size()); 
    leveldb::Status st = _db->Delete(options, key);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to delete meta info from db, group " << group;
        status.set_error(EIO, "MergedMetaStorage failed to delete group %s"
                         ", path: %s, error: %s",
                         group.c_str(), _path.c_str(), st.ToString().c_str());
        return status;
    }

    timer.stop();
    g_delete_kv_raft_meta << timer.u_elapsed(); 
    LOG(INFO) << "Deleted merged stable meta, path " << _path
              << " group " << group
              << " time: " << timer.u_elapsed();
    return status;
}

}
