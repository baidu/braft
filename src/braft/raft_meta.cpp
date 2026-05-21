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
#include <algorithm>
#include <butil/time.h>
#include <butil/logging.h>
#include <butil/file_util.h>                         // butil::CreateDirectory
#include <gflags/gflags.h>
#include <brpc/reloadable_flags.h>
#include <atomic>
#include <memory>
#include "braft/util.h"
#include "braft/protobuf_file.h"
#include "braft/local_storage.pb.h"
#include "braft/raft_meta.h"

namespace braft {

DEFINE_int32(raft_meta_write_batch, 128, 
             "Max number of tasks that can be written into db in a single batch");
BRPC_VALIDATE_GFLAG(raft_meta_write_batch, brpc::PositiveInteger);

// Warning: This design is predicated on the observation that, under heavy load testing, the time
// required to synchronously flush `raft_meta` data to disk becomes excessive, potentially
// triggering a Leader election. Our design is tailored to the following scenarios:
// 1.  Under heavy load testing, asynchronous disk flushing is employed. Since the data has already
// been written to the kernel's buffer cache, no data loss occurs even in the event of a process
// crash; data loss would only result from an unexpected power failure.
// 2.  When the system load is moderate, the design utilizes asynchronous flushing supplemented by a
// dedicated background flushing thread (configured by default to flush to disk once per second).
// 3.  Under very light load conditions, synchronous disk flushing remains enabled.
// 4.  Ultimately, all these configurations are controlled externally. External system designers do
// not adjust these parameters arbitrarily; rather, they configure specific parameters tailored to
// the requirements of each distinct operational scenario.


// Set to true after the first KVBasedMergedMetaStorageImpl::init() completes.
// Validators use this to distinguish startup-time flag setting (allowed) from
// post-init runtime changes (rejected).
static std::atomic<bool> g_raft_meta_initialized{false};

DEFINE_bool(raft_meta_enable_leveldb_tuning, false,
             "Tune LevelDB options for raft meta DB (write_buffer_size, max_open_files, etc.). "
             "Takes effect only at init() time; cannot be changed at runtime.");
// LevelDB options are applied once during DB::Open, runtime changes have no effect.
static bool validate_raft_meta_enable_leveldb_tuning(const char*, bool val) {
    if (g_raft_meta_initialized.load(std::memory_order_acquire) &&
        val != FLAGS_raft_meta_enable_leveldb_tuning) {
        LOG(ERROR) << "raft_meta_enable_leveldb_tuning cannot be changed at runtime. "
                   << "LevelDB options are applied only during init(). "
                   << "Please set this flag at startup.";
        return false;
    }
    return true;
}
BRPC_VALIDATE_GFLAG(raft_meta_enable_leveldb_tuning, validate_raft_meta_enable_leveldb_tuning);

DEFINE_bool(raft_meta_enable_preserialize, false,
             "Pre-serialize protobuf in set_term_and_votedfor() before submitting to the "
             "execution queue, moving CPU work out of the queue handler hot path.");
BRPC_VALIDATE_GFLAG(raft_meta_enable_preserialize, brpc::PassValidate);

DEFINE_bool(raft_meta_force_no_sync, false,
             "Skip fsync when writing raft metadata. "
             "Under extreme load, synchronous fsync may take too long and trigger leader election. "
             "Enabling this flag avoids that by writing to kernel buffer cache only. "
             "Process crash will NOT lose data, but machine power failure WILL lose unsynced vote records. "
             "Recommend enabling raft_meta_periodic_sync_enabled=true together.");

DEFINE_bool(raft_meta_periodic_sync_enabled, false,
             "Enable background periodic fsync for raft meta DB. "
             "When enabled, normal writes use sync=false for performance, and "
             "a background timer ensures data is fsynced periodically. "
             "Note: there is still a data loss window (up to raft_meta_periodic_sync_interval_ms) "
             "on machine power failure between two fsyncs.");

// raft_meta_periodic_sync_enabled is read only at init() time to decide whether
// to create the background sync timer.  Changing it at runtime has no effect
// because the timer is not created/destroyed dynamically, so we reject any
// runtime modification to avoid giving users a false sense of control.
static bool validate_raft_meta_periodic_sync_enabled(const char*, bool val) {
    if (g_raft_meta_initialized.load(std::memory_order_acquire) &&
        val != FLAGS_raft_meta_periodic_sync_enabled) {
        LOG(ERROR) << "raft_meta_periodic_sync_enabled cannot be changed at runtime. "
                   << "The periodic sync timer is created only during init(). "
                   << "Please set this flag at startup.";
        return false;
    }
    return true;
}
BRPC_VALIDATE_GFLAG(raft_meta_periodic_sync_enabled, validate_raft_meta_periodic_sync_enabled);

// Validator for force_no_sync: placed after periodic_sync_enabled so the flag is visible.
static bool validate_raft_meta_force_no_sync(const char*, bool val) {
    if (val && !FLAGS_raft_meta_periodic_sync_enabled) {
        LOG(WARNING) << "raft_meta_force_no_sync is being set to true "
                     << "while raft_meta_periodic_sync_enabled=false. "
                     << "This silently overrides raft_sync/raft_sync_meta and "
                     << "leaves writes non-durable against power failure. "
                     << "Consider enabling raft_meta_periodic_sync_enabled.";
    }
    return true;  // always allow, just warn
}
BRPC_VALIDATE_GFLAG(raft_meta_force_no_sync, validate_raft_meta_force_no_sync);

DEFINE_int32(raft_meta_periodic_sync_interval_ms, 1000,
             "Interval (ms) for periodic fsync when raft_meta_periodic_sync_enabled "
             "is true. Default 1000 (1 second). "
             "NOTE: setting this to 0 at runtime does NOT pause syncing — the timer "
             "continues to fire at its original interval and dirty writes are still "
             "fsynced. To actually stop periodic syncing the process must be restarted "
             "with raft_meta_periodic_sync_enabled=false. "
             "Smaller values reduce the data loss window on power failure "
             "but increase I/O overhead.");
BRPC_VALIDATE_GFLAG(raft_meta_periodic_sync_interval_ms, brpc::NonNegativeInteger);

static bvar::LatencyRecorder g_load_pb_raft_meta("raft_load_pb_raft_meta");
static bvar::LatencyRecorder g_save_pb_raft_meta("raft_save_pb_raft_meta");
static bvar::LatencyRecorder g_load_kv_raft_meta("raft_load_kv_raft_meta");
static bvar::LatencyRecorder g_save_kv_raft_meta("raft_save_kv_raft_meta");
static bvar::LatencyRecorder g_delete_kv_raft_meta("raft_delete_kv_raft_meta");

static bvar::CounterRecorder g_save_kv_raft_meta_batch_counter(
                                    "raft_save_kv_raft_meta_batch_counter");

const char* FileBasedSingleMetaStorage::_s_raft_meta = "raft_meta";

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
                       const scoped_refptr<KVBasedMergedMetaStorageImpl>& mss) {
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
    LOG(INFO) << "Succeed to init MixedMetaStorage, path: " << _path;
    return status;
}

class StableMetaClosure : public Closure {
public:
    StableMetaClosure(const int64_t term, const PeerId& votedfor, 
                      const VersionedGroupId& vgid, const std::string& path) 
        : _term(term)
        , _votedfor(votedfor)
        , _vgid(vgid)
        , _path(path)
        , _start_time_us(butil::cpuwide_time_us()) 
    {}

    ~StableMetaClosure() {}

    void Run() {
        if (!status().ok()) {
            LOG(ERROR) << "Failed to write stable meta into db, group " << _vgid
                       << " term " << _term << " vote for " << _votedfor
                       << ", path: " << _path << ", error: " << status();
        } else {
            int64_t u_elapsed = butil::cpuwide_time_us() - _start_time_us;
            g_save_kv_raft_meta << u_elapsed;
            LOG(INFO) << "Saved merged stable meta, path " << _path
                      << " group " << _vgid
                      << " term " << _term
                      << " votedfor " << _votedfor
                      << " time: " << u_elapsed;  
        }
        
        _sync.Run();
    }

    void wait() { _sync.wait(); }

private:
    int64_t _term;
    PeerId _votedfor;
    VersionedGroupId _vgid;
    std::string _path;
    int64_t _start_time_us;
    SynchronizedClosure _sync;
};

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

    StableMetaClosure done(term, peer_id, group, _path);
    _merged_impl->set_term_and_votedfor(term, peer_id, group, &done);
    done.wait();
    return done.status();
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
        StableMetaClosure done(*term, *peer_id, group, _path);
        _merged_impl->set_term_and_votedfor(*term, *peer_id, group, &done);
        done.wait();
        status = done.status();
        if (!status.ok()) {
            LOG(WARNING) << "node " << group 
                << " merged stable storage failed to set term " << *term
                << " and vote for peer " << *peer_id
                << " when catch up data, path " << _path
                << ", error: " << status.error_cstr();
            return status;
        }
        LOG(INFO) << "node " << group 
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
        LOG(INFO) << "node " << group 
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
    StableMetaClosure done(term, peer_id, group, "");
    _merged_impl->set_term_and_votedfor(term, peer_id, group, &done);
    done.wait();
    
    return done.status();
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

// MetaPeriodicSyncTimer
static int64_t get_data_loss_window_ms(void* arg) {
    MetaPeriodicSyncTimer* timer = static_cast<MetaPeriodicSyncTimer*>(arg);
    int64_t last_sync = timer->last_sync_time_ms();
    if (last_sync == 0) {
        // Never synced yet, return -1 to indicate unknown
        return -1;
    }
    return butil::monotonic_time_ms() - last_sync;
}

MetaPeriodicSyncTimer::MetaPeriodicSyncTimer(leveldb::DB* db, const std::string& path)
    : _db(db), _path(path), _dirty(false), _last_sync_time_ms(0) {
    // Initialize bvar metrics with path-specific names to support multiple instances
    std::string prefix = "raft_meta_periodic_sync_" + path;
    // Replace '/' with '_' to make valid bvar names
    std::replace(prefix.begin(), prefix.end(), '/', '_');

    _sync_success_count = std::make_unique<bvar::Adder<int64_t>>(prefix + "_success");
    _sync_failure_count = std::make_unique<bvar::Adder<int64_t>>(prefix + "_failure");
    _data_loss_window_ms = std::make_unique<bvar::PassiveStatus<int64_t>>(
        prefix + "_data_loss_window_ms", get_data_loss_window_ms, this);
}

MetaPeriodicSyncTimer::~MetaPeriodicSyncTimer() {
    // unique_ptr will auto-cleanup bvar metrics
}

void MetaPeriodicSyncTimer::run() {
    if (!_dirty.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    leveldb::WriteOptions sync_options;
    sync_options.sync = true;
    // We submit an empty WriteBatch with sync=true to force a WAL fsync without
    // writing any user data. This relies on LevelDB's behaviour of flushing and
    // syncing the WAL even for an empty batch when sync=true is set (see
    // leveldb/db/db_impl.cc, DBImpl::Write). This is not explicitly documented
    // in the public API, but has been stable across all LevelDB versions we
    // support. If LevelDB ever skips the sync for empty batches, previously
    // non-synced writes could be lost on power failure.
    leveldb::WriteBatch empty_batch;
    leveldb::Status st = _db->Write(sync_options, &empty_batch);
    if (!st.ok()) {
        LOG(WARNING) << "Periodic sync failed for meta db, path: " << _path
                     << ", error: " << st.ToString();
        // Restore dirty flag so next timer tick will retry
        _dirty.store(true, std::memory_order_release);
        (*_sync_failure_count) << 1;
    } else {
        _last_sync_time_ms.store(butil::monotonic_time_ms(), std::memory_order_release);
        (*_sync_success_count) << 1;
    }
}

int MetaPeriodicSyncTimer::adjust_timeout_ms(int timeout_ms) {
    const int current = FLAGS_raft_meta_periodic_sync_interval_ms;
    if (current > 0) {
        return current;
    }
    // Flag is 0: we cannot honour a zero-ms interval (the underlying timer
    // does not support it), so we fall back to the original interval that was
    // passed at init() time. The timer keeps firing and run() will still fsync
    // if dirty=true. Setting this flag to 0 does NOT pause syncing — it is
    // simply ignored. To stop periodic syncing, restart with
    // raft_meta_periodic_sync_enabled=false.
    return timeout_ms;
}

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

    if (FLAGS_raft_meta_enable_leveldb_tuning) {
        // Raft meta DB stores tiny KV pairs (term+votedfor per group),
        // tune for minimal I/O and compaction overhead.
        options.write_buffer_size = 1 * 1024 * 1024;   // 1MB, enough for meta
        options.max_open_files = 64;                    // tiny DB needs few fds
        options.max_file_size = 1 * 1024 * 1024;        // 1MB sstable
        options.compression = leveldb::kNoCompression;  // values too small to benefit
    }

    leveldb::Status st;
    st = leveldb::DB::Open(options, _path.c_str(), &_db);
    if (!st.ok()) {
        lck.unlock();
        LOG(ERROR) << "Fail to open db: " << st.ToString() << " path: " << _path;
        status.set_error(EIO, "Fail to open db, path: %s, error: %s", 
                         _path.c_str(), st.ToString().c_str());
        return status;
    }   

    // start execution_queue
    bthread::ExecutionQueueOptions execq_opt;
    execq_opt.bthread_attr = BTHREAD_ATTR_NORMAL;
    //execq_opt.max_tasks_size = 256;
    if (bthread::execution_queue_start(&_queue_id,
                                       &execq_opt,
                                       KVBasedMergedMetaStorageImpl::run,
                                       this) != 0) {
        status.set_error(EINVAL, "Fail to start execution_queue, path: %s",
                         _path.c_str());
        return status;
    }

    // Start periodic sync timer if enabled
    if (FLAGS_raft_meta_periodic_sync_enabled) {
        _periodic_sync_timer = std::make_unique<MetaPeriodicSyncTimer>(_db, _path);
        if (_periodic_sync_timer->init(FLAGS_raft_meta_periodic_sync_interval_ms) != 0) {
            LOG(ERROR) << "Fail to init periodic sync timer, path: " << _path;
            _periodic_sync_timer.reset();
            status.set_error(EINVAL, "Fail to init periodic sync timer, path: %s",
                         _path.c_str());
            return status;
        } else {
            _periodic_sync_timer->start();
            LOG(INFO) << "Started periodic meta sync timer, interval_ms: "
                      << FLAGS_raft_meta_periodic_sync_interval_ms
                      << ", path: " << _path;
        }
    }

    // Warn about unsafe flag combinations
    if (FLAGS_raft_meta_force_no_sync && !FLAGS_raft_meta_periodic_sync_enabled) {
        LOG(WARNING) << "UNSAFE CONFIG: raft_meta_force_no_sync=true but "
                     << "raft_meta_periodic_sync_enabled=false. "
                     << "Writes go to kernel buffer cache only, "
                     << "process crash is safe but machine power failure "
                     << "will lose unsynced vote records. "
                     << "Recommend enabling raft_meta_periodic_sync_enabled=true. "
                     << "path: " << _path;
    }

    _is_inited = true;
    g_raft_meta_initialized.store(true, std::memory_order_release);
    return status;
}


void KVBasedMergedMetaStorageImpl::run_tasks(leveldb::WriteBatch& updates, 
                                               Closure* dones[], size_t size) {
    g_save_kv_raft_meta_batch_counter << size; 
    
    leveldb::Status st;
    bool sync = raft_sync_meta() && (!FLAGS_raft_meta_force_no_sync);
    if (!sync && !FLAGS_raft_meta_periodic_sync_enabled) {
        LOG_EVERY_N(WARNING, 10000)
            << "raft meta writes are NOT durable: "
            << "force_no_sync=" << FLAGS_raft_meta_force_no_sync
            << ", periodic_sync=" << FLAGS_raft_meta_periodic_sync_enabled
            << ", raft_sync_meta=" << FLAGS_raft_sync_meta
            << ". Process crash is safe, but machine power failure "
            << "will lose unsynced vote records. "
            << "path: " << _path;
    }
    leveldb::WriteOptions write_options;
    write_options.sync = sync;
    st = _db->Write(write_options, &updates);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to write batch into db, path: " << _path
                   << ", error: " << st.ToString();
        butil::Status status;
        status.set_error(EIO, "MergedMetaStorage failed to write batch"
                              ", path: %s, error: %s", 
                              _path.c_str(), st.ToString().c_str());
        for (size_t i = 0; i < size; ++i) {
            dones[i]->status() = status; 
            run_closure_in_bthread_nosig(dones[i]);
        }
    } else {
        if (_periodic_sync_timer && !sync) {
            _periodic_sync_timer->mark_dirty();
        }
        for (size_t i = 0; i < size; ++i) {
            run_closure_in_bthread_nosig(dones[i]);
        }
    }
    bthread_flush();
}

int KVBasedMergedMetaStorageImpl::run(void* meta,
                                bthread::TaskIterator<WriteTask>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    KVBasedMergedMetaStorageImpl* mss = (KVBasedMergedMetaStorageImpl*)meta;
    const bool optimized = FLAGS_raft_meta_enable_preserialize;
    const size_t batch_size = FLAGS_raft_meta_write_batch;
    leveldb::WriteBatch updates;
    DEFINE_SMALL_ARRAY(Closure*, dones, batch_size, 256);
    size_t dones_count = 0;

    // Serialize task value: optimized path uses pre-serialized data,
    // non-optimized path serializes protobuf on the fly.
    auto put_task = [optimized](leveldb::WriteBatch& batch,
                                const WriteTask& task) {
        leveldb::Slice key(task.vgid.data(), task.vgid.size());
        if (optimized) {
            leveldb::Slice value(task.serialized_value.data(),
                                 task.serialized_value.size());
            batch.Put(key, value);
        } else {
            StablePBMeta meta;
            meta.set_term(task.term);
            meta.set_votedfor(task.votedfor.to_string());
            std::string meta_string;
            meta.SerializeToString(&meta_string);
            batch.Put(key, leveldb::Slice(meta_string.data(), meta_string.size()));
        }
    };

    for (; iter; ++iter) {
        if (dones_count == batch_size) {
            mss->run_tasks(updates, dones, dones_count);
            updates.Clear();
            dones_count = 0;
        }
        put_task(updates, *iter);
        dones[dones_count++] = iter->done;
    }
    if (dones_count > 0) {
        mss->run_tasks(updates, dones, dones_count);
    }

    return 0;
}

void KVBasedMergedMetaStorageImpl::set_term_and_votedfor(
                                const int64_t term, const PeerId& peer_id, 
                                const VersionedGroupId& group, Closure* done) {
    if (!_is_inited) {
        done->status().set_error(EINVAL, "MergedMetaStorage of group %s not"
                                 " init, path: %s", group.c_str(), _path.c_str());
        return run_closure_in_bthread(done);
    }
    
    WriteTask task;
    task.term = term;
    task.votedfor = peer_id;
    task.vgid = group;
    task.done = done;
    if (FLAGS_raft_meta_enable_preserialize) {
        // Pre-serialize protobuf to reduce CPU work inside execution queue handler
        StablePBMeta meta;
        meta.set_term(term);
        meta.set_votedfor(peer_id.to_string());
        meta.SerializeToString(&task.serialized_value);
    }

    if (bthread::execution_queue_execute(_queue_id, task) != 0) {
        task.done->status().set_error(EIO, "Failed to put task into queue");
        return run_closure_in_bthread(task.done);
    }
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
        StableMetaClosure done(*term, *peer_id, group, _path);
        set_term_and_votedfor(*term, *peer_id, group, &done);
        done.wait();
        status = done.status();
        if (!status.ok()) {
            LOG(ERROR) << "node " << group
                     << " failed to set initial term and votedfor when first time init"
                     << ", path " << _path
                     << ", error " << status.error_cstr();
            return status;
        }
        LOG(INFO) << "node " << group
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
