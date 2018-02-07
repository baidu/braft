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

#include <errno.h>
#include <butil/time.h>
#include <butil/logging.h>
#include <butil/file_util.h>                         // butil::CreateDirectory
#include "braft/util.h"
#include "braft/protobuf_file.h"
#include "braft/local_storage.pb.h"
#include "braft/raft_meta.h"

namespace braft {

const char* LocalRaftMetaStorage::_s_raft_meta = "raft_meta";

int LocalRaftMetaStorage::init() {
    if (_is_inited) {
        return 0;
    }
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        return -1;
    }

    int ret = load();
    if (ret == 0) {
        _is_inited = true;
    }
    return ret;
}

int LocalRaftMetaStorage::set_term(const int64_t term) {
    if (_is_inited) {
        _term = term;
        return save();
    } else {
        LOG(WARNING) << "LocalRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int64_t LocalRaftMetaStorage::get_term() {
    if (_is_inited) {
        return _term;
    } else {
        LOG(WARNING) << "LocalRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int LocalRaftMetaStorage::set_votedfor(const PeerId& peer_id) {
    if (_is_inited) {
        _votedfor = peer_id;
        return save();
    } else {
        LOG(WARNING) << "LocalRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int LocalRaftMetaStorage::set_term_and_votedfor(const int64_t term, const PeerId& peer_id) {
    if (_is_inited) {
        _term = term;
        _votedfor = peer_id;
        return save();
    } else {
        LOG(WARNING) << "LocalRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int LocalRaftMetaStorage::load() {

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

    return ret;
}

int LocalRaftMetaStorage::save() {
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
    LOG(INFO) << "save raft meta, path " << _path
        << " term " << _term << " votedfor " << _votedfor.to_string() << " time: " << timer.u_elapsed();
    return ret;
}

int LocalRaftMetaStorage::get_votedfor(PeerId* peer_id) {
    if (_is_inited) {
        *peer_id = _votedfor;
        return 0;
    } else {
        LOG(WARNING) << "LocalRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

RaftMetaStorage* LocalRaftMetaStorage::new_instance(const std::string& uri) const {
    return new LocalRaftMetaStorage(uri);
}

}
