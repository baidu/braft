/*
 * =====================================================================================
 *
 *       Filename:  stable.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/22 20:02:53
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <errno.h>
#include <base/time.h>
#include <base/logging.h>
#include "raft/util.h"
#include "raft/protobuf_file.h"
#include "raft/local_storage.pb.h"
#include "raft/stable.h"

namespace raft {

const char* LocalStableStorage::_s_stable_meta = "stable_meta";
int LocalStableStorage::init() {
    if (_is_inited) {
        return 0;
    }

    int ret = load();
    if (ret == 0) {
        _is_inited = true;
    }
    return ret;
}

int LocalStableStorage::set_term(const int64_t term) {
    if (_is_inited) {
        _term = term;
        return save();
    } else {
        LOG(WARNING) << "LocalStableStorage not init(), path: " << _path;
        return -1;
    }
}

int64_t LocalStableStorage::get_term() {
    if (_is_inited) {
        return _term;
    } else {
        LOG(WARNING) << "LocalStableStorage not init(), path: " << _path;
        return -1;
    }
}

int LocalStableStorage::set_votedfor(const PeerId& peer_id) {
    if (_is_inited) {
        _votedfor = peer_id;
        return save();
    } else {
        LOG(WARNING) << "LocalStableStorage not init(), path: " << _path;
        return -1;
    }
}

int LocalStableStorage::set_term_and_votedfor(const int64_t term, const PeerId& peer_id) {
    if (_is_inited) {
        _term = term;
        _votedfor = peer_id;
        return save();
    } else {
        LOG(WARNING) << "LocalStableStorage not init(), path: " << _path;
        return -1;
    }
}

int LocalStableStorage::load() {

    std::string path(_path);
    path.append("/");
    path.append(_s_stable_meta);

    ProtoBufFile pb_file(path);

    StablePBMeta meta;
    int ret = pb_file.load(&meta);
    if (ret == 0) {
        _term = meta.term();
        ret = _votedfor.parse(meta.votedfor());
    } else if (errno == ENOENT) {
        ret = 0;
    }

    return ret;
}

int LocalStableStorage::save() {
    base::Timer timer;
    timer.start();

    StablePBMeta meta;
    meta.set_term(_term);
    meta.set_votedfor(_votedfor.to_string());

    std::string path(_path);
    path.append("/");
    path.append(_s_stable_meta);

    ProtoBufFile pb_file(path);

    int ret = pb_file.save(&meta, FLAGS_raft_sync /*true*/);

    timer.stop();
    LOG(INFO) << "save stable meta, path " << _path
        << " term " << _term << " votedfor " << _votedfor.to_string() << " time: " << timer.u_elapsed();
    return ret;
}

int LocalStableStorage::get_votedfor(PeerId* peer_id) {
    if (_is_inited) {
        *peer_id = _votedfor;
        return 0;
    } else {
        LOG(WARNING) << "LocalStableStorage not init(), path: " << _path;
        return -1;
    }
}

StableStorage* create_local_stable_storage(const std::string& uri) {
    std::string local_path = fileuri2path(uri);
    if (local_path.empty()) {
        return NULL;
    }

    LocalStableStorage* storage = new LocalStableStorage(local_path);
    return storage;
}

}
