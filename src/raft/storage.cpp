/*
 * =====================================================================================
 *
 *       Filename:  storage.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年11月05日 11时40分00秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#include <errno.h>
#include <base/string_splitter.h>
#include <base/logging.h>

#include "raft/storage.h"
#include "raft/log.h"
#include "raft/stable.h"
#include "raft/snapshot.h"

namespace raft {

const int MAX_STORAGE_SIZE = 16;
static Storage s_storage_map[MAX_STORAGE_SIZE];

void init_storage() {
    Storage local_storage = {
        "file",
        create_local_log_storage,
        create_local_stable_storage,
        create_local_snapshot_storage,
    };

    if (0 != register_storage("file://", local_storage)) {
        LOG(FATAL) << "register storage failed, storage file";
        exit(1);
    }
}

int register_storage(const std::string& uri, const Storage& storage) {
    std::string uri_prefix;
    for (base::StringSplitter s(uri.c_str(), ':'); s; ++s) {
        uri_prefix = std::string(s.field(), s.length());
    }

    if (uri_prefix.length() == 0) {
        LOG(WARNING) << "storage " << uri << " bad format";
        return EINVAL;
    }

    for (int i = 0; i < MAX_STORAGE_SIZE; i++) {
        if (s_storage_map[i].name.length() == 0) {
            s_storage_map[i] = storage;
            s_storage_map[i].name = uri_prefix;
            return 0;
        }
        if (0 == uri_prefix.compare(s_storage_map[i].name)) {
            LOG(WARNING) << "storage " << uri_prefix << " register failed, storage exist";
            return EINVAL;
        }
    }
    LOG(WARNING) << "storage " << uri_prefix << " register failed, storage map full";
    return ENOSPC;
}

Storage* find_storage(const std::string& uri) {
    std::string uri_prefix;
    for (base::StringSplitter s(uri.c_str(), ':'); s; ++s) {
        uri_prefix = std::string(s.field(), s.length());
    }

    if (uri_prefix.length() == 0) {
        LOG(WARNING) << "storage " << uri << " bad format";
        return NULL;
    }

    for (int i = 0; i < MAX_STORAGE_SIZE; i++) {
        if (s_storage_map[i].name.length() == 0) {
            break;
        }
        if (0 == uri_prefix.compare(s_storage_map[i].name)) {
            return &s_storage_map[i];
        }
    }
    LOG(WARNING) << "storage " << uri << " not found";
    return NULL;
}

}
