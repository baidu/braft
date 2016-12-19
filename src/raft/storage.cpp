// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/11/05 11:34:03

#include <errno.h>
#include <base/string_printf.h>
#include <base/string_splitter.h>
#include <base/strings/string_piece.h>
#include <base/logging.h>
#include <baidu/rpc/reloadable_flags.h>

#include "raft/storage.h"
#include "raft/log.h"
#include "raft/stable.h"
#include "raft/snapshot.h"

namespace raft {

DEFINE_bool(raft_sync, true, "call fsync when need");
DEFINE_bool(raft_create_parent_directories, true,
            "Create parent directories of the path in local storage if true");
BAIDU_RPC_VALIDATE_GFLAG(raft_sync, ::baidu::rpc::PassValidate);

inline base::StringPiece parse_uri(base::StringPiece* uri, std::string* parameter) {
    // ${protocol}://${parameters}
    size_t pos = uri->find("://");
    if (pos == base::StringPiece::npos) {
        return base::StringPiece();
    }
    base::StringPiece protocol = uri->substr(0, pos);
    uri->remove_prefix(pos + 3/* length of '://' */);
    protocol.trim_spaces();
    parameter->reserve(uri->size());
    parameter->clear();
    size_t removed_spaces = 0;
    for (base::StringPiece::const_iterator 
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

LogStorage* LogStorage::create(const std::string& uri) {
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid log storage uri=`" << uri << '\'';
        return NULL;
    }
    const LogStorage* type = log_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find log storage type " << protocol;
        return NULL;
    }
    return type->new_instance(parameter);
}

SnapshotStorage* SnapshotStorage::create(const std::string& uri) {
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid snapshot storage uri=`" << uri << '\'';
        return NULL;
    }
    const SnapshotStorage* type = snapshot_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find snapshot storage type " << protocol;
        return NULL;
    }
    return type->new_instance(parameter);
}

StableStorage* StableStorage::create(const std::string& uri) {
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid stable storage uri=`" << uri << '\'';
        return NULL;
    }
    const StableStorage* type = stable_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find stable storage type " << protocol;
        return NULL;
    }
    return type->new_instance(parameter);
}

}  // namespace raft
