// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/21 16:44:15

#ifndef PUBLIC_RAFT_PROTOBUF_FILE_H
#define PUBLIC_RAFT_PROTOBUF_FILE_H

#include <string>
#include <google/protobuf/message.h>
#include "raft/file_system_adaptor.h"

namespace raft {

// protobuf file format:
// len [4B, in network order]
// protobuf data
class ProtoBufFile {
public:
    ProtoBufFile(const char* path, FileSystemAdaptor* fs = NULL);
    ProtoBufFile(const std::string& path, FileSystemAdaptor* fs = NULL);
    ~ProtoBufFile() {}

    int save(const ::google::protobuf::Message* message, bool sync);
    int load(::google::protobuf::Message* message);
private:
    std::string _path;
    scoped_refptr<FileSystemAdaptor> _fs;
};

}

#endif //~PUBLIC_RAFT_PROTOBUF_FILE_H
