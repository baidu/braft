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

#ifndef BRAFT_PROTOBUF_FILE_H
#define BRAFT_PROTOBUF_FILE_H

#include <string>
#include <google/protobuf/message.h>
#include "braft/file_system_adaptor.h"

namespace braft {

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

#endif //~BRAFT_PROTOBUF_FILE_H
