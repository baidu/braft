// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  BRAFT_FILE_SERVICE_H
#define  BRAFT_FILE_SERVICE_H

#include <butil/memory/singleton.h>
#include "braft/file_service.pb.h"
#include "braft/file_reader.h"
#include "braft/util.h"

namespace braft {

class BAIDU_CACHELINE_ALIGNMENT FileServiceImpl : public FileService {
public:
    static FileServiceImpl* GetInstance() {
        return Singleton<FileServiceImpl>::get();
    }
    void get_file(::google::protobuf::RpcController* controller,
                  const ::braft::GetFileRequest* request,
                  ::braft::GetFileResponse* response,
                  ::google::protobuf::Closure* done);
    int add_reader(FileReader* reader, int64_t* reader_id);
    int remove_reader(int64_t reader_id);
private:
friend struct DefaultSingletonTraits<FileServiceImpl>;
    FileServiceImpl();
    ~FileServiceImpl() {}
    typedef std::map<int64_t, scoped_refptr<FileReader> > Map;
    raft_mutex_t _mutex;
    int64_t _next_id;
    Map _reader_map;
};

inline FileServiceImpl* file_service()
{ return FileServiceImpl::GetInstance(); }

inline int file_service_add(FileReader* reader, int64_t* reader_id) {
    FileServiceImpl* const fs = file_service();
    return fs->add_reader(reader, reader_id);
}

inline int file_service_remove(int64_t reader_id) {
    FileServiceImpl* const fs = file_service();
    return fs->remove_reader(reader_id);
}

}  //  namespace braft

#endif  //BRAFT_FILE_SERVICE_H
