// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/04 14:31:45

#ifndef  PUBLIC_RAFT_FILE_SERVICE_H
#define  PUBLIC_RAFT_FILE_SERVICE_H

#include <base/memory/singleton.h>
#include "raft/file_service.pb.h"
#include "raft/file_reader.h"
#include "raft/util.h"

namespace raft {

class BAIDU_CACHELINE_ALIGNMENT FileServiceImpl : public FileService {
public:
    static FileServiceImpl* GetInstance() {
        return Singleton<FileServiceImpl>::get();
    }
    void get_file(::google::protobuf::RpcController* controller,
                  const ::raft::GetFileRequest* request,
                  ::raft::GetFileResponse* response,
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

}  // namespace raft

#endif  //PUBLIC_RAFT_FILE_SERVICE_H
