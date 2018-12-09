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
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef  BRAFT_FSYNC_H
#define  BRAFT_FSYNC_H

#include <unistd.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include "braft/storage.h"

namespace braft {

DECLARE_bool(raft_use_fsync_rather_than_fdatasync);

inline int raft_fsync(int fd) {
    if (FLAGS_raft_use_fsync_rather_than_fdatasync) {
        return fsync(fd);
    } else {
#ifdef __APPLE__
        return fcntl(fd, F_FULLFSYNC);
#else
        return fdatasync(fd);
#endif
    }
}

inline bool raft_sync_meta() {
    return FLAGS_raft_sync || FLAGS_raft_sync_meta;
}

}  //  namespace braft

#endif  //BRAFT_FSYNC_H
