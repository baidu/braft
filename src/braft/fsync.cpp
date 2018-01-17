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

#include "braft/fsync.h"
#include <brpc/reloadable_flags.h>  //BRPC_VALIDATE_GFLAG

namespace braft {

DEFINE_bool(raft_use_fsync_rather_than_fdatasync,
            true,
            "Use fsync rather than fdatasync to flush page cache");

BRPC_VALIDATE_GFLAG(raft_use_fsync_rather_than_fdatasync,
                         brpc::PassValidate);

}  //  namespace braft
