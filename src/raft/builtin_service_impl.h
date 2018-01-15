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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Ge,Jun(gejun@baiud.com)

#ifndef  PUBLIC_RAFT_BUILTIN_SERVICE_IMPL_H
#define  PUBLIC_RAFT_BUILTIN_SERVICE_IMPL_H

#include "raft/builtin_service.pb.h"
#include <baidu/rpc/builtin/tabbed.h>

namespace raft {

class RaftStatImpl : public raft_stat, public baidu::rpc::Tabbed {
public:
    void default_method(::google::protobuf::RpcController* controller,
                        const ::raft::IndexRequest* request,
                        ::raft::IndexResponse* response,
                        ::google::protobuf::Closure* done);

    void GetTabInfo(baidu::rpc::TabInfoList*) const;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_BUILTIN_SERVICE_IMPL_H
