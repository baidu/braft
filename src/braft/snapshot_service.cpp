// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
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

// Authors: Kai FAN(fankai@baidu.com)

#include <butil/logging.h>
#include <brpc/server.h>
#include "braft/node_manager.h"
#include "braft/snapshot_service.h"
#include "braft/node.h"
#include "braft/learner.h"

namespace braft {

void SnapshotServiceImpl::tail_snapshot(google::protobuf::RpcController *cntl_base,
                                        const TailSnapshotRequest *request,
                                        TailSnapshotResponse *response,
                                        google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    if (request->has_last_snapshot_index()) {
        scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(request->group_id(),
                                                                                peer_id);
        NodeImpl* node = node_ptr.get();
        if (!node) {
            cntl->SetFailed(ENOENT, "peer_id not exist");
            return;
        }
        node->handle_tail_snapshot(cntl, request, response);
    } else {
        scoped_refptr<LearnerImpl> node_ptr = NodeManager::GetInstance()->get_learner(
            request->group_id(), peer_id);
        LearnerImpl* node = node_ptr.get();
        if (!node) {
            cntl->SetFailed(ENOENT, "peer_id not exist");
            return;
        }
        node->handle_tail_snapshot(cntl, request, response);
    }
}

}
