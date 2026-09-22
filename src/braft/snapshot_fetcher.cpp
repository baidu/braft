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

#include "braft/node.h"
#include "braft/snapshot_fetcher.h"
#include "braft/node_manager.h"

namespace braft {

void SnapshotFetcher::fetch_snapshot(const PeerId& peer_id, SnapshotWriter* writer, Closure* done) {
    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(_node_id.group_id,
                                                                       _node_id.peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        done->status().set_error(EINVAL, "node not exist");
        done->Run();
        return;
    }

    node->fetch_snapshot(peer_id, _max_snapshot_index, writer, done);
}

void SnapshotFetcher::cancel() {
    scoped_refptr<NodeImpl> node_ptr = NodeManager::GetInstance()->get(_node_id.group_id,
                                                                       _node_id.peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        return;
    }
    node->cancel_fetch_snapshot();
}

}
