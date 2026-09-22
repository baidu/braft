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

#ifndef BRAFT_RAFT_SNAPSHOT_FETCHER_H
#define BRAFT_RAFT_SNAPSHOT_FETCHER_H

#include "braft/storage.h"

namespace braft {

//snapshot fetcher for fetching remote snapshot.
class SnapshotFetcher {
public:
    SnapshotFetcher(const NodeId& node_id) : _node_id(node_id), _max_snapshot_index(INT64_MAX) {}
    virtual ~SnapshotFetcher() {}

    // @brief Fetch remote snapshot from Learner.
    // This method should be used in on_snapshot_save function and it can be
    // used in async mode.
    //
    // @param peer_id is the learner's address.
    // @param writer is the same as the writer in on_snapshot_writer.
    // @param done is user defined function, confirm and accept the fetched snapshot.
    void fetch_snapshot(const PeerId& peer_id, SnapshotWriter* writer, Closure* done);

    // Cancel the fetching snapshot task.
    void cancel();

    // Set the max snapshot index for fetching.
    void set_max_snapshot_index(const int64_t max_snapshot_index) {
        _max_snapshot_index = max_snapshot_index;
    }

private:
    NodeId _node_id;
    int64_t _max_snapshot_index;
};

} // namespace braft

#endif
