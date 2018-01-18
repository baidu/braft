// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen (chenzhangyi01@baidu.com)

#ifndef  BRAFT_ROUTE_TABLE_H
#define  BRAFT_ROUTE_TABLE_H

#include "braft/configuration.h"                 // Configuration
#include "braft/raft.h"

// Maintain routes to raft groups

namespace braft {
namespace rtb {

// Update configuration of group in route table
int update_configuration(const GroupId& group, const Configuration& conf);
int update_configuration(const GroupId& group, const std::string& conf_str);

// Get the cached leader of group.
// Returns:
//  0 : success
//  1 : Not sure about the leader
//  -1, otherwise
int select_leader(const GroupId& group, PeerId* leader);

// Update leader
int update_leader(const GroupId& group, const PeerId& leader);
int update_leader(const GroupId& group, const std::string& leader_str);

// Blocking the thread until query_leader finishes
butil::Status refresh_leader(const GroupId& group, int timeout_ms);

// Remove this group from route table
int remove_group(const GroupId& group);

}  // namespace rtb
}  // namespace braft 

#endif  //BRAFT_ROUTE_TABLE_H
