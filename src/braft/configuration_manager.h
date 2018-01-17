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

#ifndef  PUBLIC_RAFT_CONFIGURATION_MANAGER_H
#define  PUBLIC_RAFT_CONFIGURATION_MANAGER_H

#include "braft/configuration.h"         // Configuration
#include "braft/log_entry.h"             // LogId

namespace braft {

typedef std::pair<LogId, Configuration> ConfigurationPair;

// Manager the history of configuration changing
class ConfigurationManager {
public:
    ConfigurationManager() {
        _snapshot = ConfigurationPair(LogId(), Configuration());
    }
    virtual ~ConfigurationManager() {}

    // add new configuration at index
    int add(const LogId& id, const Configuration& config);

    // [1, first_index_kept) are being discarded
    void truncate_prefix(const int64_t first_index_kept);

    // (last_index_kept, infinity) are being discarded
    void truncate_suffix(const int64_t last_index_kept);

    void set_snapshot(const LogId& id, const Configuration& config);

    void get_configuration(int64_t last_included_index, ConfigurationPair* conf);

    const ConfigurationPair& last_configuration() const;

private:

    std::deque<ConfigurationPair> _configurations;
    ConfigurationPair _snapshot;
};

}  //  namespace braft

#endif  //PUBLIC_RAFT_CONFIGURATION_MANAGER_H
