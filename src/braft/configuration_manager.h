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

#ifndef  BRAFT_CONFIGURATION_MANAGER_H
#define  BRAFT_CONFIGURATION_MANAGER_H

#include "braft/configuration.h"         // Configuration
#include "braft/log_entry.h"             // LogId

namespace braft {

struct ConfigurationEntry {
    LogId id;
    Configuration conf;
    Configuration old_conf;

    ConfigurationEntry() {}
    ConfigurationEntry(const LogEntry& entry) {
        id = entry.id;
        conf = *(entry.peers);
        if (entry.old_peers) {
            old_conf = *(entry.old_peers);
        }
    }

    bool stable() const { return old_conf.empty(); }
    bool empty() const { return conf.empty(); }
    void list_peers(std::set<PeerId>* peers) {
        peers->clear();
        conf.append_peers(peers);
        old_conf.append_peers(peers);
    }
    bool contains(const PeerId& peer) const
    { return conf.contains(peer) || old_conf.contains(peer); }
};

// Manager the history of configuration changing
class ConfigurationManager {
public:
    ConfigurationManager() {}
    ~ConfigurationManager() {}

    // add new configuration at index
    int add(const ConfigurationEntry& entry);

    // [1, first_index_kept) are being discarded
    void truncate_prefix(int64_t first_index_kept);

    // (last_index_kept, infinity) are being discarded
    void truncate_suffix(int64_t last_index_kept);

    void set_snapshot(const ConfigurationEntry& snapshot);

    void get(int64_t last_included_index, ConfigurationEntry* entry);

    const ConfigurationEntry& last_configuration() const;

private:

    std::deque<ConfigurationEntry> _configurations;
    ConfigurationEntry _snapshot;
};

}  //  namespace braft

#endif  //BRAFT_CONFIGURATION_MANAGER_H
