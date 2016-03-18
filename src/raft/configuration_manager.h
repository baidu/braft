// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/03/15 22:57:08

#ifndef  PUBLIC_RAFT_CONFIGURATION_MANAGER_H
#define  PUBLIC_RAFT_CONFIGURATION_MANAGER_H

#include "raft/configuration.h"         // Configuration
#include "raft/log_entry.h"             // LogId

namespace raft {

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

}  // namespace raft

#endif  //PUBLIC_RAFT_CONFIGURATION_MANAGER_H
