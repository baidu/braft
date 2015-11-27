/*
 * =====================================================================================
 *
 *       Filename:  configuration.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/28 18:38:28
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include "raft/configuration.h"
#include <base/logging.h>

namespace raft {

std::ostream& operator<<(std::ostream& os, const Configuration& a) {
    //std::copy(a.peers.begin(), a.peers.end(), std::ostream_iterator<PeerId>(os, ","));
    os << "Configuration{";
    std::vector<PeerId> peers;
    a.peer_vector(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        os << peers[i];
        if (i < peers.size() - 1) {
            os << ",";
        }
    }
    os << "}";
    return os;
}

void ConfigurationManager::add(const int64_t index, const Configuration& config) {
    _configurations.insert(std::pair<int64_t, Configuration>(index, config));
}

void ConfigurationManager::truncate_prefix(const int64_t first_index_kept) {
    _configurations.erase(_configurations.begin(), _configurations.lower_bound(first_index_kept));
}

void ConfigurationManager::truncate_suffix(const int64_t last_index_kept) {
    _configurations.erase(_configurations.upper_bound(last_index_kept), _configurations.end());
}

void ConfigurationManager::set_snapshot(const int64_t index, const Configuration& config) {
    CHECK(index >= _snapshot.first);
    _snapshot.first = index;
    _snapshot.second = config;
}

ConfigurationPair ConfigurationManager::get_configuration(
        const int64_t last_included_index) {
    if (_configurations.empty()) {
        //return std::pair<int64_t, Configuration>(0, Configuration());
        return _snapshot;
    }
    ConfigurationMap::iterator it = _configurations.upper_bound(last_included_index);
    if (it == _configurations.begin()) {
        //return std::pair<int64_t, Configuration>(0, Configuration());
        return _snapshot;
    }
    --it;
    return *it;
}

int64_t ConfigurationManager::last_configuration_index() {
    std::map<int64_t, Configuration>::reverse_iterator rit = _configurations.rbegin();
    if (rit != _configurations.rend()) {
        return rit->first;
    } else {
        return _snapshot.first;
    }
}

ConfigurationPair ConfigurationManager::last_configuration() {
    std::map<int64_t, Configuration>::reverse_iterator rit = _configurations.rbegin();
    if (rit != _configurations.rend()) {
        //return std::pair<int64_t, Configuration>(rit->first, rit->second);
        return *rit;
    } else {
        return _snapshot;
    }
}


}

