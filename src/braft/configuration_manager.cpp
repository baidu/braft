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

#include "braft/configuration_manager.h"

namespace braft {

int ConfigurationManager::add(const ConfigurationEntry& entry) {
    if (!_configurations.empty()) {
        if (_configurations.back().id.index >= entry.id.index) {
            CHECK(false) << "Did you forget to call truncate_suffix before "
                            " the last log index goes back";
            return -1;
        }
    }
    _configurations.push_back(entry);
    return 0;
}

void ConfigurationManager::truncate_prefix(const int64_t first_index_kept) {
    while (!_configurations.empty()
            && _configurations.front().id.index < first_index_kept) {
        _configurations.pop_front();
    }
}

void ConfigurationManager::truncate_suffix(const int64_t last_index_kept) {
    while (!_configurations.empty()
        && _configurations.back().id.index > last_index_kept) {
        _configurations.pop_back();
    }
}

void ConfigurationManager::set_snapshot(const ConfigurationEntry& entry) {
    CHECK_GE(entry.id, _snapshot.id);
    _snapshot = entry;
}

void ConfigurationManager::get(int64_t last_included_index,
                               ConfigurationEntry* conf) {
    if (_configurations.empty()) {
        CHECK_GE(last_included_index, _snapshot.id.index);
        *conf = _snapshot;
        return;
    }
    std::deque<ConfigurationEntry>::iterator it;
    for (it = _configurations.begin(); it != _configurations.end(); ++it) {
        if (it->id.index > last_included_index) {
            break;
        }
    }
    if (it == _configurations.begin()) {
        *conf = _snapshot;
        return;
    }
    --it;
    *conf = *it;
}

const ConfigurationEntry& ConfigurationManager::last_configuration() const {
    if (!_configurations.empty()) {
        return _configurations.back();
    }
    return _snapshot;
}

}  //  namespace braft
