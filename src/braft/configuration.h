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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Ge,Jun(gejun@baidu.com)

#ifndef BRAFT_RAFT_CONFIGURATION_H
#define BRAFT_RAFT_CONFIGURATION_H

#include <string>
#include <ostream>
#include <vector>
#include <set>
#include <map>
#include <butil/strings/string_piece.h>
#include <butil/endpoint.h>
#include <butil/logging.h>

namespace braft {

typedef std::string GroupId;

// Represent a participant in a replicating group.
struct PeerId {
    butil::EndPoint addr; // ip+port.
    int idx; // idx in same addr, default 0

    PeerId() : idx(0) {}
    explicit PeerId(butil::EndPoint addr_) : addr(addr_), idx(0) {}
    PeerId(butil::EndPoint addr_, int idx_) : addr(addr_), idx(idx_) {}
    /*intended implicit*/PeerId(const std::string& str) 
    { CHECK_EQ(0, parse(str)); }
    PeerId(const PeerId& id) : addr(id.addr), idx(id.idx) {}

    void reset() {
        addr.ip = butil::IP_ANY;
        addr.port = 0;
        idx = 0;
    }

    bool is_empty() const {
        return (addr.ip == butil::IP_ANY && addr.port == 0 && idx == 0);
    }

    int parse(const std::string& str) {
        reset();
        char ip_str[64];
        if (2 > sscanf(str.c_str(), "%[^:]%*[:]%d%*[:]%d", ip_str, &addr.port, &idx)) {
            reset();
            return -1;
        }
        if (0 != butil::str2ip(ip_str, &addr.ip)) {
            reset();
            return -1;
        }
        return 0;
    }

    std::string to_string() const {
        char str[128];
        snprintf(str, sizeof(str), "%s:%d", butil::endpoint2str(addr).c_str(), idx);
        return std::string(str);
    }
};

inline bool operator<(const PeerId& id1, const PeerId& id2) {
    if (id1.addr < id2.addr) {
        return true;
    } else {
        return id1.addr == id2.addr && id1.idx < id2.idx;
    }
}

inline bool operator==(const PeerId& id1, const PeerId& id2) {
    return (id1.addr == id2.addr && id1.idx == id2.idx);
}

inline bool operator!=(const PeerId& id1, const PeerId& id2) {
    return (id1.addr != id2.addr || id1.idx != id2.idx);
}

inline std::ostream& operator << (std::ostream& os, const PeerId& id) {
    return os << id.addr << ':' << id.idx;
}

struct NodeId {
    GroupId group_id;
    PeerId peer_id;

    NodeId(const GroupId& group_id_, const PeerId& peer_id_)
        : group_id(group_id_), peer_id(peer_id_) {
    }
    std::string to_string() const;
};

inline bool operator<(const NodeId& id1, const NodeId& id2) {
    const int rc = id1.group_id.compare(id2.group_id);
    if (rc < 0) {
        return true;
    } else {
        return rc == 0 && id1.peer_id < id2.peer_id;
    }
}

inline bool operator==(const NodeId& id1, const NodeId& id2) {
    return (id1.group_id == id2.group_id && id1.peer_id == id2.peer_id);
}

inline bool operator!=(const NodeId& id1, const NodeId& id2) {
    return (id1.group_id != id2.group_id || id1.peer_id != id2.peer_id);
}

inline std::ostream& operator << (std::ostream& os, const NodeId& id) {
    return os << id.group_id << ':' << id.peer_id;
}

inline std::string NodeId::to_string() const {
    std::ostringstream oss;
    oss << *this;
    return oss.str();
}

// A set of peers.
class Configuration {
public:
    typedef std::set<PeerId>::const_iterator const_iterator;
    // Construct an empty configuration.
    Configuration() {}

    // Construct from peers stored in std::vector.
    explicit Configuration(const std::vector<PeerId>& peers) {
        for (size_t i = 0; i < peers.size(); i++) {
            _peers.insert(peers[i]);
        }
    }

    // Construct from peers stored in std::set
    explicit Configuration(const std::set<PeerId>& peers) : _peers(peers) {}

    // Assign from peers stored in std::vector
    void operator=(const std::vector<PeerId>& peers) {
        _peers.clear();
        for (size_t i = 0; i < peers.size(); i++) {
            _peers.insert(peers[i]);
        }
    }

    // Assign from peers stored in std::set
    void operator=(const std::set<PeerId>& peers) {
        _peers = peers;
    }

    // Remove all peers.
    void reset() { _peers.clear(); }

    bool empty() const { return _peers.empty(); }
    size_t size() const { return _peers.size(); }

    const_iterator begin() const { return _peers.begin(); }
    const_iterator end() const { return _peers.end(); }

    // Clear the container and put peers in. 
    void list_peers(std::set<PeerId>* peers) const {
        peers->clear();
        *peers = _peers;
    }
    void list_peers(std::vector<PeerId>* peers) const {
        peers->clear();
        peers->reserve(_peers.size());
        std::set<PeerId>::iterator it;
        for (it = _peers.begin(); it != _peers.end(); ++it) {
            peers->push_back(*it);
        }
    }

    void append_peers(std::set<PeerId>* peers) {
        peers->insert(_peers.begin(), _peers.end());
    }

    // Add a peer.
    // Returns true if the peer is newly added.
    bool add_peer(const PeerId& peer) {
        return _peers.insert(peer).second;
    }

    // Remove a peer.
    // Returns true if the peer is removed.
    bool remove_peer(const PeerId& peer) {
        return _peers.erase(peer);
    }

    // True if the peer exists.
    bool contains(const PeerId& peer_id) const {
        return _peers.find(peer_id) != _peers.end();
    }

    // True if ALL peers exist.
    bool contains(const std::vector<PeerId>& peers) const {
        for (size_t i = 0; i < peers.size(); i++) {
            if (_peers.find(peers[i]) == _peers.end()) {
                return false;
            }
        }
        return true;
    }

    // True if peers are same.
    bool equals(const std::vector<PeerId>& peers) const {
        std::set<PeerId> peer_set;
        for (size_t i = 0; i < peers.size(); i++) {
            if (_peers.find(peers[i]) == _peers.end()) {
                return false;
            }
            peer_set.insert(peers[i]);
        }
        return peer_set.size() == _peers.size();
    }

    bool equals(const Configuration& rhs) const {
        if (size() != rhs.size()) {
            return false;
        }
        // The cost of the following routine is O(nlogn), which is not the best
        // approach.
        for (const_iterator iter = begin(); iter != end(); ++iter) {
            if (!rhs.contains(*iter)) {
                return false;
            }
        }
        return true;
    }
    
    // Get the difference between |*this| and |rhs|
    // |included| would be assigned to |*this| - |rhs|
    // |excluded| would be assigned to |rhs| - |*this|
    void diffs(const Configuration& rhs,
               Configuration* included,
               Configuration* excluded) const {
        *included = *this;
        *excluded = rhs;
        for (std::set<PeerId>::const_iterator 
                iter = _peers.begin(); iter != _peers.end(); ++iter) {
            excluded->_peers.erase(*iter);
        }
        for (std::set<PeerId>::const_iterator 
                iter = rhs._peers.begin(); iter != rhs._peers.end(); ++iter) {
            included->_peers.erase(*iter);
        }
    }

    // Parse Configuration from a string into |this|
    // Returns 0 on success, -1 otherwise
    int parse_from(butil::StringPiece conf);
    
private:
    std::set<PeerId> _peers;

};

std::ostream& operator<<(std::ostream& os, const Configuration& a);

}  //  namespace braft

#endif //~BRAFT_RAFT_CONFIGURATION_H
