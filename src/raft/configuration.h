/*
 * =====================================================================================
 *
 *       Filename:  configuration.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/09/28 17:34:22
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_RAFT_CONFIGURATION_H
#define PUBLIC_RAFT_RAFT_CONFIGURATION_H

#include <base/endpoint.h>
#include <string>
#include <ostream>
#include <vector>
#include <set>
#include <map>

namespace raft {

typedef std::string GroupId;
typedef int ReplicaId;
struct PeerId {
    base::EndPoint addr; // addr
    int idx; // idx in same addr, default 0

    PeerId() : idx(0) {}
    PeerId(base::EndPoint addr_) : addr(addr_), idx(0) {}
    PeerId(base::EndPoint addr_, int idx_) : addr(addr_), idx(idx_) {}
    PeerId(const std::string& str) {
        parse(str);
    }
    PeerId(const PeerId& id) {
        addr = id.addr;
        idx = id.idx;
    }

    void reset() {
        addr.ip = base::IP_ANY;
        addr.port = 0;
        idx = 0;
    }

    bool is_empty() const {
        return (addr.ip == base::IP_ANY && addr.port == 0 && idx == 0);
    }

    int parse(const std::string& str) {
        reset();
        char ip_str[64];
        //
        //char port_str[16];
        //char idx_str[64];
        //if (3 != sscanf(str.c_str(), "%[^:]:%[^:]:%[^:]s", ip_str, port_str, idx_str)) {
        //    return -1;
        //}
        //if (0 != base::str2ip(ip_str, &addr.ip)) {
        //    return -1;
        //}
        //addr.port = atoi(port_str);
        //idx = atoi(idx_str);
        //
        if (2 > sscanf(str.c_str(), "%[^:]%*[:]%d%*[:]%d", ip_str, &addr.port, &idx)) {
            reset();
            return -1;
        }
        if (0 != base::str2ip(ip_str, &addr.ip)) {
            reset();
            return -1;
        }
        return 0;
    }

    std::string to_string() const {
        char str[128];
        snprintf(str, sizeof(str), "%s:%d", base::endpoint2str(addr).c_str(), idx);
        return std::string(str);
    }
};

inline bool operator<(const PeerId& id1, const PeerId& id2) {
    if (id1.addr < id2.addr) {
        return true;
    } else if (id1.addr == id2.addr && id1.idx < id2.idx) {
        return true;
    } else {
        return false;
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
};

inline bool operator<(const NodeId& id1, const NodeId& id2) {
    if (id1.group_id < id1.group_id) {
        return true;
    } else if (id1.group_id == id2.group_id && id1.peer_id < id2.peer_id) {
        return true;
    } else {
        return false;
    }
}

inline bool operator==(const NodeId& id1, const NodeId& id2) {
    return (id1.group_id == id2.group_id && id1.peer_id == id2.peer_id);
}

inline bool operator!=(const NodeId& id1, const NodeId& id2) {
    return (id1.group_id != id2.group_id || id1.peer_id != id2.peer_id);
}

inline std::ostream& operator << (std::ostream& os, const NodeId& id) {
    char str[128];
    snprintf(str, sizeof(str), "%s:%s:%d", id.group_id.c_str(),
             base::endpoint2str(id.peer_id.addr).c_str(), id.peer_id.idx);
    os << str;
    return os;
}

class Configuration {
public:
    Configuration() {}
    Configuration(const std::vector<PeerId>& peers) {
        _peers.clear();
        for (size_t i = 0; i < peers.size(); i++) {
            _peers.insert(peers[i]);
        }
    }
    Configuration(const Configuration& config) {
        _peers.clear();
        config.peer_set(&_peers);
    }

    void reset() {
        _peers.clear();
    }
    bool empty() const {
        return _peers.size() == 0;
    }
    void peer_set(std::set<PeerId>* peers) const {
        *peers = _peers;
    }
    void peer_vector(std::vector<PeerId>* peers) const {
        std::set<PeerId>::iterator it;
        for (it = _peers.begin(); it != _peers.end(); ++it) {
            peers->push_back(*it);
        }
    }
    void set_peer(const std::vector<PeerId>& peers) {
        _peers.clear();
        for (size_t i = 0; i < peers.size(); i++) {
            _peers.insert(peers[i]);
        }
    }
    void add_peer(const PeerId& peer) {
        _peers.insert(peer);
    }
    void remove_peer(const PeerId& peer) {
        _peers.erase(peer);
    }

    bool contain(const PeerId& peer_id) {
        return _peers.find(peer_id) != _peers.end();
    }
    bool contain(const std::vector<PeerId>& peers) {
        for (size_t i = 0; i < peers.size(); i++) {
            if (_peers.find(peers[i]) == _peers.end()) {
                return false;
            }
        }
        return true;
    }
    bool equal(const std::vector<PeerId>& peers) {
        if (_peers.size() != peers.size()) {
            return false;
        }
        return contain(peers);
    }
    size_t quorum() {
        return _peers.size() / 2 + 1;
    }
private:
    std::set<PeerId> _peers;

};
std::ostream& operator<<(std::ostream& os, const Configuration& a);

typedef std::pair<int64_t, Configuration> ConfigurationPair;
class ConfigurationManager {
public:
    ConfigurationManager() {
        _snapshot = std::pair<int64_t, Configuration>(0, Configuration());
    }
    virtual ~ConfigurationManager() {}

    // add new configuration at index
    void add(const int64_t index, const Configuration& config);

    // [1, first_index_kept) are being discarded
    void truncate_prefix(const int64_t first_index_kept);

    // (last_index_kept, infinity) are being discarded
    void truncate_suffix(const int64_t last_index_kept);

    void set_snapshot(const int64_t index, const Configuration& config);

    ConfigurationPair get_configuration(const int64_t last_included_index);

    int64_t last_configuration_index();

    ConfigurationPair last_configuration();

private:

    typedef std::map<int64_t, Configuration> ConfigurationMap;
    ConfigurationMap _configurations;
    ConfigurationPair _snapshot;
};

}

#endif //~PUBLIC_RAFT_RAFT_CONFIGURATION_H
