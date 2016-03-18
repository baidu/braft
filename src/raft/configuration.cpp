// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/28 18:38:28

#include "raft/configuration.h"
#include <base/logging.h>

namespace raft {

std::ostream& operator<<(std::ostream& os, const Configuration& a) {
    os << "Configuration{";
    std::vector<PeerId> peers;
    a.list_peers(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        os << peers[i];
        if (i < peers.size() - 1) {
            os << ",";
        }
    }
    os << "}";
    return os;
}

}

