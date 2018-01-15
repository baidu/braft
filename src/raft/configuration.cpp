// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/09/28 18:38:28

#include "raft/configuration.h"
#include <base/logging.h>
#include <base/string_splitter.h>

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

int Configuration::parse_from(base::StringPiece conf) {
    reset();
    std::string peer_str;
    for (base::StringSplitter sp(conf.begin(), conf.end(), ','); sp; ++sp) {
        raft::PeerId peer;
        peer_str.assign(sp.field(), sp.length());
        if (peer.parse(peer_str) != 0) {
            LOG(ERROR) << "Fail to parse " << peer_str;
            return -1;
        }
        add_peer(peer);
    }
    return 0;
}

}

