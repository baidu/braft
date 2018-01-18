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

#include "braft/configuration.h"
#include <butil/logging.h>
#include <butil/string_splitter.h>

namespace braft {

std::ostream& operator<<(std::ostream& os, const Configuration& a) {
    std::vector<PeerId> peers;
    a.list_peers(&peers);
    for (size_t i = 0; i < peers.size(); i++) {
        os << peers[i];
        if (i < peers.size() - 1) {
            os << ",";
        }
    }
    return os;
}

int Configuration::parse_from(butil::StringPiece conf) {
    reset();
    std::string peer_str;
    for (butil::StringSplitter sp(conf.begin(), conf.end(), ','); sp; ++sp) {
        braft::PeerId peer;
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
