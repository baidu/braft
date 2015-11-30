/*
 * =====================================================================================
 *
 *       Filename:  common_cli.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/30 14:27:28
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_COMMON_CLI_H
#define PUBLIC_RAFT_EXAMPLE_COMMON_CLI_H

#include <base/endpoint.h>
#include "raft/raft.h"

namespace example {

class CommonCli {
public:
    CommonCli(const std::vector<raft::PeerId>& peers) : _peers(peers) {}
    virtual ~CommonCli() {}

    // single node operation
    static int stats(const base::EndPoint addr);
    static int snapshot(const base::EndPoint addr);
    static int shutdown(const base::EndPoint addr);
    static int set_peer(const base::EndPoint addr,
                        const std::vector<raft::PeerId>& old_peers,
                        const std::vector<raft::PeerId>& new_peers);

    // cluster operation
    int add_peer(const base::EndPoint addr);
    int remove_peer(const base::EndPoint addr);
    int set_peer(const std::vector<raft::PeerId>& new_peers);
protected:
    std::vector<raft::PeerId> _peers;
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_COMMON_CLI_H
