/*
 * =====================================================================================
 *
 *       Filename:  common_cli.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/30 14:28:55
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <vector>
#include <baidu/rpc/channel.h>
#include <baidu/rpc/controller.h>
#include <pb_to_json.h>
#include "cli.pb.h"
#include "cli.h"

namespace example {

int CommonCli::stats(const base::EndPoint addr) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        CliService_Stub stub(&channel);
        StatsRequest request;
        StatsResponse response;
        stub.stats(&cntl, &request, &response, NULL);

        if (!cntl.Failed()) {
            Pb2JsonOptions options;
            options.pretty_json = true;
            std::string json_str;
            ProtoMessageToJson(response, &json_str, options, NULL);
            LOG(NOTICE) << "stats: \n" << json_str;
            break;
        } else {
            LOG(ERROR) << "stats failed, error: " << cntl.ErrorText();
            sleep(1);
            continue;
        }
    }
    return 0;
}

int CommonCli::snapshot(const base::EndPoint addr) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        CliService_Stub stub(&channel);
        SnapshotRequest request;
        SnapshotResponse response;
        stub.snapshot(&cntl, &request, &response, NULL);

        if (!cntl.Failed()) {
            LOG(NOTICE) << "snapshot " << addr << " "
                << (response.success() ? "success" : "failed");
            break;
        } else {
            LOG(ERROR) << "snapshot failed, error: " << cntl.ErrorText();
            sleep(1);
            continue;
        }
    }
    return 0;
}

int CommonCli::shutdown(const base::EndPoint addr) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        CliService_Stub stub(&channel);
        ShutdownRequest request;
        ShutdownResponse response;
        stub.shutdown(&cntl, &request, &response, NULL);

        if (!cntl.Failed()) {
            LOG(NOTICE) << "shutdown " << addr << " "
                << (response.success() ? "success" : "failed");
            break;
        } else {
            LOG(ERROR) << "shutdown failed, error: " << cntl.ErrorText();
            sleep(1);
            continue;
        }
    }
    return 0;
}

int CommonCli::add_peer(const base::EndPoint addr) {
    std::vector<raft::PeerId> new_peers(_peers);
    new_peers.push_back(raft::PeerId(addr));
    return set_peer(new_peers);
}

int CommonCli::remove_peer(const base::EndPoint addr) {
    std::vector<raft::PeerId> new_peers;
    for (size_t i = 0; i < _peers.size(); i++) {
        if (_peers[i].addr != addr) {
            new_peers.push_back(_peers[i]);
        }
    }
    return set_peer(new_peers);
}

int CommonCli::set_peer(const std::vector<raft::PeerId>& new_peers) {
    int rr_index = 0;
    base::EndPoint leader_addr = _peers[rr_index++ % _peers.size()].addr;

    // prepare request and response
    SetPeerRequest request;
    for (size_t i = 0; i < _peers.size(); i++) {
        request.add_old_peers(_peers[i].to_string());
    }
    for (size_t i = 0; i < new_peers.size(); i++) {
        request.add_new_peers(new_peers[i].to_string());
    }
    SetPeerResponse response;

    // rpc
    while (true) {
        baidu::rpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = -1;
        baidu::rpc::Channel channel;

        if (channel.Init(leader_addr, &channel_opt) != 0) {
            LOG(ERROR) << "channel init failed, " << leader_addr;
            sleep(1);
            continue;
        }

        baidu::rpc::Controller cntl;
        CliService_Stub stub(&channel);
        stub.set_peer(&cntl, &request, &response, NULL);

        std::string operation = _peers.size() < new_peers.size() ? "add_peer" : "remove_peer";
        if (cntl.Failed()) {
            LOG(ERROR) << operation << " failed, error: " << cntl.ErrorText();
            leader_addr = _peers[rr_index++ % _peers.size()].addr;
            sleep(1);
            continue;
        }

        if (!response.success()) {
            LOG(WARNING) << operation << " failed, redirect to " << response.leader();
            base::str2endpoint(response.leader().c_str(), &leader_addr);
            sleep(1);
            continue;
        } else {
            LOG(NOTICE) << operation << " success";
            break;
        }
    }

    _peers.assign(new_peers.begin(), new_peers.end());
    return 0;
}

int CommonCli::set_peer(const base::EndPoint addr,
                        const std::vector<raft::PeerId>& old_peers,
                        const std::vector<raft::PeerId>& new_peers) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    // prepare request and response
    SetPeerRequest request;
    request.set_force(true);
    for (size_t i = 0; i < old_peers.size(); i++) {
        request.add_old_peers(old_peers[i].to_string());
    }
    for (size_t i = 0; i < new_peers.size(); i++) {
        request.add_new_peers(new_peers[i].to_string());
    }
    SetPeerResponse response;

    // rpc
    while (true) {
        baidu::rpc::Controller cntl;
        CliService_Stub stub(&channel);
        stub.set_peer(&cntl, &request, &response, NULL);

        if (!cntl.Failed()) {
            LOG(NOTICE) << "set_peer " << addr << " "
                << (response.success() ? "success" : "failed");
            break;
        } else {
            LOG(ERROR) << "set_peer failed, error: " << cntl.ErrorText();
            sleep(1);
            continue;
        }
    }

    return 0;
}

}
