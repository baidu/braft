/*
 * =====================================================================================
 *
 *       Filename:  counter_client.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月30日 15时14分10秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <stdint.h>
#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include "counter.pb.h"
#include "raft/raft.h"
#include "raft/util.h"

#include <pb_to_json.h>

DEFINE_string(peers, "", "current cluster peer set");
DEFINE_string(shutdown, "", "shutdown peer");
DEFINE_string(snapshot, "", "snapshot peer");
DEFINE_string(stats, "", "stats peer");
DEFINE_string(setpeer, "", "setpeer peer");
DEFINE_string(new_peers, "", "new cluster peer set");
DEFINE_int64(fetch_and_add_num, 0, "fetch_and_add num, 0 disable, -1 always");

static int64_t rand_int64() {
    int64_t first = raft::get_random_number(0, INT32_MAX);
    int64_t second = raft::get_random_number(0, INT32_MAX);
    return (first << 32) | second;
}

int stats(base::EndPoint addr) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        counter::CounterService_Stub stub(&channel);
        counter::StatsRequest request;
        counter::StatsResponse response;
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

int snapshot(base::EndPoint addr) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        counter::CounterService_Stub stub(&channel);
        counter::SnapshotRequest request;
        counter::SnapshotResponse response;
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

int shutdown(base::EndPoint addr) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        counter::CounterService_Stub stub(&channel);
        counter::ShutdownRequest request;
        counter::ShutdownResponse response;
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

int set_peer(base::EndPoint addr, const std::vector<raft::PeerId>& old_peers,
             const std::vector<raft::PeerId>& new_peers) {
    baidu::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1;
    baidu::rpc::Channel channel;

    if (channel.Init(addr, &channel_opt) != 0) {
        LOG(ERROR) << "channel init failed, " << addr;
        return -1;
    }

    while (true) {
        baidu::rpc::Controller cntl;
        counter::CounterService_Stub stub(&channel);
        counter::SetPeerRequest request;
        for (size_t i = 0; i < old_peers.size(); i++) {
            request.add_old_peers(old_peers[i].to_string());
        }
        for (size_t i = 0; i < new_peers.size(); i++) {
            request.add_new_peers(new_peers[i].to_string());
        }
        counter::SetPeerResponse response;
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

int add_or_remove_peer(const std::vector<raft::PeerId>& old_peers,
             const std::vector<raft::PeerId>& new_peers) {
    int rr_index = 0;
    base::EndPoint leader_addr;
    leader_addr = old_peers[rr_index++%old_peers.size()].addr;

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
        counter::CounterService_Stub stub(&channel);
        counter::SetPeerRequest request;
        for (size_t i = 0; i < old_peers.size(); i++) {
            request.add_old_peers(old_peers[i].to_string());
        }
        for (size_t i = 0; i < new_peers.size(); i++) {
            request.add_new_peers(new_peers[i].to_string());
        }
        counter::SetPeerResponse response;
        stub.set_peer(&cntl, &request, &response, NULL);

        std::string operation = old_peers.size() < new_peers.size() ? "add_peer" : "remove_peer";
        if (cntl.Failed()) {
            LOG(ERROR) << operation << " failed, error: " << cntl.ErrorText();
            leader_addr = old_peers[rr_index++%old_peers.size()].addr;
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
    return 0;
}

int fetch_and_add(const std::vector<raft::PeerId>& peers) {
    int rr_index = 0;
    base::EndPoint leader_addr;
    leader_addr = peers[rr_index++%peers.size()].addr;
    for (int64_t i = 0; FLAGS_fetch_and_add_num == -1 || i < FLAGS_fetch_and_add_num; ) {
        baidu::rpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = -1;
        baidu::rpc::Channel channel;

        if (channel.Init(leader_addr, &channel_opt) != 0) {
            continue;
        }
        baidu::rpc::Controller cntl;
        counter::CounterService_Stub stub(&channel);
        counter::FetchAndAddRequest request;
        request.set_req_id(rand_int64());
        request.set_value(1);
        counter::FetchAndAddResponse response;
        stub.fetch_and_add(&cntl, &request, &response, NULL);

        if (cntl.Failed()) {
            usleep(100);
            leader_addr = peers[rr_index++%peers.size()].addr;
            LOG(WARNING) << "fetch_and_add error: " << cntl.ErrorText();
            continue;
        }

        if (!response.success()) {
            LOG(WARNING) << "fetch_and_add failed, redirect to " << response.leader();
            base::str2endpoint(response.leader().c_str(), &leader_addr);
        } else {
            LOG(NOTICE) << "fetch_and_add success to " << leader_addr
                << " index: " << response.index() << " ret: " << response.value();
            i++;
        }
    }
    return 0;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // stats
    if (!FLAGS_stats.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_stats.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_stats.c_str(), &addr)) {
            return stats(addr);
        } else {
            LOG(ERROR) << "stats flags bad format: " << FLAGS_stats;
            return -1;
        }
    }

    // snapshot
    if (!FLAGS_snapshot.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_snapshot.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_snapshot.c_str(), &addr)) {
            return snapshot(addr);
        } else {
            LOG(ERROR) << "snapshot flags bad format: " << FLAGS_snapshot;
            return -1;
        }
    }

    // shutdown
    if (!FLAGS_shutdown.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_shutdown.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_shutdown.c_str(), &addr)) {
            return shutdown(addr);
        } else {
            LOG(ERROR) << "shutdown flags bad format: " << FLAGS_shutdown;
            return -1;
        }
    }

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }
    if (peers.size() == 0) {
        LOG(ERROR) << "need set peers flags";
        return -1;
    }

    std::vector<raft::PeerId> new_peers;
    if (!FLAGS_new_peers.empty()) {
        const char* the_string_to_split = FLAGS_new_peers.c_str();
        for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
            raft::PeerId peer(std::string(s.field(), s.length()));
            new_peers.push_back(peer);
        }
    }

    // set peer
    if (!FLAGS_setpeer.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_setpeer.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_setpeer.c_str(), &addr)) {
            return set_peer(addr, peers, new_peers);
        } else {
            LOG(ERROR) << "setpeer flags bad format: " << FLAGS_setpeer;
            return -1;
        }
    }

    // fetch_and_add
    if (FLAGS_fetch_and_add_num != 0) {
        return fetch_and_add(peers);
    }

    // add_peer/remove_peer
    if (new_peers.size() == peers.size() + 1) {
        return add_or_remove_peer(peers, new_peers);
    } else if (new_peers.size() == peers.size() - 1) {
        return add_or_remove_peer(peers, new_peers);
    } else {
        LOG(ERROR) << "flags peers and new_peers bad format, peer: " << FLAGS_peers
            << " new_peers: " << FLAGS_new_peers;
        return -1;
    }

    return 0;
}

