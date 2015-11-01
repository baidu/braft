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

#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include "counter.pb.h"
#include "raft/raft.h"

DEFINE_string(peers, "", "cluster peer set");
int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // init peers
    std::vector<base::EndPoint> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        base::EndPoint addr;
        base::str2endpoint(std::string(s.field(), s.length()).c_str(), &addr);
        peers.push_back(addr);
    }

    int rr_index = 0;
    base::EndPoint leader_addr;
    leader_addr = peers[rr_index++%peers.size()];
    for (int i = 0; i < 10; ) {
        baidu::rpc::ChannelOptions channel_opt;
        baidu::rpc::Channel channel;

        if (channel.Init(leader_addr, &channel_opt) != 0) {
            continue;
        }
        baidu::rpc::Controller cntl;
        counter::CounterService_Stub stub(&channel);
        counter::AddRequest request;
        request.set_value(1);
        counter::AddResponse response;
        stub.add(&cntl, &request, &response, NULL);

        if (cntl.Failed()) {
            usleep(100);
            leader_addr = peers[rr_index++%peers.size()];
            LOG(WARNING) << "add error: " << cntl.ErrorText();
            continue;
        }

        if (!response.success()) {
            LOG(WARNING) << "add failed, redirect to " << response.leader();
            base::str2endpoint(response.leader().c_str(), &leader_addr);
            leader_addr.port += 400;
        } else {
            LOG(NOTICE) << "add success to " << leader_addr;
            i++;
        }
    }
    return 0;
}
