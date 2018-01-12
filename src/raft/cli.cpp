// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2018/01/09 10:35:36

#include "raft/cli.h"

#include <baidu/rpc/channel.h>          // baidu::rpc::Channel
#include <baidu/rpc/controller.h>       // baidu::rpc::Controller
#include "raft/cli.pb.h"                // CliService_Stub
#include "raft/util.h"

namespace raft {
namespace cli {

static base::Status get_leader(const GroupId& group_id, const Configuration& conf,
                        PeerId* leader_id) {
    if (conf.empty()) {
        return base::Status(EINVAL, "Empty group configuration");
    }
    // Construct a brpc naming service to access all the nodes in this group
    std::string group_ns = "list://";
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        group_ns.append(base::endpoint2str(iter->addr).c_str());
        group_ns.push_back(',');
    }

    baidu::rpc::Channel channel;
    if (channel.Init(group_ns.c_str(), "rr", NULL) != 0) {
        return base::Status(-1, "Fail to init channel to %s", group_ns.c_str());
    }
    base::Status st(-1, "Fail to get leader of group %s", group_id.c_str());
    leader_id->reset();
    CliService_Stub stub(&channel);
    for (int i = 0; i < (int)conf.size() / 2 + 1; ++i) {
        GetLeaderRequest request;
        GetLeaderResponse response;
        baidu::rpc::Controller cntl;
        request.set_group_id(group_id);
        stub.get_leader(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            std::string saved_et = st.error_str();
            st.set_error(cntl.ErrorCode(), "%s, [%s] %s",  saved_et.c_str(),
                        base::endpoint2str(cntl.remote_side()).c_str(),
                        cntl.ErrorText().c_str());
            continue;
        }
        leader_id->parse(response.leader_id());
    }
    if (leader_id->is_empty()) {
        return st;
    }
    return base::Status::OK();
}

base::Status add_peer(const GroupId& group_id, const Configuration& conf,
                      const PeerId& peer_id, const CliOptions& options) {
    PeerId leader_id;
    base::Status st = get_leader(group_id, conf, &leader_id);
    RAFT_RETURN_IF(!st.ok(), st);
    baidu::rpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    AddPeerRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    request.set_peer_id(peer_id.to_string());
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        request.add_old_peers(iter->to_string());
    }
    AddPeerResponse response;
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.add_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return base::Status::OK();
}

base::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                         const PeerId& peer_id, const CliOptions& options) {
    PeerId leader_id;
    base::Status st = get_leader(group_id, conf, &leader_id);
    RAFT_RETURN_IF(!st.ok(), st);
    baidu::rpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    RemovePeerRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    request.set_peer_id(peer_id.to_string());
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        request.add_old_peers(iter->to_string());
    }
    RemovePeerResponse response;
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.remove_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return base::Status::OK();
}

base::Status set_peer(const GroupId& group_id, const PeerId& peer_id,
                      const Configuration& new_conf, const CliOptions& options) {
    if (new_conf.empty()) {
        return base::Status(EINVAL, "new_conf is empty");
    }
    baidu::rpc::Channel channel;
    if (channel.Init(peer_id.addr, NULL) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                peer_id.to_string().c_str());
    }
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SetPeerRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    for (Configuration::const_iterator
            iter = new_conf.begin(); iter != new_conf.end(); ++iter) {
        request.add_new_peers(iter->to_string());
    }
    SetPeerResponse response;
    CliService_Stub stub(&channel);
    stub.set_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return base::Status::OK();
}

base::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                      const CliOptions& options) {
    baidu::rpc::Channel channel;
    if (channel.Init(peer_id.addr, NULL) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                peer_id.to_string().c_str());
    }
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SnapshotRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    SnapshotResponse response;
    CliService_Stub stub(&channel);
    stub.snapshot(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return base::Status::OK();
}

}  // namespace cli
}  // namespace raft
