// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/19 00:00:27

#include "raft/builtin_service_impl.h"

#include <baidu/rpc/controller.h>
#include <baidu/rpc/closure_guard.h>
#include <baidu/rpc/http_status_code.h>
#include <baidu/rpc/builtin/common.h>
#include "raft/node.h"
#include "raft/replicator.h"

namespace raft {

void RaftStatImpl::GetTabInfo(baidu::rpc::TabInfoList* info_list) const {
    baidu::rpc::TabInfo* info = info_list->add();
    info->tab_name = "raft";
    info->path = "/raft_stat";
}

void RaftStatImpl::default_method(::google::protobuf::RpcController* controller,
                              const ::raft::IndexRequest* request,
                              ::raft::IndexResponse* response,
                              ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    std::string group_id = cntl->http_request().method_path();
    std::vector<scoped_refptr<NodeImpl> > nodes;
    NodeManager* nm = NodeManager::GetInstance();
    if (group_id.empty()) {
        nm->get_all_nodes(&nodes);
    } else {
        nm->get_nodes_by_group_id(group_id, &nodes);
    }
    const bool html = baidu::rpc::UseHTML(cntl->http_request());
    if (html) {
        cntl->http_response().set_content_type("text/html");
    } else {
        cntl->http_response().set_content_type("text/plain");
    }
    if (nodes.empty()) {
        cntl->http_response().set_status_code(baidu::rpc::HTTP_STATUS_NO_CONTENT);
        return;
    }
    base::IOBufBuilder os;
    if (html) {
        os << "<!DOCTYPE html><html><head>\n"
           << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
           << baidu::rpc::TabsHead() << "</head><body>";
        cntl->server()->PrintTabsBody(os, "raft");
    }

    std::string prev_group_id;
    const char *newline = html ? "<br>" : "\r\n";
    for (size_t i = 0; i < nodes.size(); ++i) {
        const NodeId node_id = nodes[i]->node_id();
        group_id = node_id.group_id;
        if (group_id != prev_group_id) {
            if (html) {
                os << "<h1>" << group_id << "</h1>";
            } else {
                os << "[" << group_id << "]" << newline;
            }
            prev_group_id = group_id;
        }
        NodeStats stat = nodes[i]->stats();
        os << "state: " << state2str(stat.state) << newline;
        os << "term: " << stat.term << newline;
        os << "last_log_index: " << stat.last_log_index << newline;
        os << "last_log_term: " << stat.last_log_term << newline;
        os << "committed_index: " << stat.committed_index << newline;
        os << "applied_index: " << stat.applied_index << newline;
        os << "last_snapshot_index: " << stat.last_snapshot_index << newline;
        os << "last_snapshot_term: " << stat.last_snapshot_term << newline;
        std::vector<PeerId> peers;
        stat.configuration.peer_vector(&peers);
        os << "peers:";
        for (size_t j = 0; j < peers.size(); ++j) {
            if (peers[j] != node_id.peer_id) {  // Not list self
                os << ' ';
                if (html) {
                    os << "<a href=\"http://" << peers[j].addr 
                       << "/raft_stat/" << group_id << "\">";
                }
                os << peers[j];
                if (html) {
                    os << "</a>";
                }
            }
        }
        os << newline;  // newline for peers

        if (stat.state == FOLLOWER) {
            PeerId leader = nodes[i]->leader_id();
            os << "leader: ";
            if (html) {
                os << "<a href=\"http://" << leader.addr
                    << "/raft_stat/" << group_id << "\">"
                    << leader << "</a>";
            } else {
                os << leader;
            }
            os << newline;
        }
        nodes[i]->_log_manager->describe(os, html);
        nodes[i]->_fsm_caller->describe(os, html);
        // TODO: list state of replicators for leader
        // 
        os << newline;

    }
    if (html) {
        os << "</body></html>";
    }
    os.move_to(cntl->response_attachment());
}

}  // namespace raft
