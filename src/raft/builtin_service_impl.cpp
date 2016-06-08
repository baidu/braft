// libraft - Quorum-based replication of states accross machines.
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
#include "raft/node_manager.h"

namespace raft {

void RaftStatImpl::GetTabInfo(baidu::rpc::TabInfoList* info_list) const {
    baidu::rpc::TabInfo* info = info_list->add();
    info->tab_name = "raft";
    info->path = "/raft_stat";
}

void RaftStatImpl::default_method(::google::protobuf::RpcController* controller,
                              const ::raft::IndexRequest* /*request*/,
                              ::raft::IndexResponse* /*response*/,
                              ::google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
    std::string group_id = cntl->http_request().unresolved_path();
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
    base::IOBufBuilder os;
    if (html) {
        os << "<!DOCTYPE html><html><head>\n"
           << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
           << baidu::rpc::TabsHead() << "</head><body>";
        cntl->server()->PrintTabsBody(os, "raft");
    }
    if (nodes.empty()) {
        if (html) {
            os << "</body></html>";
        }
        os.move_to(cntl->response_attachment());
        return;
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
        nodes[i]->describe(os, html);
        os << newline;
    }
    if (html) {
        os << "</body></html>";
    }
    os.move_to(cntl->response_attachment());
}

}  // namespace raft
