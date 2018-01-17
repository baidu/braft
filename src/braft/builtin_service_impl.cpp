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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Ge,Jun(gejun@baiud.com)

#include "braft/builtin_service_impl.h"

#include <brpc/controller.h>
#include <brpc/closure_guard.h>
#include <brpc/http_status_code.h>
#include <brpc/builtin/common.h>
#include "braft/node.h"
#include "braft/replicator.h"
#include "braft/node_manager.h"

namespace braft {

void RaftStatImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
    brpc::TabInfo* info = info_list->add();
    info->tab_name = "raft";
    info->path = "/raft_stat";
}

void RaftStatImpl::default_method(::google::protobuf::RpcController* controller,
                              const ::braft::IndexRequest* /*request*/,
                              ::braft::IndexResponse* /*response*/,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    std::string group_id = cntl->http_request().unresolved_path();
    std::vector<scoped_refptr<NodeImpl> > nodes;
    NodeManager* nm = NodeManager::GetInstance();
    if (group_id.empty()) {
        nm->get_all_nodes(&nodes);
    } else {
        nm->get_nodes_by_group_id(group_id, &nodes);
    }
    const bool html = brpc::UseHTML(cntl->http_request());
    if (html) {
        cntl->http_response().set_content_type("text/html");
    } else {
        cntl->http_response().set_content_type("text/plain");
    }
    butil::IOBufBuilder os;
    if (html) {
        os << "<!DOCTYPE html><html><head>\n"
           << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
           << brpc::TabsHead() << "</head><body>";
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

}  //  namespace braft
