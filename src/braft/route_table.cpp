// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen (chenzhangyi01@baidu.com)

#include "braft/route_table.h"

#include <gflags/gflags.h>
#include <butil/memory/singleton.h>
#include <butil/containers/doubly_buffered_data.h>
#include <butil/containers/flat_map.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include "braft/cli.pb.h"

namespace braft {
namespace rtb {

DEFINE_int32(initial_route_table_cap, 128, "Initial capacity of RouteTable");

class RouteTable {
DISALLOW_COPY_AND_ASSIGN(RouteTable);
public:
    static RouteTable* GetInstance() {
        return Singleton<RouteTable>::get();
    }
    void update_conf(const GroupId& group, const Configuration& conf) {
        _map.Modify(modify_conf, group, conf);
    }
    void update_leader(const GroupId& group, const PeerId& leader_id) {
        _map.Modify(modify_leader, group, leader_id);
    }
    int select_leader(const GroupId& group, PeerId* leader_id) {
        DbMap::ScopedPtr ptr;
        if (_map.Read(&ptr) != 0) {
            return -1;
        }
        GroupConf* gc = ptr->seek(group);
        if (!gc) {
            return -1;
        }
        if (gc->leader.is_empty()) {
            return 1;
        }
        *leader_id = gc->leader;
        return 0;
    }

    int list_conf(const GroupId& group, Configuration* conf) {
        DbMap::ScopedPtr ptr;
        if (_map.Read(&ptr) != 0) {
            return -1;
        }
        GroupConf* gc = ptr->seek(group);
        if (!gc) {
            return -1;
        }
        *conf = gc->conf;
        return 0;
    }

    int remove_group(const GroupId& group) {
        const size_t nremoved = _map.Modify(delete_group, group);
        if (nremoved == 0) {
            return -1;
        }
        return 0;
    }

private:
friend struct DefaultSingletonTraits<RouteTable>;
    RouteTable() {
        _map.Modify(init);
    }
    ~RouteTable() {}

    struct GroupConf {
        PeerId leader;
        Configuration conf;
    };

    typedef butil::FlatMap<GroupId, GroupConf> GroupMap;
    typedef butil::DoublyBufferedData<GroupMap> DbMap;

    static size_t modify_conf(GroupMap& m, const GroupId& group,
                              const Configuration& conf) {
        GroupConf& gc = m[group];
        gc.conf = conf;
        if (!gc.leader.is_empty() && !gc.conf.contains(gc.leader)) {
            gc.leader.reset();
        }
        return 1;
    }

    static size_t modify_leader(GroupMap& m, const GroupId& group,
                                const PeerId& leader_id) {
        GroupConf* gc = m.seek(group);
        if (gc == NULL) {
            return 0;
        }
        if (gc->leader == leader_id) {
            return 0;
        }
        gc->leader = leader_id;
        return 1;
    }

    static size_t delete_group(GroupMap& m, const GroupId& group) {
        GroupConf* gc = m.seek(group);
        if (gc != NULL) {
            return (size_t)m.erase(group);;
        }
        return 0;
    }

    static size_t init(GroupMap& m) {
        CHECK_EQ(0, m.init(FLAGS_initial_route_table_cap));
        return 1;
    }

    DbMap _map;
};

int update_configuration(const GroupId& group, const Configuration& conf) {
    if (conf.empty()) {
        return -1;
    }
    RouteTable* const rtb = RouteTable::GetInstance();
    rtb->update_conf(group, conf);
    return 0;
}

int update_configuration(const GroupId& group, const std::string& conf_str) {
    Configuration conf;
    if (conf.parse_from(conf_str) != 0) {
        return -1;
    }
    return update_configuration(group, conf);
}

int update_leader(const GroupId& group, const PeerId& leader_id) {
    RouteTable* const rtb = RouteTable::GetInstance();
    rtb->update_leader(group, leader_id);
    return 0;
}

int update_leader(const GroupId& group, const std::string& leader_str) {
    PeerId leader_id;
    if (!leader_str.empty() && leader_id.parse(leader_str) != 0) {
        return -1;
    }
    return update_leader(group, leader_id);
}

butil::Status refresh_leader(const GroupId& group, int timeout_ms) {
    RouteTable* const rtb = RouteTable::GetInstance();
    Configuration conf;
    if (rtb->list_conf(group, &conf) != 0) {
        return butil::Status(ENOENT, "group %s is not reistered in RouteTable",
                                    group.c_str());
    }
    butil::Status error;
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        brpc::Channel channel;
        if (channel.Init(iter->addr, NULL) != 0) {
            if (error.ok()) {
                error.set_error(-1, "Fail to init channel to %s",
                                    iter->to_string().c_str());
            } else {
                std::string saved_et = error.error_str();
                error.set_error(-1, "%s, Fail to init channel to %s",
                                         saved_et.c_str(),
                                         iter->to_string().c_str());
            }
        }
        brpc::Controller cntl;
        cntl.set_timeout_ms(timeout_ms);
        GetLeaderRequest request;
        request.set_group_id(group);
        GetLeaderResponse respones;
        CliService_Stub stub(&channel);
        stub.get_leader(&cntl, &request, &respones, NULL);
        if (!cntl.Failed()) {
            update_leader(group, respones.leader_id());
            return butil::Status::OK();
        }
        if (error.ok()) {
            error.set_error(cntl.ErrorCode(), "[%s] %s",
                                              iter->to_string().c_str(),
                                              cntl.ErrorText().c_str());
        } else {
            std::string saved_et = error.error_str();
            error.set_error(cntl.ErrorCode(), "%s, [%s] %s",
                                              saved_et.c_str(),
                                              iter->to_string().c_str(),
                                              cntl.ErrorText().c_str());
        }
    }
    return error;
}

int select_leader(const GroupId& group, PeerId* leader) {
    RouteTable* const rtb = RouteTable::GetInstance();
    return rtb->select_leader(group, leader);
}

int remove_group(const GroupId& group) {
    RouteTable* const rtb = RouteTable::GetInstance();
    return rtb->remove_group(group);
}

}  // namespace rtb
}  // namespace braft
