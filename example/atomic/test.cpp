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

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include "atomic.pb.h"

DEFINE_int32(timeout_ms, 3000, "Timeout for each request");
DEFINE_int64(atomic_id, 0, "atomic id");
DEFINE_int64(atomic_val, 0, "atomic value");
DEFINE_int64(atomic_new_val, 0, "atomic new_value");
DEFINE_string(atomic_op, "get", "atomic op: get/set/cas");
DEFINE_string(conf, "", "Configuration of the raft group");
DEFINE_string(group, "Atomic", "Id of the replication group");

int get(const int64_t id) {
    for (;;) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                        FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }
        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        // get request
        example::AtomicService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);

        example::GetRequest request;
        example::AtomicResponse response;
        request.set_id(id);
        stub.get(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << " : " << cntl.ErrorText();
            if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
                return ETIMEDOUT;
            }
            // Clear leadership since this RPC failed.
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        if (!response.success()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << ", redirecting to "
                         << (response.has_redirect() 
                                ? response.redirect() : "nowhere");
            // Update route table since we have redirect information
            braft::rtb::update_leader(FLAGS_group, response.redirect());
            continue;
        }
        // make jepsen parse output of get easily
        printf("%" PRId64"\n", response.old_value());
        break;
    }

    return 0;
}

int exchange(const int64_t id, const int64_t value) {
    for (;;) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                        FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }
        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        // get request
        example::AtomicService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);

        example::ExchangeRequest request;
        example::AtomicResponse response;
        request.set_id(id);
        request.set_value(value);
        stub.exchange(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << " : " << cntl.ErrorText();
            if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
                return ETIMEDOUT;
            }
            // Clear leadership since this RPC failed.
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        if (!response.success()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << ", redirecting to "
                         << (response.has_redirect() 
                                ? response.redirect() : "nowhere");
            // Update route table since we have redirect information
            braft::rtb::update_leader(FLAGS_group, response.redirect());
            continue;
        }
        // make jepsen parse output of get easily
        LOG(INFO) << "Exchange value of id=" << id
                  << " from old_value=" << response.old_value()
                  << " to new_value=" << response.new_value();
        break;
    }

    return 0;
}

int cas(const int64_t id, const int64_t old_value, const int64_t new_value) {
    for (;;) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                        FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        example::AtomicService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        example::CompareExchangeRequest request;
        example::AtomicResponse response;
        request.set_id(id);
        request.set_expected_value(old_value);
        request.set_new_value(new_value);

        stub.compare_exchange(&cntl, &request, &response, NULL);

        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << " : " << cntl.ErrorText();
            if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
                return ETIMEDOUT;
            }
            // Clear leadership since this RPC failed.
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }

        if (!response.success()) {
            // A redirect response
            if (!response.has_old_value()) {
                LOG(WARNING) << "Fail to send request to " << leader
                             << ", redirecting to "
                             << (response.has_redirect() 
                                    ? response.redirect() : "nowhere");
                // Update route table since we have redirect information
                braft::rtb::update_leader(FLAGS_group, response.redirect());
                continue;
            } else {
                return EIO;
            }
        };
        LOG(INFO) << "Received response from " << leader
                  << " old_value=" << response.old_value()
                  << " new_value=" << response.new_value()
                  << " latency=" << cntl.latency_us();
        break;
    }
    return 0;
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    if (FLAGS_atomic_op == "get") {
        return get(FLAGS_atomic_id);
    } else if (FLAGS_atomic_op == "set") {
        return exchange(FLAGS_atomic_id, FLAGS_atomic_val);
    } else if (FLAGS_atomic_op== "cas") {
        return cas(FLAGS_atomic_id, FLAGS_atomic_val, FLAGS_atomic_new_val);
    } else {
        LOG(ERROR) << "unexpected command " << FLAGS_atomic_op;
        return -1;
    }

    return 0;
}

