// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
// Date: 2016/03/14 13:45:32

#include <stdint.h>
#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include "atomic.pb.h"
#include "cli.pb.h"
#include "raft/raft.h"
#include "raft/util.h"
#include "cli.h"

DEFINE_int32(timeout_ms, 3000, "Timeout for each request");
DEFINE_string(cluster_ns, "", "Name service for the cluster");
DEFINE_string(atomic_op, "get", "atomic op: get/set/cas");
DEFINE_int64(atomic_id, 0, "atomic id");
DEFINE_int64(atomic_val, 0, "atomic value");
DEFINE_int64(atomic_new_val, 0, "atomic new_value");

class AtomicClient {
public:
    AtomicClient() {
        reset_leader();
        int ret = _cluster.Init(FLAGS_cluster_ns.c_str(), "rr", NULL);
        CHECK_EQ(0, ret) << "cluster channel init failed, cluster: " << FLAGS_cluster_ns;
    }
    ~AtomicClient() {}

    int get(const int64_t id, int64_t* value) {
        int ret = 0;

        while (true) {
            get_leader();

            // init leader channel
            baidu::rpc::Channel channel;
            if (channel.Init(_leader, NULL) != 0) {
                LOG(WARNING) << "leader_channel init failed.";
                return EINVAL;
            }

            // get request
            example::AtomicService_Stub stub(&channel);
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            example::GetRequest request;
            example::GetResponse response;
            request.set_id(id);

            stub.get(&cntl, &request, &response, NULL);
            if (!cntl.Failed()) {
                if (response.success()) {
                    ret = 0;
                    *value = response.value();
                    LOG(INFO) << "atomic_get success, id: " << id << " value: " << *value;
                    break;
                } else {
                    ret = EINVAL;
                    LOG(INFO) << "atomic_get failed, id: " << id;
                    break;
                }
            } else if (cntl.ErrorCode() == baidu::rpc::SYS_ETIMEDOUT ||
                       cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT){
                ret = ETIMEDOUT;
                LOG(WARNING) << "atomic_get timedout, id: " << id;
                break;
            } else {
                // not leader, retry
                reset_leader();
                continue;
            }
        }

        return ret;
    }

    int set(const int64_t id, const int64_t value) {
        int ret = 0;

        while (true) {
            get_leader();

            // init leader channel
            baidu::rpc::Channel channel;
            if (channel.Init(_leader, NULL) != 0) {
                LOG(WARNING) << "leader_channel init failed.";
                return EINVAL;
            }

            // set request
            example::AtomicService_Stub stub(&channel);
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            example::SetRequest request;
            example::SetResponse response;
            request.set_id(id);
            request.set_value(value);

            stub.set(&cntl, &request, &response, NULL);
            if (!cntl.Failed()) {
                if (response.success()) {
                    ret = 0;
                    LOG(INFO) << "atomic_set success, id: " << id << " value: " << value;
                    break;
                } else {
                    ret = EINVAL;
                    LOG(WARNING) << "atomic_set failed, id: " << id << " value: " << value;
                    break;
                }
            } else if (cntl.ErrorCode() == baidu::rpc::SYS_ETIMEDOUT ||
                       cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT){
                ret = ETIMEDOUT;
                LOG(WARNING) << "atomic_set timedout, id: " << id << " value: " << value;
                break;
            } else {
                // not leader, retry
                reset_leader();
                continue;
            }
        }

        return ret;
    }

    int cas(const int64_t id, const int64_t old_value, const int64_t new_value) {
        int ret = 0;

        while (true) {
            get_leader();

            // init leader channel
            baidu::rpc::Channel channel;
            if (channel.Init(_leader, NULL) != 0) {
                LOG(WARNING) << "leader_channel init failed.";
                return EINVAL;
            }

            // cas request
            example::AtomicService_Stub stub(&channel);
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            example::CompareExchangeRequest request;
            example::CompareExchangeResponse response;
            request.set_id(id);
            request.set_expected_value(old_value);
            request.set_new_value(new_value);

            stub.compare_exchange(&cntl, &request, &response, NULL);
            if (!cntl.Failed()) {
                if (response.success()) {
                    CHECK_EQ(old_value, response.old_value());
                    ret = 0;
                    LOG(INFO) << "atomic_cas success, id: " << id
                        << " old: " << old_value << " new: " << new_value;
                    break;
                } else {
                    ret = EINVAL;
                    LOG(WARNING) << "atomic_cas failed, id: " << id
                        << " old: " << old_value << " new: " << new_value;
                    break;
                }
            } else if (cntl.ErrorCode() == baidu::rpc::SYS_ETIMEDOUT ||
                       cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT){
                ret = ETIMEDOUT;
                LOG(WARNING) << "atomic_cas timedout, id: " << id
                    << " old: " << old_value << " new: " << new_value;
                break;
            } else {
                // not leader, retry
                reset_leader();
                continue;
            }
        }

        return ret;
    }

private:
    int get_leader() {
        if (_leader.ip != base::IP_ANY && _leader.port != 0) {
            return 0;
        }

        do {
            example::CliService_Stub stub(&_cluster);
            example::GetLeaderRequest request;
            example::GetLeaderResponse response;
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.leader(&cntl, &request, &response, NULL);
            if (cntl.Failed() || !response.success()) {
                continue;
            }
            base::str2endpoint(response.leader_addr().c_str(), &_leader);
        } while (_leader.ip == base::IP_ANY || _leader.port == 0);
        return 0;
    }
    void reset_leader() {
        _leader.ip = base::IP_ANY;
        _leader.port = 0;
    }

    baidu::rpc::Channel _cluster;
    base::EndPoint _leader;
};

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_cluster_ns.length() == 0) {
        return -1;
    }

    AtomicClient client;

    if (FLAGS_atomic_op.compare("get") == 0) {
        int64_t value = 0;
        return client.get(FLAGS_atomic_id, &value);
    } else if (FLAGS_atomic_op.compare("set") == 0) {
        return client.set(FLAGS_atomic_id, FLAGS_atomic_val);
    } else if (FLAGS_atomic_op.compare("cas") == 0) {
        return client.cas(FLAGS_atomic_id, FLAGS_atomic_val, FLAGS_atomic_new_val);
    } else {
        return -1;
    }
}

