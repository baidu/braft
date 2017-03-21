// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
//         YangWu(yangwu@baidu.com)
// Date: 2017/02/21 13:45:46

#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include <base/logging.h>
#include "db.pb.h"
#include "cli.pb.h"
#include "raft/raft.h"
#include "raft/util.h"
#include "cli.h"

DEFINE_int32(timeout_ms, 3000, "Timeout for each request");
DEFINE_string(cluster_ns, "", "Name service for the cluster, list://demo.com:1234,1.2.3.4:1234");
DEFINE_string(peer, "", "add/remove peer");
DEFINE_int32(log_level, 1, "min log level");
DEFINE_string(db_op, "get", "db op: get/put");
DEFINE_string(key, "test", "get key or put prefix key");
DEFINE_int32(put_num, 10, "put value");

namespace example {

class DbClient : public example::CommonCli {
public:
    DbClient(const std::vector<raft::PeerId>& peers) : CommonCli(peers) {
        reset_leader();
        int ret = _cluster.Init(FLAGS_cluster_ns.c_str(), "rr", NULL);
        CHECK_EQ(0, ret) << "cluster channel init failed, cluster: " << FLAGS_cluster_ns;
    }

    ~DbClient() {}

    void get(const std::string &key, std::string& value) {
        CHECK(!key.empty());
        while (true) {
            get_leader();

            // get request
            DbService_Stub stub(&_channel);
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            GetRequest request;
            request.set_key(key);
            GetResponse response;

            stub.get(&cntl, &request, &response, NULL);
            if (!cntl.Failed()) {
                value = response.value();
                LOG(NOTICE) << "get success, key: " << key << " value: " << response.value();
                break;
            } else {
                LOG(WARNING) << "rpc error, key: " << key
                    << " err_code:" << cntl.ErrorCode()
                    << ", err_msg:" << cntl.ErrorText();
                reset_leader();
                get_leader();
            }
        }
    }

    void put(const std::string& key, const std::string& value) {
        CHECK(!key.empty());
        while (true) {
            get_leader();

            DbService_Stub stub(&_channel);
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            PutRequest request;
            PutResponse response;
            request.set_key(key);
            request.set_value(key);

            stub.put(&cntl, &request, &response, NULL);
            if (!cntl.Failed()) {
                break;
                LOG(NOTICE) << "put success, key: " << request.key() << " value: " << request.value();
            } else {
                LOG(WARNING) << "put failed, key: " << request.key()
                    << ", err_code:" << cntl.ErrorCode()
                    << ", err_msg:" << cntl.ErrorText();
                // 尝试重新获取leader 
                reset_leader();
                get_leader();
            }
        }
    }

private:
    void reset_leader() {
        std::lock_guard<bthread::Mutex> lock_guard(_mutex);
        _leader.ip = base::IP_ANY;
        _leader.port = 0;
    }

    int get_leader() {
        {
            std::lock_guard<bthread::Mutex> lock_guard(_mutex);
            if (_leader.ip != base::IP_ANY && _leader.port != 0) {
                return 0;
            }
        }

        GetLeaderRequest request;
        GetLeaderResponse response;
        do {
            CliService_Stub stub(&_cluster);
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.leader(&cntl, &request, &response, NULL);
            if (cntl.Failed() || !response.success()) {
                if (cntl.Failed()) {
                    LOG(WARNING) << "cntl failed, msg:" << cntl.ErrorText();
                }
                if (!response.success()) {
                    LOG(WARNING) << "not success";
                }
                continue;
            }
            {
                std::lock_guard<bthread::Mutex> lock_guard(_mutex);
                base::str2endpoint(response.leader_addr().c_str(), &_leader);
            }
        } while (_leader.ip == base::IP_ANY || _leader.port == 0);

        if (_channel.Init(_leader, NULL) != 0) {
            LOG(WARNING) << "leader_channel init failed.";
            return EINVAL;
        }
        LOG(NOTICE) << "get_leader and init channel success, leader addr:"
            << response.leader_addr();
        return 0;
    }

    baidu::rpc::Channel _cluster;
    baidu::rpc::Channel _channel;
    base::EndPoint _leader;
    bthread::Mutex _mutex;
};

}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    logging::SetMinLogLevel(FLAGS_log_level);

    // parse cluster
    if (FLAGS_cluster_ns.length() == 0) {
        LOG(ERROR) << "db_client need set peers";
        return -1;
    }
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_cluster_ns.c_str() + strlen("list://");
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }
    if (peers.size() == 0) {
        LOG(ERROR) << "db_client need set cluster_ns";
        return -1;
    }

    example::DbClient client(peers);
    if (FLAGS_db_op.compare("get") == 0 && !FLAGS_key.empty()) {
        std::string value;
        client.get(FLAGS_key, value);
    } else if (FLAGS_db_op.compare("put") == 0 && !FLAGS_key.empty()) {
        for (int i = 0; i < FLAGS_put_num; i++) {
            std::string key(FLAGS_key);
            std::string value;
            base::string_appendf(&key, "_%d", i);
            base::string_printf(&value, "%d", i);
            client.put(key, value);
        }
    } else {
        LOG(ERROR) << "unexpected command " << FLAGS_db_op;
        return -1;
    }

    return 0;
}


