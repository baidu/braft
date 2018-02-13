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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include <map>                  // std::map
#include <gflags/gflags.h>      // google::ParseCommandLineFlags
#include <butil/string_printf.h>
#include <braft/cli.h>          // raft::cli::*

namespace braft {
namespace cli {

DEFINE_int32(timeout_ms, -1, "Timeout (in milliseconds) of the operation");
DEFINE_int32(max_retry, 3, "Max retry times of each operation");
DEFINE_string(conf, "", "Current configuration of the raft group");
DEFINE_string(peer, "", "Id of the operating peer");
DEFINE_string(new_peers, "", "Peers that the group is going to consists of");
DEFINE_string(group, "", "Id of the raft group");

#define CHECK_FLAG(flagname)                                            \
    do {                                                                \
        if ((FLAGS_ ## flagname).empty()) {                             \
            LOG(ERROR) << __FUNCTION__ << " requires --" # flagname ;   \
            return -1;                                                  \
        }                                                               \
    } while (0);                                                        \

int add_peer() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId new_peer;
    if (new_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = add_peer(FLAGS_group, conf, new_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to add_peer : " << st;
        return -1;
    }
    return 0;
}

int remove_peer() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId removing_peer;
    if (removing_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = remove_peer(FLAGS_group, conf, removing_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to remove_peer : " << st;
        return -1;
    }
    return 0;
}

int change_peers() {
    CHECK_FLAG(new_peers);
    CHECK_FLAG(conf);
    CHECK_FLAG(group);
    Configuration new_peers;
    if (new_peers.parse_from(FLAGS_new_peers) != 0) {
        LOG(ERROR) << "Fail to parse --new_peers=`" << FLAGS_new_peers << '\'';
        return -1;
    }
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = change_peers(FLAGS_group, conf, new_peers, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to change_peers : " << st;
        return -1;
    }
    return 0;
}

int reset_peer() {
    CHECK_FLAG(new_peers);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration new_peers;
    if (new_peers.parse_from(FLAGS_new_peers) != 0) {
        LOG(ERROR) << "Fail to parse --new_peers=`" << FLAGS_new_peers << '\'';
        return -1;
    }
    PeerId target_peer;
    if (target_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = reset_peer(FLAGS_group, target_peer, new_peers, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to reset_peer : " << st;
        return -1;
    }
    return 0;
}

int snapshot() {
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    PeerId target_peer;
    if (target_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = snapshot(FLAGS_group, target_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to make snapshot : " << st;
        return -1;
    }
    return 0;
}

int transfer_leader() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId target_peer;
    if (target_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = transfer_leader(FLAGS_group, conf, target_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to transfer_leader: " << st;
        return -1;
    }
    return 0;
}

int run_command(const std::string& cmd) {
    if (cmd == "add_peer") {
        return add_peer();
    }
    if (cmd == "remove_peer") {
        return remove_peer();
    }
    if (cmd == "change_peers") {
        return change_peers();
    }
    if (cmd == "reset_peer") {
        return reset_peer();
    }
    if (cmd == "snapshot") {
        return snapshot();
    }
    if (cmd == "transfer_leader") {
        return transfer_leader();
    }
    LOG(ERROR) << "Unknown command `" << cmd << '\'';
    return -1;
}

}  // namespace cli
}  // namespace raft

int main(int argc , char* argv[]) {
    const char* proc_name = strrchr(argv[0], '/');
    if (proc_name == NULL) {
        proc_name = argv[0];
    } else {
        ++proc_name;
    }
    std::string help_str;
    butil::string_printf(&help_str,
                        "Usage: %s [Command] [OPTIONS...]\n"
                        "Command:\n"
                        "  add_peer --group=$group_id "
                                    "--peer=$adding_peer --conf=$current_conf\n"
                        "  remove_peer --group=$group_id "
                                      "--peer=$removing_peer --conf=$current_conf\n"
                        "  change_peers --group=$group_id "
                                       "--conf=$current_conf --new_peers=$new_peers\n"
                        "  reset_peer --group=$group_id "
                                     "--peer==$target_peer --new_peers=$new_peers\n"
                        "  snapshot --group=$group_id --peer=$target_peer\n"
                        "  transfer_leader --group=$group_id --peer=$target_leader --conf=$current_conf\n",
                        proc_name);
    GFLAGS_NS::SetUsageMessage(help_str);
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        std::cerr << help_str;
        return -1;
    }
    return braft::cli::run_command(argv[1]);
}
