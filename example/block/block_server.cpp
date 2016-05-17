/*
 * =====================================================================================
 *
 *       Filename:  block_server.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/30 00:15:44
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#include <net/if.h>
#include <sys/ioctl.h>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <base/comlog_sink.h>
#include "block_service.h"
#include "cli_service.h"
#include "block.h"
#include "raft/util.h"

DEFINE_string(ip_and_port, "0.0.0.0:8000", "server listen address");
DEFINE_string(name, "test", "Block Name");
DEFINE_string(peers, "", "cluster peer set");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    options.async = true;
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
    //if (logging::ComlogSink::GetInstance()->SetupFromConfig("./comlog.conf") != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());

    // add service
    baidu::rpc::Server server;
    // init raft and server
    if (0 != raft::add_service(&server, FLAGS_ip_and_port.c_str())) {
        LOG(FATAL) << "Fail to init raft";
        return -1;
    }

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }

    base::EndPoint addr;
    base::str2endpoint(FLAGS_ip_and_port.c_str(), &addr);
    if (base::IP_ANY == addr.ip) {
        addr.ip = base::get_host_ip();
    }
    // init block
    block::Block* block = new block::Block(FLAGS_name, raft::PeerId(addr, 0));
    raft::NodeOptions node_options;
    node_options.election_timeout_ms = 5000;
    node_options.fsm = block;
    node_options.initial_conf = raft::Configuration(peers); // bootstrap need
    node_options.snapshot_interval_s = 30;
    node_options.log_uri = "local://./data/log";
    node_options.stable_uri = "local://./data/stable";
    node_options.snapshot_uri = "local://./data/snapshot";

    if (0 != block->init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(NOTICE) << "init Node success";

    block::BlockServiceImpl block_service_impl(block);
    if (0 != server.AddService(&block_service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }
    example::CliServiceImpl cli_service_impl(block);
    if (0 != server.AddService(&cli_service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }

    if (server.Start(FLAGS_ip_and_port.c_str(), NULL) != 0) {
        LOG(FATAL) << "Fail to start server";
        return -1;
    }

    server.RunUntilAskedToQuit();

    return 0;
}

