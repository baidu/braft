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

bool g_signal_quit = false;
static void sigint_handler(int) {
    g_signal_quit = true;
}

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
    block::BlockServiceImpl block_service_impl(NULL);
    if (0 != server.AddService(&block_service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }
    example::CliServiceImpl cli_service_impl(NULL);
    if (0 != server.AddService(&cli_service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }

    // init raft and server
    baidu::rpc::ServerOptions server_options;
    if (0 != raft::start_raft(FLAGS_ip_and_port.c_str(), &server, &server_options)) {
        LOG(FATAL) << "Fail to init raft";
        return -1;
    }

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (baidu::StringSplitter s(the_string_to_split, ','); s; ++s) {
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
    node_options.election_timeout = 5000;
    node_options.fsm = block;
    node_options.conf = raft::Configuration(peers); // bootstrap need
    //node_options.snapshot_interval = 30;
    node_options.log_uri = "file://./data/log";
    node_options.stable_uri = "file://./data/stable";
    node_options.snapshot_uri = "file://./data/snapshot";

    if (0 != block->init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(NOTICE) << "init Node success";

    block_service_impl.set_block(block);
    cli_service_impl.set_state_machine(block);

    signal(SIGINT, sigint_handler);
    while (!g_signal_quit) {
        sleep(1);
    }

    raft::stop_raft(FLAGS_ip_and_port.c_str(), NULL);
    server.Stop(200);
    server.Join();

    //TODO: block shutdown?

    return 0;
}

