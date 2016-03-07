// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
// Date: 2015/10/23 14:07:17

#include <net/if.h>
#include <sys/ioctl.h>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <base/comlog_sink.h>
#include <raft/util.h>
#include "counter_service.h"
#include "cli_service.h"
#include "counter.h"

DEFINE_string(ip_and_port, "0.0.0.0:8000", "server listen address");
DEFINE_string(name, "test", "Counter Name");
DEFINE_string(peers, "", "cluster peer set");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    options.async = true;
    options.process_name = "counter_server";
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
    // init counter
    counter::Counter* counter = new counter::Counter(FLAGS_name, raft::PeerId(addr, 0));
    raft::NodeOptions node_options;
    node_options.election_timeout = 5000;
    node_options.fsm = counter;
    node_options.conf = raft::Configuration(peers); // bootstrap need
    node_options.snapshot_interval = 30;
    node_options.log_uri = "file://./data/log";
    node_options.stable_uri = "file://./data/stable";
    node_options.snapshot_uri = "file://./data/snapshot";

    if (0 != counter->init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(NOTICE) << "init Node success";

    counter::CounterServiceImpl counter_service_impl(counter);
    if (0 != server.AddService(&counter_service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }
    example::CliServiceImpl cli_service_impl(counter);
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

