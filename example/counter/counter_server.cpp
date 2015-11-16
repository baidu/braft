/*
 * =====================================================================================
 *
 *       Filename:  main.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月23日 17时00分00秒
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
#include "counter_service.h"
#include "counter.h"
#include "raft/util.h"

DEFINE_int32(port, 8000, "TCP Port of CounterServer");
DEFINE_string(name, "test", "Counter Name");
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
    options.process_name = "counter_server";
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());

#if 0
    int ret = com_loadlog(".", "comlog.conf");
    if (ret != 0) {
        fprintf(stderr, "com_loadlog failed\n");
    }
#endif

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (baidu::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }

    // init raft
    if (0 != raft::init_raft(NULL)) {
        LOG(FATAL) << "Fail to init raft";
        return -1;
    }

    // init counter
    counter::Counter* counter = new counter::Counter(FLAGS_name, 0);
    raft::NodeOptions node_options;
    node_options.election_timeout = 5000;
    node_options.fsm = counter;
    node_options.conf = raft::Configuration(peers);
    node_options.log_uri = "file://./data/log";
    node_options.snapshot_uri = "file://./data/snapshot";
    node_options.stable_uri = "file://./data/stable";
    if (0 != counter->init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(NOTICE) << "init Node at " << counter->self();

    // add service
    counter::CounterServiceImpl service_impl(counter);
    baidu::rpc::Server server;
    if (0 != server.AddService(&service_impl, baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }

    // server start
    baidu::rpc::ServerOptions server_options;
    if (0 != server.Start(FLAGS_port, &server_options)) {
        LOG(FATAL) << "Fail to Start";
        return -1;
    }

    signal(SIGINT, sigint_handler);
    while (!g_signal_quit) {
        sleep(1);
    }

    server.Stop(200);
    server.Join();

    //TODO: counter shutdown?

    return 0;
}

