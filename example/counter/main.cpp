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
#include <gflags/gflags.h>
#include <base/logging.h>
#include "counter_service.h"
#include "counter.h"

DEFINE_int32(port, 8000, "TCP Port of CounterServer");
DEFINE_string(name, "test", "Counter Name");
DEFINE_string(peers, "", "cluster peer set");

bool g_signal_quit = false;
static void sigint_handler(int) {
    g_signal_quit = true;
}

namespace base {

uint32_t get_host_ip_by_interface(const char* interface) {
    int sockfd = -1;
    struct ::ifreq req;
    uint32_t ip = 0;
    if ((sockfd = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
        return 0;
    }

    memset(&req, 0, sizeof(struct ::ifreq));
    sprintf(req.ifr_name, "%s", interface);

    if (!ioctl(sockfd, SIOCGIFADDR, (char*)&req)) {
        struct in_addr ip_addr;
        ip_addr.s_addr = *((int*) &req.ifr_addr.sa_data[2]);
        ip = ip_addr.s_addr;
    }
    close(sockfd);
    return int2ip(ip);
}

ip_t get_host_ip() {
    const char* interfaces[] = { "xgbe0", "xgbe1", "eth1", "eth0", "bond0", "br-ex" };
    uint32_t ip = 0;
    ip_t ip = { IP_ANY };

    for (size_t i = 0; i < 6; ++i) {
        ip = get_host_ip_by_interface(interfaces[i]);
        if (INADDR_ANY != ip.s_addr) {
            break;
        }
    }

    if (INADDR_ANY == ip.s_addr) {
        LOG(FATAL) << "can not get a valid ip";
    }

    return ip;
}

}


int main(int argc, const char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_jns.c_str();
    for (baidu::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }

    // init counter
    counter::Counter counter;
    base::EndPoint addr;
    addr.ip = base::get_host_ip();
    addr.port = FLAGS_port;
    Raft::NodeOptions node_options;
    node_options.user = &counter;
    node_options.conf = raft::Configuration(peers);
    if (0 != counter.init(FLAGS_name, raft::PeerId(addr), &options)) {
        LOG(FATAL) << "Fail to init";
        return -1;
    }

    // add service
    counter::CounterServiceImpl service_impl(&counter);
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

