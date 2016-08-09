// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/08/03 14:05:54
//
#include <stdint.h>
#include <fstream>
#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <bthread.h>
#include <baidu/rpc/channel.h>
#include <raft/raft.h>
#include "log.pb.h"

DEFINE_int32(threads, 50, "Number of work threads");
DEFINE_int32(timeout_ms, 100, "Timeout for each request");
DEFINE_int32(num_requests, -1, "Quit after sending so many requests");
DEFINE_string(cluster_ns, "list", "Name service for the cluster");
DEFINE_int32(log_size, 1024, "Size of each log");
DEFINE_string(output, "output.txt", "The output file of final statictics");
DEFINE_int32(runtime_s, 0, "Limit the running time if this is a positive number");

bvar::LatencyRecorder g_latency_recorder("log_client");
boost::atomic<int> g_nthreads(0);
boost::atomic<int64_t> nsent(0);
bvar::Adder<int64_t> g_throughput;
bvar::PerSecond<bvar::Adder<int64_t> > g_throughput_ps("block_client_tp", &g_throughput, 10);
volatile bool g_signal_quit = false;
bvar::detail::Percentile g_percentile;

void sigint_handler(int) { 
    g_signal_quit = true;
}

int get_leader(baidu::rpc::Channel* channel, base::EndPoint* leader_addr) {
    benchmark::LogService_Stub stub(channel);
    benchmark::GetLeaderRequest request;
    benchmark::GetLeaderResponse response;
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    stub.leader(&cntl, &request, &response, NULL);
    if (cntl.Failed() || !response.success()) {
        return -1;
    }
    return base::str2endpoint(response.leader_addr().c_str(), leader_addr);
}

static void* sender(void* arg) {
    baidu::rpc::Channel* cluster = (baidu::rpc::Channel*)arg;
    const long sleep_ms = 1000;
    // Current value of the maintained atomic
    base::IOBuf data;
    data.resize(FLAGS_log_size, 'a');
    bthread_usleep(base::fast_rand_less_than(10000));
    while (!g_signal_quit) {

        bthread_usleep(sleep_ms * 1000L);
        // Ask the cluster for the leader
        base::EndPoint leader_addr;
        if (get_leader(cluster, &leader_addr) != 0) {
            LOG(WARNING) << "Fail to get leader, sleep for " << sleep_ms << "ms";
            bthread_usleep(sleep_ms * 1000L);
            continue;
        }

        // Now we know who is the leader, construct a Channel directed to the
        // leader
        baidu::rpc::Channel leader;
        if (leader.Init(leader_addr, NULL) != 0) {
            LOG(ERROR) << "Fail to create channel to leader, sleep for "
                       << sleep_ms << "ms";
            bthread_usleep(sleep_ms * 1000L);
            continue;
        }
        benchmark::LogService_Stub stub(&leader);

        // Send request to leader until failure
        while (!g_signal_quit) {
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            // Using CAS to increase value
            benchmark::WriteRequest request;
            benchmark::WriteResponse response;
            cntl.request_attachment() = data;
            const int64_t start_time = base::cpuwide_time_us();
            stub.write(&cntl, &request, &response, NULL);
            const int64_t end_time = base::cpuwide_time_us();
            const int64_t elp = end_time - start_time;
            if (!cntl.Failed()) {
                g_latency_recorder << elp;
                g_throughput << data.size();
                g_percentile << elp;
                if (FLAGS_num_requests > 0 && 
                        nsent.fetch_add(1, boost::memory_order_relaxed) 
                            >= FLAGS_num_requests) {
                    g_signal_quit = true;
                }
            } else {
                LOG(WARNING) << "Fail to issue rpc, " << cntl.ErrorText();
                break;
            }
        }
    }
    return NULL;
}

void* sleep_and_quit(void* /*arg*/) {
    bthread_usleep(FLAGS_runtime_s * 1000L * 1000L);
    g_signal_quit = true;
    return NULL;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    signal(SIGINT, sigint_handler);
    baidu::rpc::Channel cluster;
    if (cluster.Init(FLAGS_cluster_ns.c_str(), "rr", NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to `" << FLAGS_cluster_ns << "'";
        return -1;
    }
    bthread_t threads[FLAGS_threads];
    for (int i = 0; i < FLAGS_threads; ++i) {
        if (bthread_start_background(&threads[i], NULL, sender, &cluster) != 0) {
            LOG(ERROR) << "Fail to create bthread";
            return -1;
        }
    }
    sleep(2);
    if (FLAGS_runtime_s > 0) {
        bthread_t tid;
        bthread_start_background(&tid, NULL, sleep_and_quit, NULL);
    }
    const long start_time_us = base::cpuwide_time_us();
    const int64_t count0 = g_latency_recorder.count();
    const int64_t tp0 = g_throughput.get_value();
    long end_time_us = 0;
    long tp1 = 0;
    long count1 = 0;
    while (!g_signal_quit) {
        count1 = g_latency_recorder.count();
        tp1 = g_throughput.get_value();
        end_time_us = base::cpuwide_time_us();
        LOG(INFO) << "Sendin WriteRequest at qps=" << g_latency_recorder.qps(1)
                  << " latency=" << g_latency_recorder.latency(1)
                  << " tp=" << g_throughput_ps.get_value(1) / 1024.0 / 1024.0 
                  << "MB/s";
        sleep(1);
    }
    const long elp = end_time_us - start_time_us;
    LOG(INFO) << "LogClient is going to quit";
    for (int i = 0; i < FLAGS_threads; ++i) {
        bthread_join(threads[i], NULL);
    }
    std::ostringstream oss;
    oss << "QPS=" << (count1 - count0) * 1000000.0 /  elp << '\n'
        << "TP=" << (tp1 - tp0) * 1000000.0 / elp / 1024.0 / 1024.0 << "MB/s\n";
    bvar::detail::Percentile::value_type cdf = g_percentile.get_value(); 
    for (int i = 1; i <= 99; ++i) {
        const double ratio = (double) i / 100.0;
        const int value = cdf.get_number(ratio);
        oss << i << '\t' << value << '\n';
    }
    oss << "99.9\t" << cdf.get_number(0.999) << '\n';
    oss << "99.95\t" << cdf.get_number(0.9995) << '\n';
    oss << "99.99\t" << cdf.get_number(0.9999) << '\n';
    LOG(INFO) << "Statistics: \n" << oss.str() << '\n';
    if (!FLAGS_output.empty()) {
        std::ofstream ofs(FLAGS_output.c_str());
        ofs << oss.str();
    }
    return 0;
}

