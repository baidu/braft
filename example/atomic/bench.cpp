// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/01/14 13:45:32

#include <stdint.h>
#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include "atomic.pb.h"
#include "cli.pb.h"
#include "raft/raft.h"
#include "raft/util.h"
#include "cli.h"

DEFINE_int32(threads, 50, "Number of work threads");
DEFINE_int32(timeout_ms, 100, "Timeout for each request");
DEFINE_int32(num_requests, -1, "Quit after sending so many requests");
DEFINE_string(cluster_ns, "list", "Name service for the cluster");

volatile bool g_signal_quit = false;

void sigint_handler(int) { 
    g_signal_quit = true;
}

bvar::LatencyRecorder g_latency_recorder("atomic_client");
boost::atomic<int> g_nthreads(0);
boost::atomic<int64_t> nsent(0);

int get_leader(baidu::rpc::Channel* channel, base::EndPoint* leader_addr) {
    example::CliService_Stub stub(channel);
    example::GetLeaderRequest request;
    example::GetLeaderResponse response;
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
    // Each thread maintains a unique atomic
    const int id = g_nthreads.fetch_add(1);
    // Current value of the maintained atomic
    int64_t value = 0; 
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
        example::AtomicService_Stub stub(&leader);

        // Send request to leader until failure
        while (!g_signal_quit) {
            baidu::rpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            // Using CAS to increase value
            example::CompareExchangeRequest request;
            request.set_id(id);
            request.set_expected_value(value);
            request.set_new_value(value + 1);
            example::CompareExchangeResponse response;
            const int64_t start_time = base::cpuwide_time_us();
            stub.compare_exchange(&cntl, &request, &response, NULL);
            const int64_t end_time = base::cpuwide_time_us();
            const int64_t elp = end_time - start_time;
            if (!cntl.Failed()) {
                g_latency_recorder << elp;
                if (response.success()) {
                    if (value != response.old_value()) {
                        CHECK_EQ(value, response.old_value());
                        exit(-1);
                    }
                    ++value;
                } else if (value != response.old_value()) {
                    if (value == 0 || response.old_value() == value + 1) {
                        //   ^^^                          ^^^
                        // It's initalized value          ^^^
                        //                          There was false negative
                        value = response.old_value();
                    } else {
                        CHECK_EQ(value, response.old_value());
                        exit(-1);
                    }
                } else {
                    break;
                }
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
    while (!g_signal_quit) {
        sleep(1);
        LOG(INFO) << "Sending CompareExchangeRequest at qps=" << g_latency_recorder.qps(1)
                  << " latency=" << g_latency_recorder.latency(1);
    }
    LOG(INFO) << "AtomicClient is going to quit";
    for (int i = 0; i < FLAGS_threads; ++i) {
        bthread_join(threads[i], NULL);
    }
    return 0;
}

