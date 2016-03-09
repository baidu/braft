// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/01/14 13:45:32

#include <stdint.h>
#include <algorithm>
#include <gflags/gflags.h>
#include <base/containers/flat_map.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include <baidu/rpc/errno.pb.h>
#include "raft/util.h"
#include "ebs.pb.h"

static const int64_t GB = 1024*1024*1024;

DEFINE_int32(threads, 1, "io threads");
DEFINE_int32(write_percent, 100, "write percent");
DEFINE_int64(block_size, 1 * GB, "Size of each block");
DEFINE_int64(block_num, 32, "Number of block");
DEFINE_string(cluster_ns, "list://", "Name service for the cluster");
DEFINE_int32(timeout_ms, 10000, "Timeout for each request");
DEFINE_int32(request_size, 4096, "Size of each request");
DEFINE_bool(random, true, "Write randomly");

bvar::LatencyRecorder g_latency_recorder("block_client");
bvar::Adder<int64_t> g_throughput;
bvar::PerSecond<bvar::Adder<int64_t> > g_throughput_ps("block_client_tp", &g_throughput, 10);

using namespace baidu::ebs;

int get_leader(baidu::rpc::Channel* channel, uint64_t block_id,  base::EndPoint* leader_addr) {
    BlockServiceAdaptor_Stub stub(channel);
    GetLeaderRequest request;
    request.set_block_id(block_id);
    GetLeaderResponse response;
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    stub.GetLeader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to get leader, " << cntl.ErrorText();
        return -1;
    }
    int rc = base::str2endpoint(response.leader_addr().c_str(), leader_addr);
    if (rc != 0) {
        return rc;
    }
    if (leader_addr->ip == base::IP_ANY) {
        return -1;
    }
    return 0;
}

struct IOArg {
    int64_t start;
    int64_t end;
    baidu::rpc::Channel* cluster;
};

typedef base::FlatMap<uint64_t, baidu::rpc::Channel*> LeaderMap;

volatile bool g_signal_quit = false;

int write(baidu::rpc::Channel* cluster, uint64_t block_id, off_t offset, 
          int64_t len, LeaderMap& leader_map) {
    WriteRequest request;
    AckResponse response;
    request.set_block_id(block_id);
    request.set_offset(offset);
    request.mutable_data()->resize(len);
    while (!g_signal_quit) {
        baidu::rpc::Channel*& channel = leader_map[block_id];
        if (channel == NULL) {
            base::EndPoint leader_addr;
            if (get_leader(cluster, block_id, &leader_addr) != 0) {
                LOG(WARNING) << "Fail to get leader of block_id=" << block_id;
                bthread_usleep(100 * 1000);
                continue;
            }
            channel = new baidu::rpc::Channel;
            if (channel->Init(leader_addr, NULL) != 0) {
                LOG(WARNING) << "Fail to get leader of block_id=" << block_id;
                delete channel;
                channel = NULL;
                bthread_usleep(100 * 1000);
            }
        }
        baidu::rpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        BlockServiceAdaptor_Stub stub(channel);
        stub.Write(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            return 0;
        }
        if (cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT) {
            bthread_usleep(100 * 1000);
            continue;
        }
        CHECK(cntl.ErrorCode() == EINVAL || cntl.ErrorCode() == EPERM);
        LOG(WARNING) << "Fail to write, " << cntl.ErrorText();
        delete channel;
        channel = NULL;
    }
    return -1;
}

int read(baidu::rpc::Channel* cluster, uint64_t block_id, off_t offset, 
         int64_t len, LeaderMap& leader_map) {
    ReadRequest request;
    ReadResponse response;
    request.set_block_id(block_id);
    request.set_offset(offset);
    request.set_size(len);
    while (!g_signal_quit) {
        baidu::rpc::Channel*& channel = leader_map[block_id];
        if (channel == NULL) {
            base::EndPoint leader_addr;
            if (get_leader(cluster, block_id, &leader_addr) != 0) {
                LOG(WARNING) << "Fail to get leader of block_id=" << block_id;
                bthread_usleep(100 * 1000);
                continue;
            }
            channel = new baidu::rpc::Channel;
            if (channel->Init(leader_addr, NULL) != 0) {
                LOG(WARNING) << "Fail to get leader of block_id=" << block_id;
                delete channel;
                channel = NULL;
                bthread_usleep(100 * 1000);
            }
        }
        baidu::rpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        BlockServiceAdaptor_Stub stub(channel);
        stub.Read(&cntl, &request, &response, NULL);
        if (!cntl.Failed() && response.error_code() != EINVAL) {
            CHECK_EQ(0, response.error_code());
            CHECK_EQ((size_t)len, response.data().size());
            return 0;
        }
        if (cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT) {
            bthread_usleep(100 * 1000);
            continue;
        }
        LOG(WARNING) << "Fail to read, " << cntl.ErrorText();
        delete channel;
        channel = NULL;
    }
    return -1;
}


void *io(void* arg) {
    IOArg* io_arg = (IOArg*)arg;
    LeaderMap leader_map;
    leader_map.init(FLAGS_block_num * 2);
    int64_t last_end_pos = io_arg->start;
    LOG(INFO) << "Started io thread, start=" << io_arg->start
              << " end=" << io_arg->end;
    while (!g_signal_quit) {
        int64_t start_pos;
        if (FLAGS_random) {
            start_pos = base::fast_rand_in(io_arg->start, io_arg->end);
        } else {
            start_pos = last_end_pos + FLAGS_request_size;
            if (start_pos >= io_arg->end) {
                start_pos = io_arg->start;
            }
        }
        int64_t block_id = start_pos / FLAGS_block_size;
        int64_t end_pos = std::min(start_pos + FLAGS_request_size, io_arg->end);
        end_pos = std::min(end_pos, FLAGS_block_size * (block_id + 1));
        last_end_pos = end_pos;
        const int64_t start_time = base::cpuwide_time_us();
        int rc = 0;
        if ((int)base::fast_rand_less_than(100) < FLAGS_write_percent) {
            rc = write(io_arg->cluster, block_id, 
                       start_pos - block_id * FLAGS_block_size,
                       end_pos - start_pos, leader_map);
        } else {
            rc = read(io_arg->cluster, block_id, 
                      start_pos - block_id * FLAGS_block_size,
                      end_pos - start_pos, leader_map);
        }
        const int64_t end_time = base::cpuwide_time_us();
        const int64_t elp = end_time - start_time;
        if (rc == 0) {
            g_latency_recorder << elp;
            g_throughput << end_pos - start_pos;
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    baidu::rpc::Channel cluster;
    if (cluster.Init(FLAGS_cluster_ns.c_str(), "rr", NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to `" << FLAGS_cluster_ns << "'";
        return -1;
    }
    bthread_t threads[FLAGS_threads];
    IOArg args[FLAGS_threads];
    int64_t range = FLAGS_block_num * FLAGS_block_size / FLAGS_threads;
    for (int i = 0; i < FLAGS_threads; ++i) {
        args[i].cluster = &cluster;
        args[i].start = i * range;
        args[i].end = (i + 1) * range;
        if (bthread_start_background(&threads[i], NULL, io, &args[i]) != 0) {
            PLOG(FATAL) << "Fail to start bthread";
            return -1;
        }
    }
    while (!g_signal_quit) {
        sleep(1);
        LOG(INFO) << "Sending BlockRequest "
                  << (FLAGS_random? "randomly" : "sequentially")
                  << " read/write=" 
                  << (100 - FLAGS_write_percent) << '/' << FLAGS_write_percent
                  << " with thread_num=" << FLAGS_threads
                  << " request_size=" << FLAGS_request_size
                  << " at qps=" << g_latency_recorder.qps(1)
                  << " latency=" << g_latency_recorder.latency(1)
                  << " tp=" << g_throughput_ps.get_value(1) / 1024.0 / 1024.0 
                  << "MB/s";
    }

    LOG(INFO) << "BlockClient is going to quit";
    for (int i = 0; i < FLAGS_threads; ++i) {
        bthread_join(threads[i], NULL);
    }

    return 0;
}

