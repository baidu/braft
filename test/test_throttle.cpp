// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved

// Author: Xiong Kai (xiongkai@baidu.com)
// Date: 2017/09/07 14:06:13

#include <gtest/gtest.h>
#include <butil/logging.h>
#include "braft/raft.h"
#include "braft/util.h"
#include "braft/snapshot_throttle.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

struct ArgThrottle {
    braft::ThroughputSnapshotThrottle* throttle;
    volatile int64_t total_throughput;
    volatile bool stopped;
};

void *read_across_throttle(void* arg) {
    ArgThrottle *a = (ArgThrottle*)arg;
    while (!a->stopped) {
        if (a->throttle == NULL) {
            EXPECT_TRUE(false);
            break;
        }
        // reading size of every time 
        int64_t request_bytes = 128 * 1024;
        int64_t ret = a->throttle->throttled_by_throughput(request_bytes);
        a->throttle->return_unused_throughput(ret, 0, 0);
        ret = a->throttle->throttled_by_throughput(request_bytes);
        a->total_throughput += ret;
        // if current reading is totally throttled, try again 100ms later
        if (ret == 0) {
            usleep(100000);
        }
        // reading time interval is 1ms
        usleep(1000);
    }
    return NULL;
}

TEST_F(TestUsageSuits, throttle_functioning) {
    // disk limit: 30M/s, cycles: 10 times/s
    int64_t limit = 30 * 1024 * 1024;
    const int64_t cycles = 10;
    // limit_per_cycle is 3 * 1024 * 1024
    const int64_t limit_per_cycle = limit / cycles;
    // every cycle time is 100,000us 
    const int64_t cycle_time = 1 * 1000 * 1000 / cycles;
    // test aligning time
    for (int i = 0; i < 10; ++i) {
        usleep(0.8 * cycle_time);
        int64_t now = butil::cpuwide_time_us();
        int64_t aligning_time = braft::caculate_check_time_us(now, cycles);
        ASSERT_TRUE(aligning_time % cycle_time == 0);
        LOG(INFO) << "Time now: " << now << ", aligning time: " << aligning_time;
    }

    int64_t request_1 = 1 * 1024 * 1024;
    int64_t request_2 = 2 * 1024 * 1024;
    int64_t request_3 = 3 * 1024 * 1024;
    braft::ThroughputSnapshotThrottle throttle(limit, cycles);
    // 1M is ok
    int64_t ret1 = throttle.throttled_by_throughput(request_1);
    int64_t time1 = throttle._last_throughput_check_time_us;
    ASSERT_EQ(ret1, request_1);
    // another 2M is ok
    int64_t ret2 = throttle.throttled_by_throughput(request_2);
    int64_t time2 = throttle._last_throughput_check_time_us;
    ASSERT_EQ(ret2, request_2);
    ASSERT_EQ(time1, time2);
    // another 1M is refused in current cycle
    int64_t ret3 = throttle.throttled_by_throughput(request_1);
    ASSERT_EQ(ret3, 0);
    // return unsed 1M
    throttle.return_unused_throughput(request_1, 0, 10);
    ret3 = throttle.throttled_by_throughput(request_1);
    ASSERT_EQ(ret3, request_1);
    // return unsed 1M, too long ago
    throttle.return_unused_throughput(request_1, 0, cycle_time * 2);
    ret3 = throttle.throttled_by_throughput(request_1);
    ASSERT_EQ(ret3, 0);
    // return unsed 1M - 10
    throttle.return_unused_throughput(request_1, request_1 - 10, 0);
    ret3 = throttle.throttled_by_throughput(request_1);
    ASSERT_EQ(ret3, 10);
    usleep(cycle_time);
    // previous 1M is ok in next cycle
    ret3 = throttle.throttled_by_throughput(request_1);
    int64_t time3 = throttle._last_throughput_check_time_us;
    ASSERT_EQ(ret3, request_1);
    ASSERT_EQ(time3, time2 + cycle_time);
    // another 3M will be throttled to 2M(3-1=2)
    int ret4 = throttle.throttled_by_throughput(request_3);
    int64_t time4 = throttle._last_throughput_check_time_us;
    ASSERT_EQ(ret4, limit_per_cycle - ret3);
    ASSERT_EQ(time4, time3);
    
    usleep(cycle_time);
    
    // use multi-thread to test total throughput 
    ArgThrottle arg;
    arg.throttle = &throttle;
    arg.total_throughput = 0;
    arg.stopped = false;
    const int thread_num = 10;
    pthread_t readers[thread_num];
    for (int i= 0; i < thread_num; ++i) {
        ASSERT_EQ(0, pthread_create(&readers[i], NULL, read_across_throttle, &arg));
    }
    // run for 10s
    int run_time = 2;
    usleep(run_time * 1000 * 1000);
    arg.stopped = true;
    for (int i = 0; i < thread_num; ++i) {
        pthread_join(readers[i], NULL);
    }
    // 
    int64_t expect_throughput = run_time * limit;
    ASSERT_LE(arg.total_throughput, 1.1 * expect_throughput);
    ASSERT_GE(arg.total_throughput, 0.9 * expect_throughput);
    LOG(INFO) << "Total throughput in run_time: " << arg.total_throughput
              << " Upper bound: " << 1.1 * expect_throughput
              << " Lowwer bound: " << 0.9 * expect_throughput;  
    
}


















