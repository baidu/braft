// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/11/02 21:12:26

#include <gtest/gtest.h>
#include "braft/repeated_timer_task.h"

class RepeatedTimerTaskTest : public testing::Test {
};

class MockTimer : public braft::RepeatedTimerTask {
protected:
    MockTimer()
        : _blocking(false)
        , _run_times(0)
        , _on_destroy_times(0)
        , _adjust_timeout_times(0)
    {}
    ~MockTimer() { destroy(); }

    void run() {
        ++_run_times;
        while (_blocking) {
            usleep(100);
        }
    }

    void on_destroy() {
        ++_on_destroy_times;
    }

    int adjust_timeout_ms(int timeout_ms) {
        ++_adjust_timeout_times;
        return timeout_ms;
    }
    
    bool _blocking;
    int _run_times;
    int _on_destroy_times;
    int _adjust_timeout_times;
};

TEST_F(RepeatedTimerTaskTest, sanity) {
    MockTimer timer;
    ASSERT_EQ(0, timer.init(10));
    timer.start();
    usleep(100500);
    const int run_times = timer._run_times;
    LOG(INFO) << "run_times=" << run_times;
    ASSERT_TRUE(run_times >= 8 && run_times <= 11) << run_times;
    timer.stop();
    usleep(10000);
    ASSERT_LE(abs(run_times - timer._run_times), 1);
    timer.destroy();
    timer.destroy();
    ASSERT_EQ(1, timer._on_destroy_times);
}

TEST_F(RepeatedTimerTaskTest, stop_while_running) {
    MockTimer timer;
    ASSERT_EQ(0, timer.init(10));
    timer._blocking = true;
    timer.start();
    usleep(100000);
    ASSERT_EQ(1, timer._run_times);
    timer.stop();
    timer._blocking = false;
    usleep(1000);
    ASSERT_EQ(1, timer._run_times);
    timer.destroy();
    timer.destroy();
    ASSERT_EQ(1, timer._on_destroy_times);
}

TEST_F(RepeatedTimerTaskTest, destroy_while_running) {
    MockTimer timer;
    ASSERT_EQ(0, timer.init(10));
    timer._blocking = true;
    timer.start();
    usleep(100000);
    ASSERT_EQ(1, timer._run_times);
    timer.destroy();
    ASSERT_EQ(0, timer._on_destroy_times);
    timer._blocking = false;
    usleep(100000);
    ASSERT_EQ(1, timer._on_destroy_times);
    ASSERT_EQ(1, timer._run_times);
    timer.destroy();
    ASSERT_EQ(1, timer._on_destroy_times);
    timer.destroy();
    ASSERT_EQ(1, timer._on_destroy_times);
}

TEST_F(RepeatedTimerTaskTest, restart_while_running) {
    MockTimer timer;
    ASSERT_EQ(0, timer.init(10));
    timer._blocking = true;
    timer.start();
    usleep(100000);
    ASSERT_EQ(1, timer._run_times);
    timer.stop();
    usleep(50000);
    timer.start();
    usleep(50000);
    ASSERT_EQ(1, timer._run_times);
    timer.destroy();
    ASSERT_EQ(0, timer._on_destroy_times);
    timer._blocking = false;
    usleep(50000);
    timer.destroy();
    ASSERT_EQ(1, timer._on_destroy_times);
    ASSERT_EQ(1, timer._run_times);
}
