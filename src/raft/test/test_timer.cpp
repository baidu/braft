// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2016/03/31 15:42:37

#include <gtest/gtest.h>
#include "raft/timer.h"
#include <base/time.h>
#include <stdlib.h>
#include <string.h>

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

static void on_timer(void* arg) {
    char* str = static_cast<char*>(arg);
    LOG(INFO) << "ontimer: " << str;
    free(arg);
}

TEST_F(TestUsageSuits, function) {
    raft::raft_timer_t timer;
    int rc = raft::raft_timer_add(&timer, base::milliseconds_from_now(2000),
                                  on_timer, strdup("case1"));
    ASSERT_EQ(rc, 0);

    ++timer.id;
    rc = raft::raft_timer_del(timer);
    ASSERT_EQ(rc, EINVAL);

    timer.id--;
    rc = raft::raft_timer_del(timer);
    ASSERT_EQ(rc, 0);

    for (int i = 0; i < 30; i++) {
        raft::raft_timer_t timer;
        char name[64];
        snprintf(name, sizeof(name), "case:%d", i);
        raft::raft_timer_add(&timer, base::milliseconds_from_now(2000),
                                      on_timer, strdup(name));
    }
    sleep(3);
}



