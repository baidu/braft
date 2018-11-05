// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/02/23 16:22:15

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include <butil/fd_guard.h>
#include <butil/time.h>
#include <butil/logging.h>

class FsyncTest : public testing::Test {
};

TEST_F(FsyncTest, benchmark_append) {
    butil::fd_guard fd(::open("fsync.data", O_RDWR | O_CREAT | O_TRUNC, 0644));
    ASSERT_NE(-1, fd);
    char buf[1024];
    butil::Timer timer;
    const size_t N = 1000;
    timer.start();
    for (size_t i = 0; i < N; ++i) {
        int left = 1024;
        while (left > 0) {
            ssize_t nw = write(fd, buf, left);
            ASSERT_NE(-1, nw);
            left -= nw;
        }
        fsync(fd);
    }
    timer.stop();
    LOG(INFO) << "fsync takes " << timer.u_elapsed();
    timer.start();
    fd.reset(-1);
    fd.reset(::open("fsync.data", O_RDWR | O_CREAT | O_TRUNC, 0644));
    for (size_t i = 0; i < N; ++i) {
        int left = 1024;
        while (left > 0) {
            ssize_t nw = write(fd, buf, left);
            ASSERT_NE(-1, nw);
            left -= nw;
        }
#ifdef __APPLE__
        fcntl(fd, F_FULLFSYNC);
#else
        fdatasync(fd);
#endif
    }
    timer.stop();
    LOG(INFO) << "fdatasync takes " << timer.u_elapsed();
}

TEST_F(FsyncTest, benchmark_randomly_write) {
}
