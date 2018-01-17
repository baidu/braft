// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/04/11 12:15:37

#include <gtest/gtest.h>
#include <braft/util.h>
#include <butil/crc32c.h>

class ChecksumTest : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(ChecksumTest, benchmark) {
    char data[4096];
    for (size_t i = 0; i < ARRAY_SIZE(data); ++i) {
        data[i] = butil::fast_rand_in('a', 'z');
    }
    butil::Timer timer;
    const size_t N = 10000;
    timer.start();
    for (size_t i = 0; i < N; ++i) {
        braft::murmurhash32(data, sizeof(data));
    }
    timer.stop();
    const long mur_elp = timer.u_elapsed();
    
    timer.start();
    for (size_t i = 0; i < N; ++i) {
        butil::crc32c::Value(data, sizeof(data));
    }
    timer.stop();
    const long crc_elp = timer.u_elapsed();

    LOG(INFO) << "murmurhash32_TP=" << sizeof(data) * N / (double)mur_elp << "MB/s"
              << " base_crc32_TP=" << sizeof(data) * N / (double)crc_elp << "MB/s";
    LOG(INFO) << "base_is_fast_crc32_support=" << butil::crc32c::IsFastCrc32Supported();

}
