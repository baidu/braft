/*
 * =====================================================================================
 *
 *       Filename:  test_util.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年11月06日 17时48分54秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include <base/logging.h>

#include "raft/util.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

struct LockMeta {
    bthread_mutex_t* mutex;
    int64_t value;
};

void* run_lock_guard(void *arg) {
    LockMeta* meta = (LockMeta*)arg;

    for (int i = 0; i < 10000; i++) {
        std::lock_guard<bthread_mutex_t> guard(*(meta->mutex));
        meta->value++;
    }
    return NULL;
}

TEST_F(TestUsageSuits, lock) {
    bthread_mutex_t mutex;
    bthread_mutex_init(&mutex, NULL);

    // bthread lock guard
    LockMeta meta;
    meta.value = 0;
    meta.mutex = &mutex;

    bthread_t tids[10];
    for (int i = 0; i < 10; i++) {
        bthread_start_background(&tids[i], &BTHREAD_ATTR_NORMAL, run_lock_guard, &meta);
    }

    for (int i = 0; i < 10; i++) {
        bthread_join(tids[i], NULL);
    }

    ASSERT_EQ(meta.value, 10*10000);
    bthread_mutex_destroy(&mutex);
}

TEST_F(TestUsageSuits, get_host_ip) {
    base::ip_t not_exist_ip = base::get_host_ip_by_interface("not_exist");
    ASSERT_EQ(not_exist_ip, base::IP_ANY);

    base::ip_t host_ip = base::get_host_ip();

    base::ip_t xgb0_ip = base::get_host_ip_by_interface("xgb0");
    base::ip_t xgb1_ip = base::get_host_ip_by_interface("xgb1");
    base::ip_t eth1_ip = base::get_host_ip_by_interface("eth1");
    base::ip_t eth0_ip = base::get_host_ip_by_interface("eth0");
    base::ip_t bond0_ip = base::get_host_ip_by_interface("bond0");
    base::ip_t brex_ip = base::get_host_ip_by_interface("br-ex");

    ASSERT_TRUE(host_ip == xgb0_ip || host_ip == xgb1_ip ||
                host_ip == eth1_ip || host_ip == eth0_ip ||
                host_ip == bond0_ip || host_ip == brex_ip);
}

TEST_F(TestUsageSuits, random) {
    for (int i = 0; i < 10000; i++) {
        int32_t value = raft::get_random_number(0, 10000);
        ASSERT_TRUE(value >= 0 && value <= 10000);
    }

    int32_t rand_time = raft::random_timeout(300);
    ASSERT_TRUE(rand_time >= 0 && rand_time <= 600);
}

TEST_F(TestUsageSuits, murmurhash) {
    int32_t val1 = raft::murmurhash32("hello, world", strlen("hello, world"));

    char* data = (char*)malloc(1024*1024);
    for (int i = 0; i < 1024*1024; i++) {
        data[i] = 'a' + rand() % 26;
    }
    int32_t val2 = raft::murmurhash32(data, 1024*1024);

    base::IOBuf buf;
    for (int i = 0; i < 1024; i++) {
        buf.append("hello, world");
        char c = 'a' + rand() % 26;
        buf.push_back(c);
    }
    int32_t val3 = raft::murmurhash32(buf);
}

TEST_F(TestUsageSuits, fileuri) {
    {
        ASSERT_EQ(raft::fileuri2path("./data/log"), std::string("./data/log"));
    }

    {
        ASSERT_EQ(raft::fileuri2path("file://data/log"), std::string("data/log"));
        ASSERT_EQ(raft::fileuri2path("file://data"), std::string("data"));
        ASSERT_EQ(raft::fileuri2path("file://./data/log"), std::string("./data/log"));
        ASSERT_EQ(raft::fileuri2path("file://./data"), std::string("./data"));

        ASSERT_EQ(raft::fileuri2path("file://1.2.3.4:80/data/log"), std::string("data/log"));
        ASSERT_EQ(raft::fileuri2path("file://1.2.3.4:80/data"), std::string("data"));
        ASSERT_EQ(raft::fileuri2path("file://1.2.3.4:80//data/log"), std::string("/data/log"));
        ASSERT_EQ(raft::fileuri2path("file://1.2.3.4:80//data"), std::string("/data"));

        ASSERT_EQ(raft::fileuri2path("file://www.baidu.com:80/data/log"), std::string("data/log"));
        ASSERT_EQ(raft::fileuri2path("file://www.baidu.com:80/data"), std::string("data"));
        ASSERT_EQ(raft::fileuri2path("file://www.baidu.com:80//data/log"), std::string("/data/log"));
        ASSERT_EQ(raft::fileuri2path("file://www.baidu.com:80//data"), std::string("/data"));
    }

    {
        int ret = 0;
        base::EndPoint addr;
        std::string path;

        ret = raft::fileuri_parse("./a/b/c", &addr, &path);
        ASSERT_NE(ret, 0);

        std::string uri("file://127.0.0.1:1000/a/b/c");
        ret = raft::fileuri_parse(uri, &addr, &path);
        ASSERT_EQ(ret, 0);
        base::EndPoint point;
        base::str2endpoint("127.0.0.1:1000", &point);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(point, addr);
        ASSERT_EQ(path, "a/b/c");
    }
}
