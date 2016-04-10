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
    raft_mutex_t* mutex;
    int64_t value;
};

void* run_lock_guard(void *arg) {
    LockMeta* meta = (LockMeta*)arg;

    for (int i = 0; i < 10000; i++) {
        std::lock_guard<raft_mutex_t> guard(*(meta->mutex));
        meta->value++;
    }
    return NULL;
}

TEST_F(TestUsageSuits, lock) {
    raft_mutex_t mutex;

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

/*
TEST_F(TestUsageSuits, random) {
    for (int i = 0; i < 10000; i++) {
        int32_t value = base::fast_rand_in(0, 10000);
        ASSERT_TRUE(value >= 0 && value <= 10000);
    }

    int32_t rand_time = raft::random_timeout(300);
    ASSERT_TRUE(rand_time >= 0 && rand_time <= 600);
}
//*/

TEST_F(TestUsageSuits, murmurhash) {
    char* data = (char*)malloc(1024*1024);
    for (int i = 0; i < 1024*1024; i++) {
        data[i] = 'a' + i % 26;
    }
    int32_t val1 = raft::murmurhash32(data, 1024*1024);

    base::IOBuf buf;
    for (int i = 0; i < 1024 * 1024; i++) {
        char c = 'a' + i % 26;
        buf.push_back(c);
    }
    int32_t val2 = raft::murmurhash32(buf);
    ASSERT_EQ(val1, val2);
    free(data);
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

TEST_F(TestUsageSuits, pread_pwrite) {
    int fd = ::open("./pread_pwrite.data", O_CREAT | O_TRUNC | O_RDWR, 0644);

    base::IOPortal portal;
    ssize_t nread = raft::file_pread(&portal, fd, 1000, 10);
    ASSERT_EQ(nread, 0);

    base::IOBuf data;
    data.append("hello");
    ssize_t nwriten = raft::file_pwrite(data, fd, 1000);
    ASSERT_EQ(nwriten, data.size());

    portal.clear();
    nread = raft::file_pread(&portal, fd, 1000, 10);
    ASSERT_EQ(nread, data.size());
    ASSERT_EQ(raft::murmurhash32(data), raft::murmurhash32(portal));

    ::close(fd);
    ::unlink("./pread_pwrite.data");
}

TEST_F(TestUsageSuits, FileSegData) {
    raft::FileSegData seg_writer;
    for (uint64_t i = 0; i < 10UL; i++) {
        char buf[1024];
        snprintf(buf, sizeof(buf), "hello %lu", i);
        seg_writer.append(buf, 1000 * i, strlen(buf));
    }

    raft::FileSegData seg_reader(seg_writer.data());
    uint64_t seg_offset = 0;
    base::IOBuf seg_data;
    uint64_t index = 0;
    while (0 != seg_reader.next(&seg_offset, &seg_data)) {
        ASSERT_EQ(index * 1000, seg_offset);

        char buf[1024] = {0};
        snprintf(buf, sizeof(buf), "hello %lu", index);

        char new_buf[1024] = {0};
        seg_data.copy_to(new_buf, strlen(buf));
        printf("index:%lu old: %s new: %s\n", index, buf, new_buf);
        ASSERT_EQ(raft::murmurhash32(seg_data), raft::murmurhash32(buf, strlen(buf)));

        seg_data.clear();
        index ++;
    }
    ASSERT_EQ(index, 10UL);
}

TEST_F(TestUsageSuits, PathACL) {

    std::string real_path;
    ASSERT_TRUE(raft::PathACL::normalize_path("/a/b/c/", &real_path));
    ASSERT_EQ(real_path, std::string("/a/b/c"));
    ASSERT_TRUE(raft::PathACL::normalize_path("//a/b/c/", &real_path));
    ASSERT_EQ(real_path, std::string("/a/b/c"));
    ASSERT_TRUE(raft::PathACL::normalize_path("//a/b//c/", &real_path));
    ASSERT_EQ(real_path, std::string("/a/b/c"));
    ASSERT_TRUE(raft::PathACL::normalize_path("../a//./b/c/", &real_path));
    ASSERT_EQ(real_path, std::string("../a/b/c"));
    ASSERT_TRUE(raft::PathACL::normalize_path(".//a//./b/c/", &real_path));
    ASSERT_EQ(real_path, std::string("a/b/c"));
    ASSERT_TRUE(raft::PathACL::normalize_path("./a//../b/c/", &real_path));
    ASSERT_EQ(real_path, std::string("b/c"));

    raft::PathACL* acl = raft::PathACL::GetInstance();
    ASSERT_TRUE(acl->add("./test1/"));
    ASSERT_FALSE(acl->add("./test1/test2/"));
    ASSERT_FALSE(acl->add("./test1/"));

    ASSERT_FALSE(acl->check("./test3"));
    ASSERT_FALSE(acl->check("./est1"));
    ASSERT_FALSE(acl->check("./test12"));
    ASSERT_FALSE(acl->check("./test"));
    ASSERT_TRUE(acl->check("./test1/test"));
    ASSERT_TRUE(acl->check("./test1/test3"));
    ASSERT_TRUE(acl->check("./test1/test2/test3"));
    ASSERT_TRUE(acl->check("./test1"));
    ASSERT_TRUE(acl->check("./test1/test2"));
    ASSERT_FALSE(acl->check("./test1/../test2"));
    ASSERT_TRUE(acl->check("./test1/test2/../test3"));
    ASSERT_FALSE(acl->check("/"));

    ASSERT_TRUE(acl->remove("./test1"));
    ASSERT_FALSE(acl->remove("./test1/test2"));

    ASSERT_TRUE(acl->add("./test1/test2/"));
    ASSERT_FALSE(acl->add("./test1/"));
    ASSERT_FALSE(acl->add("./test1/test2/"));
    ASSERT_FALSE(acl->check("./test1/test"));
    ASSERT_FALSE(acl->check("./test1/test3"));
    ASSERT_TRUE(acl->check("./test1/test2/test3"));
    ASSERT_FALSE(acl->check("./test1"));
    ASSERT_TRUE(acl->check("./test1/test2"));
    ASSERT_TRUE(acl->remove("./test1/test2"));
    ASSERT_FALSE(acl->remove("./test1"));

    int acl_num = 10000;
    int64_t start = base::monotonic_time_us();
    for (int i = 0; i < acl_num; i++) {
        char name[256];
        snprintf(name, sizeof(name), "data/block_%d", i);

        ASSERT_TRUE(acl->add(name));
    }
    int64_t end = base::monotonic_time_us();
    LOG(INFO) << "build " << acl_num << " acl, time: " << end - start << " us";

    int round = 100000;
    start = base::monotonic_time_us();
    for (int i = 0; i < round; i++) {
        int index = rand() % acl_num;
        char name[256];
        snprintf(name, sizeof(name), "data/block_%d", index);

        ASSERT_TRUE(acl->check(name));
    }
    end = base::monotonic_time_us();
    LOG(INFO) << "rand " << round << " test on " << acl_num
        << " total " << end - start << " us"
        << " per " << (end - start) / round << " us";

    for (int i = 0; i < acl_num / 2; i++) {
        char name[256];
        snprintf(name, sizeof(name), "data/block_%d", 2*i);

        ASSERT_TRUE(acl->remove(name));
    }
    start = base::monotonic_time_us();
    for (int i = 0; i < round; i++) {
        int index = rand() % acl_num;
        char name[256];
        snprintf(name, sizeof(name), "data/block_%d", index);

        if (index % 2 == 0) {
            ASSERT_FALSE(acl->check(name));
        } else {
            ASSERT_TRUE(acl->check(name));
        }
    }
    end = base::monotonic_time_us();
    LOG(INFO) << "rand " << round << " test on " << acl_num / 2
        << " total " << end - start << " us"
        << " per " << (end - start) / round << " us";
}
