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
#include <base/files/scoped_temp_dir.h>

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
    base::ip_t xgbe0_ip = base::get_host_ip_by_interface("xgbe0");
    base::ip_t xgbe1_ip = base::get_host_ip_by_interface("xgbe1");
    base::ip_t eth1_ip = base::get_host_ip_by_interface("eth1");
    base::ip_t eth0_ip = base::get_host_ip_by_interface("eth0");
    base::ip_t bond0_ip = base::get_host_ip_by_interface("bond0");
    base::ip_t brex_ip = base::get_host_ip_by_interface("br-ex");

    ASSERT_TRUE(host_ip == xgbe0_ip || host_ip == xgbe1_ip ||
                host_ip == eth1_ip || host_ip == eth0_ip ||
                host_ip == bond0_ip || host_ip == brex_ip);
}

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
        snprintf(buf, sizeof(buf), "raw hello %lu", i);
        seg_writer.append(buf, 1000 * i, strlen(buf));
    }
    for (uint64_t i = 10; i < 20UL; i++) {
        char buf[1024];
        snprintf(buf, sizeof(buf), "iobuf hello %lu", i);
        base::IOBuf piece_buf;
        piece_buf.append(buf, strlen(buf));
        seg_writer.append(piece_buf, 1000 * i);
    }

    raft::FileSegData seg_reader(seg_writer.data());
    uint64_t seg_offset = 0;
    base::IOBuf seg_data;
    uint64_t index = 0;
    while (0 != seg_reader.next(&seg_offset, &seg_data)) {
        ASSERT_EQ(index * 1000, seg_offset);

        char buf[1024] = {0};
        if (index < 10) {
            snprintf(buf, sizeof(buf), "raw hello %lu", index);
        } else {
            snprintf(buf, sizeof(buf), "iobuf hello %lu", index);
        }

        char new_buf[1024] = {0};
        seg_data.copy_to(new_buf, strlen(buf));
        printf("index:%lu old: %s new: %s\n", index, buf, new_buf);
        ASSERT_EQ(raft::murmurhash32(seg_data), raft::murmurhash32(buf, strlen(buf)));

        seg_data.clear();
        index ++;
    }
    ASSERT_EQ(index, 20UL);
}

TEST_F(TestUsageSuits, crc32) {
    char* data = (char*)malloc(1024*1024);
    for (int i = 0; i < 1024*1024; i++) {
        data[i] = 'a' + i % 26;
    }
    int32_t val1 = raft::crc32(data, 1024*1024);

    base::IOBuf buf;
    for (int i = 0; i < 1024 * 1024; i++) {
        char c = 'a' + i % 26;
        buf.push_back(c);
    }
    int32_t val2 = raft::crc32(buf);
    ASSERT_EQ(val1, val2);

    free(data);
}

bool is_zero1(const char *buff, size_t size) {
    while (size--) {
        if (*buff++) {
            return false;
        }
    }
    return true;
}

int is_zero2(const char *buff, const size_t size) {
    return (0 == *buff && 0 == memcmp(buff, buff + 1, size - 1));
}

int is_zero3(const char *buff, const size_t size) {
    return (0 == *(uint64_t*)buff &&
            0 == memcmp(buff, buff + sizeof(uint64_t), size - sizeof(uint64_t)));
}

int is_zero4(const char *buff, const size_t size) {
    return (0 == *(wchar_t*)buff &&
            0 ==  wmemcmp((wchar_t*)buff, (wchar_t*)buff + 1, size / sizeof(wchar_t) - 1));
}

int is_zero5(const char *buff, size_t size) {
    for (size_t i = 0; i < size / sizeof(uint64_t); i++) {
        if (*(uint64_t*)buff != 0) {
            return false;
        }
        buff += sizeof(uint64_t);
    }
    size %= sizeof(uint64_t);
    for (size_t i = 0; i < size / sizeof(uint8_t); i++) {
        if (*(uint8_t*)buff != 0) {
            return false;
        }
        buff += sizeof(uint8_t);
    }
    return true;
}

int is_zero_memcmp(const char* buff, size_t size) {
    static char static_zero_1m_buf[1024*1024] = {0};
    return 0 == memcmp(buff, static_zero_1m_buf, size);
}

#define IS_ZERO_TEST(func, size)                                               \
    do {                                                                       \
        int64_t start = base::detail::clock_cycles();                          \
        ASSERT_TRUE(func(data, size));                                         \
        int64_t end = base::detail::clock_cycles();                            \
        LOG(INFO) << #func << " cycle: " << end - start;     \
    } while (0)

TEST_F(TestUsageSuits, is_zero) {
    char* data = (char*)malloc(1024*1024);
    memset(data, 0, 1024*1024);

    {
        char* tmp_data = (char*)malloc(1024*1024);
        memset(tmp_data, 'a', 1024*1024);
        free(tmp_data);
        ASSERT_EQ(0, memcmp(data, tmp_data, 0));
    }

    int test_sizes[] = {4*1024, 8*1024, 16*1024, 64*1024, 128*1024, 256*1024, 512*1024, 1024*1024};
    for (size_t i = 0; i < sizeof(test_sizes) / sizeof(int); i++) {
        LOG(INFO) << "is_zero size: " << test_sizes[i];
        IS_ZERO_TEST(is_zero1, test_sizes[i]);
        IS_ZERO_TEST(is_zero2, test_sizes[i]);
        IS_ZERO_TEST(is_zero3, test_sizes[i]);
        IS_ZERO_TEST(is_zero4, test_sizes[i]);
        IS_ZERO_TEST(is_zero5, test_sizes[i]);
        IS_ZERO_TEST(is_zero_memcmp, test_sizes[i]);
        IS_ZERO_TEST(raft::is_zero, test_sizes[i]);
    }

    for (int i = 1024; i >= 1; i--) {
        ASSERT_TRUE(raft::is_zero(data, i*1024));
    }
    for (int i = 1; i < 8; i++) {
        ASSERT_TRUE(raft::is_zero(data, i));
    }

    int rand_pos = rand() % (1024 * 1024);
    data[rand_pos] = 'a' + rand() % 26;
    ASSERT_FALSE(raft::is_zero(data, 1024 * 1024));
    ASSERT_TRUE(raft::is_zero(data, rand_pos));
    ASSERT_TRUE(raft::is_zero(data + rand_pos + 1, 1024 * 1024 - 1 - rand_pos));

    memset(data, 0, 1024*1024);
    rand_pos = rand() % 8;
    data[rand_pos] = 'a' + rand() % 26;
    ASSERT_FALSE(raft::is_zero(data, 8));
    ASSERT_TRUE(raft::is_zero(data, rand_pos));
    ASSERT_TRUE(raft::is_zero(data + rand_pos + 1, 8 - 1 - rand_pos));

    free(data);
}

TEST_F(TestUsageSuits, file_path) {
    base::FilePath path("dir/");
    LOG(INFO) << "file path=" << path.value()
              << ", dir_name=" << path.DirName().value()
              << " base_name=" << path.BaseName().value();
    
    path = base::FilePath("dir");
    LOG(INFO) << "file path=" << path.value()
              << ", dir_name=" << path.DirName().value()
              << " base_name=" << path.BaseName().value();
    
    path = base::FilePath("dir/subdir/file.txt");
    LOG(INFO) << "file path=" << path.value()
              << ", dir_name=" << path.DirName().value()
              << " base_name=" << path.BaseName().value();
    
    // can not deal with ..
    path = base::FilePath("../dir/");
    LOG(INFO) << "file path=" << path.value()
              << ", dir_name=" << path.DirName().value()
              << " base_name=" << path.BaseName().value();
    
    path = base::FilePath("../");
    LOG(INFO) << "file path=" << path.value()
              << ", dir_name=" << path.DirName().value()
              << " base_name=" << path.BaseName().value();
    
    // get real path of a file
    ::system("mkdir ./log");
    ::system("touch ./log/log_meta.txt");
    path = base::FilePath("./log/log_meta.txt");
    base::FilePath real_path;
    if (base::NormalizeFilePath(path, &real_path)) {
        LOG(INFO) << "file path=" << path.value()
                  << ", real_path is: " << real_path.value();
    } else {
        LOG(INFO) << "file path=" << path.value()
                  << ", has no real path: " << real_path.value()
                  << " Errno: " << errno;
    }
    ::system("rm -rf ./log");

    // can not get real path when path is a directory
    path = base::FilePath("./");
    real_path.clear();
    if (base::NormalizeFilePath(path, &real_path)) {
        LOG(INFO) << "file path=" << path.value()
                  << ", real_path is: " << real_path.value();
    } else {
        const int errsv = errno;
        LOG(INFO) << "file path=" << path.value()
                  << ", has no real path: " << real_path.value()
                  << " Errno: " << errsv;
    }

    // system function realpath can do both 
    char buf[1024];
    ::system("mkdir ./log");
    ::system("touch ./log/log_meta.txt");
    char* tmp_path = "./log/log_meta.txt";
    char* res = realpath(tmp_path, buf);
    if (res) {
        LOG(INFO) << "path: " << tmp_path << ", real path is: " << buf;
    } else {
        LOG(INFO) << "no real path.";
    }
    ::system("rm -rf ./log");

    // also ok when path is a directory
    tmp_path = "./";
    res = realpath(tmp_path, buf);
    if (res) {
        LOG(INFO) << "path: " << tmp_path << ", real path is: " << buf;
    } else {
        LOG(INFO) << "no real path.";
    }

    // can deal with ..
    ::system("mkdir ./log");
    ::system("touch ./log/log_meta.txt");
    tmp_path = "./log/../log/log_meta.txt";
    res = realpath(tmp_path, buf);
    if (res) {
        LOG(INFO) << "path: " << tmp_path << ", real path is: " << buf;
    } else {
        LOG(INFO) << "no real path.";
    }
    ::system("rm -rf ./log");

    path = base::FilePath("../sub4/sub5/dir");
    LOG(INFO) << path.ReferencesParent();
}

TEST_F(TestUsageSuits, sync_parent_dir) {
    // no such path
    std::string path = "./log/log_meta";
    base::Status status = raft::sync_parent_dir(path.c_str(), true);
    ASSERT_TRUE(!status.ok());
    if (!status.ok()) {
        LOG(WARNING) << "sync dir failed. status: " << status;
    } else {
        LOG(INFO) << "sync dir success. status: " << status;
    }

    ::system("mkdir ./log/");
    ::system("touch ./log/log_meta");
    status = raft::sync_parent_dir(path.c_str(), true);
    ASSERT_TRUE(status.ok());
    if (!status.ok()) {
        LOG(WARNING) << "sync dir failed. status: " << status;
    } else {
        LOG(WARNING) << "sync dir success. status: " << status;
    }
    ::system("rm -rf ./log");

    ::system("mkdir ./log/");
    ::system("touch ./log/log_meta");
    path = "./log/../log/log_meta";
    status = raft::sync_parent_dir(path.c_str(), true);
    ASSERT_TRUE(status.ok());
    if (!status.ok()) {
        LOG(WARNING) << "sync dir failed. status: " << status;
    } else {
        LOG(WARNING) << "sync dir success. status: " << status;
    }
    ::system("rm -rf ./log");

    path = "./";
    status = raft::sync_parent_dir(path.c_str(), true);
    ASSERT_TRUE(status.ok());
    if (!status.ok()) {
        LOG(WARNING) << "sync dir failed. status: " << status;
    } else {
        LOG(WARNING) << "sync dir success. status: " << status;
    } 
}

TEST_F(TestUsageSuits, file_rename) {

    std::string tmp_path = "./log/log_meta.tmp";
    std::string new_path = "./log/log_meta";
    // no such file
    base::Status status = raft::file_rename(tmp_path.c_str(), new_path.c_str(), true);
    ASSERT_TRUE(!status.ok());
    if (!status.ok()) {
        LOG(WARNING) << "Rename failed. status: " << status;
    } else {
        LOG(WARNING) << "Rename success. status: " << status;
    }

    ::system("mkdir ./log/");
    ::system("touch ./log/log_meta.tmp"); 
    status = raft::file_rename(tmp_path.c_str(), new_path.c_str(), true);
    ASSERT_TRUE(status.ok());
    if (!status.ok()) {
        LOG(WARNING) << "Rename failed. status: " << status;
    } else {
        LOG(WARNING) << "Rename success. status: " << status;
    }
    ::system("rm -rf ./log");
}

