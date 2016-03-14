/*
 * =====================================================================================
 *
 *       Filename:  test_io_man.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2016年03月13日 21时12分00秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

#include <gtest/gtest.h>
#include <base/logging.h>

#include <bthread.h>
#include <bthread/task_group.h>

#include "raft/io_man.h"

boost::atomic<int> g_finished;
void* io_func(void* arg) {
    char* file_name = static_cast<char*>(arg);
    int fd = ::open(file_name, O_CREAT | O_RDWR | O_TRUNC, 0644);
    for (int i = 0; i < 10000; i++) {
        off_t old_off = lseek(fd, 0, SEEK_CUR);
        char buf[1024];
        snprintf(buf, sizeof(buf), "DATA:%d\n", i);
        int ret = raft::bthread_write(fd, buf, strlen(buf));
        assert(ret == strlen(buf));

        char buf2[1024];
        lseek(fd, old_off, SEEK_SET);
        ret = raft::bthread_read(fd, buf2, strlen(buf));
        assert(ret == strlen(buf));
        assert(0 == strncmp(buf, buf2, strlen(buf)));
    }

    off_t curr_off = lseek(fd, 0, SEEK_CUR);
    char buf[1024];
    snprintf(buf, sizeof(buf), "DATA END\n");
    int ret = raft::bthread_pwrite(fd, buf, strlen(buf), curr_off);
    assert(ret == strlen(buf));
    char buf2[1024];
    ret = raft::bthread_pread(fd, buf2, strlen(buf), curr_off);
    assert(ret == strlen(buf));
    assert(0 == strncmp(buf, buf2, strlen(buf)));

    ::close(fd);
    g_finished++;

    return NULL;
}

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, io) {
    for (int i = 0; i < 3; i++) {
        char file_name[64];
        snprintf(file_name, sizeof(file_name), "./file_%d", i);
        bthread_t tid;
        bthread_start_background(&tid, NULL, io_func, strdup(file_name));
    }
    while (g_finished != 3) {
        sleep(1);
    }

    raft::IOMan::GetInstance()->Release();
    sleep(1);
}
