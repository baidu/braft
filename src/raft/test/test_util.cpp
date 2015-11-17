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

TEST_F(TestUsageSuits, fileuri) {
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
        std::string uri("file://127.0.0.1:1000/a/b/c");
        base::EndPoint addr;
        std::string path;
        int ret = raft::fileuri_parse(uri, &addr, &path);

        base::EndPoint point;
        base::str2endpoint("127.0.0.1:1000", &point);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(point, addr);
        ASSERT_EQ(path, "a/b/c");
    }
}
