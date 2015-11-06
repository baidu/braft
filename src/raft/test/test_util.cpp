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

TEST_F(TestUsageSuits, fileuri_parse) {
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
