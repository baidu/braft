// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/06 15:40:45

#include <gtest/gtest.h>

#include <baidu/rpc/server.h>
#include "raft/file_service.h"
#include "raft/remote_path_copier.h"

class FileServiceTest : public testing::Test {
};

TEST_F(FileServiceTest, sanity) {
    raft::FileServiceImpl service;
    baidu::rpc::Server server;
    ASSERT_EQ(0, server.AddService(&service, baidu::rpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(60006, NULL));
    raft::RemotePathCopier copier;
    base::EndPoint point;
    ASSERT_EQ(0, str2endpoint("127.0.0.1:60006", &point));
    ASSERT_EQ(0, copier.init(point, "./tmp"));
    ASSERT_EQ(0, system("rm -rf a; mkdir a; mkdir a/b; echo '123' > a/c"));
    ASSERT_EQ(0, copier.copy("./a", "./b", NULL));
}
