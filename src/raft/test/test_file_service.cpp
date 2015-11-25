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
    // bad addr init
    point.port = 65536;
    ASSERT_NE(0, copier.init(point));
    // normal init
    ASSERT_EQ(0, str2endpoint("127.0.0.1:60006", &point));
    ASSERT_EQ(0, copier.init(point));

    // normal copy dir
    system("chmod -R 755 ./a; chmod -R 755 ./b");
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; mkdir a; mkdir a/b; echo '123' > a/c"));
    ASSERT_EQ(0, copier.copy("./a", "./b", NULL));

    // normal copy dir and options;
    raft::CopyOptions options;
    ASSERT_EQ(0, copier.copy("./a", "./b", &options));

    // noent copy
    ASSERT_NE(0, copier.copy("./c", "./d", NULL));

    // src no permission read
    ASSERT_EQ(0, system("chmod 000 a/c"));
    ASSERT_NE(0, copier.copy("./a", "./b", NULL));
    ASSERT_EQ(0, system("chmod -R 755 ./a"));

    // dest no permisson delete
    ASSERT_EQ(0, system("chmod -R 555 ./b"));
    ASSERT_NE(0, copier.copy("./a", "./b", NULL));
    ASSERT_EQ(0, system("chmod -R 755 ./b"));

    // remove dir before copy
    ASSERT_EQ(0, system("rm -rf a; rm -rf b;"));
    ASSERT_NE(0, copier.copy("./a", "./b", NULL));

    // bigfile copy dir
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; mkdir a; mkdir a/b;"
                        "dd if=/dev/zero of=./a/1M.data bs=1024 count=1024"));
    ASSERT_EQ(0, copier.copy("./a", "./b", NULL));

    // normal copy file
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; mkdir a;"
                        "dd if=/dev/zero of=./1M.data bs=1024 count=1024"));
    ASSERT_EQ(0, copier.copy("./1M.data", "./b", NULL));

    // dest exist, copy file
    ASSERT_EQ(0, system("rm -rf b; mkdir b;"));
    ASSERT_EQ(0, copier.copy("./1M.data", "./b", NULL));

    // remove file before copy
    ASSERT_EQ(0, system("rm -rf b; rm -rf 1M.data;"));
    ASSERT_NE(0, copier.copy("./1M.data", "./b", NULL));

    ASSERT_EQ(0, system("rm -rf a; rm -rf b;"));
}
