// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/06 15:40:45

#include <gtest/gtest.h>
#include <base/logging.h>

#include <baidu/rpc/server.h>
#include "raft/file_service.h"
#include "raft/util.h"
#include "raft/remote_file_copier.h"

class FileServiceTest : public testing::Test {
protected:
    void SetUp() {
        logging::FLAGS_verbose = 90;
        ASSERT_EQ(0, _server.AddService(raft::file_service(), 
                                        baidu::rpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, _server.Start(60006, NULL));
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    baidu::rpc::Server _server;
};

TEST_F(FileServiceTest, sanity) {
    scoped_refptr<raft::LocalDirReader> reader(new raft::LocalDirReader("a"));
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));
    std::string uri;
    base::string_printf(&uri, "remote://127.0.0.1:60006/%ld", reader_id);
    raft::RemoteFileCopier copier;
    ASSERT_NE(0, copier.init("local://127.0.0.1:60006/123456"));
    ASSERT_NE(0, copier.init("remote://127.0.0.1:60006//123456"));
    ASSERT_NE(0, copier.init("remote://127.0.1:60006//123456"));
    ASSERT_NE(0, copier.init("remote://127.0.0.1//123456"));
    ASSERT_EQ(0, copier.init(uri));

    // normal copy dir
    system("chmod -R 755 ./a; chmod -R 755 ./b");
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; mkdir a; mkdir a/b; echo '123' > a/c"));
    ASSERT_EQ(0, copier.copy_to_file("c", "./b/c", NULL));
    base::IOBuf c_data;
    ASSERT_EQ(0, copier.copy_to_iobuf("c", &c_data, NULL));
    ASSERT_TRUE(c_data.equals("123\n")) << c_data.to_string();
    // Copy Directory is not allowed
    ASSERT_NE(0, copier.copy_to_file("b", "./b/b", NULL));

    // Copy non-existed file
    ASSERT_NE(0, copier.copy_to_file("d", "./b/d", NULL));

    // src no permission read
    ASSERT_EQ(0, system("chmod 000 a/c"));
    ASSERT_NE(0, copier.copy_to_file("c", "./b/cc", NULL));
    ASSERT_EQ(0, system("chmod -R 755 ./a"));

    ASSERT_EQ(0, raft::file_service_remove(reader_id));

    // Copy after reader is remove
    ASSERT_NE(0, copier.copy_to_file("c", "./b/d", NULL));
    ASSERT_EQ(0, system("rm -rf a; rm -rf b;"));
}

TEST_F(FileServiceTest, hole_file) {
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; mkdir a;"));

    LOG(INFO) << "build hole file";
    int fd = ::open("./a/hole.data", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    ASSERT_GE(fd, 0);
    for (int i = 0; i < 10; i++) {
        char buf[1024] = {0};
        snprintf(buf, sizeof(buf), "hello %d", i);
        ssize_t nwriten = pwrite(fd, buf, strlen(buf), 128 * 1024 * i);
        ASSERT_EQ(static_cast<size_t>(nwriten), strlen(buf));
    }
    ::close(fd);
    scoped_refptr<raft::LocalDirReader> reader(new raft::LocalDirReader("a"));
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));

    raft::RemoteFileCopier copier;
    std::string uri;
    base::string_printf(&uri, "remote://127.0.0.1:60006/%ld", reader_id);
    // normal init
    ASSERT_EQ(0, copier.init(uri));
    ASSERT_EQ(0, copier.copy_to_file("hole.data", "./b/hole.data", NULL));
}
