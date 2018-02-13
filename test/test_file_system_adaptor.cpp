// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved

// Author: ZhengPengFei (zhengpengfei@baidu.com)
// Date: 2017/06/16 10:29:05

#include <gtest/gtest.h>
#include "braft/file_system_adaptor.h"

class TestFileSystemAdaptorSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestFileSystemAdaptorSuits, read_write) {
    ::system("rm -f test_file");
    ::system("rm -f test_file1");
    scoped_refptr<braft::FileSystemAdaptor> fs = new braft::PosixFileSystemAdaptor();
    butil::File::Error e;
    braft::FileAdaptor* file = fs->open("test_file", O_CREAT | O_TRUNC | O_RDWR, NULL, &e);
    ASSERT_TRUE(file != NULL);
    ASSERT_EQ(file->size(), 0);

    butil::IOBuf data;
    data.append("ccccc");
    ASSERT_EQ(data.size(), file->write(data, 0));
    ASSERT_EQ(data.size(), file->write(data, data.size() * 2));
    ASSERT_EQ(file->size(), data.size() * 3);

    butil::IOPortal portal;
    ASSERT_EQ(data.size(), file->read(&portal, 0, data.size()));
    ASSERT_EQ(portal.to_string(), data.to_string());
    ASSERT_EQ(2, file->read(&portal, data.size() * 3 - 2, 10));
    ASSERT_EQ(0, file->read(&portal, data.size() * 3 + 1, 10));
    delete file;

    file = fs->open("test_file", O_RDWR, NULL, &e);
    portal.clear();
    ASSERT_EQ(data.size(), file->read(&portal, 0, data.size()));
    ASSERT_EQ(portal.to_string(), data.to_string());
    ASSERT_EQ(2, file->read(&portal, data.size() * 3 - 2, 10));
    ASSERT_EQ(0, file->read(&portal, data.size() * 3 + 1, 10));
    delete file;

    file = fs->open("test_file1", O_RDWR, NULL, &e);
    ASSERT_TRUE(file == NULL);
    ASSERT_EQ(butil::File::FILE_ERROR_NOT_FOUND, e);

    ::system("rm -f test_file");
    ::system("rm -f test_file1");
}

TEST_F(TestFileSystemAdaptorSuits, delete_file) {
    ::system("rm -f test_file");
    ::system("touch test_file");
    scoped_refptr<braft::FileSystemAdaptor> fs = new braft::PosixFileSystemAdaptor();
    ASSERT_TRUE(fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_file"));
    ASSERT_TRUE(fs->delete_file("test_file", false));
    ASSERT_TRUE(!fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_file"));
    ASSERT_TRUE(fs->delete_file("test_file", false));
    ASSERT_TRUE(!fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_file"));

    ::system("rm -rf test_dir/");
    ::system("mkdir -p test_dir/test_dir/ && touch test_dir/test_dir/test_file");
    ASSERT_TRUE(fs->path_exists("test_dir"));
    ASSERT_TRUE(fs->directory_exists("test_dir"));
    ASSERT_TRUE(fs->path_exists("test_dir/test_dir/"));
    ASSERT_TRUE(fs->directory_exists("test_dir/test_dir/"));
    ASSERT_TRUE(fs->path_exists("test_dir/test_dir/test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_dir/test_dir/test_file"));

    ASSERT_TRUE(!fs->delete_file("test_dir", false));
    ASSERT_TRUE(!fs->delete_file("test_dir/test_dir", false));
    ASSERT_TRUE(fs->delete_file("test_dir/test_dir", true));
    ASSERT_TRUE(fs->delete_file("test_dir", false));
}

TEST_F(TestFileSystemAdaptorSuits, rename) {
    ::system("rm -f test_file");
    ::system("touch test_file");
    scoped_refptr<braft::FileSystemAdaptor> fs = new braft::PosixFileSystemAdaptor();
    ASSERT_TRUE(fs->rename("test_file", "test_file2"));
    ASSERT_TRUE(fs->rename("test_file2", "test_file2"));
    ::system("touch test_file");
    ASSERT_TRUE(fs->rename("test_file2", "test_file"));
    ASSERT_TRUE(fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->path_exists("test_file2"));

    ::system("rm -rf test_dir");
    ::system("mkdir test_dir");
    ASSERT_TRUE(!fs->rename("test_file", "test_dir"));
    ASSERT_TRUE(fs->rename("test_file", "test_dir/test_file"));

    ::system("rm -rf test_dir1");
    ::system("mkdir test_dir1 && touch test_dir1/test_file");
    ASSERT_TRUE(!fs->rename("test_dir", "test_dir1"));

    ::system("rm -f test_dir1/test_file");
    ASSERT_TRUE(fs->rename("test_dir", "test_dir1"));
    ASSERT_TRUE(!fs->directory_exists("test_dir"));
    ASSERT_TRUE(fs->directory_exists("test_dir1"));
    ASSERT_TRUE(fs->path_exists("test_dir1/test_file"));

    ::system("rm -rf test_dir1");
}

TEST_F(TestFileSystemAdaptorSuits, create_directory) {
    ::system("rm -rf test_dir");
    scoped_refptr<braft::FileSystemAdaptor> fs = new braft::PosixFileSystemAdaptor();
    butil::File::Error error;
    ASSERT_TRUE(fs->create_directory("test_dir", &error, false));
    ASSERT_TRUE(fs->create_directory("test_dir", &error, false));
    ASSERT_TRUE(!fs->create_directory("test_dir/test_dir/test_dir", &error, false));
    ASSERT_EQ(error, butil::File::FILE_ERROR_NOT_FOUND);
    ASSERT_TRUE(fs->create_directory("test_dir/test_dir/test_dir", &error, true));
    ASSERT_TRUE(fs->create_directory("test_dir/test_dir", &error, true));

    ::system("touch test_dir/test_file");
    ASSERT_TRUE(!fs->create_directory("test_dir/test_file", &error, true));
    ASSERT_EQ(error, butil::File::FILE_ERROR_EXISTS);

    ASSERT_TRUE(!braft::create_sub_directory("test_dir/test_dir2", "test_dir2/test2", fs, &error));
    ASSERT_EQ(error, butil::File::FILE_ERROR_NOT_FOUND);

    ASSERT_TRUE(braft::create_sub_directory("test_dir", "test_dir2/test2", fs, &error));
    ASSERT_TRUE(fs->directory_exists("test_dir/test_dir2/test2"));

    ::system("rm -rf test_dir");
}

TEST_F(TestFileSystemAdaptorSuits, directory_reader) {
    ::system("rm -rf test_dir");
    ::system("mkdir -p test_dir/test_dir && touch test_dir/test_file");
    scoped_refptr<braft::FileSystemAdaptor> fs = new braft::PosixFileSystemAdaptor();
    braft::DirReader* dir_reader = fs->directory_reader("test_dir");
    std::set<std::string> names;
    names.insert("test_dir");
    names.insert("test_file");
    ASSERT_TRUE(dir_reader->is_valid());
    while (dir_reader->next())  {
        std::string n = dir_reader->name();
        ASSERT_EQ(1, names.count(n));
        names.erase(dir_reader->name());
    }
    ASSERT_TRUE(names.empty());
    delete dir_reader;

    ::system("rm -rf test_dir");
    dir_reader = fs->directory_reader("test_dir");
    ASSERT_TRUE(!dir_reader->is_valid());
    delete dir_reader;
}

TEST_F(TestFileSystemAdaptorSuits, create_sub_directory) {
    ::system("rm -rf test_dir");
    ::system("mkdir test_dir");
    scoped_refptr<braft::FileSystemAdaptor> fs = new braft::PosixFileSystemAdaptor();
    std::string parent_path = "test_dir/sub1/";
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "/", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "/sub2", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "/sub2/sub3", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "sub4/sub5", fs, NULL));
    ASSERT_FALSE(fs->directory_exists(parent_path + "sub2/sub3"));
    ASSERT_FALSE(fs->directory_exists(parent_path + "sub4/sub5"));
    ASSERT_FALSE(fs->directory_exists(parent_path));
    ASSERT_TRUE(fs->create_directory(parent_path, NULL, false));
    ASSERT_TRUE(braft::create_sub_directory(parent_path, "/sub2/sub3", fs, NULL));
    ASSERT_TRUE(braft::create_sub_directory(parent_path, "sub4/sub5", fs, NULL));
    ASSERT_TRUE(fs->directory_exists(parent_path + "sub2/sub3"));
    ASSERT_TRUE(fs->directory_exists(parent_path + "sub4/sub5"));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "../sub4/sub5", fs, NULL));
    ::system("rm -rf test_dir");
}
