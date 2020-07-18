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

class TestFileReadAdaptor : public braft::BufferedSequentialReadFileAdaptor {
public:
    TestFileReadAdaptor(int bytes, int align_size) {
        for (int i = 0; i < bytes; ++i) {
            char c = butil::fast_rand() % 26 + 'a';
            _buf.append(&c, 1);
        }
        _error = 0;
        _align_size = align_size;
    }
    butil::IOBuf& data() { return _buf; }
    void inject_error(int error) { _error = error; }

protected:
    virtual int do_read(butil::IOPortal* portal, size_t need_count, size_t* nread) {
        if (_error == 0) {
            need_count = (need_count + _align_size - 1) / _align_size * _align_size;
            *nread = std::min(need_count, _buf.size());
            _buf.cutn(portal, *nread);
        }
        int e = _error;
        _error = 0;
        return e;
    }

private:
    butil::IOBuf _buf;
    int _error;
    int _align_size;
};

TEST_F(TestFileSystemAdaptorSuits, test_buffered_sequential_read_file_adaptor_success) {
    int read_size[]  = { 1, 1, 10, 10, 13, 13, 201,         201, 1024, 1024 };
    int align_size[] = { 1, 4,  1, 11,  1, 20,   1, 1024 * 1024,    1, 1024 * 1024 + 1 };
    int index = 0;
    for (auto& rs : read_size) {
        TestFileReadAdaptor* file = new TestFileReadAdaptor(1 * 1024 * 1024, align_size[index++]);
        butil::IOBuf expected_data = file->data();
        butil::IOBuf read_data;
        size_t remain_size = expected_data.size();
        off_t offset = 0;
        while (remain_size > 0) {
            butil::IOPortal portal;
            ssize_t nread = file->read(&portal, offset, rs);

            if (nread < 0) {
                LOG(INFO) << "readsize: " << rs << ", offset: " << offset
                    << ", align_size: " << align_size[index]
                    << ", nread: " << nread << ", remain_size: " << remain_size;
            }
            ASSERT_TRUE(nread >= 0);
            ASSERT_TRUE(nread <= rs);
            ASSERT_TRUE(nread <= (ssize_t)remain_size);
            remain_size -= nread;
            read_data.append(portal);
            offset += nread;
            
        }
        ASSERT_EQ(expected_data, read_data);
        delete file;
    }
}

TEST_F(TestFileSystemAdaptorSuits, test_buffered_sequential_read_file_adaptor_fail) {
    int rs = 1024;
    TestFileReadAdaptor* file = new TestFileReadAdaptor(1 * 1024 * 1024, 1);
    butil::IOPortal portal;
    ssize_t nread = file->read(&portal, 0, rs);
    ASSERT_TRUE(nread == rs);
    nread = file->read(&portal, rs, rs);
    ASSERT_TRUE(nread == rs);
    nread = file->read(&portal, 0, rs);
    ASSERT_TRUE(nread < 0);
    file->inject_error(EIO);
    nread = file->read(&portal, rs, rs);
    ASSERT_TRUE(nread == rs);
    nread = file->read(&portal, 2 * rs, rs);
    ASSERT_TRUE(nread < 0);
    nread = file->read(&portal, 2 * rs, rs);
    ASSERT_TRUE(nread < 0);
    nread = file->read(&portal, 3 * rs, rs);
    ASSERT_TRUE(nread < 0);
    delete file;
}

// align each write to `_align_size'
class TestFileWriteAdapor : public braft::BufferedSequentialWriteFileAdaptor {
public:
    TestFileWriteAdapor(butil::IOPortal& portal, int align_size) {
        _portal = portal;
        _align_size = align_size;
        _error = 0;
    }
    butil::IOPortal& data() { return _portal; }
    void inject_error(int error) { _error = error; }

protected:
   
    virtual int do_write(const butil::IOBuf& data, size_t* nwrite) {
        butil::IOBuf piece_data(data);
        ssize_t left = piece_data.size();
        if (_error == 0) {
            while (left >= (size_t)_align_size) {
                BRAFT_VLOG << "current data size: " << left
                          << " align_size: " << _align_size
                          << " error: " << _error;
                size_t cut_size = piece_data.cutn(&_portal, _align_size);
                CHECK_EQ(cut_size, _align_size);
                *nwrite += cut_size;
                left -= cut_size;
            }
        }
        int e = _error;
        _error = 0;
        return e;
    }

private:
    butil::IOPortal _portal;
    int _error;
    int _align_size;
};

TEST_F(TestFileSystemAdaptorSuits, test_buffered_sequential_write_file_adaptor) {
    const int align_size = 8;
    butil::IOPortal target_file;
    TestFileWriteAdapor* file = new TestFileWriteAdapor(target_file, align_size);
    ASSERT_TRUE(file != NULL);
    butil::IOBuf src_file;
    for (int i = 0; i < 1024; i++) {
        char c = butil::fast_rand() % 26 + 'a';
        src_file.append(&c, 1);
    }
    std::string src_string = src_file.to_string(); 
    int offset = 0;
    size_t remain_size = src_file.size();
    int times = 0;
    butil::IOBuf tmp_buf;
    while (remain_size > 0) {
        ++times;
        tmp_buf.clear();
        int rc = src_file.cutn(&tmp_buf, align_size/2);        
        ASSERT_EQ(rc, align_size/2);
        remain_size -= align_size/2;
        
        ssize_t nwrite = file->write(tmp_buf, offset);
        BRAFT_VLOG << "write into adaptor No_" << times << ", offset: " << offset
                  << ", size: " << tmp_buf.size()
                  << ", data: " << tmp_buf
                  << ", nwrite: " << nwrite;
        ASSERT_EQ(nwrite, align_size/2);
        offset += nwrite;
        if (times % 2 == 0) {
            ASSERT_EQ(file->data().size(), times * (align_size/2)); 
        } else {
            ASSERT_EQ(file->data().size(), (times - 1) * (align_size/2)); 
        } 
    }
    ASSERT_EQ(src_string, file->data().to_string());
    delete file;
}

class TestFileReadAdaptorWithHole : public braft::BufferedSequentialReadFileAdaptor {
public:
    TestFileReadAdaptorWithHole(const std::string& path, int align_size) 
        : _path(path)
        , _align_size(align_size)
    {
        _fd = ::open(_path.c_str(), O_RDONLY | O_CLOEXEC, 0644);
        _error = 0;
    }
    void inject_error(int error) { _error = error; }

protected:
    virtual int do_read(butil::IOPortal* portal, size_t need_count, size_t* nread) {
        if (_error == 0) {
            need_count = (need_count + _align_size - 1) / _align_size * _align_size;
            *nread = portal->append_from_file_descriptor(_fd, need_count);
            LOG(INFO) << "do_read need_count: " << need_count << ", nread: " << *nread
                      << ", portal size: " << portal->size()
                      << ", portal data: " << *portal;
        }
        int e = _error;
        _error = 0;
        return e;
    }

private:
    std::string _path;
    int _fd;
    int _error;
    int _align_size;
};

class TestFileWriteAdaptorWithHole : public braft::BufferedSequentialWriteFileAdaptor {
public:
    TestFileWriteAdaptorWithHole(const std::string& path, int align_size) 
        : _path(path) 
        , _align_size(align_size)
    {
        _fd = open(_path.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        _error = 0;
    }
    void inject_error(int error) { _error = error; }

protected:
   
    virtual int do_write(const butil::IOBuf& data, size_t* nwrite) {
        butil::IOBuf piece_data(data);
        size_t left = piece_data.size();
        if (_error == 0) {
            while (left >= (size_t)_align_size) {
                LOG(INFO) << "do_write current data size: " << left
                          << " align_size: " << _align_size
                          << " error: " << _error;
                ssize_t write_size = piece_data.cut_into_file_descriptor(_fd, _align_size);
                CHECK_EQ(write_size, _align_size);
                *nwrite += (size_t)write_size;
                left -= (size_t)write_size;
            }
        }
        int e = _error;
        _error = 0;
        return e;
    }

    virtual void seek(off_t offset) {
        ::ftruncate(_fd, offset); 
        off_t ret_off = ::lseek(_fd, 0, SEEK_END);
        if (ret_off < 0) {
            LOG(ERROR) << "Fail to lseek fd= " << _fd << " to offset= " << offset
                      << ", path: " << _path;
        }
        LOG(INFO) << "Succeed to lseek fd= " << _fd << " to offset= " << offset
                  << ", path: " << _path;
        _buffer_offset = offset;
    }

private:
    std::string _path;
    int _fd;
    int _align_size;
    int _error;
};

TEST_F(TestFileSystemAdaptorSuits, test_buffered_sequential_writer_with_hole) {
    ::system("rm -rf test_reader_with_hole");
    ::system("rm -rf test_writer_with_hole");
    const std::string reader_path = "test_reader_with_hole";
    int fd = open(reader_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    char tmp_buf[9];
    snprintf(tmp_buf, sizeof(tmp_buf), "hello000");
    ssize_t nwritten = pwrite(fd, tmp_buf, strlen(tmp_buf), 0);
    ASSERT_EQ((size_t)nwritten, strlen(tmp_buf));
    snprintf(tmp_buf, sizeof(tmp_buf), "hello111");
    nwritten = pwrite(fd, tmp_buf, strlen(tmp_buf), 16);
    ASSERT_EQ((size_t)nwritten, strlen(tmp_buf)); 
    
    const int align_size = 8;
    TestFileReadAdaptorWithHole* reader = new TestFileReadAdaptorWithHole(reader_path, align_size);
    ASSERT_TRUE(reader != NULL);

    const std::string writer_path = "test_writer_with_hole";
    TestFileWriteAdaptorWithHole* writer = new TestFileWriteAdaptorWithHole(writer_path, align_size);
    ASSERT_TRUE(writer != NULL);
   
    { 
        butil::IOPortal buf; 
        off_t offset = 0;
        bool is_eof = false;
        const size_t per_rpc_size = 8;
        while (!is_eof) {
            // read
            ssize_t nread = reader->read(&buf, offset, per_rpc_size);
            ASSERT_TRUE(nread >= 0); 
            if ((size_t)nread < per_rpc_size) {
                is_eof = true;
            } else {
                ssize_t file_size = reader->size();
                ASSERT_TRUE(file_size >= 0);
                if ((size_t)file_size == offset + (size_t)per_rpc_size) {
                    is_eof = true;
                }
            }
            if (!braft::is_zero(buf.to_string().c_str(), buf.size())) {
                // write
                ssize_t nwrite = writer->write(buf, offset);
                ASSERT_TRUE(nwrite >= 0);
            }
            
            buf.clear();
            offset += nread;
        }
    }
    // check data, including hole
    int ret = system("diff -a test_reader_with_hole test_writer_with_hole");
    ASSERT_EQ(0, ret);
    delete reader;
    delete writer;
}
