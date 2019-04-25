// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zheng,PengFei(zhengpengfei@baidu.com)

#ifndef  BRAFT_FILE_SYSTEM_ADAPTOR_H
#define  BRAFT_FILE_SYSTEM_ADAPTOR_H

#include <fcntl.h>
#include <butil/file_util.h>
#include <butil/files/file.h>                        // butil::File
#include <butil/files/dir_reader_posix.h>            // butil::DirReaderPosix
#include <butil/memory/ref_counted.h>                // butil::RefCountedThreadSafe
#include <butil/memory/singleton.h>                  // Singleton
#include <google/protobuf/message.h>                // google::protobuf::Message
#include "braft/util.h"
#include "braft/fsync.h"

#ifndef O_CLOEXEC  
#define O_CLOEXEC   02000000    /*  define close_on_exec if not defined in fcntl.h*/  
#endif

namespace braft {

// DirReader iterates a directory to get sub directories and files, `.' and `..'
// should be ignored
class DirReader {
public:
    DirReader() {}
    virtual ~DirReader() {}

    // Check if this dir reader is valid
    virtual bool is_valid() const = 0;

    // Move to next entry(directory or file) in the directory
    // Return true if a entry can be found, false otherwise
    virtual bool next() = 0;

    // Get the name of current entry
    virtual const char* name() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(DirReader);
};

template <typename T>
struct DestroyObj {
    void operator()(T* const obj) { obj->close(); delete obj; }
};


class FileAdaptor {
public:
    virtual ~FileAdaptor() {}
    // Write to the file. Different from posix ::pwrite(), write will retry automatically
    // when occur EINTR.
    // Return |data.size()| if successful, -1 otherwise.
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) = 0;

    // Read from the file. Different from posix ::pread(), read will retry automatically
    // when occur EINTR.
    // Return a non-negative integer less than or equal to |size| if successful, -1 otherwise.
    // In the case of EOF, the return value is a non-negative integer less than |size|.
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) = 0;

    // Get the size of the file
    virtual ssize_t size() = 0;

    // Sync data of the file to disk device
    virtual bool sync() = 0;

    // Close the descriptor of this file adaptor
    virtual bool close() = 0;

protected:

    FileAdaptor() {}

private:
    DISALLOW_COPY_AND_ASSIGN(FileAdaptor);
};

class FileSystemAdaptor : public butil::RefCountedThreadSafe<FileSystemAdaptor> {
public:
    FileSystemAdaptor() {}
    virtual ~FileSystemAdaptor() {}

    // Open a file, oflag can be any valid combination of flags used by posix ::open(),
    // file_meta can be used to pass additinal metadata, it won't be modified, and should 
    // be valid until the file is destroyed.
    virtual FileAdaptor* open(const std::string& path, int oflag, 
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e) = 0;

    // Deletes the given path, whether it's a file or a directory. If it's a directory,
    // it's perfectly happy to delete all of the directory's contents. Passing true to 
    // recursive deletes subdirectories and their contents as well.
    // Returns true if successful, false otherwise. It is considered successful
    // to attempt to delete a file that does not exist.
    virtual bool delete_file(const std::string& path, bool recursive) = 0;

    // The same as posix ::rename(), will change the name of the old path.
    virtual bool rename(const std::string& old_path, const std::string& new_path) = 0;

    // The same as posix ::link(), will link the old path to the new path.
    virtual bool link(const std::string& old_path, const std::string& new_path) = 0;

    // Creates a directory. If create_parent_directories is true, parent directories
    // will be created if not exist, otherwise, the create operation will fail.
    // Returns 'true' on successful creation, or if the directory already exists. 
    virtual bool create_directory(const std::string& path, 
                                  butil::File::Error* error,
                                  bool create_parent_directories) = 0;

    // Returns true if the given path exists on the filesystem, false otherwise.
    virtual bool path_exists(const std::string& path) = 0;

    // Returns true if the given path exists and is a directory, false otherwise.
    virtual bool directory_exists(const std::string& path) = 0;

    // Get a directory reader to read all sub entries inside a directory. It will
    // not recursively search the directory.
    virtual DirReader* directory_reader(const std::string& path) = 0;

    // This method will be called at the very begin before read snapshot file.
    // The default implemention is return 'true' directly.
    virtual bool open_snapshot(const std::string& /*snapshot_path*/) { return true; }
    
    // This method will be called after read all snapshot files or failed.
    // The default implemention is return directly.
    virtual void close_snapshot(const std::string& /*snapshot_path*/) {}
private:
    DISALLOW_COPY_AND_ASSIGN(FileSystemAdaptor);
};

// DirReader iterates a directory to get names of all sub directories and files,
// except `.' and `..'.
class PosixDirReader : public DirReader {
friend class PosixFileSystemAdaptor;
public:
    virtual ~PosixDirReader() {}

    // Check if the dir reader is valid
    virtual bool is_valid() const;

    // Move to next entry in the directory
    // Return true if a entry can be found, false otherwise
    virtual bool next();

    // Get the name of current entry
    virtual const char* name() const;

protected:
    PosixDirReader(const std::string& path) : _dir_reader(path.c_str()) {}

private:
    butil::DirReaderPosix _dir_reader;
};

class PosixFileAdaptor : public FileAdaptor {
friend class PosixFileSystemAdaptor;
public:
    virtual ~PosixFileAdaptor();

    virtual ssize_t write(const butil::IOBuf& data, off_t offset);
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size);
    virtual ssize_t size();
    virtual bool sync();
    virtual bool close();

protected:
    PosixFileAdaptor(int fd) : _fd(fd) {}

private:
    int _fd;
};

class PosixFileSystemAdaptor : public FileSystemAdaptor {
public:
    PosixFileSystemAdaptor() {}
    virtual ~PosixFileSystemAdaptor() {}

    virtual FileAdaptor* open(const std::string& path, int oflag, 
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e);
    virtual bool delete_file(const std::string& path, bool recursive);
    virtual bool rename(const std::string& old_path, const std::string& new_path);
    virtual bool link(const std::string& old_path, const std::string& new_path);
    virtual bool create_directory(const std::string& path, 
                                  butil::File::Error* error,
                                  bool create_parent_directories);
    virtual bool path_exists(const std::string& path);
    virtual bool directory_exists(const std::string& path);
    virtual DirReader* directory_reader(const std::string& path);
};

// Get a default file system adapotor, it's a singleton PosixFileSystemAdaptor.
FileSystemAdaptor* default_file_system();

// Convert butil::File::Error to os error
int file_error_to_os_error(butil::File::Error e);

// Create a sub directory of an existing |parent_path|. Requiring that
// |parent_path| must exist.
// Returns true on successful creation, or if the directory already exists.
// Returns false on failure and sets *error appropriately, if it is non-NULL.
bool create_sub_directory(const std::string& parent_path,
                          const std::string& sub_path,
                          FileSystemAdaptor* fs = NULL,
                          butil::File::Error* error = NULL);

} //  namespace braft

#endif // BRAFT_FILE_SYSTEM_ADAPTOR_H
