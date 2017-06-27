// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved

// Author: ZhengPengFei (zhengpengfei@baidu.com)
// Date: 2017/06/24 15:32:46

#include <base/fd_utility.h>                        // base::make_close_on_exec
#include <base/memory/singleton_on_pthread_once.h>  // base::get_leaky_singleton
#include "raft/file_system_adaptor.h"

namespace raft {

bool PosixDirReader::is_valid() const {
    return _dir_reader.IsValid();
}

bool PosixDirReader::next() {
    bool rc = _dir_reader.Next();
    while (rc && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
        rc = _dir_reader.Next();
    }
    return rc;
}

const char* PosixDirReader::name() const {
    return _dir_reader.name();
}

PosixFileAdaptor::~PosixFileAdaptor() {
    ::close(_fd);
}

ssize_t PosixFileAdaptor::write(const base::IOBuf& data, off_t offset) {
    return raft::file_pwrite(data, _fd, offset);
}

ssize_t PosixFileAdaptor::read(base::IOPortal* portal, off_t offset, size_t size) {
    return raft::file_pread(portal, _fd, offset, size);
}

ssize_t PosixFileAdaptor::size() {
    off_t sz = lseek(_fd, 0, SEEK_END);
    return ssize_t(sz);
}

bool PosixFileAdaptor::sync() {
    return raft_fsync(_fd) == 0;
}

static pthread_once_t s_check_cloexec_once = PTHREAD_ONCE_INIT;
static bool s_support_cloexec_on_open = false;

static void check_cloexec(void) {
    int fd = ::open("/dev/zero", O_RDONLY | O_CLOEXEC, 0644);
    s_support_cloexec_on_open = (fd != -1);
}

FileAdaptor* PosixFileSystemAdaptor::open(const std::string& path, int oflag, 
                                     const ::google::protobuf::Message* file_meta,
                                     base::File::Error* e) {
    (void) file_meta;
    pthread_once(&s_check_cloexec_once, check_cloexec);
    bool cloexec = (oflag & O_CLOEXEC);
    if (cloexec && !s_support_cloexec_on_open) {
        oflag &= (~O_CLOEXEC);
    }
    int fd = ::open(path.c_str(), oflag, 0644);
    if (e) {
        *e = (fd == -1) ? base::File::OSErrorToFileError(errno) : base::File::FILE_OK;
    }
    if (fd == -1) {
        return NULL;
    }
    if (cloexec && !s_support_cloexec_on_open) {
        base::make_close_on_exec(fd);
    }
    return new PosixFileAdaptor(fd);
}

bool PosixFileSystemAdaptor::delete_file(const std::string& path, bool recursive) {
    base::FilePath file_path(path);
    return base::DeleteFile(file_path, recursive);
}

bool PosixFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
    return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool PosixFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
    return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool PosixFileSystemAdaptor::create_directory(const std::string& path, 
                                         base::File::Error* error,
                                         bool create_parent_directories) {
    base::FilePath dir(path);
    return base::CreateDirectoryAndGetError(dir, error, create_parent_directories);
}

bool PosixFileSystemAdaptor::path_exists(const std::string& path) {
    base::FilePath file_path(path);
    return base::PathExists(file_path);
}

bool PosixFileSystemAdaptor::directory_exists(const std::string& path) {
    base::FilePath file_path(path);
    return base::DirectoryExists(file_path);
}

DirReader* PosixFileSystemAdaptor::directory_reader(const std::string& path) {
    return new PosixDirReader(path.c_str());
}

FileSystemAdaptor* default_file_system() {
    static scoped_refptr<PosixFileSystemAdaptor> fs = 
        base::get_leaky_singleton<PosixFileSystemAdaptor>();
    return fs.get();
}

int file_error_to_os_error(base::File::Error e) {
    switch (e) {
        case base::File::FILE_OK: 
            return 0;
        case base::File::FILE_ERROR_ACCESS_DENIED:
            return EACCES;
        case base::File::FILE_ERROR_EXISTS:
            return EEXIST;
        case base::File::FILE_ERROR_NOT_FOUND:
            return ENOENT;
        case base::File::FILE_ERROR_TOO_MANY_OPENED:
            return EMFILE;
        case base::File::FILE_ERROR_NO_MEMORY:
            return ENOMEM;
        case base::File::FILE_ERROR_NO_SPACE:
            return ENOSPC;
        case base::File::FILE_ERROR_NOT_A_DIRECTORY:
            return ENOTDIR;
        case base::File::FILE_ERROR_IO:
            return EIO;
        default:
            return EINVAL;
    };
}

bool create_sub_directory(const std::string& parent_path,
                          const std::string& sub_path,
                          FileSystemAdaptor* fs,
                          base::File::Error* error) {
    if (!fs) {
        fs = default_file_system();
    }
    if (!fs->directory_exists(parent_path)) {
        if (error) {
            *error = base::File::FILE_ERROR_NOT_FOUND;
        }
        return false;
    }
    base::FilePath sub_dir_path(sub_path);
    if (sub_dir_path.ReferencesParent()) {
        if (error) {
            *error = base::File::FILE_ERROR_INVALID_URL;
        }
        return false;
    }
    std::vector<base::FilePath> subpaths;

    // Collect a list of all parent directories.
    base::FilePath last_path = sub_dir_path;
    subpaths.push_back(sub_dir_path.BaseName());
    for (base::FilePath path = last_path.DirName();
            path.value() != last_path.value(); path = path.DirName()) {
        subpaths.push_back(path.BaseName());
        last_path = path;
    }
    base::FilePath full_path(parent_path);
    for (std::vector<base::FilePath>::reverse_iterator i = subpaths.rbegin();
            i != subpaths.rend(); ++i) {
        if (i->value() == "/") {
            continue;
        }
        if (i->value() == ".") {
            continue;
        }
        full_path = full_path.Append(*i);
        DLOG(INFO) << "Creating " << full_path.value();
        if (!fs->create_directory(full_path.value(), error, false)) {
            return false;
        }
    }
    return true;
}


} // namespace raft
