// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/02 01:49:50

#include "raft/util.h"

#include <stdlib.h>
#include <base/macros.h>
#include <base/raw_pack.h>                     // base::RawPacker
#include <base/file_util.h>

#include "raft/raft.h"

namespace raft {

static void* run_closure(void* arg) {
    ::google::protobuf::Closure *c = (google::protobuf::Closure*)arg;
    if (c) {
        c->Run();
    }
    return NULL;
}

void run_closure_in_bthread(google::protobuf::Closure* closure) {
    DCHECK(closure);
    bthread_t tid;
    int ret = bthread_start_urgent(&tid, NULL, run_closure, closure);
    if (0 != ret) {
        PLOG(ERROR) << "Fail to start bthread";
        return closure->Run();
    }
}

void run_closure_in_bthread_nosig(google::protobuf::Closure* closure) {
    DCHECK(closure);
    bthread_t tid;
    bthread_attr_t attr;
    if (bthread_attr_init(&attr) != 0) {
        PLOG(ERROR) << "Fail to init bthread_attr";
        return closure->Run();
    }
    attr.flags = BTHREAD_NOSIGNAL;
    int ret = bthread_start_background(&tid, &attr, run_closure, closure);
    if (0 != ret) {
        PLOG(ERROR) << "Fail to start bthread";
        return closure->Run();
    }
}

std::string fileuri2path(const std::string& uri) {
    std::string path;
    std::size_t prefix_found = uri.find("file://");
    if (std::string::npos == prefix_found) {
        if (std::string::npos == uri.find("://")) {
            path = uri;
        }
    } else {
        // file://data -> data
        // file://./data/log -> data/log
        // file://data/log -> data/log
        // file://1.2.3.4:5678/data/log -> data/log
        // file://www.baidu.com:80/data/log -> data/log
        base::EndPoint addr;
        if (0 != fileuri_parse(uri, &addr, &path)) {
            std::size_t cursor = prefix_found + strlen("file://");
            path.assign(uri, cursor, uri.size() - cursor);
        }
    }

    return path;
}

int fileuri_parse(const std::string& uri, base::EndPoint* addr, std::string* path) {
    std::size_t prefix_found = uri.find("file://");
    if (std::string::npos == prefix_found) {
        return EINVAL;
    }

    std::size_t path_found = uri.find("/", prefix_found + strlen("file://") + 1);
    if (std::string::npos == path_found) {
        return EINVAL;
    }

    std::size_t addr_found = prefix_found + strlen("file://");
    std::string addr_str;
    addr_str.assign(uri, addr_found, path_found - addr_found);
    path->clear();
    // skip first /
    path->assign(uri, path_found + 1, uri.size() - (path_found + 1));

    if (0 != base::str2endpoint(addr_str.c_str(), addr) &&
        //            ^^^ Put str2endpoint in front as it's much faster than
        //            hostname2endpoint
        0 != base::hostname2endpoint(addr_str.c_str(), addr)) {
        return EINVAL;
    }

    return 0;
}

ssize_t file_pread(base::IOPortal* portal, int fd, off_t offset, size_t size) {
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t read_len = portal->pappend_from_file_descriptor(
                fd, offset, static_cast<size_t>(left));
        if (read_len > 0) {
            left -= read_len;
            offset += read_len;
        } else if (read_len == 0) {
            break;
        } else if (errno == EINTR) {
            continue;
        } else {
            LOG(WARNING) << "read failed, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            return -1;
        }
    }

    return size - left;
}

ssize_t file_pwrite(const base::IOBuf& data, int fd, off_t offset) {
    size_t size = data.size();
    base::IOBuf piece_data(data);
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t writen = piece_data.pcut_into_file_descriptor(fd, offset, left);
        if (writen >= 0) {
            offset += writen;
            left -= writen;
        } else if (errno == EINTR) {
            continue;
        } else {
            LOG(WARNING) << "write falied, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            return -1;
        }
    }

    return size - left;
}

void FileSegData::append(void* data, uint64_t offset, uint32_t len) {
    if (0 != _seg_offset && offset == (_seg_offset + _seg_len)) {
        // append to last segment
        _seg_len += len;
        _data.append(data, len);
    } else {
        // close last segment
        char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
        if (_seg_len > 0) {
            ::base::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
            CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
        }

        // start new segment
        _seg_offset = offset;
        _seg_len = len;
        _seg_header = _data.reserve(sizeof(seg_header));
        CHECK(_seg_header != base::IOBuf::INVALID_AREA);
        _data.append(data, len);
    }
}

void FileSegData::close() {
    char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
    if (_seg_len > 0) {
        ::base::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
        CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
    }

    _seg_offset = 0;
    _seg_len = 0;
}

size_t FileSegData::next(uint64_t* offset, base::IOBuf* data) {
    if (_data.length() == 0) {
        return 0;
    }

    char header_buf[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
    size_t header_len = _data.cutn(header_buf, sizeof(header_buf));
    CHECK_EQ(header_len, sizeof(header_buf)) << "header_len: " << header_len;

    uint64_t seg_offset = 0;
    uint32_t seg_len = 0;
    ::base::RawUnpacker(header_buf).unpack64(seg_offset).unpack32(seg_len);

    *offset = seg_offset;
    size_t body_len = _data.cutn(data, seg_len);
    CHECK_EQ(body_len, seg_len) << "seg_len: " << seg_len << " body_len: " << body_len;
    return seg_len;
}

bool PathACL::check(const std::string& path) {
    std::string real_path;
    if (!PathACL::normalize_path(path, &real_path)) {
        return false;
    }

    base::DoublyBufferedData<AccessList>::ScopedPtr ptr;
    if (_acl.Read(&ptr) != 0) {
        return false;
    }

    AccessList::const_iterator it = ptr->upper_bound(real_path);
    if (it == ptr->begin()) {
        return false;
    }
    --it;

    int rc = real_path.compare(*it);
    if (rc == 0) {
        return true;
    } else if (rc < 0) {
        return false;
    } else if (PathACL::is_sub_path((*it), real_path)) {
        return true;
    }

    return false;
}

bool PathACL::add(const std::string& path) {
    std::string real_path;
    if (!PathACL::normalize_path(path, &real_path)) {
        return false;
    }

    return _acl.Modify(add_path, real_path) != 0;
}

bool PathACL::remove(const std::string& path) {
    std::string real_path;
    if (!PathACL::normalize_path(path, &real_path)) {
        return false;
    }

    return _acl.Modify(remove_path, real_path) != 0;
}

bool PathACL::is_sub_path(const std::string& parent, const std::string& child) {
    if (child.length() >= parent.length() &&
        strncmp(child.c_str(), parent.c_str(), parent.length()) == 0) {
        if (child.length() == parent.length() ||
            (child.length() > parent.length() && child.at(parent.length()) == '/')) {
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

bool PathACL::normalize_path(const std::string& path, std::string* real_path) {
    base::FilePath real_file_path;
    base::FilePath file_path(path);
    std::vector<base::FilePath::StringType> components;
    file_path.GetComponents(&components);

    bool is_absolute = false;
    std::vector<base::FilePath::StringType> real_components;
    for (size_t i = 0; i < components.size(); i++) {
        // maybe is // at head
        if (components[i].at(0) == '/') {
            if (i == 0) {
                is_absolute = true;
            }
            continue;
        } else if (components[i] == ".") {
            continue;
        } else if (components[i] == "..") {
            if (real_components.size() > 0 && real_components.back() != "..") {
                real_components.erase(real_components.end() - 1);
            } else {
                real_components.push_back(components[i]);
            }
        } else {
            real_components.push_back(components[i]);
        }
    }

    real_path->clear();
    if (is_absolute) {
        real_path->append("/");
    }
    for (size_t i = 0; i < real_components.size(); i++) {
        real_path->append(real_components[i]);
        if (i < real_components.size() - 1) {
            real_path->append("/");
        }
    }

    return (real_path->length() > 0);
}

size_t PathACL::add_path(AccessList& m, const std::string& path) {
    for (AccessList::iterator it = m.begin(); it != m.end(); ++it) {
        if (is_sub_path((*it), path) || is_sub_path(path, (*it))) {
            LOG(WARNING) << "add_path " << path << " has inherit relationship with " << *it;
            return 0;
        }
    }
    m.insert(path);
    return 1;
}

size_t PathACL::remove_path(AccessList& m, const std::string& path) {
    return m.erase(path);
}

}  // namespace raft
