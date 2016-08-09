// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/15 15:42:37

#ifndef PUBLIC_RAFT_RAFT_UTIL_H
#define PUBLIC_RAFT_RAFT_UTIL_H

#include <zlib.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <limits.h>
#include <stdlib.h>
#include <string>
#include <set>
#include <iterative_murmurhash3.h>
#include <base/endpoint.h>
#include <base/scoped_lock.h>
#include <base/fast_rand.h>
#include <base/time.h>
#include <base/logging.h>
#include <base/iobuf.h>
#include <base/unique_ptr.h>
#include <base/memory/singleton.h>
#include <base/containers/doubly_buffered_data.h>
#include <base/crc32c.h>
#include <bthread.h>
#include "raft/macros.h"
#include "raft/timer.h"

namespace base {

inline ip_t get_host_ip_by_interface(const char* interface) {
    int sockfd = -1;
    struct ::ifreq req;
    ip_t ip = IP_ANY;
    if ((sockfd = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
        return ip;
    }

    memset(&req, 0, sizeof(struct ::ifreq));
    snprintf(req.ifr_name, sizeof(req.ifr_name), "%s", interface);

    if (!ioctl(sockfd, SIOCGIFADDR, (char*)&req)) {
        struct in_addr ip_addr;
        ip_addr = ((struct sockaddr_in*)&req.ifr_addr)->sin_addr;
        //ip_addr.s_addr = *((int*) &req.ifr_addr.sa_data[2]);
        ip.s_addr = ip_addr.s_addr;
    }
    close(sockfd);
    return ip;
}

inline ip_t get_host_ip() {
    const char* interfaces[] = { "xgbe0", "xgbe1", "eth1", "eth0", "bond0", "br-ex" };
    ip_t ip = IP_ANY;

    for (size_t i = 0; i < 6; ++i) {
        ip = get_host_ip_by_interface(interfaces[i]);
        if (INADDR_ANY != ip.s_addr) {
            break;
        }
    }

    if (INADDR_ANY == ip.s_addr) {
        LOG(FATAL) << "can not get a valid ip";
    }

    return ip;
}

}  // namespace std

namespace raft {
class Closure;

inline uint32_t murmurhash32(const void *key, int len) {
    uint32_t hash = 0;
    MurmurHash3_x86_32(key, len, 0, &hash);
    return hash;
}

inline uint32_t murmurhash32(const base::IOBuf& buf) {
    MurmurHash3_x86_32_Context ctx;
    MurmurHash3_x86_32_Init(&ctx, 0);
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        base::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            MurmurHash3_x86_32_Update(&ctx, sp.data(), sp.size());
        }
    }
    uint32_t hash = 0;
    MurmurHash3_x86_32_Final(&hash, &ctx);
    return hash;
}

inline uint32_t crc32(const void* key, int len) {
    return base::crc32c::Value((const char*)key, len);
}

inline uint32_t crc32(const base::IOBuf& buf) {
    uint32_t hash = 0;
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        base::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            hash = base::crc32c::Extend(hash, sp.data(), sp.size());
        }
    }
    return hash;
}

// Start a bthread to run closure
void run_closure_in_bthread(::google::protobuf::Closure* closure,
                           bool in_pthread = false);

struct RunClosureInBthread {
    void operator()(google::protobuf::Closure* done) {
        return run_closure_in_bthread(done);
    }
};

typedef std::unique_ptr<google::protobuf::Closure, RunClosureInBthread>
        AsyncClosureGuard;

// Start a bthread to run closure without signal other worker thread to steal
// it. You should call bthread_flush() at last.
void run_closure_in_bthread_nosig(::google::protobuf::Closure* closure,
                                  bool in_pthread = false);

struct RunClosureInBthreadNoSig {
    void operator()(google::protobuf::Closure* done) {
        return run_closure_in_bthread_nosig(done);
    }
};

std::string fileuri2path(const std::string& uri);

int fileuri_parse(const std::string& uri, base::EndPoint* addr, std::string* path);

ssize_t file_pread(base::IOPortal* portal, int fd, off_t offset, size_t size);

ssize_t file_pwrite(const base::IOBuf& data, int fd, off_t offset);

// unsequence file data, reduce the overhead of copy some files have hole.
class FileSegData {
public:
    // for reader
    FileSegData(const base::IOBuf& data)
        : _data(data), _seg_header(base::IOBuf::INVALID_AREA), _seg_offset(0), _seg_len(0) {}
    // for writer
    FileSegData() : _seg_header(base::IOBuf::INVALID_AREA), _seg_offset(0), _seg_len(0) {}
    ~FileSegData() {
        close();
    }

    // writer append
    void append(void* data, uint64_t offset, uint32_t len);

    // writer get
    base::IOBuf& data() {
        close();
        return _data;
    }

    // read next, NEED clear data when call next in loop
    size_t next(uint64_t* offset, base::IOBuf* data);

private:
    void close();

    base::IOBuf _data;
    base::IOBuf::Area _seg_header;
    uint64_t _seg_offset;
    uint32_t _seg_len;
};

// PathACL, path must is dir, and not inherit relationship with any path in acl
class PathACL {
public:
    static PathACL* GetInstance() {
        return Singleton<PathACL>::get();
    }

    bool check(const std::string& path);

    bool add(const std::string& path);

    bool remove(const std::string& path);

private:
    PathACL() {}
    ~PathACL() {}
    DISALLOW_COPY_AND_ASSIGN(PathACL);
    friend struct DefaultSingletonTraits<PathACL>;

    typedef std::set<std::string> AccessList;

    static bool normalize_path(const std::string& path, std::string* real_path);
    static bool is_sub_path(const std::string& parent, const std::string& child);
    static size_t add_path(AccessList& m, const std::string& path);
    static size_t remove_path(AccessList& m, const std::string& path);

    base::DoublyBufferedData<AccessList> _acl;
};

}  // namespace raft

#endif // PUBLIC_RAFT_RAFT_UTIL_H
