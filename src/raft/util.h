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
#include <base/third_party/murmurhash3/murmurhash3.h>
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
#include <base/file_util.h>
#include <bthread.h>
#include "raft/macros.h"
#include "raft/timer.h"

#define RAFT_GET_ARG3(arg1, arg2, arg3, ...)  arg3

#define RAFT_RETURN_IF1(expr, rc)       \
    do {                                \
        if ((expr)) {                   \
            return (rc);                \
        }                               \
    } while (0)

#define RAFT_RETURN_IF0(expr)           \
    do {                                \
        if ((expr)) {                   \
            return;                     \
        }                               \
    } while (0)

#define RAFT_RETURN_IF(expr, args...)   \
        RAFT_GET_ARG3(1, ##args, RAFT_RETURN_IF1, RAFT_RETURN_IF0)(expr, ##args)

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

}  // namespace base

namespace raft {
class Closure;

// http://stackoverflow.com/questions/1493936/faster-approach-to-checking-for-an-all-zero-buffer-in-c
inline bool is_zero(const char* buff, const size_t size) {
    if (size >= sizeof(uint64_t)) {
        return (0 == *(uint64_t*)buff) &&
            (0 == memcmp(buff, buff + sizeof(uint64_t), size - sizeof(uint64_t)));
    } else if (size > 0) {
        return (0 == *(uint8_t*)buff) &&
            (0 == memcmp(buff, buff + sizeof(uint8_t), size - sizeof(uint8_t)));
    } else {
        return 0;
    }
}

inline uint32_t murmurhash32(const void *key, int len) {
    uint32_t hash = 0;
    base::MurmurHash3_x86_32(key, len, 0, &hash);
    return hash;
}

inline uint32_t murmurhash32(const base::IOBuf& buf) {
    base::MurmurHash3_x86_32_Context ctx;
    base::MurmurHash3_x86_32_Init(&ctx, 0);
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        base::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            base::MurmurHash3_x86_32_Update(&ctx, sp.data(), sp.size());
        }
    }
    uint32_t hash = 0;
    base::MurmurHash3_x86_32_Final(&hash, &ctx);
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

// Create a sub directory of an existing |parent_path|. Requiring that
// |parent_path| must exist.
// Returns true on successful creation, or if the directory already exists.
// Returns false on failure and sets *error appropriately, if it is non-NULL.
bool create_sub_directory(const base::FilePath& parent_path,
                          const base::FilePath& sub_path,
                          base::File::Error* error = NULL);

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
    void append(const base::IOBuf& data, uint64_t offset);
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

}  // namespace raft

#endif // PUBLIC_RAFT_RAFT_UTIL_H
