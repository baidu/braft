// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/15 15:42:37

#ifndef PUBLIC_RAFT_RAFT_UTIL_H
#define PUBLIC_RAFT_RAFT_UTIL_H

#include <zlib.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <string>
#include <iterative_murmurhash3.h>
#include <base/endpoint.h>
#include <base/scoped_lock.h>
#include <base/fast_rand.h>
#include <base/time.h>
#include <base/logging.h>
#include <base/iobuf.h>
#include <bthread.h>
#include "raft/macros.h"

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

inline int random_timeout(int timeout_ms) {
    return base::fast_rand_in(timeout_ms, timeout_ms << 1);
}

inline uint32_t murmurhash32(const void *key, int len) {
    uint32_t hash = 0;
    MurmurHash3_x86_32(key, len, 0, &hash);
    return hash;
}

inline uint32_t murmurhash32(const base::IOBuf& buf) {
    MurmurHash3_x86_32_Context ctx;
    MurmurHash3_x86_32_Init(&ctx, 0);
    base::IOBufAsZeroCopyInputStream wrapper(buf);
    const void *data = NULL;
    int len = 0;
    while (wrapper.Next(&data, &len)) {
        MurmurHash3_x86_32_Update(&ctx, data, len);
    }
    uint32_t hash = 0;
    MurmurHash3_x86_32_Final(&hash, &ctx);
    return hash;
}

void run_closure_in_bthread(::google::protobuf::Closure* closure);

void run_closure_in_bthread_nosig(::google::protobuf::Closure* closure);

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

}  // namespace raft

#endif // PUBLIC_RAFT_RAFT_UTIL_H
