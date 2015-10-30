/*
 * =====================================================================================
 *
 *       Filename:  util.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/15 15:42:37
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_RAFT_UTIL_H
#define PUBLIC_RAFT_RAFT_UTIL_H

#include <net/if.h>
#include <sys/ioctl.h>
#include <base/endpoint.h>
#include <base/scoped_lock.h>
#include <base/rand_util.h>
#include <base/time.h>
#include <bthread.h>

namespace std {

template <> class lock_guard<bthread_mutex_t> {
public:
    explicit lock_guard(bthread_mutex_t & mutex) : _pmutex(&mutex) {
#if !defined(NDEBUG)
        const int rc = bthread_mutex_lock(_pmutex);
        if (rc) {
            char buf[64] = "";
            strerror_r(rc, buf, sizeof(buf));
            LOG(FATAL) << "Fail to lock bthread_mutex_t=" << _pmutex << ", " << buf;
            _pmutex = NULL;
        }
#else
        bthread_mutex_lock(_pmutex);
#endif  // NDEBUG
    }

    ~lock_guard() {
#ifndef NDEBUG
        if (_pmutex) {
            bthread_mutex_unlock(_pmutex);
        }
#else
        bthread_mutex_unlock(_pmutex);
#endif
    }

private:
    DISALLOW_COPY_AND_ASSIGN(lock_guard);
    bthread_mutex_t* _pmutex;
};

}

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
        ip_addr.s_addr = *((int*) &req.ifr_addr.sa_data[2]);
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

}

namespace raft {

inline int64_t random_timeout(int64_t timeout_ms) {
    return timeout_ms + (base::RandUint64() % (timeout_ms));
}

}

#endif //~PUBLIC_RAFT_RAFT_UTIL_H
