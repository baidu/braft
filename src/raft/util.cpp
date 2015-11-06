// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/02 01:49:50

#include "raft/util.h"

#include <stdlib.h>
#include <base/macros.h>
#include <baidu/rpc/random_number_seed.h>

namespace raft {

static __thread uint32_t __tls_seed = 0;

int get_random_number(int min, int max) {
    if (BAIDU_UNLIKELY(__tls_seed == 0)) {
        __tls_seed = baidu::rpc::RandomNumberSeed();
    }
    long range = max - min;
    int result  = min + range * rand_r(&__tls_seed) / RAND_MAX;
    return result;
}

std::string fileuri2path(const std::string& uri) {
    std::string path;
    std::size_t prefix_found = uri.find("file://");
    if (std::string::npos == prefix_found) {
        if (std::string::npos == uri.find("://")) {
            path = uri;
        }
    } else {
        path.assign(uri, prefix_found + strlen("file://"),
                    uri.size() - (prefix_found + strlen("file://")));
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

    if (0 != base::hostname2endpoint(addr_str.c_str(), addr) &&
        0 != base::str2endpoint(addr_str.c_str(), addr)) {
        return EINVAL;
    }

    return 0;
}

}  // namespace raft
