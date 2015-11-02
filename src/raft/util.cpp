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

}  // namespace raft
