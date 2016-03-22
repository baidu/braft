// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/01/14 13:45:32

#ifndef PUBLIC_RAFT_EVM_EXECUTOR_API_H
#define PUBLIC_RAFT_EVM_EXECUTOR_API_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct EvmAttachParam {
    char user_id[256];
    char volume_cinder_id[256];
};

typedef int (*evm_volumn_cb)(int64_t length, void *context);

int evm_init(void);

void evm_exit(void);

int evm_attach_volume(uint64_t volume_id, struct EvmAttachParam *attach_param,
                      void **volume);

int evm_detach_volume(uint64_t volume_id, void *volume);

int evm_read_volume(void *volume, uint64_t offset,
                    void *buffer, uint64_t length,
                    evm_volumn_cb callback, void *context);

int evm_write_volume(void *volume, uint64_t offset,
                     void *buffer, uint64_t length,
                     evm_volumn_cb callback, void *context);

int64_t evm_get_volume_size(void *volume);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // PUBLIC_RAFT_EVM_EXECUTOR_API_H
