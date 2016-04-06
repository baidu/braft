// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/03/18 23:28:32

#include "evm_executor_api.h"

#include <gflags/gflags.h>
#include <base/containers/flat_map.h>               // base::FlatMap
#include <base/containers/doubly_buffered_data.h>   // base::DoublyBufferedData
#include <base/comlog_sink.h>                       // logging::ComlogSink
#include <base/errno.h>
#include <bthread.h>                                // bthread_start_background
#include <bthread_unstable.h>                       // bthread_flush
#include <base/object_pool.h>                       // base::get_object
#include <baidu/rpc/channel.h>                      // baidu::rpc::Channel
#include <baidu/rpc/errno.pb.h>                     // baidu::rpc::ERPCTIMEDOUT
#include "ebs.pb.h"

static const int64_t GB = 1024*1024*1024;

DEFINE_int64(block_size, 1 * GB, "Size of each block");
DEFINE_int64(block_num, 32, "Number of block");
DEFINE_string(cluster_ns, "list://", "Name service for the cluster");
DEFINE_int32(timeout_ms, 10000, "Timeout for each request");

pthread_mutex_t g_leader_mutex = PTHREAD_MUTEX_INITIALIZER;

namespace example {
using namespace ::baidu::ebs;

typedef base::FlatMap<int64_t/*block_id*/, base::EndPoint> LeaderMap;

static baidu::rpc::Channel* g_cluster = NULL;
static base::DoublyBufferedData<LeaderMap>* g_leader_map;
static pthread_once_t g_init_once = PTHREAD_ONCE_INIT;


inline size_t init_leader_map(LeaderMap& leader_map) {
    return leader_map.init(1024) == 0;
}

static void global_init() {
    g_cluster = new baidu::rpc::Channel;
    baidu::rpc::ChannelOptions options;
    options.timeout_ms = FLAGS_timeout_ms;
    if (g_cluster->Init(FLAGS_cluster_ns.c_str(), "rr", &options) != 0) {
        LOG(FATAL) << "Fail to init channel";
        exit(-1);
    }
    g_leader_map = new base::DoublyBufferedData<LeaderMap>;
    if (!g_leader_map->Modify(init_leader_map)) {
        LOG(FATAL) << "Fail to init leader map";
        exit(-1);
    }
}

int read_leader(int64_t block_id, base::EndPoint* leader) {
    base::DoublyBufferedData<LeaderMap>::ScopedPtr ptr;
    g_leader_map->Read(&ptr);
    base::EndPoint* addr = ptr->seek(block_id);
    if (addr == NULL) {
        return -1;
    }
    *leader = *addr;
    return 0;
}

size_t modify_leader_map(LeaderMap& leader_map, 
                         int64_t block_id, base::EndPoint new_leader) {
    leader_map[block_id] = new_leader;
    return true;
}

int update_leader(int64_t block_id, base::EndPoint new_leader) {
    g_leader_map->Modify(modify_leader_map, block_id, new_leader);
    return 0;
}

int get_leader(baidu::rpc::Channel* channel, uint64_t block_id,  base::EndPoint* leader_addr) {
    BlockServiceAdaptor_Stub stub(channel);
    GetLeaderRequest request;
    request.set_block_id(block_id);
    GetLeaderResponse response;
    baidu::rpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    stub.GetLeader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to get leader, " << cntl.ErrorText();
        return -1;
    }
    int rc = base::str2endpoint(response.leader_addr().c_str(), leader_addr);
    if (rc != 0) {
        return rc;
    }
    if (leader_addr->ip == base::IP_ANY) {
        return -1;
    }
    return 0;
}

int write(uint64_t block_id, const void* buf, int64_t len, off_t offset) {
    WriteRequest request;
    AckResponse response;
    request.set_block_id(block_id);
    request.set_offset(offset);
    request.mutable_data()->assign((const char*)buf, len);
    bool should_get_leader = false;
    base::EndPoint leader_addr;
    if (read_leader(block_id, &leader_addr) != 0) {
        should_get_leader = true;
    }

    while (true) {
        if (should_get_leader) {
            if (get_leader(g_cluster, block_id, &leader_addr) != 0) {
                LOG(WARNING) << "Fail to get leader of block_id=" << block_id;
                bthread_usleep(100 * 1000);
                continue;
            }
            should_get_leader = false;
            update_leader(block_id, leader_addr);
        }
        baidu::rpc::Channel channel;
        if (channel.Init(leader_addr, NULL) != 0) {
            LOG(WARNING) << "Fail to init Channel";
            bthread_usleep(100 * 1000);
            should_get_leader = true;
            continue;
        }
        baidu::rpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        BlockServiceAdaptor_Stub stub(&channel);
        stub.Write(&cntl, &request, &response, NULL);
        if (!cntl.Failed() && response.error_code() == 0) {
            return 0;
        }
        if (cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT) {
            bthread_usleep(100 * 1000);
            continue;
        }
        const int ec = cntl.Failed() ? cntl.ErrorCode() : response.error_code();
        const char* error_text = cntl.Failed() ? cntl.ErrorText().c_str() 
                                               : response.error_message().c_str();
        CHECK(ec == EPERM || ec == EINVAL);
        LOG(WARNING) << "Fail to write, " << error_text;
        should_get_leader = true;
    }

    return -1;
}

int read(uint64_t block_id, void* buf, int64_t len, off_t offset) {
    ReadRequest request;
    ReadResponse response;
    request.set_block_id(block_id);
    request.set_offset(offset);
    request.set_size(len);
    bool should_get_leader = false;
    base::EndPoint leader_addr;
    if (read_leader(block_id, &leader_addr) != 0) {
        should_get_leader = true;
    }
    while (true) {
        if (should_get_leader) {
            if (get_leader(g_cluster, block_id, &leader_addr) != 0) {
                LOG(WARNING) << "Fail to get leader of block_id=" << block_id;
                bthread_usleep(100 * 1000);
                continue;
            }
            should_get_leader = false;
            update_leader(block_id, leader_addr);
        }
        baidu::rpc::Channel channel;
        if (channel.Init(leader_addr, NULL) != 0) {
            LOG(WARNING) << "Fail to init Channel";
            bthread_usleep(100 * 1000);
            should_get_leader = true;
            continue;
        }
        baidu::rpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        BlockServiceAdaptor_Stub stub(&channel);
        stub.Read(&cntl, &request, &response, NULL);
        if (!cntl.Failed() && response.error_code() == 0)  {
            if ((size_t)len != response.data().size()) {
                LOG(ERROR) << "Fail to read block_id=" << block_id
                           << " offset=" << offset
                           << " length=" << len
                           << " data_size=" << response.data().size()
                           << " server=" << cntl.remote_side();
                return -1;
            }
            memcpy(buf, response.data().data(), len);
            return 0;
        }
        if (cntl.ErrorCode() == baidu::rpc::ERPCTIMEDOUT) {
            bthread_usleep(100 * 1000);
            continue;
        }
        const int ec = cntl.Failed() ? cntl.ErrorCode() : response.error_code();
        CHECK(ec == EPERM || ec == EINVAL) << "ec=" << ec << ", " << berror(ec);
        should_get_leader = true;
    }
    return -1;
}

struct IOMeta {
    union {
        void* volume;
        uint64_t block_id;
    };
    uint64_t offset;
    void* buffer;
    uint64_t length;
    evm_volumn_cb callback;
    void *context;
};

static void* volume_io(void *arg, void* (*block_io)(void*arg)) {
    IOMeta* meta = (IOMeta*)arg;
    if (meta->length == 0) {
        meta->callback(meta->length, meta->context);
        base::return_object(meta);
        return NULL;
    }
    const uint64_t first_block = meta->offset / FLAGS_block_size;
    const uint64_t last_block = (meta->offset + meta->length - 1) / FLAGS_block_size;
    const size_t num_blocks = last_block - first_block + 1;
    IOMeta block_meta[num_blocks];
    size_t consumed = 0;
    size_t offset = meta->offset;
    for (size_t i = first_block; i <= last_block; ++i) {
        IOMeta& bm = block_meta[i - first_block];
        bm.block_id = i;
        bm.offset = offset - i * FLAGS_block_size;
        bm.buffer = (char*)meta->buffer + consumed;
        bm.length = std::min(meta->length + meta->offset, 
                             (i + 1) * FLAGS_block_size)
                    - offset;
        consumed += bm.length;
        offset += bm.length;
    }
    CHECK_EQ(consumed, meta->length);

    if (num_blocks == 1) {
        block_io(&block_meta[0]);
    } else {
        bthread_t tids[num_blocks];
        memset(tids, 0, num_blocks * sizeof(bthread_t));
        for (size_t i = 0; i < num_blocks; ++i) {
            CHECK_NE(0u, block_meta[i].length);
            bthread_attr_t dummy = BTHREAD_ATTR_NORMAL | BTHREAD_NOSIGNAL;
            if (bthread_start_background(
                        &tids[i], &dummy, block_io, &block_meta[i]) != 0) {
                PLOG(ERROR) << "Fail to start bthread";
                block_io(&block_meta[i]);
            }
        }
        bthread_flush();
        for (size_t i = 0; i < num_blocks; ++i) {
            bthread_join(tids[i], NULL);
        }
    }
    meta->callback(meta->length, meta->context);
    base::return_object(meta);
    return NULL;
}

static void* read_block(void* arg) {
    IOMeta* meta = (IOMeta*)arg;
    CHECK_EQ(0, read(meta->block_id, meta->buffer, meta->length, meta->offset));
    return NULL;
}

static void* write_block(void* arg) {
    IOMeta* meta = (IOMeta*)arg;
    CHECK_EQ(0, write(meta->block_id, meta->buffer, meta->length, meta->offset));
    return NULL;
}


static void* read_volume(void* arg) {
    return volume_io(arg, read_block);
}

void *write_volume(void* arg) {
    return volume_io(arg, write_block);
}

}  // namespace example

int evm_init(void) {
    google::SetCommandLineOption("flagfile", "conf/ebs.flags");
    logging::ComlogSinkOptions options;
    options.async = false;
    options.process_name = "libexecutor";
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    return pthread_once(&example::g_init_once, example::global_init);
}

void evm_exit(void) {
}

int evm_attach_volume(uint64_t volume_id, struct EvmAttachParam *attach_param,
                      void **volume) {
    return 0;
}

int evm_detach_volume(uint64_t volume_id, void *volume) {
    return 0;
}

int evm_read_volume(void *volume, uint64_t offset,
                    void *buffer, uint64_t length,
                    evm_volumn_cb callback, void *context) {
    example::IOMeta* meta = base::get_object<example::IOMeta>();
    if (meta == NULL) {
        return -1;
    }
    meta->volume = volume;
    meta->offset = offset;
    meta->buffer = buffer;
    meta->length = length;
    meta->callback = callback;
    meta->context = context;
    bthread_t tid;
    if (bthread_start_background(&tid, NULL, example::read_volume, meta) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        base::return_object(meta);
        return -1;
    }
    return 0;
}

int evm_write_volume(void *volume, uint64_t offset,
                     void *buffer, uint64_t length,
                     evm_volumn_cb callback, void *context) {
    example::IOMeta* meta = base::get_object<example::IOMeta>();
    if (meta == NULL) {
        return -1;
    }
    meta->volume = volume;
    meta->offset = offset;
    meta->buffer = const_cast<void*>(buffer);
    meta->length = length;
    meta->callback = callback;
    meta->context = context;
    bthread_t tid;
    if (bthread_start_background(&tid, NULL, example::write_volume, meta) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        base::return_object(meta);
        return -1;
    }
    return 0;
}

int64_t evm_get_volume_size(void *volume) {
    return FLAGS_block_num * FLAGS_block_size;
}

