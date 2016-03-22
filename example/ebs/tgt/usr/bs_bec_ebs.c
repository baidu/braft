/*
 * Bec_ebs backing store
 *
 * Copyright (C) 2014 Li,Zhiyong <lizhiyong01@baidu.com>
 */

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/eventfd.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "scsi.h"

#include "evm_executor_api.h"

#ifndef O_DIRECT
#define O_DIRECT 040000
#endif

#define AIO_MAX_IODEPTH    128
#define MAX_UINT64_STR_LEN 20

enum io_type {
    IO_CMD_READ,
    IO_CMD_WRITE,
};

typedef int (*io_done_cb)(int64_t, void*);
typedef int (*io_start_cb)(void *, uint64_t, void*, uint64_t, io_done_cb, void*);

static int executor_io_callback(int64_t size, void* data);

static int make_non_blocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }
    if (flags & O_NONBLOCK) {
        return 0;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int make_blocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }
    if (flags & O_NONBLOCK) {
        return fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    }
    return 0;
}

struct bs_bec_ebs_async_io_context {
    enum io_type        type;

    uint64_t            volume_id;

    uint64_t            io_size;
    uint64_t            io_offset;
    void*               io_buffer;

    io_start_cb         io_start;
    io_done_cb          io_done;

    struct bs_bec_ebs_info* info;
    struct scsi_cmd*        cmd;
    uint8_t                 cmd_offset;
};

struct bs_bec_ebs_info {
    void *finished_queue;
    struct list_head finished_list;
    pthread_mutex_t finished_lock;

    uint64_t volume_id;
    unsigned int iodepth;

    int resubmit;

    struct scsi_lu *lu;

    void *user_arg;
    int pipe[2];
};

static struct list_head bs_bec_ebs_dev_list = LIST_HEAD_INIT(bs_bec_ebs_dev_list);

static inline struct bs_bec_ebs_info *BS_AIO_I(struct scsi_lu *lu)
{
    return (struct bs_bec_ebs_info *) ((char *)lu + sizeof(*lu));
}

static uint64_t convert_string_to_uint64(const char *num_string, uint64_t *u64_num)
{
    if (strlen(num_string) > MAX_UINT64_STR_LEN) {
        return -1;
    }
    sscanf(num_string, "%lu", u64_num);

    return 0;
}

static int bs_bec_ebs_aio_ctx_prep(struct bs_bec_ebs_info *info, struct scsi_cmd *cmd,
                                    struct bs_bec_ebs_async_io_context* aio_ctx)
{
    unsigned int scsi_op = (unsigned int)cmd->scb[0];

    aio_ctx->info   = info;
    aio_ctx->cmd = cmd;
    aio_ctx->io_done    = executor_io_callback;
    aio_ctx->io_offset  = cmd->offset;

    aio_ctx->volume_id = info->volume_id;

    switch (scsi_op) {
    case WRITE_6:
    case WRITE_10:
    case WRITE_12:
    case WRITE_16:
        aio_ctx->type = IO_CMD_WRITE;
        aio_ctx->io_start   = evm_write_volume;
        aio_ctx->io_size    = scsi_get_out_length(cmd);
        aio_ctx->io_buffer  = scsi_get_out_buffer(cmd);

        dprintf("write prep WR cmd:%p op:%x sz:%lx\n",
            cmd, scsi_op, aio_ctx->io_size);
        break;

    case READ_6:
    case READ_10:
    case READ_12:
    case READ_16:
        aio_ctx->type = IO_CMD_READ;
        aio_ctx->io_start   = evm_read_volume;
        aio_ctx->io_size    = scsi_get_in_length(cmd);
        aio_ctx->io_buffer  = scsi_get_in_buffer(cmd);

        dprintf("read prep RD cmd:%p op:%x sz:%lx\n",
            cmd, scsi_op, aio_ctx->io_size);
        break;

    default:
        return -1;
    }
    return 0;
}

static int bs_bec_ebs_submit(struct bs_bec_ebs_info *info, struct scsi_cmd* cmd)
{
    struct bs_bec_ebs_async_io_context aio_ctx;
    if (bs_bec_ebs_aio_ctx_prep(info, cmd, &aio_ctx) != 0) {
        return -1;
    }
    int err = aio_ctx.io_start(info->user_arg, aio_ctx.io_offset,
                                aio_ctx.io_buffer, aio_ctx.io_size,
                                aio_ctx.io_done, (void*)cmd);
    if (0 == err) {
        // eprintf("add a task success, success\n");
    } else {
        eprintf("add a task failed\n");
        return -1;
    }

    return 0;
}

static int bs_bec_ebs_cmd_submit(struct scsi_cmd *cmd)
{
    struct scsi_lu *lu = cmd->dev;
    struct bs_bec_ebs_info *info = BS_AIO_I(lu);
    unsigned int scsi_op = (unsigned int)cmd->scb[0];

    switch (scsi_op) {
    case WRITE_6:
    case WRITE_10:
    case WRITE_12:
    case WRITE_16:
    case READ_6:
    case READ_10:
    case READ_12:
    case READ_16:
        break;

    case WRITE_SAME:
    case WRITE_SAME_16:
        eprintf("WRITE_SAME not yet supported for AIO backend.\n");
        return -1;

    case SYNCHRONIZE_CACHE:
    case SYNCHRONIZE_CACHE_16:
    default:
        dprintf("skipped cmd:%p op:%x\n", cmd, scsi_op);
        return 0;
    }

    set_cmd_async(cmd);

    return bs_bec_ebs_submit(info, cmd);
}

static int executor_io_callback(int64_t size, void *data)
{
    struct scsi_cmd* cmd = (struct scsi_cmd*)data;
    struct scsi_lu *lu = cmd->dev;
    struct bs_bec_ebs_info *info = BS_AIO_I(lu);
    int result;

    if (likely(size != -1)) {
        result = SAM_STAT_GOOD;
    }
    else {
        dprintf("finish ebs io, error code: size not match.\n");
        sense_data_build(cmd, MEDIUM_ERROR, 0);
        result = SAM_STAT_CHECK_CONDITION;
    }

    scsi_set_result(cmd, result);

    //QueuePush(info->finished_queue, (void *)cmd);

    int ret = write(info->pipe[1], &cmd, sizeof(struct scsi_cmd*));
    if (ret == -1) {
        eprintf("write notifier to tgt for io done failed, fd: %d, "
                "error code: %s\n", info->pipe[1], strerror(errno));
    }

    return 0;
}

static void handle_finish_task(int fd, int events, void* data) {
    LIST_HEAD(ack_list);
    struct scsi_cmd* cmd[PIPE_BUF / sizeof(void*)];

    for(;;) {
        ssize_t nr = read(fd, cmd, sizeof(cmd));
        if (nr <= 0) {
            if (nr < 0 && errno != EINTR && errno != EAGAIN) {
                eprintf("Fail to read from fd=%d, %m", fd);
            }
            return;
        }
        const size_t ncmd = (size_t)nr / sizeof(void*);
        size_t i = 0;
        for (i = 0; i < ncmd; ++i) {
            target_cmd_io_done(cmd[i], scsi_get_result(cmd[i]));
        }
    }
}

static int bs_bec_ebs_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
    dprintf("bs_bec_ebs open start\n");
    struct bs_bec_ebs_info *info = BS_AIO_I(lu);
    uint32_t blksize = 0;
    int ret = 0;

    lu->fd = 0;

    info->iodepth = AIO_MAX_IODEPTH;
    dprintf("create aio context for tgt:%d lun:%"PRId64 ", max iodepth:%d\n",
        info->lu->tgt->tid, info->lu->lun, info->iodepth);

    char* saveptr = NULL;
    const char* volume_id_str = strtok_r(path, ":", &saveptr);
    const char* user_id_str   = strtok_r(NULL, ":", &saveptr);
    const char* cinder_id_str = strtok_r(NULL, ":", &saveptr);
    if (!(volume_id_str && user_id_str && cinder_id_str)) {
        eprintf("path invalid, path: %s", path);
        return TGTADM_UNKNOWN_ERR;
    }

    uint64_t volume_id = 0;
    int err = convert_string_to_uint64(volume_id_str, &volume_id);
    if (err) {
        eprintf("volume_id[%s] can not convert uint64_t, volume_id invalid\n", volume_id_str);
        return TGTADM_UNKNOWN_ERR;
    }
    dprintf("volume_id valid: %lu\n, user_id: %s, cinder volume_id: %s",
            volume_id, user_id_str, cinder_id_str);

    struct EvmAttachParam attach_param;
    snprintf(attach_param.user_id, 256, "%s", user_id_str);
    snprintf(attach_param.volume_cinder_id, 256, "%s", cinder_id_str);

    err = evm_attach_volume(volume_id, &attach_param, &info->user_arg);
    if (err == -1) {
        eprintf("attach volume [%lu] failed\n", volume_id);
        return TGTADM_UNKNOWN_ERR;
    }

    uint64_t volume_size = evm_get_volume_size(info->user_arg);
    if (volume_size < 0) {
        eprintf("get volume [%lu] size failed", volume_id);
        return TGTADM_UNKNOWN_ERR;
    }
    eprintf("volume size : %lu", volume_size);
    *size = volume_size;

    info->volume_id = volume_id;

    ret = pipe(info->pipe);
    if (ret != 0) {
        eprintf("failed to create pipe for tgt:%d lun:%"PRId64 ", %m\n",
            info->lu->tgt->tid, info->lu->lun);
        goto err_exit;
    }

    make_non_blocking(info->pipe[0]);
    make_blocking(info->pipe[1]);

    eprintf("pipe:%d->%d for tgt:%d lun:%"PRId64 "\n",
        info->pipe[1], info->pipe[0], info->lu->tgt->tid, info->lu->lun);

    ret = tgt_event_add(info->pipe[0], EPOLLIN, handle_finish_task, info);
    if (ret)
        goto close_eventfd;

    eprintf("%s opened successfully for tgt:%d lun:%"PRId64 "\n",
        path, info->lu->tgt->tid, info->lu->lun);

    if (!lu->attrs.no_auto_lbppbe)
        update_lbppbe(lu, blksize);

    return 0;

close_eventfd:
    close(info->pipe[0]);
    close(info->pipe[1]);
err_exit:
    return ret;
}

static int bs_bec_ebs_close(struct scsi_lu *lu)
{
    struct bs_bec_ebs_info *info = BS_AIO_I(lu);
    uint64_t volume_id = info->volume_id;
    eprintf("bs_bec_ebs close , volume_id[%lu]\n", volume_id);

    int ret = evm_detach_volume(volume_id, info->user_arg);
    if (ret == -1) {
        eprintf("close bs_bec_ebs [volume_id: %lu] error\n", volume_id);
    }

    return -1;
}

static tgtadm_err bs_bec_ebs_init(struct scsi_lu *lu, char *bsopts)
{
    struct bs_bec_ebs_info *info = BS_AIO_I(lu);

    memset(info, 0, sizeof(*info));
    info->pipe[0] = info->pipe[1] = -1;
    info->lu = lu;

    return TGTADM_SUCCESS;
}

static void bs_bec_ebs_exit(struct scsi_lu *lu)
{
    struct bs_bec_ebs_info *info = BS_AIO_I(lu);
    if (info->pipe[0] >= 0) {
        tgt_event_del(info->pipe[0]);
        close(info->pipe[0]);
        close(info->pipe[1]);
    }
}

static struct backingstore_template bec_ebs_bst = {
    .bs_name        = "bec_ebs",
    .bs_datasize    = sizeof(struct bs_bec_ebs_info),
    .bs_init        = bs_bec_ebs_init,
    .bs_exit        = bs_bec_ebs_exit,
    .bs_open        = bs_bec_ebs_open,
    .bs_close       = bs_bec_ebs_close,
    .bs_cmd_submit  = bs_bec_ebs_cmd_submit,
};

void register_bs_module(void)
{
    register_backingstore_template(&bec_ebs_bst);
    // init executor
    int ret = evm_init();
    if (ret != 0) {
        eprintf("evm init failed, ret : %d\n", ret);
        exit(ret);
    }
}
