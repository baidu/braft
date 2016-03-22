/*
 * AIO backing store
 *
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
 * Copyright (C) 2011 Alexander Nezhinsky <alexandern@mellanox.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <libaio.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "scsi.h"

#ifndef O_DIRECT
#define O_DIRECT 040000
#endif

#define AIO_MAX_IODEPTH    128

struct bs_aio_info {
	struct list_head dev_list_entry;
	io_context_t ctx;

	struct list_head cmd_wait_list;
	unsigned int nwaiting;
	unsigned int npending;
	unsigned int iodepth;

	int resubmit;

	struct scsi_lu *lu;
	int evt_fd;

	struct iocb iocb_arr[AIO_MAX_IODEPTH];
	struct iocb *piocb_arr[AIO_MAX_IODEPTH];
	struct io_event io_evts[AIO_MAX_IODEPTH];
};

static struct list_head bs_aio_dev_list = LIST_HEAD_INIT(bs_aio_dev_list);

static inline struct bs_aio_info *BS_AIO_I(struct scsi_lu *lu)
{
	return (struct bs_aio_info *) ((char *)lu + sizeof(*lu));
}

static void bs_aio_iocb_prep(struct bs_aio_info *info, int idx,
			     struct scsi_cmd *cmd)
{
	struct iocb *iocb = &info->iocb_arr[idx];
	unsigned int scsi_op = (unsigned int)cmd->scb[0];

	iocb->data = cmd;
	iocb->key = 0;
	iocb->aio_reqprio = 0;
	iocb->aio_fildes = info->lu->fd;

	switch (scsi_op) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		iocb->aio_lio_opcode = IO_CMD_PWRITE;
		iocb->u.c.buf = scsi_get_out_buffer(cmd);
		iocb->u.c.nbytes = scsi_get_out_length(cmd);

		dprintf("prep WR cmd:%p op:%x buf:0x%p sz:%lx\n",
			cmd, scsi_op, iocb->u.c.buf, iocb->u.c.nbytes);
		break;

	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		iocb->aio_lio_opcode = IO_CMD_PREAD;
		iocb->u.c.buf = scsi_get_in_buffer(cmd);
		iocb->u.c.nbytes = scsi_get_in_length(cmd);

		dprintf("prep RD cmd:%p op:%x buf:0x%p sz:%lx\n",
			cmd, scsi_op, iocb->u.c.buf, iocb->u.c.nbytes);
		break;

	default:
		return;
	}

	iocb->u.c.offset = cmd->offset;
	iocb->u.c.flags |= (1 << 0); /* IOCB_FLAG_RESFD - use eventfd file desc. */
	iocb->u.c.resfd = info->evt_fd;
}

static int bs_aio_submit_dev_batch(struct bs_aio_info *info)
{
	int nsubmit, nsuccess;
	struct scsi_cmd *cmd, *next;
	int i = 0;

	nsubmit = info->iodepth - info->npending; /* max allowed to submit */
	if (nsubmit > info->nwaiting)
		nsubmit = info->nwaiting;

	dprintf("nsubmit:%d waiting:%d pending:%d, tgt:%d lun:%"PRId64 "\n",
		nsubmit, info->nwaiting, info->npending,
		info->lu->tgt->tid, info->lu->lun);

	if (!nsubmit)
		return 0;

	list_for_each_entry_safe(cmd, next, &info->cmd_wait_list, bs_list) {
		bs_aio_iocb_prep(info, i, cmd);
		list_del(&cmd->bs_list);
		if (++i == nsubmit)
			break;
	}

	nsuccess = io_submit(info->ctx, nsubmit, info->piocb_arr);
	if (unlikely(nsuccess < 0)) {
		if (nsuccess == -EAGAIN) {
			eprintf("delayed submit %d cmds to tgt:%d lun:%"PRId64 "\n",
				nsubmit, info->lu->tgt->tid, info->lu->lun);
			nsuccess = 0; /* leave the dev pending with all cmds */
		}
		else {
			eprintf("failed to submit %d cmds to tgt:%d lun:%"PRId64
				", err: %d\n",
				nsubmit, info->lu->tgt->tid,
				info->lu->lun, -nsuccess);
			return nsuccess;
		}
	}
	if (unlikely(nsuccess < nsubmit)) {
		for (i=nsubmit-1; i >= nsuccess; i--) {
			cmd = info->iocb_arr[i].data;
			list_add(&cmd->bs_list, &info->cmd_wait_list);
		}
	}

	info->npending += nsuccess;
	info->nwaiting -= nsuccess;
	/* if no cmds remain, remove the dev from the pending list */
	if (likely(!info->nwaiting))
			list_del(&info->dev_list_entry);

	dprintf("submitted %d of %d cmds to tgt:%d lun:%"PRId64
		", waiting:%d pending:%d\n",
		nsuccess, nsubmit, info->lu->tgt->tid, info->lu->lun,
		info->nwaiting, info->npending);
	return 0;
}

static int bs_aio_submit_all_devs(void)
{
	struct bs_aio_info *dev_info, *next_dev;
	int err;

	/* pass over all devices having some queued cmds and submit */
	list_for_each_entry_safe(dev_info, next_dev, &bs_aio_dev_list, dev_list_entry) {
		err = bs_aio_submit_dev_batch(dev_info);
		if (unlikely(err))
			return err;
	}
	return 0;
}

static int bs_aio_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu = cmd->dev;
	struct bs_aio_info *info = BS_AIO_I(lu);
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

	list_add_tail(&cmd->bs_list, &info->cmd_wait_list);
	if (!info->nwaiting)
		list_add_tail(&info->dev_list_entry, &bs_aio_dev_list);
	info->nwaiting++;
	set_cmd_async(cmd);

	if (!cmd_not_last(cmd)) /* last cmd in batch */
		return bs_aio_submit_all_devs();

	if (info->nwaiting == info->iodepth - info->npending)
		return bs_aio_submit_dev_batch(info);

	return 0;
}

static void bs_aio_complete_one(struct io_event *ep)
{
	struct scsi_cmd *cmd = (void *)(unsigned long)ep->data;
	uint32_t length;
	int result;

	switch (cmd->scb[0]) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		length = scsi_get_out_length(cmd);
		break;
	default:
		length = scsi_get_in_length(cmd);
		break;
	}

	if (likely(ep->res == length))
		result = SAM_STAT_GOOD;
	else {
		sense_data_build(cmd, MEDIUM_ERROR, 0);
		result = SAM_STAT_CHECK_CONDITION;
	}
	dprintf("cmd: %p\n", cmd);
	target_cmd_io_done(cmd, result);
}

static void bs_aio_get_completions(int fd, int events, void *data)
{
	struct bs_aio_info *info = data;
	int i, ret;
	/* read from eventfd returns 8-byte int, fails with the error EINVAL
	   if the size of the supplied buffer is less than 8 bytes */
	uint64_t evts_complete;
	unsigned int ncomplete, nevents;

retry_read:
	ret = read(info->evt_fd, &evts_complete, sizeof(evts_complete));
	if (unlikely(ret < 0)) {
		eprintf("failed to read AIO completions, %m\n");
		if (errno == EAGAIN || errno == EINTR)
			goto retry_read;

		return;
	}
	ncomplete = (unsigned int) evts_complete;

	while (ncomplete) {
		nevents = min_t(unsigned int, ncomplete, ARRAY_SIZE(info->io_evts));
retry_getevts:
		ret = io_getevents(info->ctx, 1, nevents, info->io_evts, NULL);
		if (likely(ret > 0)) {
			nevents = ret;
			info->npending -= nevents;
		} else {
			if (ret == -EINTR)
				goto retry_getevts;
			eprintf("io_getevents failed, err:%d\n", -ret);
			return;
		}
		dprintf("got %d ioevents out of %d, pending %d\n",
			nevents, ncomplete, info->npending);

		for (i = 0; i < nevents; i++)
			bs_aio_complete_one(&info->io_evts[i]);
		ncomplete -= nevents;
	}

	if (info->nwaiting) {
		dprintf("submit waiting cmds to tgt:%d lun:%"PRId64 "\n",
			info->lu->tgt->tid, info->lu->lun);
		bs_aio_submit_dev_batch(info);
	}
}

static int bs_aio_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
	struct bs_aio_info *info = BS_AIO_I(lu);
	int ret, afd;
	uint32_t blksize = 0;

	info->iodepth = AIO_MAX_IODEPTH;
	eprintf("create aio context for tgt:%d lun:%"PRId64 ", max iodepth:%d\n",
		info->lu->tgt->tid, info->lu->lun, info->iodepth);
	ret = io_setup(info->iodepth, &info->ctx);
	if (ret) {
		eprintf("failed to create aio context, %m\n");
		return -1;
	}

	afd = eventfd(0, O_NONBLOCK);
	if (afd < 0) {
		eprintf("failed to create eventfd for tgt:%d lun:%"PRId64 ", %m\n",
			info->lu->tgt->tid, info->lu->lun);
		ret = afd;
		goto close_ctx;
	}
	dprintf("eventfd:%d for tgt:%d lun:%"PRId64 "\n",
		afd, info->lu->tgt->tid, info->lu->lun);

	ret = tgt_event_add(afd, EPOLLIN, bs_aio_get_completions, info);
	if (ret)
		goto close_eventfd;
	info->evt_fd = afd;

	eprintf("open %s, RW, O_DIRECT for tgt:%d lun:%"PRId64 "\n",
		path, info->lu->tgt->tid, info->lu->lun);
	*fd = backed_file_open(path, O_RDWR|O_LARGEFILE|O_DIRECT, size,
				&blksize);
	/* If we get access denied, try opening the file in readonly mode */
	if (*fd == -1 && (errno == EACCES || errno == EROFS)) {
		eprintf("open %s, READONLY, O_DIRECT for tgt:%d lun:%"PRId64 "\n",
			path, info->lu->tgt->tid, info->lu->lun);
		*fd = backed_file_open(path, O_RDONLY|O_LARGEFILE|O_DIRECT,
				       size, &blksize);
		lu->attrs.readonly = 1;
	}
	if (*fd < 0) {
		eprintf("failed to open %s, for tgt:%d lun:%"PRId64 ", %m\n",
			path, info->lu->tgt->tid, info->lu->lun);
		ret = *fd;
		goto remove_tgt_evt;
	}

	eprintf("%s opened successfully for tgt:%d lun:%"PRId64 "\n",
		path, info->lu->tgt->tid, info->lu->lun);

	if (!lu->attrs.no_auto_lbppbe)
		update_lbppbe(lu, blksize);

	return 0;

remove_tgt_evt:
	tgt_event_del(afd);
close_eventfd:
	close(afd);
close_ctx:
	io_destroy(info->ctx);
	return ret;
}

static void bs_aio_close(struct scsi_lu *lu)
{
	close(lu->fd);
}

static tgtadm_err bs_aio_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_aio_info *info = BS_AIO_I(lu);
	int i;

	memset(info, 0, sizeof(*info));
	INIT_LIST_HEAD(&info->dev_list_entry);
	INIT_LIST_HEAD(&info->cmd_wait_list);
	info->lu = lu;

	for (i=0; i < ARRAY_SIZE(info->iocb_arr); i++)
		info->piocb_arr[i] = &info->iocb_arr[i];

	return TGTADM_SUCCESS;
}

static void bs_aio_exit(struct scsi_lu *lu)
{
	struct bs_aio_info *info = BS_AIO_I(lu);

	close(info->evt_fd);
	io_destroy(info->ctx);
}

static struct backingstore_template aio_bst = {
	.bs_name		= "aio",
	.bs_datasize    	= sizeof(struct bs_aio_info),
	.bs_init		= bs_aio_init,
	.bs_exit		= bs_aio_exit,
	.bs_open		= bs_aio_open,
	.bs_close       	= bs_aio_close,
	.bs_cmd_submit  	= bs_aio_cmd_submit,
};

__attribute__((constructor)) static void register_bs_module(void)
{
	register_backingstore_template(&aio_bst);
}

