/*
 * SCSI Generic I/O backing store
 *
 * Copyright (C) 2008 Alexander Nezhinsky <nezhinsky@gmail.com>
 *
 * Added linux/block/bsg.c support using struct sg_io_v4.
 * Copyright (C) 2010 Nicholas A. Bellinger <nab@linux-iscsi.org>
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
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/major.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <scsi/sg.h>

#include "bsg.h" /* Copied from include/linux/bsg.h */
#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "scsi.h"
#include "spc.h"
#include "tgtadm_error.h"

#define BS_SG_RESVD_SZ  (512 * 1024)

static unsigned int sg_timeout = 30 * 1000; /* 30 seconds */

static int graceful_read(int fd, void *p_read, int to_read)
{
	int err;

	while (to_read > 0) {
		err = read(fd, p_read, to_read);
		if (err >= 0) {
			to_read -= err;
			p_read += err;
		} else if (errno == EINTR)
			continue;
		else {
			eprintf("sg device %d read failed, errno: %d\n",
				fd, errno);
			return errno;
		}
	}
	return 0;
}

static int graceful_write(int fd, void *p_write, int to_write)
{
	int err;

	while (to_write > 0) {
		err = write(fd, p_write, to_write);
		if (err >= 0) {
			to_write -= err;
			p_write += err;
		} else if (errno == EINTR)
			continue;
		else {
			eprintf("sg device %d write failed, errno: %d\n",
				fd, errno);
			return errno;
		}
	}
	return 0;
}

static int bs_sg_rw(int host_no, struct scsi_cmd *cmd)
{
	int ret;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_LUN_NOT_SUPPORTED;

	ret = cmd->dev->bst->bs_cmd_submit(cmd);
	if (ret) {
		key = HARDWARE_ERROR;
		asc = ASC_INTERNAL_TGT_FAILURE;
	} else
		return SAM_STAT_GOOD;

	cmd->offset = 0;
	scsi_set_in_resid_by_actual(cmd, 0);
	scsi_set_out_resid_by_actual(cmd, 0);

	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int set_cmd_failed(struct scsi_cmd *cmd)
{
	int result = SAM_STAT_CHECK_CONDITION;
	uint16_t asc = ASC_READ_ERROR;
	uint8_t key = MEDIUM_ERROR;

	scsi_set_result(cmd, result);
	sense_data_build(cmd, key, asc);

	return result;
}

static int bs_bsg_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *dev = cmd->dev;
	int fd = dev->fd;
	struct sg_io_v4 io_hdr;
	int err = 0;

	memset(&io_hdr, 0, sizeof(io_hdr));
	/*
	 * Following linux/include/linux/bsg.h
	 */
	/* [i] 'Q' to differentiate from v3 */
	io_hdr.guard = 'Q';
	io_hdr.protocol = BSG_PROTOCOL_SCSI;
	io_hdr.subprotocol = BSG_SUB_PROTOCOL_SCSI_CMD;
	io_hdr.request_len = cmd->scb_len;
	io_hdr.request = (unsigned long )cmd->scb;

	io_hdr.dout_xfer_len = scsi_get_out_length(cmd);
	io_hdr.dout_xferp = (unsigned long)scsi_get_out_buffer(cmd);
	io_hdr.din_xfer_len = scsi_get_in_length(cmd);
	io_hdr.din_xferp = (unsigned long)scsi_get_in_buffer(cmd);

	io_hdr.max_response_len = sizeof(cmd->sense_buffer);
	/* SCSI: (auto)sense data */
	io_hdr.response = (unsigned long)cmd->sense_buffer;
	/* Using the same 2000 millisecond timeout.. */
	io_hdr.timeout = sg_timeout;
	/* [i->o] unused internally */
	io_hdr.usr_ptr = (unsigned long)cmd;
	dprintf("[%d] Set io_hdr->usr_ptr from cmd: %p\n", getpid(), cmd);
	/* Bsg does Q_AT_HEAD by default */
	io_hdr.flags |= BSG_FLAG_Q_AT_TAIL;

	dprintf("[%d] Calling graceful_write for CDB: 0x%02x\n", getpid(), cmd->scb[0]);
	err = graceful_write(fd, &io_hdr, sizeof(io_hdr));
	if (!err)
		set_cmd_async(cmd);
	else {
		eprintf("failed to start cmd 0x%p\n", cmd);
		return set_cmd_failed(cmd);
	}

	return 0;
}

static void bs_sg_cmd_setup(struct sg_io_hdr *hdr,
			   unsigned char *cmd, int cmd_len,
			   void *data, int data_len, int direction,
			   void *sense, int sense_len,
			   int timeout)
{
	memset(hdr, 0, sizeof(*hdr));
	hdr->interface_id = 'S';
	hdr->cmdp = cmd;
	hdr->cmd_len = cmd_len;

	hdr->dxfer_direction = direction;
	hdr->dxfer_len = data_len;
	hdr->dxferp = data;

	hdr->mx_sb_len = sense_len;
	hdr->sbp = sense;
	hdr->timeout = timeout;
	hdr->pack_id = -1;
	hdr->usr_ptr = NULL;
	hdr->flags = 0;
}

static int bs_sg_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *dev = cmd->dev;
	int fd = dev->fd;
	struct sg_io_hdr io_hdr;
	int err = 0;

	memset(&io_hdr, 0, sizeof(io_hdr));
	io_hdr.interface_id = 'S';
	io_hdr.cmd_len = cmd->scb_len;
	io_hdr.cmdp = cmd->scb;

	if (scsi_get_data_dir(cmd) == DATA_WRITE) {
		io_hdr.dxfer_direction = SG_DXFER_TO_DEV;
		io_hdr.dxfer_len = scsi_get_out_length(cmd);
		io_hdr.dxferp = (void *)scsi_get_out_buffer(cmd);
	} else {
		io_hdr.dxfer_direction = SG_DXFER_FROM_DEV;
		io_hdr.dxfer_len = scsi_get_in_length(cmd);
		io_hdr.dxferp = (void *)scsi_get_in_buffer(cmd);
	}
	io_hdr.mx_sb_len = sizeof(cmd->sense_buffer);
	io_hdr.sbp = cmd->sense_buffer;
	io_hdr.timeout = sg_timeout;
	io_hdr.pack_id = -1;
	io_hdr.usr_ptr = cmd;
	io_hdr.flags |= SG_FLAG_DIRECT_IO;

	err = graceful_write(fd, &io_hdr, sizeof(io_hdr));
	if (!err)
		set_cmd_async(cmd);
	else {
		eprintf("failed to start cmd 0x%p\n", cmd);
		return set_cmd_failed(cmd);
	}
	return 0;
}

static void bs_bsg_cmd_complete(int fd, int events, void *data)
{
	struct sg_io_v4 io_hdr;
	struct scsi_cmd *cmd;
	int err;

	dprintf("[%d] bs_bsg_cmd_complete() called!\n", getpid());
	memset(&io_hdr, 0, sizeof(io_hdr));
	/* [i] 'Q' to differentiate from v3 */
	io_hdr.guard = 'Q';

	err = graceful_read(fd, &io_hdr, sizeof(io_hdr));
	if (err)
		return;

	cmd = (struct scsi_cmd *)(unsigned long)io_hdr.usr_ptr;
	dprintf("BSG Using cmd: %p for io_hdr.usr_ptr\n", cmd);
	/*
	 * Check SCSI: command completion status
	 * */
	if (!io_hdr.device_status) {
		uint32_t actual_len;

		if (io_hdr.dout_resid) {
			actual_len = scsi_get_out_length(cmd) - io_hdr.dout_resid;
			scsi_set_out_resid_by_actual(cmd, actual_len);
		}
		if (io_hdr.din_resid) {
			actual_len = scsi_get_in_length(cmd) - io_hdr.din_resid;
			scsi_set_in_resid_by_actual(cmd, actual_len);
		}
	} else {
		/*
		 * NAB: Used by linux/block/bsg.c:bsg_ioctl(), is this
		 * right..?
		 */
		cmd->sense_len = SCSI_SENSE_BUFFERSIZE;
		scsi_set_out_resid_by_actual(cmd, 0);
		scsi_set_in_resid_by_actual(cmd, 0);
	}

	target_cmd_io_done(cmd, io_hdr.device_status);
}

static void bs_sg_cmd_complete(int fd, int events, void *data)
{
	struct sg_io_hdr io_hdr;
	struct scsi_cmd *cmd;
	int err;
	uint32_t actual_len;

	memset(&io_hdr, 0, sizeof(io_hdr));
	io_hdr.interface_id = 'S';
	io_hdr.pack_id = -1;

	err = graceful_read(fd, &io_hdr, sizeof(io_hdr));
	if (err)
		return;

	cmd = (struct scsi_cmd *)io_hdr.usr_ptr;
	if (!io_hdr.status) {
		actual_len = io_hdr.dxfer_len - io_hdr.resid;
	} else {
		/* NO SENSE | ILI (Incorrect Length Indicator) */
		if (io_hdr.sbp[2] == 0x20)
			actual_len = io_hdr.dxfer_len - io_hdr.resid;
		else
			actual_len = 0;

		cmd->sense_len = io_hdr.sb_len_wr;
	}
	if (!actual_len || io_hdr.resid) {
		if (io_hdr.dxfer_direction == SG_DXFER_TO_DEV)
			scsi_set_out_resid_by_actual(cmd, actual_len);
		else
			scsi_set_in_resid_by_actual(cmd, actual_len);
	}

	target_cmd_io_done(cmd, io_hdr.status);
}

static int get_bsg_major(char *path)
{
	FILE *devfd;
	int majorno, n;
	char dev[64];
	char tmp[16];

	sscanf(path, "/dev/bsg/%s", tmp);
	sprintf(dev, "/sys/class/bsg/%s/dev", tmp);
	devfd = fopen(dev, "r");
	if (!devfd) {
		eprintf("%s open failed errno: %d\n", dev, errno);
		return -1;
	}
	n = fscanf(devfd, "%d:", &majorno);
	fclose(devfd);
	if (n != 1) {
		if (n < 0)
			eprintf("reading major from %s failed errno: %d\n", dev, errno);
		else
			eprintf("reading major from %s failed: invalid input\n", dev);
		return -1;
	}
	return majorno;
}

static int chk_sg_device(char *path)
{
	struct stat st;

	if (stat(path, &st) < 0) {
		eprintf("stat() failed errno: %d\n", errno);
		return -1;
	}

	if (!S_ISCHR(st.st_mode)) {
		eprintf("Not a character device: %s\n", path);
		return -1;
	}

	/* Check for SG_IO major first.. */
	if (major(st.st_rdev) == SCSI_GENERIC_MAJOR)
		return 0;

	if (!strncmp("/dev/bsg", path, 8)) {
		if (major(st.st_rdev) == get_bsg_major(path))
			return 1;
	}

	return -1;
}

static int init_bsg_device(int fd)
{
	int t, err;

	err = ioctl(fd, SG_GET_COMMAND_Q, &t);
	if (err < 0) {
		eprintf("SG_GET_COMMAND_Q for bsd failed: %d\n", err);
		return -1;
	}
	eprintf("bsg: Using max_queue: %d\n", t);

	t = BS_SG_RESVD_SZ;
	err = ioctl(fd, SG_SET_RESERVED_SIZE, &t);
	if (err < 0) {
		eprintf("SG_SET_RESERVED_SIZE errno: %d\n", errno);
		return -1;
	}

	return 0;
}

static int init_sg_device(int fd)
{
	int t, err;
	struct sg_io_hdr hdr;
	unsigned char cmd[6];
	unsigned char resp[36];

	err = ioctl(fd, SG_GET_VERSION_NUM, &t);
	if ((err < 0) || (t < 30000)) {
		eprintf("sg driver prior to 3.x\n");
		return -1;
	}

	t = BS_SG_RESVD_SZ;
	err = ioctl(fd, SG_SET_RESERVED_SIZE, &t);
	if (err < 0) {
		eprintf("SG_SET_RESERVED_SIZE errno: %d\n", errno);
		return -1;
	}

	memset(&cmd, 0, sizeof(cmd));
	memset(&resp, 0, sizeof(resp));

	cmd[0] = INQUIRY;
	cmd[4] = sizeof(resp);

	bs_sg_cmd_setup(&hdr, cmd, sizeof(cmd), resp, sizeof(resp),
			SG_DXFER_FROM_DEV, NULL, 0, 30000);

	err = ioctl(fd, SG_IO, &hdr);

	if (!err && (resp[0] & 0x1f) == TYPE_TAPE)
		sg_timeout = 14000 * 1000;

	return 0;
}

static tgtadm_err bs_sg_init(struct scsi_lu *lu, char *bsopts)
{
	/*
	 * Setup struct scsi_lu->cmd_perform() passthrough pointer
	 * (if available) for the underlying device type.
	 */
	lu->cmd_perform = &target_cmd_perform_passthrough;

	/*
	 * Setup struct scsi_lu->cmd_done() passthrough pointer using
	 * usr/target.c:__cmd_done_passthrough().
	 */
	lu->cmd_done = &__cmd_done_passthrough;
	return TGTADM_SUCCESS;
}

static int bs_sg_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
	void (*cmd_complete)(int, int, void *) = NULL;
	int sg_fd, err, bsg = 0;

	bsg = chk_sg_device(path);
	if (bsg < 0) {
		eprintf("Not recognized %s as an SG device\n", path);
		return -EINVAL;
	}

	sg_fd = open(path, O_RDWR);
	if (sg_fd < 0) {
		eprintf("Could not open %s, %m\n", path);
		return sg_fd;
	}

	if (bsg) {
		cmd_complete = &bs_bsg_cmd_complete;
		err = init_bsg_device(sg_fd);
	} else {
		cmd_complete = &bs_sg_cmd_complete;
		err = init_sg_device(sg_fd);
	}
	if (err) {
		eprintf("Failed to initialize sg device %s\n", path);
		return err;
	}

	err = tgt_event_add(sg_fd, EPOLLIN, cmd_complete, NULL);
	if (err) {
		eprintf("Failed to add sg device event %s\n", path);
		return err;
	}

	*fd = sg_fd;
	*size = 0;
	return 0;
}

static void bs_sg_close(struct scsi_lu *lu)
{
	close(lu->fd);
}

static tgtadm_err bs_sg_lu_init(struct scsi_lu *lu)
{
	if (spc_lu_init(lu))
		return TGTADM_NOMEM;

	return TGTADM_SUCCESS;
}

static struct backingstore_template sg_bst = {
	.bs_name		= "sg",
	.bs_datasize		= 0,
	.bs_init		= bs_sg_init,
	.bs_open		= bs_sg_open,
	.bs_close		= bs_sg_close,
	.bs_cmd_submit		= bs_sg_cmd_submit,
};

static struct backingstore_template bsg_bst = {
	.bs_name		= "bsg",
	.bs_datasize		= 0,
	.bs_init		= bs_sg_init,
	.bs_open		= bs_sg_open,
	.bs_close		= bs_sg_close,
	.bs_cmd_submit		= bs_bsg_cmd_submit,
};

static struct device_type_template sg_template = {
	.type			= TYPE_PT,
	.lu_init		= bs_sg_lu_init,
	.lu_config		= spc_lu_config,
	.lu_online		= spc_lu_online,
	.lu_offline		= spc_lu_offline,
	.lu_exit		= spc_lu_exit,
	.cmd_passthrough	= bs_sg_rw,
};

__attribute__((constructor)) static void bs_sg_constructor(void)
{
	register_backingstore_template(&sg_bst);
	register_backingstore_template(&bsg_bst);
	device_type_register(&sg_template);
}
