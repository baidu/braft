/*
 * SCSI stream command processing
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <linux/fs.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "tgtadm_error.h"
#include "target.h"
#include "driver.h"
#include "scsi.h"
#include "spc.h"
#include "ssc.h"
#include "tgtadm_error.h"

#define GRANULARITY	9

#define MAX_BLK_SIZE	1048576
#define MIN_BLK_SIZE	4

static inline uint32_t ssc_get_block_length(struct scsi_lu *lu)
{
	return get_unaligned_be24(lu->mode_block_descriptor + 5);
}

static int ssc_mode_page_update(struct scsi_cmd *cmd, uint8_t *data,
				int *changed)
{
	return 1;
}

static int ssc_mode_sense(int host_no, struct scsi_cmd *cmd)
{
	int ret;
	uint8_t *data, mode6;

	ret = spc_mode_sense(host_no, cmd);
	if (ret != SAM_STAT_GOOD)
		return ret;

	mode6 = (cmd->scb[0] == 0x1a);
	data = scsi_get_in_buffer(cmd);

	/* set write protect bit to 1 for readonly devices */
	if (cmd->dev->attrs.readonly) {
		if (mode6)
			data[2] |= 0x80;
		else
			data[3] |= 0x80;
	}

	/* set the device to report BUFFERED MODE for writes */
	if (mode6)
		data[2] |= 0x10;
	else
		data[3] |= 0x10;


	return ret;
}

static int ssc_mode_select(int host_no, struct scsi_cmd *cmd)
{
	return spc_mode_select(host_no, cmd, ssc_mode_page_update);
}

static int ssc_rw(int host_no, struct scsi_cmd *cmd)
{
	int ret;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_LUN_NOT_SUPPORTED;

	ret = device_reserved(cmd);
	if (ret)
		return SAM_STAT_RESERVATION_CONFLICT;

	if (cmd->dev->attrs.removable && !cmd->dev->attrs.online) {
		key = NOT_READY;
		asc = ASC_MEDIUM_NOT_PRESENT;
		goto sense;
	}

	if (cmd->dev->attrs.readonly) {
		switch (cmd->scb[0]) {
		case ERASE:
		case SPACE:
		case WRITE_6:
		case WRITE_FILEMARKS:
			key = DATA_PROTECT;
			asc = ASC_WRITE_PROTECT;
			goto sense;
			break;
		}
	}

	ret = cmd->dev->bst->bs_cmd_submit(cmd);
	if (ret) {
		key = HARDWARE_ERROR;
		asc = ASC_INTERNAL_TGT_FAILURE;
	} else
		return SAM_STAT_GOOD;

sense:
	cmd->offset = 0;
	scsi_set_in_resid_by_actual(cmd, 0);
	scsi_set_out_resid_by_actual(cmd, 0);

	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

#define READ_BLK_LIMITS_SZ	6
static int ssc_read_block_limit(int host_no, struct scsi_cmd *cmd)
{
	uint8_t buf[READ_BLK_LIMITS_SZ];
	uint8_t block_length = ssc_get_block_length(cmd->dev);

	memset(buf, 0, sizeof(buf));

	buf[0] = GRANULARITY;
	if (block_length) {
		/* Fixed block size */
		put_unaligned_be24(block_length, buf + 1);
		put_unaligned_be16(block_length, buf + 4);
	} else {
		/* Variable block size */
		put_unaligned_be24(MAX_BLK_SIZE, buf + 1);
		put_unaligned_be16(MIN_BLK_SIZE, buf + 4);
	}

	memcpy(scsi_get_in_buffer(cmd), buf, READ_BLK_LIMITS_SZ);
	eprintf("In ssc_read_block_limit \n");
	return SAM_STAT_GOOD;
}

static tgtadm_err ssc_lu_init(struct scsi_lu *lu)
{
	uint8_t *data;
	struct ssc_info *ssc;

	ssc = zalloc(sizeof(struct ssc_info));
	if (!ssc)
		return TGTADM_NOMEM;
	dtype_priv(lu) = ssc;

	if (spc_lu_init(lu))
		return TGTADM_NOMEM;

	strncpy(lu->attrs.product_id, "VIRTUAL-TAPE",
		sizeof(lu->attrs.product_id));

	/* use only fixed for now */
	lu->attrs.sense_format = 0;
	lu->attrs.version_desc[0] = 0x0200; /* SSC no version claimed */
	lu->attrs.version_desc[1] = 0x0960; /* iSCSI */
	lu->attrs.version_desc[2] = 0x0300; /* SPC-3 */
	lu->attrs.removable = 1;

	data = lu->mode_block_descriptor;

	/* SSC devices do not need to set number of blks */
	put_unaligned_be24(0, data + 1);

	/* Set default blk size */
	put_unaligned_be24(0, data + 5);

	/* Vendor uniq - However most apps seem to call for mode page 0*/
	add_mode_page(lu, "0:0:0");
	/* Read-Write Error Recovery - Mandatory - SSC3 8.3.5 */
	add_mode_page(lu, "1:0:10:0:8:0:0:0:0:8:0:0:0");
	/* Disconnect page - Mandatory - SPC-4 */
	add_mode_page(lu, "2:0:14:0x80:0x80:0:0xa:0:0:0:0:0:0:0:0:0:0");
	/* Control page - Mandatory - SPC-4 */
	add_mode_page(lu, "0x0a:0:10:2:0:0:0:0:0:0:0:2:0");
	/* Control Extensions mode page:  TCMOS:1 */
	add_mode_page(lu, "0x0a:1:0x1c:0x04:0x00:0x00");
	/* Data Compression - Mandatory - SSC3 8.3.2 */
	add_mode_page(lu, "15:0:14:0:0:0:0:0:0:0:0:0:0:0:0:0:0");
	/* Device Configuration - Mandatory - SSC3 8.3.3 */
	add_mode_page(lu, "16:0:14:0:0:0:128:128:0:0:0:0:0:0:0:0:0");
	/* Informational Exceptions Control page - Mandatory - SSC3 8.3.6 */
	add_mode_page(lu, "0x1c:0:10:8:0:0:0:0:0:0:0:0:0");
	/* Medium Configuration - Mandatory - SSC3 8.3.7 */
	add_mode_page(lu, "0x1d:0:0x1e:1:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0"
				":0:0:0:0:0:0:0:0:0:0:0:0:0");
	return TGTADM_SUCCESS;
}

static struct device_type_template ssc_template = {
	.type		= TYPE_TAPE,
	.lu_init	= ssc_lu_init,
	.lu_config	= spc_lu_config,
	.lu_online	= spc_lu_online,
	.lu_offline	= spc_lu_offline,
	.lu_exit	= spc_lu_exit,

	.ops		= {
		{spc_test_unit,},
		{ssc_rw,},
		{spc_illegal_op,},
		{spc_request_sense,},
		{spc_illegal_op,},
		{ssc_read_block_limit,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{ssc_rw,},
		{spc_illegal_op,},
		{ssc_rw,},
		{ssc_rw,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		/* 0x10 */
		{ssc_rw,},
		{ssc_rw,},
		{spc_inquiry,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{ssc_mode_select,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{ssc_mode_sense,},
		{spc_start_stop,},
		{spc_illegal_op,},
		{spc_send_diagnostics,},
		{spc_prevent_allow_media_removal,},
		{spc_illegal_op,},

		/* 0x20 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		/* 0x30 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{ssc_rw,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0x40 ... 0x4f] = {spc_illegal_op,},

		/* 0x50 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{ssc_mode_sense,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0x60 ... 0x7f] = {spc_illegal_op,},

		/* 0x80 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		/* 0x90 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		/* 0xA0 */
		{spc_report_luns,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_service_action, maint_in_service_actions,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0xb0 ... 0xff] = {spc_illegal_op},
	}
};

__attribute__((constructor)) static void ssc_init(void)
{
	device_type_register(&ssc_template);
}
