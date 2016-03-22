/*
 * NULL I/O backing store routine
 *
 * Copyright (C) 2008 Alexander Nezhinsky <nezhinsky@gmail.com>
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

#include "list.h"
#include "tgtd.h"
#include "scsi.h"

#define NULL_BS_DEV_SIZE        (1ULL << 40)

int bs_null_cmd_submit(struct scsi_cmd *cmd)
{
	scsi_set_result(cmd, SAM_STAT_GOOD);
	return 0;
}

static int bs_null_open(struct scsi_lu *lu, char *path,
			int *fd, uint64_t *size)
{
	*size = NULL_BS_DEV_SIZE;
	dprintf("NULL backing store open, size: %" PRIu64 "\n", *size);
	return 0;
}

static void bs_null_close(struct scsi_lu *lu)
{
}

static struct backingstore_template null_bst = {
	.bs_name		= "null",
	.bs_datasize		= 0,
	.bs_open		= bs_null_open,
	.bs_close		= bs_null_close,
	.bs_cmd_submit		= bs_null_cmd_submit,
};

__attribute__((constructor)) static void bs_null_constructor(void)
{
	register_backingstore_template(&null_bst);
}
