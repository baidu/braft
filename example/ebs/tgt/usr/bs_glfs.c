/*
 * Synchronous glfs backing store routines
 *
 * Modified from bs_rdb.c
 * Copyright (C) 2013 Dan Lambright <dlambrig@redhat.com>
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
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
#define _XOPEN_SOURCE 600

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <linux/fs.h>
#include <sys/epoll.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "scsi.h"
#include "spc.h"
#include "bs_thread.h"

#include "glfs.h"

struct active_glfs {
	char *name;
	glfs_t *fs;
	glfs_fd_t *gfd;
	char *logfile;
	int loglevel;
};

#define ALLOWED_BSOFLAGS (O_SYNC | O_DIRECT | O_RDWR | O_LARGEFILE)

#define GLUSTER_PORT 24007

#define GFSP(lu)	((struct active_glfs *) \
				((char *)lu + \
				sizeof(struct scsi_lu) + \
				sizeof(struct bs_thread_info)) \
			)

static void set_medium_error(int *result, uint8_t *key, uint16_t *asc)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = MEDIUM_ERROR;
	*asc = ASC_READ_ERROR;
}

static int bs_glfs_discard(glfs_fd_t *gfd, off_t offset, size_t len)
{
#ifdef BS_GLFS_DISCARD
	return glfs_discard(gfd, offset, len);
#endif
	return 0;
}

static void bs_glfs_request(struct scsi_cmd *cmd)
{
	glfs_fd_t *gfd = GFSP(cmd->dev)->gfd;
	struct scsi_lu *lu = cmd->dev;
	int ret;
	uint32_t length;
	int result = SAM_STAT_GOOD;
	uint8_t key;
	uint16_t asc;
	char *tmpbuf;
	size_t blocksize;
	uint64_t offset = cmd->offset;
	uint32_t tl     = cmd->tl;
	int do_verify = 0;
	int i;
	char *ptr;
	const char *write_buf = NULL;
	ret = length = 0;
	key = asc = 0;

	switch (cmd->scb[0]) {
	case ORWRITE_16:
		length = scsi_get_out_length(cmd);

		tmpbuf = malloc(length);
		if (!tmpbuf) {
			result = SAM_STAT_CHECK_CONDITION;
			key = HARDWARE_ERROR;
			asc = ASC_INTERNAL_TGT_FAILURE;
			break;
		}

		ret = glfs_pread(gfd, tmpbuf, length, offset, lu->bsoflags);

		if (ret != length) {
			set_medium_error(&result, &key, &asc);
			free(tmpbuf);
			break;
		}

		ptr = scsi_get_out_buffer(cmd);
		for (i = 0; i < length; i++)
			ptr[i] |= tmpbuf[i];

		free(tmpbuf);

		write_buf = scsi_get_out_buffer(cmd);
		goto write;
	case COMPARE_AND_WRITE:
		/* Blocks are transferred twice, first the set that
		 * we compare to the existing data, and second the set
		 * to write if the compare was successful.
		 */
		length = scsi_get_out_length(cmd) / 2;
		if (length != cmd->tl) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		tmpbuf = malloc(length);
		if (!tmpbuf) {
			result = SAM_STAT_CHECK_CONDITION;
			key = HARDWARE_ERROR;
			asc = ASC_INTERNAL_TGT_FAILURE;
			break;
		}

		ret = glfs_pread(gfd, tmpbuf, length, offset, SEEK_SET);

		if (ret != length) {
			set_medium_error(&result, &key, &asc);
			free(tmpbuf);
			break;
		}

		if (memcmp(scsi_get_out_buffer(cmd), tmpbuf, length)) {
			uint32_t pos = 0;
			char *spos = scsi_get_out_buffer(cmd);
			char *dpos = tmpbuf;

			/*
			 * Data differed, this is assumed to be 'rare'
			 * so use a much more expensive byte-by-byte
			 * comparasion to find out at which offset the
			 * data differs.
			 */
			for (pos = 0; pos < length && *spos++ == *dpos++;
			     pos++)
				;
			result = SAM_STAT_CHECK_CONDITION;
			key = MISCOMPARE;
			asc = ASC_MISCOMPARE_DURING_VERIFY_OPERATION;
			free(tmpbuf);
			break;
		}

		free(tmpbuf);

		write_buf = scsi_get_out_buffer(cmd) + length;
		goto write;
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
		/* TODO */
		length = (cmd->scb[0] == SYNCHRONIZE_CACHE) ? 0 : 0;

		if (cmd->scb[1] & 0x2) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
		} else {
			glfs_fdatasync(gfd);
		}
		break;
	case WRITE_VERIFY:
	case WRITE_VERIFY_12:
	case WRITE_VERIFY_16:
		do_verify = 1;
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		length = scsi_get_out_length(cmd);
		write_buf = scsi_get_out_buffer(cmd);
write:
		ret = glfs_pwrite(gfd, write_buf, length, offset, lu->bsoflags);

		if (ret == length) {
			struct mode_pg *pg;

			/*
			 * it would be better not to access to pg
			 * directy.
			 */
			pg = find_mode_page(cmd->dev, 0x08, 0);
			if (pg == NULL) {
				result = SAM_STAT_CHECK_CONDITION;
				key = ILLEGAL_REQUEST;
				asc = ASC_INVALID_FIELD_IN_CDB;
				break;
			}
			if (((cmd->scb[0] != WRITE_6) && (cmd->scb[1] & 0x8)) ||
			    !(pg->mode_data[0] & 0x04))
				glfs_fdatasync(gfd);
		} else
			set_medium_error(&result, &key, &asc);

		if (do_verify)
			goto verify;
		break;
	case WRITE_SAME:
	case WRITE_SAME_16:
		/* WRITE_SAME used to punch hole in file */
		if (cmd->scb[1] & 0x08) {
			ret = bs_glfs_discard(gfd, offset, tl);
			if (ret != 0) {
				eprintf("Failed WRITE_SAME command\n");
				result = SAM_STAT_CHECK_CONDITION;
				key = HARDWARE_ERROR;
				asc = ASC_INTERNAL_TGT_FAILURE;
				break;
			}
			break;
		}
		while (tl > 0) {
			blocksize = 1 << cmd->dev->blk_shift;
			tmpbuf = scsi_get_out_buffer(cmd);

			switch (cmd->scb[1] & 0x06) {
			case 0x02: /* PBDATA==0 LBDATA==1 */
				put_unaligned_be32(offset, tmpbuf);
				break;
			case 0x04: /* PBDATA==1 LBDATA==0 */
				/* physical sector format */
				put_unaligned_be64(offset, tmpbuf);
				break;
			}

			ret = glfs_pwrite(gfd, tmpbuf, blocksize,
					offset, lu->bsoflags);

			if (ret != blocksize)
				set_medium_error(&result, &key, &asc);

			offset += blocksize;
			tl     -= blocksize;
		}
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		length = scsi_get_in_length(cmd);
		ret = glfs_pread(gfd, scsi_get_in_buffer(cmd),
				length, offset, SEEK_SET);

		if (ret != length) {
			eprintf("Error on read %x %x", ret, length);
			set_medium_error(&result, &key, &asc);
		}
		break;
	case PRE_FETCH_10:
	case PRE_FETCH_16:
		if (ret != 0)
			set_medium_error(&result, &key, &asc);
		break;
	case VERIFY_10:
	case VERIFY_12:
	case VERIFY_16:
verify:
		length = scsi_get_out_length(cmd);

		tmpbuf = malloc(length);
		if (!tmpbuf) {
			result = SAM_STAT_CHECK_CONDITION;
			key = HARDWARE_ERROR;
			asc = ASC_INTERNAL_TGT_FAILURE;
			break;
		}

		ret = glfs_pread(gfd, tmpbuf, length, offset, lu->bsoflags);

		if (ret != length)
			set_medium_error(&result, &key, &asc);
		else if (memcmp(scsi_get_out_buffer(cmd), tmpbuf, length)) {
			result = SAM_STAT_CHECK_CONDITION;
			key = MISCOMPARE;
			asc = ASC_MISCOMPARE_DURING_VERIFY_OPERATION;
		}

		free(tmpbuf);
		break;
	case UNMAP:
		if (!cmd->dev->attrs.thinprovisioning) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		length = scsi_get_out_length(cmd);
		tmpbuf = scsi_get_out_buffer(cmd);

		if (length < 8)
			break;

		length -= 8;
		tmpbuf += 8;

		while (length >= 16) {
			offset = get_unaligned_be64(&tmpbuf[0]);
			offset = offset << cmd->dev->blk_shift;

			tl = get_unaligned_be32(&tmpbuf[8]);
			tl = tl << cmd->dev->blk_shift;

			if (offset + tl > cmd->dev->size) {
				eprintf("UNMAP beyond EOF\n");
				result = SAM_STAT_CHECK_CONDITION;
				key = ILLEGAL_REQUEST;
				asc = ASC_LBA_OUT_OF_RANGE;
				break;
			}

			if (tl > 0) {
				if (bs_glfs_discard(gfd, offset, tl) != 0) {
					eprintf("Failed UNMAP\n");
					result = SAM_STAT_CHECK_CONDITION;
					key = HARDWARE_ERROR;
					asc = ASC_INTERNAL_TGT_FAILURE;
					break;
				}
			}

			length -= 16;
			tmpbuf += 16;
		}
		break;
	default:
		break;
	}

	dprintf("io done %p %x %d %u\n", cmd, cmd->scb[0], ret, length);

	scsi_set_result(cmd, result);

	if (result != SAM_STAT_GOOD) {
		eprintf("io error %p %x %x %d %d %" PRIu64 ", %m\n",
			cmd, result, cmd->scb[0], ret, length, offset);
		sense_data_build(cmd, key, asc);
	}
}

static void parse_imagepath(char *image, char **server, char **vol, char **path)
{
	char *origp = strdup(image);
	char *p, *sep;

	p = origp;
	sep = strchr(p, '@');
	if (sep == NULL) {
		*server = "";
	} else {
		*sep = '\0';
		*server = strdup(p);
		p = sep + 1;
	}
	sep = strchr(p, ':');
	if (sep == NULL) {
		*vol = "";
	} else {
		*vol = strdup(sep + 1);
		*sep = '\0';
	}

	/* p points to path\0 */
	*path = strdup(p);
	free(origp);
}

static int bs_glfs_open(struct scsi_lu *lu, char *image, int *fd,
			uint64_t *size)
{
	int ret = 0;
	char *servername;
	char *volname;
	char *pathname;
	int bsoflags = ALLOWED_BSOFLAGS;
	glfs_t *fs = 0;

	parse_imagepath(image, &volname, &pathname, &servername);

	if (volname && servername && pathname) {
		glfs_fd_t *gfd = NULL;
		struct stat st;

		fs = glfs_new(volname);
		if (!fs)
			goto fail;

		ret = glfs_set_volfile_server(fs, "tcp", servername,
						GLUSTER_PORT);

		ret = glfs_init(fs);
		if (ret)
			goto fail;

		GFSP(lu)->fs = fs;

		if (lu->bsoflags)
			bsoflags = lu->bsoflags;

		gfd = glfs_open(fs, pathname, bsoflags);
		if (gfd == NULL)
			goto fail;

		ret = glfs_lstat(fs, pathname, &st);
		if (ret)
			goto fail;

		GFSP(lu)->gfd = gfd;

		*size = (long) st.st_size;

		if (GFSP(lu)->logfile)
			glfs_set_logging(fs, GFSP(lu)->logfile,
					GFSP(lu)->loglevel);

		return 0;
	}
fail:
	if (fs)
		glfs_fini(fs);

	return -EIO;
}

static void bs_glfs_close(struct scsi_lu *lu)
{
	if (GFSP(lu)->gfd)
		glfs_close(GFSP(lu)->gfd);

	if (GFSP(lu)->gfd)
		glfs_fini(GFSP(lu)->fs);
}

static char *slurp_to_semi(char **p)
{
	char *end = index(*p, ';');
	char *ret;
	int len;

	if (end == NULL)
		end = *p + strlen(*p);
	len = end - *p;
	ret = malloc(len + 1);
	strncpy(ret, *p, len);
	ret[len] = '\0';
	*p = end;
	/* Jump past the semicolon, if we stopped at one */
	if (**p == ';')
		*p = end + 1;
	return ret;
}

static char *slurp_value(char **p)
{
	char *equal = index(*p, '=');
	if (equal) {
		*p = equal + 1;
		return slurp_to_semi(p);
	} else {
		return NULL;
	}
}

static int is_opt(const char *opt, char *p)
{
	int ret = 0;
	if ((strncmp(p, opt, strlen(opt)) == 0) &&
		(p[strlen(opt)] == '=')) {
		ret = 1;
	}
	return ret;
}

static tgtadm_err bs_glfs_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
	char *logfile = NULL;
	int loglevel = 0;
	char *sloglevel;

	while (bsopts && strlen(bsopts)) {
		if (is_opt("logfile", bsopts))
			logfile = slurp_value(&bsopts);
		else if (is_opt("loglevel", bsopts)) {
			sloglevel = slurp_value(&bsopts);
			loglevel = atoi(sloglevel);
		}
	}

	GFSP(lu)->logfile = logfile;
	GFSP(lu)->loglevel = loglevel;

	return bs_thread_open(info, bs_glfs_request, nr_iothreads);
}

static void bs_glfs_exit(struct scsi_lu *lu)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);

	if (GFSP(lu)->gfd)
		glfs_close(GFSP(lu)->gfd);

	if (GFSP(lu)->fs)
		glfs_fini(GFSP(lu)->fs);

	bs_thread_close(info);
}

static struct backingstore_template glfs_bst = {
	.bs_name		= "glfs",
	.bs_datasize		= sizeof(struct active_glfs) +
					sizeof(struct bs_thread_info),
	.bs_open		= bs_glfs_open,
	.bs_close		= bs_glfs_close,
	.bs_init		= bs_glfs_init,
	.bs_exit		= bs_glfs_exit,
	.bs_cmd_submit		= bs_thread_cmd_submit,
	.bs_oflags_supported    = ALLOWED_BSOFLAGS
};

void register_bs_module(void)
{
	register_backingstore_template(&glfs_bst);
}
