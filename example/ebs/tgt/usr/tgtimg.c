/*
 *	Create media files for TGTD devices
 *
 * Copyright (C) 2008 Mark Harvey markh794@gmail.com
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#define _XOPEN_SOURCE 600
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "media.h"
#include "bs_ssc.h"
#include "ssc.h"
#include "libssc.h"
#include "scsi.h"
#include "util.h"

#define NO_LOGGING
#include "log.h"

enum {
	OP_NEW,
	OP_SHOW,
};

static char program_name[] = "tgtimg";

static char *short_options = "ho:Y:b:s:t:f:";

struct option const long_options[] = {
	{"help", no_argument, NULL, 'h'},
	{"op", required_argument, NULL, 'o'},
	{"device-type", required_argument, NULL, 'Y'},
	{"barcode", required_argument, NULL, 'b'},
	{"size", required_argument, NULL, 's'},
	{"type", required_argument, NULL, 't'},
	{"file", required_argument, NULL, 'f'},
	{"thin-provisioning", no_argument, NULL, 'T'},
	{NULL, 0, NULL, 0},
};

static void usage(int status)
{
	if (status != 0)
		fprintf(stderr, "Try `%s --help' for more information.\n", program_name);
	else {
		printf("Usage: %s [OPTION]\n", program_name);
		printf("\
Linux SCSI Target Framework Image File Utility, version %s\n\
\n\
  --op new --device-type tape --barcode=[code] --size=[size] --type=[type] --file=[path] [--thin-provisioning]\n\
			create a new tape image file.\n\
			[code] is a string of chars.\n\
			[size] is media size(in megabytes).\n\
			[type] is media type \n\
				(data, clean or WORM) for tape devices\n\
				(dvd+r) for cd devices\n\
				(disk) for disk devices\n\
			[path] is a newly created file\n\
  --op show --device-type tape --file=[path]\n\
			dump the tape image file contents.\n\
			[path] is the tape image file\n\
  --thin-provisioning   create a sparse file for the media\n\
  --help                display this help and exit\n\
\n\
Report bugs to <stgt@vger.kernel.org>.\n", TGT_VERSION);
	}
	exit(status == 0 ? 0 : EINVAL);
}

static int str_to_device_type(char *str)
{
	if (!strcmp(str, "tape"))
		return TYPE_TAPE;
	else if (!strcmp(str, "cd"))
		return TYPE_MMC;
	else if (!strcmp(str, "disk"))
		return TYPE_DISK;
	else {
		eprintf("unknown target type: %s\n", str);
		exit(EINVAL);
	}
}

static int str_to_op(char *str)
{
	if (!strcmp("new", str))
		return OP_NEW;
	else if (!strcmp("show", str))
		return OP_SHOW;
	else {
		eprintf("unknown operation: %s\n", str);
		exit(1);
	}
}

static void print_current_header(struct blk_header_info *pos)
{
	switch (pos->blk_type) {
	case BLK_UNCOMPRESS_DATA:
		printf(" Uncompressed data");
		break;
	case BLK_FILEMARK:
		printf("         Filemark");
		break;
	case BLK_BOT:
		printf("Beginning of Tape");
		break;
	case BLK_EOD:
		printf("      End of Data");
		break;
	case BLK_NOOP:
		printf("      No Operation");
		break;
	default:
		printf("      Unknown type");
		break;
	}
	if (pos->blk_type == BLK_BOT)
		printf("(%d): Capacity %d MB, Blk No.: %" PRId64
		", prev %" PRId64 ", curr %" PRId64 ", next %" PRId64 "\n",
			pos->blk_type,
			pos->blk_sz,
			pos->blk_num,
			(uint64_t)pos->prev,
			(uint64_t)pos->curr,
			(uint64_t)pos->next);
	else
		printf("(%d): Blk No. %" PRId64 ", prev %" PRId64 ""
			", curr %" PRId64 ",  next %" PRId64 ", sz %d\n",
			pos->blk_type,
			pos->blk_num,
			(uint64_t)pos->prev,
			(uint64_t)pos->curr,
			(uint64_t)pos->next,
			pos->ondisk_sz);
}

static int skip_to_next_header(int fd, struct blk_header_info *pos)
{
	int ret;

	ret = ssc_read_blkhdr(fd, pos, pos->next);
	if (ret)
		printf("Could not read complete blk header - short read!!\n");

	return ret;
}

static int ssc_show(char *path)
{
	int ofp;
	loff_t nread;
	struct MAM_info mam;
	struct blk_header_info current_position;
	time_t t;
	int a;
	unsigned char *p;

	ofp = open(path, O_RDONLY|O_LARGEFILE);
	if (ofp < 0) {
		eprintf("can't open %s, %m\n", path);
		exit(1);
	}

	nread = ssc_read_blkhdr(ofp, &current_position, 0);
	if (nread) {
		perror("Could not read blk header");
		exit(1);
	}

	nread = ssc_read_mam_info(ofp, &mam);
	if (nread) {
		perror("Could not read MAM");
		exit(1);
	}

	if (mam.tape_fmt_version != TGT_TAPE_VERSION) {
		printf("Unknown media format version %x\n",
		       mam.tape_fmt_version);
		exit(1);
	}

	printf("Media     : %s\n", mam.barcode);
	switch (mam.medium_type) {
	case CART_UNSPECIFIED:
		printf(" type     : Unspecified\n");
		break;
	case CART_DATA:
		printf(" type     : Data\n");
		break;
	case CART_CLEAN:
		printf(" type     : Cleaning\n");
		break;
	case CART_DIAGNOSTICS:
		printf(" type     : Diagnostics\n");
		break;
	case CART_WORM:
		printf(" type     : WORM\n");
		break;
	case CART_MICROCODE:
		printf(" type     : Microcode\n");
		break;
	default:
		printf(" type     : Unknown\n");
	}
	printf("Media serial number : %s, ", mam.medium_serial_number);

	for (a = strlen((const char *)mam.medium_serial_number); a > 0; a--)
		if (mam.medium_serial_number[a] == '_')
			break;
	if (a) {
		a++;
		p = &mam.medium_serial_number[a];
		t = atoll((const char *)p);
		printf("created %s", ctime(&t));
	}
	printf("\n");

	print_current_header(&current_position);
	while (current_position.blk_type != BLK_EOD) {
		nread = skip_to_next_header(ofp, &current_position);
		if (nread)
			break;
		print_current_header(&current_position);
	}

	return 0;
}

static int ssc_new(int op, char *path, char *barcode, char *capacity,
		   char *media_type)
{
	struct blk_header_info hdr, *h = &hdr;
	struct MAM_info mi;
	int fd, ret;
	uint8_t current_media[1024];
	uint64_t size;

	sscanf(capacity, "%" SCNu64, &size);
	if (size == 0)
		size = 8000;

	memset(h, 0, sizeof(*h));
	h->blk_type = BLK_BOT;
	h->blk_num = 0;
	h->blk_sz = size;
	h->prev = 0;
	h->curr = 0;
	h->next = sizeof(struct MAM) + SSC_BLK_HDR_SIZE;

	printf("blk_sz: %d, next %" PRId64 ", %" PRId64 "\n",
				h->blk_sz, h->next, h->next);
	printf("Sizeof(mam): %" PRId64 ", sizeof(h): %" PRId64 "\n",
	       (uint64_t)sizeof(struct MAM), (uint64_t)SSC_BLK_HDR_SIZE);

	memset(&mi, 0, sizeof(mi));

	mi.tape_fmt_version = TGT_TAPE_VERSION;
	mi.max_capacity = size * 1048576;
	mi.remaining_capacity = size * 1048576;
	mi.MAM_space_remaining = sizeof(mi.vendor_unique);
	mi.medium_length = 384;	/* 384 tracks */
	mi.medium_width = 127;		/* 127 x tenths of mm (12.7 mm) */
	memcpy(mi.medium_manufacturer, "Foo     ", 8);
	memcpy(mi.application_vendor, "Bar     ", 8);

	if (!strncasecmp("clean", media_type, 5)) {
		mi.medium_type = CART_CLEAN;
		mi.medium_type_information = 20; /* Max cleaning loads */
	} else if (!strncasecmp("WORM", media_type, 4))
		mi.medium_type = CART_WORM;
	else
		mi.medium_type = CART_DATA;

	sprintf((char *)mi.medium_serial_number, "%s_%d", barcode,
		(int)time(NULL));
	sprintf((char *)mi.barcode, "%-31s", barcode);
	sprintf((char *)current_media, "%s", barcode);

	syslog(LOG_DAEMON|LOG_INFO, "TAPE %s being created", path);

	fd = creat(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
	if (fd < 0) {
		perror("Failed creating file");
		exit(2);
	}

	ret = ssc_write_blkhdr(fd, h, 0);
	if (ret) {
		perror("Unable to write header");
		exit(1);
	}

	ret = ssc_write_mam_info(fd, &mi);
	if (ret) {
		perror("Unable to write MAM");
		exit(1);
	}

	memset(h, 0, sizeof(*h));
	h->blk_type = BLK_EOD;
	h->blk_num = 1;
	h->prev = 0;
	h->next = lseek64(fd, 0, SEEK_CUR);
	h->curr = h->next;

	ret = ssc_write_blkhdr(fd, h, h->next);
	if (ret) {
		perror("Unable to write header");
		exit(1);
	}
	close(fd);

	return 0;
}

static int ssc_ops(int op, char *path, char *barcode, char *capacity,
		   char *media_type)
{
	if (op == OP_NEW) {
		if (!media_type) {
			eprintf("Missing media type: WORM, CLEAN or DATA\n");
			usage(1);
		}
		if (strncasecmp("data", media_type, 4)
		&& strncasecmp("clean", media_type, 5)
		&& strncasecmp("worm", media_type, 4)) {
			eprintf("Media type must be WORM, CLEAN or DATA"
				       " for tape devices\n");
			usage(1);
		}
		if (!barcode) {
			eprintf("Missing the barcode param\n");
			usage(1);
		}
		if (!capacity) {
			eprintf("Missing the capacity param\n");
			usage(1);
		}
		return ssc_new(op, path, barcode, capacity, media_type);
	} else if (op == OP_SHOW)
		return ssc_show(path);
	else {
		eprintf("unknown the operation type\n");
		usage(1);
	}

	return 0;
}

static int mmc_new(int op, char *path, char *media_type)
{
	int fd;

	if (!strncasecmp("dvd+r", media_type, 5)) {
		fd = creat(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
		if (fd < 0) {
			perror("Failed creating file");
			exit(2);
		}
		close(fd);

		printf("Created blank DVD+R image file : %s\n", path);
		syslog(LOG_DAEMON|LOG_INFO, "DVD+R %s being created", path);
	} else {
		eprintf("unknown media type when creating cd\n");
		usage(1);
	}

	return 0;
}


static int mmc_ops(int op, char *path, char *media_type)
{
	if (op == OP_NEW) {
		if (!media_type) {
			eprintf("Missing media type: DVD+R\n");
			usage(1);
		}
		if (strncasecmp("dvd+r", media_type, 5)) {
			eprintf("Media type must be DVD+R for cd devices\n");
			usage(1);
		}
		return mmc_new(op, path, media_type);
	} else {
		eprintf("unknown the operation type\n");
		usage(1);
	}

	return 0;
}

static int sbc_new(int op, char *path, char *capacity, char *media_type, int thin)
{
	int fd;

	if (!strncasecmp("disk", media_type, 4)) {
		uint32_t size;
		char *buf;

		sscanf(capacity, "%d", &size);
		if (size == 0) {
			printf("Capacity must be > 0\n");
			exit(3);
		}

		buf = malloc(1024*1024);
		if (buf == NULL) {
			printf("Failed to malloc buffer\n");
			exit(4);
		}
		fd = creat(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
		if (fd < 0) {
			perror("Failed creating file");
			exit(2);
		}
		if (thin) {
			if (ftruncate(fd, size*1024*1024LL) != 0) {
				perror("Failed to set file size");
				exit(6);
			}
			if (unmap_file_region(fd, 0, size*1024*1024LL) != 0) {
				perror("Thin provisioning not available on"
					" this file");
				exit(5);
			}
		} else {
			if (posix_fallocate(fd, 0, size*1024*1024LL) == -1) {
				perror("posix_fallocate failed.");
				exit(3);
			}
		}

		free(buf);
		close(fd);

		printf("Created blank DISK image file : %s\n", path);
		syslog(LOG_DAEMON|LOG_INFO, "DISK %s being created", path);
	} else {
		eprintf("unknown media type when creating disk\n");
		usage(1);
	}

	return 0;
}

static int sbc_ops(int op, char *path, char *capacity, char *media_type, int thin)
{
	if (op == OP_NEW) {
		if (!media_type) {
			eprintf("Missing media type: DISK\n");
			usage(1);
		}
		if (strncasecmp("disk", media_type, 4)) {
			eprintf("Media type must be DISK for disk devices\n");
			usage(1);
		}
		if (!capacity) {
			eprintf("Missing the capacity param\n");
			usage(1);
		}
		return sbc_new(op, path, capacity, media_type, thin);
	} else {
		eprintf("unknown the operation type\n");
		usage(1);
	}

	return 0;
}

int main(int argc, char **argv)
{
	int ch, longindex;
	char *barcode = NULL;
	char *media_type = NULL;
	char *media_capacity = NULL;
	int dev_type = TYPE_TAPE;
	int op = -1;
	char *path = NULL;
	int thin = 0;

	while ((ch = getopt_long(argc, argv, short_options,
				 long_options, &longindex)) >= 0) {
		switch (ch) {
		case 'o':
			op = str_to_op(optarg);
			break;
		case 'Y':
			dev_type = str_to_device_type(optarg);
			break;
		case 'b':
			barcode = optarg;
			break;
		case 's':
			media_capacity = optarg;
			break;
		case 't':
			media_type = optarg;
			break;
		case 'f':
			path = optarg;
			break;
		case 'h':
			usage(0);
			break;
		case 'T':
			thin = 1;
			break;
		default:
			eprintf("unrecognized option '%s'\n", optarg);
			usage(1);
		}
	}

	if (optind < argc) {
		eprintf("unrecognized option '%s'\n", argv[optind]);
		usage(1);
	}

	if (op < 0) {
		eprintf("specify the operation type\n");
		usage(1);
	}

	if (!path) {
		eprintf("specify a newly created file\n");
		usage(1);
	}

	switch (dev_type) {
	case TYPE_TAPE:
		ssc_ops(op, path, barcode, media_capacity, media_type);
		break;
	case TYPE_MMC:
		mmc_ops(op, path, media_type);
		break;
	case TYPE_DISK:
		sbc_ops(op, path, media_capacity, media_type, thin);
		break;
	default:
		eprintf("unsupported the device type operation\n");
		usage(1);
	}

	return 0;
}
