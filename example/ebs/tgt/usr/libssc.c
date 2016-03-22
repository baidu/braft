/*
 * helpers for assessing to ssc on-disk structures
 *
 * Copyright (C) 2008 FUJITA Tomonori <tomof@acm.org>
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

#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include "bs_ssc.h"
#include "ssc.h"
#include "be_byteshift.h"
#include "crc32c.h"

#define SSC_1ST_HDR_OFFSET (sizeof(struct MAM) + SSC_BLK_HDR_SIZE)

#define SSC_GET_MAM_INFO_VAL(member, bits)\
{\
	(i)->member = get_unaligned_be##bits(&((m)->member));\
}

#define SSC_GET_MAM_INFO_ARRAY(member)\
{\
	memcpy((i)->member, (m)->member, sizeof((m)->member));\
}

static inline uint8_t get_unaligned_be8(const uint8_t *p)
{
	return p[0];
}

int ssc_read_mam_info(int fd, struct MAM_info *i)
{
	struct MAM mam, *m;
	int ret;

	m = &mam;

	ret = pread(fd, m, sizeof(struct MAM), SSC_BLK_HDR_SIZE);
	if (ret != sizeof(struct MAM))
		return 1;

	if (lseek64(fd, SSC_1ST_HDR_OFFSET, SEEK_SET) != SSC_1ST_HDR_OFFSET)
		return 1;

	SSC_GET_MAM_INFO_VAL(tape_fmt_version, 32);

	SSC_GET_MAM_INFO_VAL(remaining_capacity, 64);
	SSC_GET_MAM_INFO_VAL(max_capacity, 64);
	SSC_GET_MAM_INFO_VAL(TapeAlert, 64);
	SSC_GET_MAM_INFO_VAL(load_count, 64);
	SSC_GET_MAM_INFO_VAL(MAM_space_remaining, 64);

	SSC_GET_MAM_INFO_ARRAY(assigning_organization_1);
	SSC_GET_MAM_INFO_VAL(formatted_density_code, 8);
	SSC_GET_MAM_INFO_ARRAY(initialization_count);
	SSC_GET_MAM_INFO_ARRAY(dev_make_serial_last_load);

	SSC_GET_MAM_INFO_VAL(written_in_medium_life, 64);
	SSC_GET_MAM_INFO_VAL(read_in_medium_life, 64);
	SSC_GET_MAM_INFO_VAL(written_in_last_load, 64);
	SSC_GET_MAM_INFO_VAL(read_in_last_load, 64);

	SSC_GET_MAM_INFO_ARRAY(medium_manufacturer);
	SSC_GET_MAM_INFO_ARRAY(medium_serial_number);
	SSC_GET_MAM_INFO_VAL(medium_length, 32);
	SSC_GET_MAM_INFO_VAL(medium_width, 32);
	SSC_GET_MAM_INFO_ARRAY(assigning_organization_2);
	SSC_GET_MAM_INFO_VAL(medium_density_code, 8);
	SSC_GET_MAM_INFO_ARRAY(medium_manufacture_date);
	SSC_GET_MAM_INFO_VAL(MAM_capacity, 64);
	SSC_GET_MAM_INFO_VAL(medium_type, 8);
	SSC_GET_MAM_INFO_VAL(medium_type_information, 16);

	SSC_GET_MAM_INFO_ARRAY(application_vendor);
	SSC_GET_MAM_INFO_ARRAY(application_name);
	SSC_GET_MAM_INFO_ARRAY(application_version);
	SSC_GET_MAM_INFO_ARRAY(user_medium_text_label);
	SSC_GET_MAM_INFO_ARRAY(date_time_last_written);
	SSC_GET_MAM_INFO_VAL(localization_identifier, 8);
	SSC_GET_MAM_INFO_ARRAY(barcode);
	SSC_GET_MAM_INFO_ARRAY(owning_host_textual_name);
	SSC_GET_MAM_INFO_ARRAY(media_pool);

	SSC_GET_MAM_INFO_ARRAY(vendor_unique);

	SSC_GET_MAM_INFO_VAL(dirty, 8);

	return 0;
}

#define SSC_PUT_MAM_INFO_VAL(member, bits)\
{\
	put_unaligned_be##bits((i)->member, &((m)->member));\
}

#define SSC_PUT_MAM_INFO_ARRAY(member)\
{\
	memcpy((m)->member, (i)->member, sizeof((m)->member));\
}

static inline void put_unaligned_be8(uint8_t val, uint8_t *p)
{
	*p = val;
}

int ssc_write_mam_info(int fd, struct MAM_info *i)
{
	struct MAM mam, *m;
	int ret;

	m = &mam;
	memset(m, 0, sizeof(struct MAM));

	SSC_PUT_MAM_INFO_VAL(tape_fmt_version, 32);

	SSC_PUT_MAM_INFO_VAL(remaining_capacity, 64);
	SSC_PUT_MAM_INFO_VAL(max_capacity, 64);
	SSC_PUT_MAM_INFO_VAL(TapeAlert, 64);
	SSC_PUT_MAM_INFO_VAL(load_count, 64);
	SSC_PUT_MAM_INFO_VAL(MAM_space_remaining, 64);

	SSC_PUT_MAM_INFO_ARRAY(assigning_organization_1);
	SSC_PUT_MAM_INFO_VAL(formatted_density_code, 8);
	SSC_PUT_MAM_INFO_ARRAY(initialization_count);
	SSC_PUT_MAM_INFO_ARRAY(dev_make_serial_last_load);

	SSC_PUT_MAM_INFO_VAL(written_in_medium_life, 64);
	SSC_PUT_MAM_INFO_VAL(read_in_medium_life, 64);
	SSC_PUT_MAM_INFO_VAL(written_in_last_load, 64);
	SSC_PUT_MAM_INFO_VAL(read_in_last_load, 64);

	SSC_PUT_MAM_INFO_ARRAY(medium_manufacturer);
	SSC_PUT_MAM_INFO_ARRAY(medium_serial_number);
	SSC_PUT_MAM_INFO_VAL(medium_length, 32);
	SSC_PUT_MAM_INFO_VAL(medium_width, 32);
	SSC_PUT_MAM_INFO_ARRAY(assigning_organization_2);
	SSC_PUT_MAM_INFO_VAL(medium_density_code, 8);
	SSC_PUT_MAM_INFO_ARRAY(medium_manufacture_date);
	SSC_PUT_MAM_INFO_VAL(MAM_capacity, 64);
	SSC_PUT_MAM_INFO_VAL(medium_type, 8);
	SSC_PUT_MAM_INFO_VAL(medium_type_information, 16);

	SSC_PUT_MAM_INFO_ARRAY(application_vendor);
	SSC_PUT_MAM_INFO_ARRAY(application_name);
	SSC_PUT_MAM_INFO_ARRAY(application_version);
	SSC_PUT_MAM_INFO_ARRAY(user_medium_text_label);
	SSC_PUT_MAM_INFO_ARRAY(date_time_last_written);
	SSC_PUT_MAM_INFO_VAL(localization_identifier, 8);
	SSC_PUT_MAM_INFO_ARRAY(barcode);
	SSC_PUT_MAM_INFO_ARRAY(owning_host_textual_name);
	SSC_PUT_MAM_INFO_ARRAY(media_pool);

	SSC_PUT_MAM_INFO_ARRAY(vendor_unique);

	SSC_PUT_MAM_INFO_VAL(dirty, 8);

	ret = pwrite(fd, m, sizeof(struct MAM), SSC_BLK_HDR_SIZE);
	if (ret != sizeof(struct MAM))
		return 1;

	if (lseek64(fd, SSC_1ST_HDR_OFFSET, SEEK_SET) != SSC_1ST_HDR_OFFSET)
		return 1;

	return  0;
}

int ssc_read_blkhdr(int fd, struct blk_header_info *i, loff_t offset)
{
	size_t count;
	struct blk_header h, *m = &h;
	uint32_t crc = ~0;

	count = pread64(fd, m, SSC_BLK_HDR_SIZE, offset);
	if (count != SSC_BLK_HDR_SIZE)
		return 1;

	crc = crc32c(crc, &m->ondisk_sz, SSC_BLK_HDR_SIZE - sizeof(m->h_csum));

	if (*(uint32_t *)m->h_csum != ~crc)
		fprintf(stderr, "crc error\n");

	SSC_GET_MAM_INFO_VAL(ondisk_sz, 32);
	SSC_GET_MAM_INFO_VAL(blk_sz, 32);
	SSC_GET_MAM_INFO_VAL(blk_type, 32);
	SSC_GET_MAM_INFO_VAL(blk_num, 64);
	SSC_GET_MAM_INFO_VAL(prev, 64);
	SSC_GET_MAM_INFO_VAL(curr, 64);
	SSC_GET_MAM_INFO_VAL(next, 64);

	return 0;
}

int ssc_write_blkhdr(int fd, struct blk_header_info *i, loff_t offset)
{
	size_t count;
	struct blk_header h, *m = &h;
	uint32_t crc = ~0;

	SSC_PUT_MAM_INFO_VAL(ondisk_sz, 32);
	SSC_PUT_MAM_INFO_VAL(blk_sz, 32);
	SSC_PUT_MAM_INFO_VAL(blk_type, 32);
	SSC_PUT_MAM_INFO_VAL(blk_num, 64);
	SSC_PUT_MAM_INFO_VAL(prev, 64);
	SSC_PUT_MAM_INFO_VAL(curr, 64);
	SSC_PUT_MAM_INFO_VAL(next, 64);

	crc = crc32c(crc, &m->ondisk_sz, SSC_BLK_HDR_SIZE - sizeof(m->h_csum));
	*(uint32_t *)m->h_csum = ~crc;

	count = pwrite64(fd, m, SSC_BLK_HDR_SIZE, offset);
	if (count != SSC_BLK_HDR_SIZE)
		return 1;

	return 0;
}
