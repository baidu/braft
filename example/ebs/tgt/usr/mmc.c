/*
 * SCSI multimedia command processing
 *
 * Copyright (C) 2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2007 Mike Christie <michaelc@cs.wisc.edu>
 * Copyright (C) 2008 Ronnie Sahlberg <ronniesahlberg@gmail.com>
 *
 * This code is based on Ardis's iSCSI implementation.
 *   http://www.ardistech.com/iscsi/
 *   Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>
 *
 * This code is also based on Ming's mmc work for IET.
 * Copyright (C) 2005-2007 Ming Zhang <blackmagic02881@gmail.com>
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
#include <sys/stat.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "tgtadm_error.h"
#include "driver.h"
#include "scsi.h"
#include "spc.h"
#include "tgtadm_error.h"

#define MMC_BLK_SHIFT 11

#define PROFILE_NO_PROFILE		0x0000
#define PROFILE_DVD_ROM			0x0010
#define PROFILE_DVD_PLUS_R		0x001b

struct mmc_info {
	int current_profile;
	uint64_t reserve_track_len;
};

static int mmc_rw(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int ret;
	uint64_t end_offset;
	uint64_t offset, length;

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		switch (cmd->scb[0]) {
		case WRITE_6:
		case WRITE_10:
		case WRITE_12:
		case WRITE_16:
			scsi_set_in_resid_by_actual(cmd, 0);
			sense_data_build(cmd, ILLEGAL_REQUEST,
					 ASC_INCOMPATIBLE_FORMAT);
			return SAM_STAT_CHECK_CONDITION;
		}
		break;
	case PROFILE_DVD_PLUS_R:
		switch (cmd->scb[0]) {
		case READ_6:
		case READ_10:
		case READ_12:
		case READ_16:
			scsi_set_in_resid_by_actual(cmd, 0);
			sense_data_build(cmd, ILLEGAL_REQUEST,
					 ASC_LBA_OUT_OF_RANGE);
			return SAM_STAT_CHECK_CONDITION;
		}
		break;
	}

	offset = scsi_rw_offset(cmd->scb);
	cmd->offset = (offset << MMC_BLK_SHIFT);

	/* update the size of the device */
	length = scsi_rw_count(cmd->scb);
	end_offset = cmd->offset + (length << MMC_BLK_SHIFT);

	if (end_offset > cmd->dev->size)
		cmd->dev->size = end_offset;

	ret = cmd->dev->bst->bs_cmd_submit(cmd);
	if (ret) {
		cmd->offset = 0;
		if (scsi_get_data_dir(cmd) == DATA_WRITE)
			scsi_set_out_resid_by_actual(cmd, 0);
		else
			scsi_set_in_resid_by_actual(cmd, 0);

		sense_data_build(cmd, ILLEGAL_REQUEST, ASC_LUN_NOT_SUPPORTED);
		return SAM_STAT_CHECK_CONDITION;
	} else {
		if ((mmc->current_profile == PROFILE_DVD_PLUS_R) &&
			(mmc->reserve_track_len == (offset + length))) {
			/* once we close the track it becomes a DVD_ROM */
			mmc->current_profile = PROFILE_DVD_ROM;
		}
		return SAM_STAT_GOOD;
	}
	return 0;
}

static int mmc_read_capacity(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	uint64_t size;
	uint32_t *data;

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	if (scsi_get_in_length(cmd) < 8)
		goto overflow;

	data = scsi_get_in_buffer(cmd);
	size = cmd->dev->size >> MMC_BLK_SHIFT;

	if (size)
		data[0] = (size >> 32) ?
			__cpu_to_be32(0xffffffff) : __cpu_to_be32(size - 1);
	else
		data[0] = 0; /* A blank DVD */

	data[1] = __cpu_to_be32(1U << MMC_BLK_SHIFT);
overflow:
	scsi_set_in_resid_by_actual(cmd, 8);
	return SAM_STAT_GOOD;
}

static int mmc_read_toc(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	uint8_t *data;
	uint8_t buf[32];
	int toc_time, toc_format, toc_track;
	unsigned long tsa;

	if (!cmd->dev->attrs.online) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		/* a blank disk has no tracks */
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return SAM_STAT_CHECK_CONDITION;
	}

	toc_time = cmd->scb[1] & 0x02;
	toc_format = cmd->scb[2] & 0x0f;
	toc_track = cmd->scb[6];

	memset(buf, 0, sizeof(buf));
	data = buf;

	switch (toc_format) {
	case 0:	/* formatted toc */
		if (toc_track != 0 && toc_track != 1) {
			/* we only do single session data disks so only
			   the first track (track 1) exists.
			   Since this command returns the data for all 
			   tracks equal to toc_track or higher,
			   we must allow either track 0 or track 1
			   as valid and both will return the data for
			   track 1.
			*/
			scsi_set_in_resid_by_actual(cmd, 0);
			sense_data_build(cmd, NOT_READY,
					 ASC_INVALID_FIELD_IN_CDB);
			return SAM_STAT_CHECK_CONDITION;
		}

		if (toc_time)
			tsa = 0x00ff3b4a;
		else
			tsa = cmd->dev->size >> MMC_BLK_SHIFT; /* lba */

		/* size of return data */
		data[0] = 0;
		data[1] = 0x12;

		data[2] = 1;	/* first track */
		data[3] = 1;	/* last track */

		/* data track */
		data[4] = 0;		/* reserved */
		data[5] = 0x14;
		data[6] = 1;		/* track number */
		data[7] = 0;		/* reserved */
		data[8] = 0;		/* track start address : 0 */
		data[9] = 0;
		if (toc_time)
			data[10] = 2;	/* time 00:00:02:00 */
		else
			data[10] = 0;
		data[11] = 0;

		/* leadout track */
		data[12] = 0;		/* reserved */
		data[13] = 0x14;
		data[14] = 0xaa;	/* track number */
		data[15] = 0;		/* reserved */
		data[16] = (tsa >> 24) & 0xff;/* track start address */
		data[17] = (tsa >> 16) & 0xff;
		data[18] = (tsa >> 8) & 0xff;
		data[19] = tsa & 0xff;

		break;
	case 1:	/* multi session info */
		/* size of return data */
		data[0] = 0;
		data[1] = 0x0a;

		data[2] = 1;	/* first session */
		data[3] = 1;	/* last session */

		/* data track */
		data[4] = 0;		/* reserved */
		data[5] = 0x14;
		data[6] = 1;		/* track number */
		data[7] = 0;		/* reserved */
		data[8] = 0;		/* track start address : 0 */
		data[9] = 0;
		if (toc_time)
			data[10] = 2;	/* time 00:00:02:00 */
		else
			data[10] = 0;
		data[11] = 0;

		break;
	case 2: /* raw toc */
	case 3: /* pma */
	case 4: /* atip */
	case 5: /* cd-text */
		/* not implemented yet */
	default:
		eprintf("read_toc: format %x not implemented\n", toc_format);

		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return SAM_STAT_CHECK_CONDITION;
	}

	memcpy(scsi_get_in_buffer(cmd), data,
	       min(scsi_get_in_length(cmd), (uint32_t) sizeof(buf)));

	scsi_set_in_resid_by_actual(cmd, data[1]);

	return SAM_STAT_GOOD;
}

static int mmc_close_track(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);

	/* once we close the track it becomes a DVD_ROM */
	mmc->current_profile = PROFILE_DVD_ROM;

	return SAM_STAT_GOOD;
}

static int mmc_read_disc_information(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	unsigned char buf[34];

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	memset(buf, 0, sizeof(buf));

	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		/* disk information length */
		buf[0] = 0x00;
		buf[1] = 0x20;

		/*
		 * erasable:0 state of last session:complete disc
		 * status:finalized
		 */
		buf[2] = 0x0e;

		/* number of first track on disk */
		buf[3] = 1;

		/* number of sessions LSB */
		buf[4] = 1;

		/* first track in last session LSB */
		buf[5] = 1;

		/* last track in last session LSB */
		buf[6] = 1;

		/* did_v:0 dbc_v:0 uru:0 dac_v:0 dbit:0 bg format:0 */
		buf[7] = 0x00;

		/* disc type */
		buf[8] = 0;

		/* number of sessions MSB */
		buf[9] = 0;

		/* first track in last session MSB */
		buf[10] = 0;

		/* last track in last session MSB */
		buf[11] = 0;

		/* disc identification */
		buf[12] = 0;
		buf[13] = 0;
		buf[14] = 0;
		buf[15] = 0;

		/* last session lead-in start address */
		buf[16] = 0;
		buf[17] = 0;
		buf[18] = 0;
		buf[19] = 0;

		/* last possible lead-out start address */
		buf[20] = 0;
		buf[21] = 0;
		buf[22] = 0;
		buf[23] = 0;

		/* disc bar code */
		buf[24] = 0;
		buf[25] = 0;
		buf[26] = 0;
		buf[27] = 0;
		buf[28] = 0;
		buf[29] = 0;
		buf[30] = 0;
		buf[31] = 0;

		/* disc application code */
		buf[32] = 0;

		/* number of opc tables */
		buf[33] = 0;

		break;
	case PROFILE_DVD_PLUS_R:
		/* disk information length */
		buf[0] = 0x00;
		buf[1] = 0x20;

		/* erasable:0 state of last session:empty disc status:empty */
		buf[2] = 0;

		/* number of first track on disk */
		buf[3] = 1;

		/* number of sessions LSB */
		buf[4] = 1;

		/* first track in last session LSB */
		buf[5] = 1;

		/* last track in last session LSB */
		buf[6] = 1;

		/* did_v:0 dbc_v:0 uru:0 dac_v:1 dbit:0 bg format:0 */
		buf[7] = 0x10;

		/* disc type */
		buf[8] = 0;

		/* number of sessions MSB */
		buf[9] = 0;

		/* first track in last session MSB */
		buf[10] = 0;

		/* last track in last session MSB */
		buf[11] = 0;

		/* disc identification */
		buf[12] = 0;
		buf[13] = 0;
		buf[14] = 0;
		buf[15] = 0;

		/* last session lead-in start address */
		buf[16] = 0;
		buf[17] = 0;
		buf[18] = 0;
		buf[19] = 0;

		/* last possible lead-out start address */
		buf[20] = 0x00;
		buf[21] = 0x23;
		buf[22] = 0x05;
		buf[23] = 0x40;

		/* disc bar code */
		buf[24] = 0;
		buf[25] = 0;
		buf[26] = 0;
		buf[27] = 0;
		buf[28] = 0;
		buf[29] = 0;
		buf[30] = 0;
		buf[31] = 0;

		/* disc application code */
		buf[32] = 0;

		/* number of opc tables */
		buf[33] = 0;
		break;
	default:
		/* we do not understand/support this command for this profile */
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return SAM_STAT_CHECK_CONDITION;
	}

	memcpy(scsi_get_in_buffer(cmd), buf,
	       min_t(uint32_t, scsi_get_in_length(cmd), sizeof(buf)));
	return SAM_STAT_GOOD;
}

static void profile_dvd_rom(struct scsi_cmd *cmd, char *data)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);

	/* profile number */
	*data++ = 0;
	*data++ = 0x10;

	/* current ? */
	if (mmc->current_profile == PROFILE_DVD_ROM)
		*data++ = 0x01;
	else
		*data++ = 0;

	/* reserved */
	*data++ = 0;
}

static void profile_dvd_plus_r(struct scsi_cmd *cmd, char *data)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);

	/* profile number */
	*data++ = 0;
	*data++ = 0x1b;

	/* current ? */
	if (mmc->current_profile == PROFILE_DVD_PLUS_R)
		*data++ = 0x01;
	else
		*data++ = 0;

	/* reserved */
	*data++ = 0;
}

struct profile_descriptor {
	int profile;
	void (*func)(struct scsi_cmd *cmd, char *data);
};
struct profile_descriptor profiles[] = {
	{PROFILE_DVD_ROM,	profile_dvd_rom},
	{PROFILE_DVD_PLUS_R,	profile_dvd_plus_r},
	{0, NULL}
};

/*
 * these features are mandatory for profile DVD_ROM
 * FEATURE_PROFILE_LIST
 * FEATURE_CORE
 * FEATURE_MORHPING
 * FEATURE_REMOVABLE_MEDIUM
 * FEATURE_RANDOM_READABLE
 * FEATURE_DVD_READ
 * FEATURE_POWER_MANAGEMENT
 * FEATURE_TIMEOUT
 * FEATURE_REAL_TIME_STREAMING
 *
 * these features are mandatory for profile DVD+R
 * FEATURE_PROFILE_LIST
 * FEATURE_CORE
 * FEATURE_MORHPING
 * FEATURE_REMOVABLE_MEDIUM
 * FEATURE_RANDOM_READABLE
 * FEATURE_DVD_READ
 * FEATURE_DVD_PLUS_R
 * FEATURE_POWER_MANAGEMENT
 * FEATURE_TIMEOUT
 * FEATURE_REAL_TIME_STREAMING
 * FEATURE_DCBS
 *
 * additional features
 * FEATURE_MULIT_READ
 * FEATURE_LUN_SERIAL_NO
 */
static char *feature_profile_list(struct scsi_cmd *cmd, char *data,
				  int only_current)
{
	struct profile_descriptor *p;
	char *additional;

	/* feature code */
	*data++ = 0;
	*data++ = 0;

	/* version 0  always persistent, always current */
	*data++ = 0x03;

	/* additional length start at 0*/
	additional = data++;
	*additional = 0;

	/* all all profiles we support */
	for (p = profiles; p->func; p++) {
		p->func(cmd, data);
		*additional = (*additional) + 4;
		data += 4;
	}

	return data;
}

static char *feature_core(struct scsi_cmd *cmd, char *data, int only_current)
{
	/* feature code */
	*data++ = 0;
	*data++ = 0x01;

	/* version 0  always persistent, always current */
	*data++ = 0x03;

	/* additional length */
	*data++ = 4;

	/* physical interface standard : atapi*/
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;
	*data++ = 2;

	return data;
}

static char *feature_morphing(struct scsi_cmd *cmd, char *data,
			      int only_current)
{
	/* feature code */
	*data++ = 0;
	*data++ = 0x02;

	/* version 0  always persistent, always current */
	*data++ = 0x03;

	/* additional length */
	*data++ = 4;

	/* dont support ocevent or async */
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;

	return data;
}

static char *feature_removable_medium(struct scsi_cmd *cmd, char *data,
				      int only_current)
{
	/* feature code */
	*data++ = 0;
	*data++ = 0x03;

	/* version 0  always persistent, always current */
	*data++ = 0x03;

	/* additional length */
	*data++ = 4;

	/* loading mechanism:tray
	 * ejectable through STARTSTOPUNIT loej
	 * vnt : PREVENTALLOWMEDIUMREMOVAL supported
	 * lock : medium can be locked
	*/
	*data++ = 0x29;

	/* reserved */
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;

	return data;
}

static char *feature_random_readable(struct scsi_cmd *cmd, char *data,
				     int only_current)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int is_current;

	/* this feature is only current in DVD_ROM */
	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		is_current = 1;
		break;
	default:
		is_current = 0;
	}

	if (only_current && !is_current)
		return data;

	/* feature code */
	*data++ = 0;
	*data++ = 0x10;

	/* version 0 , never persistent */
	*data = 0;
	if (is_current)
		*data |= 0x01;
	data++;

	/* additional length */
	*data++ = 8;

	/* logical block size */
	*data++ = 0;
	*data++ = 0;
	*data++ = 8;
	*data++ = 0;

	/* blocking is always 0x10 for dvd devices */
	*data++ = 0;
	*data++ = 0x10;

	/* pp is supported */
	*data++ = 0x01;

	/* reserved */
	*data++ = 0;

	return data;
}

static char *feature_dvd_read(struct scsi_cmd *cmd, char *data,
			      int only_current)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int is_current;

	/* this feature is only current in DVD_ROM */
	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		is_current = 1;
		break;
	default:
		is_current = 0;
	}

	if (only_current && !is_current)
		return data;

	/* feature code */
	*data++ = 0;
	*data++ = 0x1f;

	/* version 0, never persistent */
	*data = 0;
	if (is_current)
		*data |= 0x01;
	data++;

	/* additional length */
	*data++ = 0;

	return data;
}

static char *feature_power_management(struct scsi_cmd *cmd, char *data,
				      int only_current)
{
	/* feature code */
	*data++ = 0x01;
	*data++ = 0x00;

	/* version 0   always persistent always current */
	*data++ = 0x03;

	/* additional length */
	*data++ = 0;

	return data;
}

static char *feature_timeout(struct scsi_cmd *cmd, char *data, int only_current)
{
	/* feature code */
	*data++ = 0x01;
	*data++ = 0x05;

	/* version 0   always persistent always current */
	*data++ = 0x03;

	/* additional length */
	*data++ = 0;

	return data;
}

static char *feature_real_time_streaming(struct scsi_cmd *cmd, char *data,
					 int only_current)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int is_current;

	/* this feature is only current in DVD_ROM */
	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		is_current = 1;
		break;
	default:
		is_current = 0;
	}

	if (only_current && !is_current)
		return data;

	/* feature code */
	*data++ = 0x01;
	*data++ = 0x07;

	/* version 3 */
	*data = 0x0c;
	if (is_current)
		*data |= 0x01;
	data++;

	/* additional length */
	*data++ = 4;

	/* flags */
	*data++ = 0x1f;

	/* reserved */
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;

	return data;
}

static char *feature_dvd_plus_r(struct scsi_cmd *cmd, char *data,
				int only_current)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int is_current;

	/* this feature is only current in DVD+R */
	switch (mmc->current_profile) {
	case PROFILE_DVD_PLUS_R:
		is_current = 1;
		break;
	default:
		is_current = 0;
	}

	if (only_current && !is_current)
		return data;

	/* feature code */
	*data++ = 0;
	*data++ = 0x2b;

	/* version 0 */
	*data = 0;
	if (is_current)
		*data |= 0x01;
	data++;

	/* additional length */
	*data++ = 4;

	/* we support WRITE of DVD+R when profile is DVD+R*/
	*data++ = 0x01;

	/* reserved */
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;

	return data;
}

static char *feature_lun_serial_no(struct scsi_cmd *cmd, char *data,
				   int only_current)
{
	struct lu_phy_attr *attrs;
	struct vpd *vpd_pg;

	/* feature code */
	*data++ = 0x01;
	*data++ = 0x08;

	/* version 0 */
	*data++ = 0x03;

	/* additional length */
	*data++ = 8;

	/* serial number */
	attrs = &cmd->dev->attrs;
	vpd_pg = attrs->lu_vpd[PCODE_OFFSET(0x80)];
	if (vpd_pg->size == 8)
		memcpy(data, vpd_pg->data, 8);

	data += 8;

	return data;
}

static char *feature_multi_read(struct scsi_cmd *cmd, char *data,
				int only_current)
{
	/* feature code */
	*data++ = 0;
	*data++ = 0x1d;

	/* version 0 */
	*data++ = 0x00;

	/* additional length */
	*data++ = 0;

	return data;
}

static char *feature_dcbs(struct scsi_cmd *cmd, char *data, int only_current)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int is_current;

	/* this feature is only current in DVD+R */
	switch (mmc->current_profile) {
	case PROFILE_DVD_PLUS_R:
		is_current = 1;
		break;
	default:
		is_current = 0;
	}

	if (only_current && !is_current)
		return data;

	/* feature code */
	*data++ = 0x01;
	*data++ = 0x0a;

	/* version 0 */
	*data = 0;
	if (is_current)
		*data |= 0x01;
	data++;

	/* additional length : 12 */
	*data++ = 0x0c;

	/* DCB entry 0 */
	*data++ = 0x46;
	*data++ = 0x44;
	*data++ = 0x43;
	*data++ = 0x00;

	/* DCB entry 1 */
	*data++ = 0x53;
	*data++ = 0x44;
	*data++ = 0x43;
	*data++ = 0x00;

	/* DCB entry 2 */
	*data++ = 0x54;
	*data++ = 0x4f;
	*data++ = 0x43;
	*data++ = 0x00;

	return data;
}

#define FEATURE_PROFILE_LIST		0x0000
#define FEATURE_CORE			0x0001
#define FEATURE_MORHPING		0x0002
#define FEATURE_REMOVABLE_MEDIUM	0x0003
#define FEATURE_RANDOM_READABLE		0x0010
#define FEATURE_MULTI_READ		0x001d
#define FEATURE_DVD_READ		0x001f
#define FEATURE_DVD_PLUS_R		0x002b
#define FEATURE_POWER_MANAGEMENT	0x0100
#define FEATURE_TIMEOUT			0x0105
#define FEATURE_REAL_TIME_STREAMING	0x0107
#define FEATURE_LUN_SERIAL_NO		0x0108
#define FEATURE_DCBS			0x010a
struct feature_descriptor {
	int feature;
	char *(*func)(struct scsi_cmd *cmd, char *data, int only_current);
};
/* this array MUST list the features in numerical order */
struct feature_descriptor features[] = {
	{FEATURE_PROFILE_LIST,		feature_profile_list},
	{FEATURE_CORE,			feature_core},
	{FEATURE_MORHPING,		feature_morphing},
	{FEATURE_REMOVABLE_MEDIUM,	feature_removable_medium},
	{FEATURE_RANDOM_READABLE,	feature_random_readable},
	{FEATURE_MULTI_READ,		feature_multi_read},
	{FEATURE_DVD_READ,		feature_dvd_read},
	{FEATURE_DVD_PLUS_R,		feature_dvd_plus_r},
	{FEATURE_POWER_MANAGEMENT,	feature_power_management},
	{FEATURE_TIMEOUT,		feature_timeout},
	{FEATURE_REAL_TIME_STREAMING,	feature_real_time_streaming},
	{FEATURE_LUN_SERIAL_NO,		feature_lun_serial_no},
	{FEATURE_DCBS,			feature_dcbs},
	{0, NULL},
};

static int mmc_get_configuration(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	char *data;
	char buf[1024];
	int rt, start;
	struct feature_descriptor *f;
	int tmp;

	rt = cmd->scb[1] & 0x03;

	start = cmd->scb[2];
	start = (start << 8) | cmd->scb[3];

	memset(buf, 0, sizeof(buf));
	data = buf;
	/* skip past size */
	data += 4;

	/* reserved */
	*data++ = 0;
	*data++ = 0;
	/* current profile */
	*data++ = (mmc->current_profile >> 8) & 0xff;
	*data++ = mmc->current_profile & 0xff;

	/* add the features */
	for (f = features; f->func; f++) {
		/* only return features >= the start feature */
		if (f->feature < start)
			continue;
		/* if rt==2 we skip all other features except start */
		if (rt == 2 && f->feature != start)
			continue;

		data = f->func(cmd, data, rt == 1);
	}

	tmp = data-buf;
	tmp -= 4;

	buf[0] = (tmp >> 24) & 0xff;
	buf[1] = (tmp >> 16) & 0xff;
	buf[2] = (tmp >> 8) & 0xff;
	buf[3] = tmp & 0xff;

	tmp = data-buf;

	memcpy(scsi_get_in_buffer(cmd), buf,
	       min_t(uint32_t, scsi_get_in_length(cmd), sizeof(buf)));

	/* dont report overflow/underflow for GET CONFIGURATION */
	return SAM_STAT_GOOD;
}

static unsigned char *track_type_lba(struct scsi_cmd *cmd, unsigned char *data,
				     unsigned int lba)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	unsigned long tmp;

	switch (mmc->current_profile) {
	case PROFILE_DVD_PLUS_R:
		/* track number LSB */
		*data++ = 1;

		/* session number LSB */
		*data++ = 1;

		/* reserved */
		*data++ = 0;

		/* damage:0 copy:0 track_mode:DVD+R */
		*data++ = 0x07;

		/* rt:0 blank:1 packet/inc:0 fp:0 data mode:1 */
		*data++ = 0x41;

		/* lra_v:0 nwa_v:1 */
		*data++ = 0x01;

		/* track start address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* next writeable address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* free blocks */
		*data++ = 0x00;
		*data++ = 0x23;
		*data++ = 0x05;
		*data++ = 0x40;

		/* blocking factor */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x10;

		/* track size */
		*data++ = 0x00;
		*data++ = 0x23;
		*data++ = 0x05;
		*data++ = 0x40;

		/* last recorded address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* track number MSB */
		*data++ = 0;

		/* session number MSB */
		*data++ = 0;

		/* reserved */
		data += 2;

		/* read compat lba */
		*data++ = 0x00;
		*data++ = 0x04;
		*data++ = 0x0d;
		*data++ = 0x0e;

		return data;
	case PROFILE_DVD_ROM:
		/* track number LSB */
		*data++ = 1;

		/* session number LSB */
		*data++ = 1;

		/* reserved */
		*data++ = 0;

		/* damage:0 copy:0 track_mode:other media */
		*data++ = 0x04;

		/* rt:0 blank:0 packet/inc:0 fp:0 data mode:1 */
		*data++ = 0x01;

		/* lra_v:1 nwa_v:0 */
		*data++ = 0x02;

		/* track start address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* next writeable address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* free blocks */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* blocking factor */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x10;

		/* track size */
		tmp = cmd->dev->size >> MMC_BLK_SHIFT;
		*data++ = (tmp >> 24) & 0xff;
		*data++ = (tmp >> 16) & 0xff;
		*data++ = (tmp >> 8) & 0xff;
		*data++ = tmp & 0xff;

		/* last recorded address */
		tmp--;  /* one less */
		*data++ = (tmp >> 24) & 0xff;
		*data++ = (tmp >> 16) & 0xff;
		*data++ = (tmp >> 8) & 0xff;
		*data++ = tmp & 0xff;

		/* track number MSB */
		*data++ = 0;

		/* session number MSB */
		*data++ = 0;

		/* reserved */
		data += 2;

		return data;
	}

	/* we do not understand/support this profile */
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
	return NULL;
}

static unsigned char *track_type_track(struct scsi_cmd *cmd,
				       unsigned char *data, unsigned int lba)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	unsigned long tmp;

	switch (mmc->current_profile) {
	case PROFILE_DVD_PLUS_R:
		if (!lba) {
			scsi_set_in_resid_by_actual(cmd, 0);
			sense_data_build(cmd, NOT_READY,
					 ASC_INVALID_FIELD_IN_CDB);
			return NULL;
		}

		/* track number LSB */
		*data++ = 1;

		/* session number LSB */
		*data++ = 1;

		/* reserved */
		*data++ = 0;

		/* damage:0 copy:0 track_mode:DVD+R */
		*data++ = 0x07;

		/* rt:0 blank:1 packet/inc:0 fp:0 data mode:1 */
		*data++ = 0x41;

		/* lra_v:0 nwa_v:1 */
		*data++ = 0x01;

		/* track start address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* next writeable address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* free blocks */
		*data++ = 0x00;
		*data++ = 0x23;
		*data++ = 0x05;
		*data++ = 0x40;

		/* blocking factor */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x10;

		/* track size */
		*data++ = 0x00;
		*data++ = 0x23;
		*data++ = 0x05;
		*data++ = 0x40;

		/* last recorded address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* track number MSB */
		*data++ = 0;

		/* session number MSB */
		*data++ = 0;

		/* reserved */
		data += 2;

		/* read compat lba */
		*data++ = 0x00;
		*data++ = 0x04;
		*data++ = 0x0d;
		*data++ = 0x0e;

		return data;
	case PROFILE_DVD_ROM:
		/* we only have one track */
		if (lba != 1) {
			scsi_set_in_resid_by_actual(cmd, 0);
			sense_data_build(cmd, NOT_READY,
					 ASC_INVALID_FIELD_IN_CDB);
			return NULL;
		}

		/* track number LSB */
		*data++ = 1;

		/* session number LSB */
		*data++ = 1;

		/* reserved */
		*data++ = 0;

		/* damage:0 copy:0 track_mode:other media */
		*data++ = 0x04;

		/* rt:0 blank:0 packet/inc:0 fp:0 data mode:1 */
		*data++ = 0x01;

		/* lra_v:1 nwa_v:0 */
		*data++ = 0x02;

		/* track start address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* next writeable address */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* free blocks */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* blocking factor */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x10;

		/* track size */
		tmp = cmd->dev->size >> MMC_BLK_SHIFT;
		*data++ = (tmp >> 24) & 0xff;
		*data++ = (tmp >> 16) & 0xff;
		*data++ = (tmp >> 8) & 0xff;
		*data++ = tmp & 0xff;

		/* last recorded address */
		tmp--;  /* one less */
		*data++ = (tmp >> 24) & 0xff;
		*data++ = (tmp >> 16) & 0xff;
		*data++ = (tmp >> 8) & 0xff;
		*data++ = tmp & 0xff;

		/* track number MSB */
		*data++ = 0;

		/* session number MSB */
		*data++ = 0;

		/* reserved */
		data += 2;

		return data;
	}

	/* we do not understand/support this profile */
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
	return NULL;
}


#define TRACK_INFO_LBA		0
#define TRACK_INFO_TRACK	1

struct track_type {
	int type;
	unsigned char *(*func)(struct scsi_cmd *cmd, unsigned char *data,
			       unsigned int lba);
};
struct track_type track_types[] = {
	{TRACK_INFO_LBA,     track_type_lba},
	{TRACK_INFO_TRACK,   track_type_track},
	{0, NULL}
};

static int mmc_read_track_information(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	struct track_type *t;
	unsigned char *data;
	unsigned char buf[4096];
	int type;
	int lba;

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	type = cmd->scb[1]&0x03;

	lba = cmd->scb[2];
	lba = (lba<<8) | cmd->scb[3];
	lba = (lba<<8) | cmd->scb[4];
	lba = (lba<<8) | cmd->scb[5];

	memset(buf, 0, sizeof(buf));
	data = &buf[2];

	for (t = track_types; t->func; t++) {
		int tmp;

		if (t->type != type)
			continue;

		data = t->func(cmd, data, lba);

		if (!data)
			return SAM_STAT_CHECK_CONDITION;

		tmp = data-&buf[2];
		buf[0] = (tmp >> 8) & 0xff;
		buf[1] = tmp & 0xff;
		memcpy(scsi_get_in_buffer(cmd), buf,
		       min_t(uint32_t, scsi_get_in_length(cmd), sizeof(buf)));
		return SAM_STAT_GOOD;
	}

	/* we do not understand this track type */
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
	return SAM_STAT_CHECK_CONDITION;
}

static int mmc_read_buffer_capacity(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int blocks;
	unsigned char buf[12];
	long tmp;

	memset(buf, 0, sizeof(buf));
	blocks = cmd->scb[1]&0x01;

	switch (mmc->current_profile) {
	case PROFILE_NO_PROFILE:
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	case PROFILE_DVD_ROM:
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, ILLEGAL_REQUEST, ASC_IMCOMPATIBLE_FORMAT);
		return SAM_STAT_CHECK_CONDITION;
	case PROFILE_DVD_PLUS_R:
		/* data length */
		buf[0] = 0x00;
		buf[1] = 0x0a;

		/* 4096 blocks */
		tmp = 0x1000;
		if (!blocks) {
			/* convert to bytes */
			tmp <<= MMC_BLK_SHIFT;
		}

		/* length of buffer */
		buf[4] = (tmp >> 24) & 0xff;
		buf[5] = (tmp >> 16) & 0xff;
		buf[6] = (tmp >> 8) & 0xff;
		buf[7] = tmp & 0xff;

		/* available length of buffer (always half) */
		tmp = tmp >> 1;
		buf[8]  = (tmp >> 24) & 0xff;
		buf[9]  = (tmp >> 16) & 0xff;
		buf[10] = (tmp >> 8) & 0xff;
		buf[11] = tmp & 0xff;

		memcpy(scsi_get_in_buffer(cmd), buf,
		       min(scsi_get_in_length(cmd), (uint32_t) sizeof(buf)));
		scsi_set_in_resid_by_actual(cmd, buf[1] + 2);
		return SAM_STAT_GOOD;
	}

	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, MEDIUM_ERROR, ASC_INVALID_FIELD_IN_CDB);
	return SAM_STAT_CHECK_CONDITION;
}

static int mmc_synchronize_cache(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	return SAM_STAT_GOOD;
}

static unsigned char *perf_type_write_speed(struct scsi_cmd *cmd,
					    unsigned char *data,
					    unsigned int type,
					    unsigned int data_type)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);

	/* write/except */
	*data++ = 0x00;

	data += 3;

	switch (mmc->current_profile) {
	case PROFILE_NO_PROFILE:
		/* descriptor 0 */
		/* wrcc:CLV rdd:0 exact:0 mrw:0 */
		*data++ = 0x00;

		/* reserved */
		data += 3;

		/* end lba */
		*data++ = 0x00;
		*data++ = 0x25;
		*data++ = 0x99;
		*data++ = 0x99;

		/* read speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x0a;
		*data++ = 0xd2;

		/* write speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x0a;
		*data++ = 0xd2;

		/* descriptor 1 */
		/* wrcc:CLV rdd:0 exact:0 mrw:0 */
		*data++ = 0x00;

		/* reserved */
		data += 3;

		/* end lba */
		*data++ = 0x00;
		*data++ = 0x25;
		*data++ = 0x99;
		*data++ = 0x99;

		/* read speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x05;
		*data++ = 0x69;

		/* write speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x05;
		*data++ = 0x69;

		return data;
		break;
	case PROFILE_DVD_PLUS_R:
		/* descriptor 0 */
		/* wrcc:CLV rdd:0 exact:0 mrw:1 */
		*data++ = 0x01;

		/* reserved */
		data += 3;

		/* end lba */
		*data++ = 0x00;
		*data++ = 0x23;
		*data++ = 0x05;
		*data++ = 0x3f;

		/* read speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x0c;
		*data++ = 0xfc;

		/* write speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x0c;
		*data++ = 0xfc;

		return data;
		break;
	case PROFILE_DVD_ROM:
		/* descriptor 0 */
		/* wrcc:CLV rdd:0 exact:0 mrw:0 */
		*data++ = 0x00;

		/* reserved */
		data += 3;

		/* end lba */
		*data++ = 0x00;
		*data++ = 0x25;
		*data++ = 0x99;
		*data++ = 0x99;

		/* read speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x0a;
		*data++ = 0xd2;

		/* write speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x0a;
		*data++ = 0xd2;

		/* descriptor 1 */
		/* wrcc:CLV rdd:0 exact:0 mrw:0 */
		*data++ = 0x00;

		/* reserved */
		data += 3;

		/* end lba */
		*data++ = 0x00;
		*data++ = 0x25;
		*data++ = 0x99;
		*data++ = 0x99;

		/* read speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x05;
		*data++ = 0x69;

		/* write speed */
		*data++ = 0x00;
		*data++ = 0x00;
		*data++ = 0x05;
		*data++ = 0x69;

		return data;
		break;
	}

	/* we do not understand/support this command */
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
	return NULL;
}

static unsigned char *perf_type_perf_data(struct scsi_cmd *cmd,
					  unsigned char *data,
					  unsigned int type,
					  unsigned int data_type)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	int tolerance;
	int write_flag;
	int except;
	long tmp;

	tolerance  = (data_type >> 3) & 0x03;
	write_flag = (data_type >> 2) & 0x01;
	except = data_type & 0x03;

	/* all other values for tolerance are reserved */
	if (tolerance != 0x02) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return NULL;
	}

	switch (except) {
	case 1:
	case 2:
		/* write/except */
		*data++ = 0x01;

		/* reserved */
		data += 3;

		/* no actual descriptor returned here */
		return data;
	case 3:
		/* reserved */
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return NULL;
	}

	/* write/except */
	if (write_flag)
		*data++ = 0x02;
	else
		*data++ = 0x00;

	/* reserved */
	data += 3;

	/* start lba */
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;
	*data++ = 0;

	/* start performance */
	*data++ = 0x00;
	*data++ = 0x00;
	*data++ = 0x15;
	*data++ = 0xa4;

	/* end lba */
	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		tmp = (cmd->dev->size >> MMC_BLK_SHIFT) - 1;
		break;
	default:
		tmp = 0x23053f;
	}

	*data++ = (tmp >> 24) & 0xff;
	*data++ = (tmp >> 16) & 0xff;
	*data++ = (tmp >> 8) & 0xff;
	*data++ = tmp & 0xff;

	/* end performance */
	*data++ = 0x00;
	*data++ = 0x00;
	*data++ = 0x15;
	*data++ = 0xa4;

	return data;
}

#define PERF_TYPE_PERF_DATA		0x00
#define PERF_TYPE_WRITE_SPEED		0x03
struct perf_type {
	int type;
	unsigned char *(*func)(struct scsi_cmd *cmd, unsigned char *data,
			       unsigned int type, unsigned int data_type);
};
struct perf_type perf_types[] = {
	{PERF_TYPE_PERF_DATA, perf_type_perf_data},
	{PERF_TYPE_WRITE_SPEED, perf_type_write_speed},
	{0, NULL}
};

static int mmc_get_performance(int host_no, struct scsi_cmd *cmd)
{
	unsigned int type;
	unsigned int num_desc;
	unsigned long lba;
	unsigned int data_type;
	struct perf_type *p;
	unsigned char *data;
	unsigned char buf[256];

	memset(buf, 0, sizeof(buf));
	data = &buf[4];

	data_type = cmd->scb[1]&0x1f;

	lba = cmd->scb[2];
	lba = (lba<<8)|cmd->scb[3];
	lba = (lba<<8)|cmd->scb[4];
	lba = (lba<<8)|cmd->scb[5];

	num_desc = cmd->scb[8];
	num_desc = (num_desc<<8)|cmd->scb[9];

	type = cmd->scb[10];

	for (p = perf_types; p->func; p++) {
		int tmp;

		if (p->type != type)
			continue;

		data = p->func(cmd, data, type, data_type);
		if (!data)
			return SAM_STAT_CHECK_CONDITION;

		tmp = data-&buf[4];
		buf[0] = (tmp >> 24) & 0xff;
		buf[1] = (tmp >> 16) & 0xff;
		buf[2] = (tmp >> 8) & 0xff;
		buf[3] = tmp & 0xff;
		memcpy(scsi_get_in_buffer(cmd), buf,
		       min_t(uint32_t, scsi_get_in_length(cmd), sizeof(buf)));
		return SAM_STAT_GOOD;

	}
	/* we do not understand/support this command */
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
	return SAM_STAT_CHECK_CONDITION;
}


static int mmc_set_streaming(int host_no, struct scsi_cmd *cmd)
{
	return SAM_STAT_GOOD;
}

#define DVD_FORMAT_PHYS_INFO		0x00
#define DVD_FORMAT_DVD_COPYRIGHT_INFO	0x01
#define DVD_FORMAT_ADIP_INFO		0x11
#define DVD_FORMAT_DVD_STRUCTURE_LIST	0xff

static unsigned char *dvd_format_phys_info(struct scsi_cmd *cmd,
					   unsigned char *data, int format,
					   int layer, int write_header)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	unsigned char *old_data;

	if (write_header) {
		*data++ = DVD_FORMAT_PHYS_INFO;
		*data++ = 0x40;
		*data++ = 0x08;
		*data++ = 0x02;	/* 0x800 bytes data, 2 reserved bytes */
		return data;
	}

	if (layer) {
		/* we only support single layer disks */
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return NULL;
	}

	switch (mmc->current_profile) {
	case PROFILE_DVD_ROM:
		/* book type DVD-ROM, part version */
		*data++ = 0x01;

		/* disk size 120mm, maximum rate 10mbit/s */
		*data++ = 0x02;

		/* num layers:1  layer type: embossed*/
		*data++ = 0x01;

		/* linear density: track density: */
		*data++ = 0x10;

		*data++ = 0;

		/* starting physical sector number of data area */
		*data++ = 3;
		*data++ = 0;
		*data++ = 0;

		/* end physical sector number of data area */
		*data++ = 0x1b;
		*data++ = 0xc1;
		*data++ = 0x7f;

		*data++ = 0;

		/* end physical sector number in layer 0 */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* bca */
		*data++ = 0;

		/* just leave the media specific area as 0 */
		data += 2031;
		break;
	case PROFILE_DVD_PLUS_R:
		/* book type DVD+R, part version */
		*data++ = 0xa1;

		/* disk size 120mm, maximum rate nof specified */
		*data++ = 0x0f;

		/* num layers:1  layer type:recordable */
		*data++ = 0x02;

		/* linear density: track density: */
		*data++ = 0x00;

		*data++ = 0;

		/* starting physical sector number of data area */
		*data++ = 3;
		*data++ = 0;
		*data++ = 0;

		*data++ = 0;

		/* end physical sector number of data area */
		*data++ = 0x26;
		*data++ = 0x05;
		*data++ = 0x3f;

		*data++ = 0;

		/* end physical sector number in layer 0 */
		*data++ = 0;
		*data++ = 0;
		*data++ = 0;

		/* bca */
		*data++ = 0;


		/* data, copied from a blank disk */
		/* not in t.10 spec */
		old_data = data;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00; *data++ = 0x00;
		*data++ = 0x00; *data++ = 0x00; *data++ = 0x00;

		data = old_data + 2031;

		break;
	default:
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return NULL;
	}

	return data;
}

static unsigned char *dvd_format_adip_info(struct scsi_cmd *cmd,
					   unsigned char *data, int format,
					   int layer, int write_header)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);

	if (write_header) {
		*data++ = DVD_FORMAT_ADIP_INFO;
		switch (mmc->current_profile) {
		case PROFILE_DVD_PLUS_R:
			*data++ = 0x40;	/* readable */
			break;
		default:
			*data++ = 0;
		}
		*data++ = 0x01;
		*data++ = 0x02;	/* 0x100 bytes data, 2 reserved bytes */
		return data;
	}

	switch (mmc->current_profile) {
	case PROFILE_DVD_PLUS_R:
		break;
	default:
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return NULL;
	}

	/* adip information */
	/* adip for DVD+R not in t.10 mmc spec, */
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;
	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;	*data++ = 0x00;

	return data;
}

static unsigned char *dvd_format_copyright_info(struct scsi_cmd *cmd,
						unsigned char *data,
						int format, int layer,
						int write_header)
{
	if (write_header) {
		*data++ = DVD_FORMAT_DVD_COPYRIGHT_INFO;
		*data++ = 0x40;	/* readable */
		*data++ = 0;
		*data++ = 6;	/* 4 bytes data, 2 reserved bytes */
		return data;
	}

	if (layer) {
		/* we only support single layer disks */
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
		return NULL;
	}

	/* copyright protection system type : no */
	*data++ = 0;

	/* region management info : no regions blocked */
	*data++ = 0;

	/* reserved */
	*data++ = 0;
	*data++ = 0;

	return data;
}

static unsigned char *dvd_format_dvd_structure_list(struct scsi_cmd *cmd,
						    unsigned char *data,
						    int format,
						    int layer,
						    int write_header);
struct dvd_format {
	int format;
	unsigned char *(*func)(struct scsi_cmd *cmd, unsigned char *data,
			       int format, int layer, int write_header);
};

struct dvd_format dvd_formats[] = {
	{DVD_FORMAT_PHYS_INFO,		dvd_format_phys_info},
	{DVD_FORMAT_DVD_COPYRIGHT_INFO,	dvd_format_copyright_info},
	{DVD_FORMAT_ADIP_INFO,		dvd_format_adip_info},
	{DVD_FORMAT_DVD_STRUCTURE_LIST, dvd_format_dvd_structure_list},
	{0, NULL}
};

static unsigned char *dvd_format_dvd_structure_list(struct scsi_cmd *cmd,
						    unsigned char *data,
						    int format,
						    int layer, int write_header)
{
	struct dvd_format *f;

	/* list all format headers */
	for (f = dvd_formats; f->func; f++) {
		/* we dont report ourself back in the format list */
		if (f->format == 0xff)
			continue;

		data = f->func(cmd, data, format, layer, 1);
		if (!data)
			return NULL;
	}

	return data;
}

static int mmc_read_dvd_structure(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	long address;
	int format, layer;
	unsigned char *data;
	unsigned char buf[4096];
	struct dvd_format *f;

	if (mmc->current_profile == PROFILE_NO_PROFILE) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
		return SAM_STAT_CHECK_CONDITION;
	}

	address = cmd->scb[2];
	address = (address<<8) | cmd->scb[3];
	address = (address<<8) | cmd->scb[4];
	address = (address<<8) | cmd->scb[5];
	layer = cmd->scb[6];
	format = cmd->scb[7];

	memset(buf, 0, sizeof(buf));
	data = &buf[4];

	for (f = dvd_formats; f->func; f++) {
		if (f->format == format) {
			int tmp;

			data = f->func(cmd, data, format, layer, 0);
			if (!data)
				return SAM_STAT_CHECK_CONDITION;

			tmp = data - buf;
			tmp -= 2;
			buf[0] = (tmp >> 8) & 0xff;
			buf[1] = tmp & 0xff;
			buf[2] = 0;
			buf[3] = 0;
			memcpy(scsi_get_in_buffer(cmd), buf,
			       min_t(uint32_t,
				     scsi_get_in_length(cmd), sizeof(buf)));
			return SAM_STAT_GOOD;
		}
	}

	/* we do not understand this format */
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, NOT_READY, ASC_INVALID_FIELD_IN_CDB);
	return SAM_STAT_CHECK_CONDITION;
}

static int mmc_reserve_track(int host_no, struct scsi_cmd *cmd)
{
	struct mmc_info *mmc = dtype_priv(cmd->dev);
	uint64_t tmp;

	tmp = cmd->scb[5];
	tmp = (tmp << 8) | cmd->scb[6];
	tmp = (tmp << 8) | cmd->scb[7];
	tmp = (tmp << 8) | cmd->scb[8];

	mmc->reserve_track_len = tmp;

	return SAM_STAT_GOOD;
}

static int mmc_mode_select(int host_no, struct scsi_cmd *cmd)
{
	return SAM_STAT_GOOD;
}

static int mmc_set_cd_speed(int host_no, struct scsi_cmd *cmd)
{
	return SAM_STAT_GOOD;
}

static int mmc_mode_sense(int host_no, struct scsi_cmd *cmd)
{
	uint8_t *scb = cmd->scb;

	/* MMC devices always return descriptor block */
	scb[1] |= 8;
	return spc_mode_sense(host_no, cmd);
}

static tgtadm_err mmc_lu_init(struct scsi_lu *lu)
{
	struct backingstore_template *bst;
	struct mmc_info *mmc;

	mmc = zalloc(sizeof(struct mmc_info));
	if (!mmc)
		return TGTADM_NOMEM;

	lu->xxc_p = mmc;

	if (spc_lu_init(lu))
		return TGTADM_NOMEM;

	/* MMC devices always use rdwr backingstore */
	bst = get_backingstore_template("rdwr");
	if (!bst) {
		eprintf("failed to find bstype, rdwr\n");
		return TGTADM_INVALID_REQUEST;
	}
	lu->bst = bst;

	strncpy(lu->attrs.product_id, "VIRTUAL-CDROM",
		sizeof(lu->attrs.product_id));
	lu->attrs.sense_format = 0;
	lu->attrs.version_desc[0] = 0x02A0; /* MMC3, no version claimed */
	lu->attrs.version_desc[1] = 0x0960; /* iSCSI */
	lu->attrs.version_desc[2] = 0x0300; /* SPC-3 */

	lu->attrs.removable = 1;

	/*
	 * Set up default mode pages
	 * Ref: mmc6r00.pdf 7.2.2 (Table 649)
	 */

	/* Vendor uniq - However most apps seem to call for mode page 0*/
	add_mode_page(lu, "0:0:0");
	/* Read/Write Error Recovery */
	add_mode_page(lu, "1:0:10:0:8:0:0:0:0:8:0:0:0");
	/* MRW */
	add_mode_page(lu, "3:0:6:0:0:0:0:0:0");
	/* Write Parameter
	 * Somebody who knows more about this mode page should be setting
	 * defaults.
	add_mode_page(lu, "5:0:0");
	 */
	/* Caching Page */
	add_mode_page(lu, "8:0:10:0:0:0:0:0:0:0:0:0:0");
	/* Control page */
	add_mode_page(lu, "0x0a:0:10:2:0:0:0:0:0:0:0:2:0");
	/* Control Extensions mode page:  TCMOS:1 */
	add_mode_page(lu, "0x0a:1:0x1c:0x04:0x00:0x00");
	/* Power Condition */
	add_mode_page(lu, "0x1a:0:10:8:0:0:0:0:0:0:0:0:0");
	/* Informational Exceptions Control page */
	add_mode_page(lu, "0x1c:0:10:8:0:0:0:0:0:0:0:0:0");
	/* Timeout & Protect */
	add_mode_page(lu, "0x1d:0:10:0:0:7:0:0:2:0:2:0:20");
	/* MM capabilities */
	add_mode_page(lu, "0x2a:0x00:0x3e:0x3f:0x37:0xf3:0xf3:0x29:"
			  "0x23:0x10:0x8a:0x01:0x00:0x08:0x00:0x10:"
			  "0x8a:0x00:0x00:0x10:0x8a:0x10:0x8a:0x00:"
			  "0x01:0x00:0x00:0x00:0x00:0x10:0x8a:0x00:"
			  "0x05:0x00:0x00:0x10:0x8a:0x00:0x00:0x0b:"
			  "0x06:0x00:0x00:0x08:0x45:0x00:0x00:0x05:"
			  "0x83:0x00:0x00:0x02:0xc2:0x00:0x00:0x00:"
			  "0x00:0x00:0x00:0x00:0x00:0x00:0x00:0x00:"
			  "0x00");
	/* Write parameters */
	add_mode_page(lu, "0x05:0:0x32:0x62:5:8:0x10:0:0:0:0:0:0:0:"
			  "0x10:0:0x96:0:0:0:0:0:0:0:0:0:0:0:0:0:0:"
			  "0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:"
			  "0:0");
	return TGTADM_SUCCESS;
}

static tgtadm_err mmc_lu_online(struct scsi_lu *lu)
{
	struct mmc_info *mmc = dtype_priv(lu);
	struct stat st;

	mmc->current_profile = PROFILE_NO_PROFILE;
	mmc->reserve_track_len = 0;

	if (lu->fd == -1)
		return TGTADM_INVALID_REQUEST;

	lu->attrs.online = 1;

	if (stat(lu->path, &st)) {
		mmc->current_profile = PROFILE_NO_PROFILE;
		lu->attrs.online = 0;
	} else {
		if (!st.st_size)
			mmc->current_profile = PROFILE_DVD_PLUS_R;
		else
			mmc->current_profile = PROFILE_DVD_ROM;
	}

	return TGTADM_SUCCESS;
}

static struct device_type_template mmc_template = {
	.type		= TYPE_MMC,
	.lu_init	= mmc_lu_init,
	.lu_config	= spc_lu_config,
	.lu_online	= mmc_lu_online,
	.lu_offline	= spc_lu_offline,
	.lu_exit	= spc_lu_exit,
	.ops		= {
		{spc_test_unit,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_request_sense,},
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

		/* 0x10 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_inquiry,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_start_stop,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_prevent_allow_media_removal,},
		{spc_illegal_op,},

		/* 0x20 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_read_capacity,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{mmc_rw},
		{spc_illegal_op,},
		{mmc_rw},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_test_unit},

		/* 0x30 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_synchronize_cache,},
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

		/* 0x40 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_read_toc,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_get_configuration,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		/* 0x50 */
		{spc_illegal_op,},
		{mmc_read_disc_information,},
		{mmc_read_track_information,},
		{mmc_reserve_track,},
		{spc_illegal_op,},
		{mmc_mode_select,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_mode_sense,},
		{mmc_close_track,},
		{mmc_read_buffer_capacity,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0x60 ... 0x9f] = {spc_illegal_op,},

		/* 0xA0 */
		{spc_report_luns,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{mmc_rw},
		{spc_illegal_op,},
		{mmc_rw},
		{spc_illegal_op,},
		{mmc_get_performance,},
		{mmc_read_dvd_structure,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		/* 0xB0 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_set_streaming,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{mmc_set_cd_speed,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0xc0 ... 0xff] = {spc_illegal_op},
	}
};

__attribute__((constructor)) static void mmc_init(void)
{
	device_type_register(&mmc_template);
}
