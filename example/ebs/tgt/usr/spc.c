/*
 * SCSI primary command processing
 *
 * Copyright (C) 2005-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2005-2007 Mike Christie <michaelc@cs.wisc.edu>
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
#include <stdint.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "parser.h"
#include "target.h"
#include "driver.h"
#include "tgtadm_error.h"
#include "scsi.h"
#include "spc.h"

#define INQUIRY_EVPD	0x01

#define PRODUCT_REV	"0"

#define DESG_HDR_LEN	4

/*
 * Protocol Identifier Values
 *
 * 0 Fibre Channel (FCP-2)
 * 1 Parallel SCSI (SPI-5)
 * 2 SSA (SSA-S3P)
 * 3 IEEE 1394 (SBP-3)
 * 4 SCSI Remote Direct Memory Access (SRP)
 * 5 iSCSI
 * 6 SAS Serial SCSI Protocol (SAS)
 * 7 Automation/Drive Interface (ADT)
 * 8 AT Attachment Interface (ATA/ATAPI-7)
 */
#define PIV_FCP 0
#define PIV_SPI 1
#define PIV_S3P 2
#define PIV_SBP 3
#define PIV_SRP 4
#define PIV_ISCSI 5
#define PIV_SAS 6
#define PIV_ADT 7
#define PIV_ATA 8

#define PIV_VALID 0x80

/*
 * Code Set
 *
 *  1 - Designator fild contains binary values
 *  2 - Designator field contains ASCII printable chars
 *  3 - Designaotor field contains UTF-8
 */
#define INQ_CODE_BIN 1
#define INQ_CODE_ASCII 2
#define INQ_CODE_UTF8 3

/*
 * Association field
 *
 * 00b - Associated with Logical Unit
 * 01b - Associated with target port
 * 10b - Associated with SCSI Target device
 * 11b - Reserved
 */
#define ASS_LU	0
#define ASS_TGT_PORT 0x10
#define ASS_TGT_DEV 0x20

/*
 * Designator type - SPC-4 Reference
 *
 * 0 - Vendor specific - 7.6.3.3
 * 1 - T10 vendor ID - 7.6.3.4
 * 2 - EUI-64 - 7.6.3.5
 * 3 - NAA - 7.6.3.6
 * 4 - Relative Target port identifier - 7.6.3.7
 * 5 - Target Port group - 7.6.3.8
 * 6 - Logical Unit group - 7.6.3.9
 * 7 - MD5 logical unit identifier - 7.6.3.10
 * 8 - SCSI name string - 7.6.3.11
 */
#define DESG_VENDOR 0
#define DESG_T10 1
#define DESG_EUI64 2
#define DESG_NAA 3
#define DESG_REL_TGT_PORT 4
#define DESG_TGT_PORT_GRP 5
#define DESG_LU_GRP 6
#define DESG_MD5 7
#define DESG_SCSI 8

#define NAA_IEEE_EXTD		0x2
#define NAA_LOCAL		0x3
#define NAA_IEEE_REGD		0x5
#define NAA_IEEE_REGD_EXTD	0x6

#define NAA_DESG_LEN		0x8
#define NAA_DESG_LEN_EXTD	0x10


static void update_vpd_80(struct scsi_lu *lu, void *sn)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0x80)];
	char *data = (char *)vpd_pg->data;

	memset(data, 0x20, vpd_pg->size);

	if (strlen(sn)) {
		int tmp = strlen(sn);
		char *p, *q;

		p = data + vpd_pg->size - 1;
		q = sn + tmp - 1;
		for (; tmp > 0; tmp--)
			*(p--) = *(q--);
	}
}

static void update_vpd_83(struct scsi_lu *lu, void *id)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0x83)];
	uint8_t	*data = vpd_pg->data;

	char *id_str = id;
	char substring[] = "0";
	uint64_t a = 0;
	uint64_t b = 0;
	uint64_t c;

	data[0] = INQ_CODE_ASCII;
	data[1] = DESG_T10;
	data[3] = SCSI_ID_LEN;
	data += DESG_HDR_LEN;

	strncpy((char *)data, id, SCSI_ID_LEN);
	data += SCSI_ID_LEN;

	data[0] = INQ_CODE_BIN;
	data[1] = DESG_NAA;
	data[3] = NAA_DESG_LEN;
	data += DESG_HDR_LEN;

	put_unaligned_be64(lu->attrs.numeric_id, data);
	data[0] |= NAA_LOCAL << 4;

	data += NAA_DESG_LEN;
	data[0] = INQ_CODE_BIN;
	data[1] = DESG_NAA;
	data[3] = NAA_DESG_LEN_EXTD;
	data += DESG_HDR_LEN;
	/*
	 * The NAA_DESG_LEN_EXTD field is a 128 bit field
	 * which contains a numeric value. This loop converts
	 * the string pointed to by 'id_str' into a right
	 * adjusted numeric value.
	 */
	while (*id_str) {
		substring[0] = *id_str++;
		c = a >> 60;
		a <<= 4;
		b <<= 4;
		b |= c;
		a |= strtoul(substring, NULL, 16);
	}
	put_unaligned_be64(b, data);
	put_unaligned_be64(a, data + 8);
	data[0] &= 0x0F;
	data[0] |= NAA_IEEE_REGD_EXTD << 4;
}

static void update_vpd_b2(struct scsi_lu *lu, void *id)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0xb2)];
	uint8_t	*data = vpd_pg->data;

	if (lu->attrs.thinprovisioning) {
		data[0] = 0;		/* threshold exponent */
		data[1] = 0xe4;		/* LBPU LBPWS(10) LBPRZ */
		data[2] = 0x02;		/* provisioning type */
		data[3] = 0;
	} else {
		data[0] = 0;
		data[1] = 0;
		data[2] = 0;
		data[3] = 0;
	}
}

static void update_vpd_b0(struct scsi_lu *lu, void *id)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0xb0)];

	/* maximum compare and write length : 64kb */
	vpd_pg->data[1] = 128;

	if (lu->attrs.thinprovisioning) {
		/* maximum unmap lba count : maximum*/
		put_unaligned_be32(0xffffffff, vpd_pg->data + 16);

		/* maximum unmap block descriptor count : maximum*/
		put_unaligned_be32(0xffffffff, vpd_pg->data + 20);
	} else {
		put_unaligned_be32(0, vpd_pg->data + 16);
		put_unaligned_be32(0, vpd_pg->data + 20);
	}
}

static void update_b0_opt_xfer_gran(struct scsi_lu *lu, int opt_xfer_gran)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0xb0)];

	/* 4 byte VPD header omitted from data buff */
	put_unaligned_be16(opt_xfer_gran, vpd_pg->data + 2);
}

static void update_b0_opt_xfer_len(struct scsi_lu *lu, int opt_xfer_len)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0xb0)];

	/* 4 byte VPD header omitted from data buff */
	put_unaligned_be32(opt_xfer_len, vpd_pg->data + 8);
}

int spc_inquiry(int host_no, struct scsi_cmd *cmd)
{
	int ret = SAM_STAT_CHECK_CONDITION;
	uint8_t *data;
	uint8_t *scb = cmd->scb;
	int evpd = (scb[1] & INQUIRY_EVPD) ? 1 : 0;
	int pcode = scb[2];
	uint32_t alloc_len, avail_len, actual_len;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t devtype = 0;
	struct lu_phy_attr *attrs;
	struct vpd *vpd_pg;
	uint8_t buf[256];

	if (!evpd && pcode)
		goto sense;

	alloc_len = (uint32_t)get_unaligned_be16(&scb[3]);
	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	memset(buf, 0, sizeof(buf));
	data = buf;
	avail_len = 0;

	dprintf("%x %x\n", (int)scb[1], pcode);

	attrs = &cmd->dev->attrs;

	devtype = (attrs->qualifier & 0x7) << 5;
	devtype |= (attrs->device_type & 0x1f);

	if (!evpd) { /* no VPD, return standard Inquiry data */
		int i;
		uint16_t *desc;

		data[0] = devtype;
		data[1] = (attrs->removable) ? 0x80 : 0;
		data[2] = 5;	/* SPC-3 */
		data[3] = 0x12;
		data[7] = 0x02;

		memset(data + 8, 0x20, 28);
		strncpy((char *)data + 8, attrs->vendor_id, VENDOR_ID_LEN);
		strncpy((char *)data + 16, attrs->product_id, PRODUCT_ID_LEN);
		strncpy((char *)data + 32, attrs->product_rev, PRODUCT_REV_LEN);

		desc = (uint16_t *)(data + 58);
		for (i = 0; i < ARRAY_SIZE(attrs->version_desc); i++)
			*desc++ = __cpu_to_be16(attrs->version_desc[i]);

		avail_len = 66;
		data[4] = avail_len - 5; /* Additional Length */
		ret = SAM_STAT_GOOD;
	} else { /* return requested page of VPD */
		if (pcode == 0x0) {
			uint8_t *p;
			int i, cnt;

			data[0] = devtype;
			data[1] = 0;
			data[2] = 0;

			cnt = 1;
			p = data + 5;
			for (i = 0; i < ARRAY_SIZE(attrs->lu_vpd); i++) {
				if (attrs->lu_vpd[i]) {
					*p++ = i | 0x80;
					cnt++;
				}
			}
			data[3] = cnt;
			data[4] = 0x0;
			avail_len = cnt + 4;
			ret = SAM_STAT_GOOD;
		} else if (attrs->lu_vpd[PCODE_OFFSET(pcode)]) {
			vpd_pg = attrs->lu_vpd[PCODE_OFFSET(pcode)];

			data[0] = devtype;
			data[1] = (uint8_t)pcode;
			data[2] = (vpd_pg->size >> 8);
			data[3] = vpd_pg->size & 0xff;
			memcpy(&data[4], vpd_pg->data, vpd_pg->size);
			avail_len = vpd_pg->size + 4;
			ret = SAM_STAT_GOOD;
		}
	}

	if (ret != SAM_STAT_GOOD)
		goto sense;

	if (cmd->dev->lun != cmd->dev_id)
		data[0] = TYPE_NO_LUN;

	actual_len = spc_memcpy(scsi_get_in_buffer(cmd), &alloc_len,
				data, avail_len);
	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

int spc_report_luns(int host_no, struct scsi_cmd *cmd)
{
	struct scsi_lu *lu;
	struct list_head *dev_list = &cmd->c_target->device_list;
	uint32_t alloc_len, avail_len, remain_len, actual_len;
	uint64_t lun, *data, *plun;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t *scb = cmd->scb;

	alloc_len = get_unaligned_be32(&scb[6]);
	if (alloc_len < 16)
		goto sense;

	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	data = scsi_get_in_buffer(cmd);
	plun = data + 1;
	remain_len = alloc_len - 8;
	actual_len = 8;
	avail_len = 0; /* accumulate LUN list length */

	list_for_each_entry(lu, dev_list, device_siblings) {
		if (remain_len) {
			lun = lu->lun;
			lun = ((lun > 0xff) ? (0x1 << 30) : 0) |
			      ((0x3fff & lun) << 16);
			lun = __cpu_to_be64(lun << 32);
		}
		actual_len += spc_memcpy((uint8_t *)plun, &remain_len,
					 (uint8_t *)&lun, 8);
		avail_len += 8;
		plun++;
	}

	*data = 0;
	*((uint32_t *) data) = __cpu_to_be32(avail_len);
	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

int spc_start_stop(int host_no, struct scsi_cmd *cmd)
{
	uint8_t *scb = cmd->scb;
	int start, loej, pwrcnd;

	scsi_set_in_resid_by_actual(cmd, 0);

	if (device_reserved(cmd))
		return SAM_STAT_RESERVATION_CONFLICT;

	pwrcnd = scb[4] & 0xf0;
	if (pwrcnd)
		return SAM_STAT_GOOD;

	loej   = scb[4] & 0x02;
	start  = scb[4] & 0x01;

	if (loej && !start && cmd->dev->attrs.removable) {
		if (lu_prevent_removal(cmd->dev)) {
			if (cmd->dev->attrs.online) {
				/*  online == media is present */
				sense_data_build(cmd, ILLEGAL_REQUEST,
					ASC_MEDIUM_REMOVAL_PREVENTED);
			} else {
				/* !online == media is not present */
				sense_data_build(cmd, NOT_READY,
				ASC_MEDIUM_REMOVAL_PREVENTED);
			}
			return SAM_STAT_CHECK_CONDITION;
		}
		spc_lu_offline(cmd->dev);
	}
	if (loej && start && cmd->dev->attrs.removable)
		spc_lu_online(cmd->dev);

	return SAM_STAT_GOOD;
}

int spc_test_unit(int host_no, struct scsi_cmd *cmd)
{
	/* how should we test a backing-storage file? */

	if (device_reserved(cmd))
		return SAM_STAT_RESERVATION_CONFLICT;
	if (cmd->dev->attrs.online)
		return SAM_STAT_GOOD;
	if (cmd->dev->attrs.removable)
		sense_data_build(cmd, NOT_READY, ASC_MEDIUM_NOT_PRESENT);
	else
		sense_data_build(cmd, NOT_READY, ASC_BECOMING_READY);

	return SAM_STAT_CHECK_CONDITION;
}

int spc_prevent_allow_media_removal(int host_no, struct scsi_cmd *cmd)
{
	uint8_t *scb = cmd->scb;
	struct it_nexus_lu_info *itn_lu_info = cmd->itn_lu_info;

	if (device_reserved(cmd))
		return SAM_STAT_RESERVATION_CONFLICT;

	itn_lu_info->prevent = scb[4] & PREVENT_MASK;

	return SAM_STAT_GOOD;
}

int spc_mode_select(int host_no, struct scsi_cmd *cmd,
		    int (*update)(struct scsi_cmd *, uint8_t *, int *))
{
	uint8_t *scb = cmd->scb;
	uint8_t *data = NULL;
	uint8_t pf, sp, pcode;
	uint32_t in_len;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_PARMS;
	uint32_t offset = 0;
	uint8_t parameter_header_len;
	uint16_t block_descriptor_len;
	int add_ua = 0;

	if (device_reserved(cmd))
		return SAM_STAT_RESERVATION_CONFLICT;

	pf = scb[1] & 0x10;
	sp = scb[1] & 0x01;

	if (!pf || sp) {
		asc = ASC_INVALID_FIELD_IN_CDB;
		goto sense;
	}

	in_len = scsi_get_out_length(cmd);
	data = scsi_get_out_buffer(cmd);

	if (scb[0] == MODE_SELECT) {
		in_len = min_t(uint32_t, scb[4], in_len);
		parameter_header_len = 4;
	} else if (scb[0] == MODE_SELECT_10) {
		in_len = min_t(uint32_t, (scb[7] << 8) + scb[8], in_len);
		parameter_header_len = 8;
	} else {
		eprintf("bug %u\n", scb[0]);
		exit(1);
	}

	if (in_len < parameter_header_len) {
		asc = ASC_INVALID_FIELD_IN_CDB;
		goto sense;
	}

	offset = parameter_header_len;

	if (scb[0] == MODE_SELECT)
		block_descriptor_len = data[3];
	else
		block_descriptor_len = (data[6] << 8) + data[7];

	if (block_descriptor_len) {
		if (block_descriptor_len != BLOCK_DESCRIPTOR_LEN)
			goto sense;

		memcpy(cmd->dev->mode_block_descriptor, data + offset,
		       BLOCK_DESCRIPTOR_LEN);

		offset += 8;
	}

	while (in_len > offset + 2) {
		struct mode_pg *pg;
		uint8_t *mask;
		int i, ret, changed;

		if (0x80 & data[offset])
			goto sense;

		pcode = data[offset] & 0x3f;

		pg = find_mode_page(cmd->dev, pcode, 0);
		if (!pg)
			goto sense;

		if (in_len - (offset + 2) < pg->pcode_size)
			goto sense;

		mask = pg->mode_data + pg->pcode_size;
		for (i = 0; i < pg->pcode_size; i++) {
			uint8_t *p, v;

			p = data + (offset + 2);

			v = p[i] ^ pg->mode_data[i];
			if (v && (v & ~mask[i]))
				goto sense;
		}

		changed = 0;
		ret = update(cmd, data + offset, &changed);
		if (ret)
			goto sense;

		if (changed)
			add_ua = 1;

		offset += (data[offset + 1] + 2);
	}

	if (add_ua)
		ua_sense_add_other_it_nexus(cmd->cmd_itn_id, cmd->dev,
					    ASC_MODE_PARAMETERS_CHANGED);

	if (in_len != offset) {
		asc = ASC_PARAMETER_LIST_LENGTH_ERR;
		goto sense;
	}

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

struct mode_pg *find_mode_page(struct scsi_lu *lu, uint8_t pcode,
			       uint8_t subpcode)
{
	struct mode_pg *pg;

	list_for_each_entry(pg, &lu->mode_pages, mode_pg_siblings) {
		if (pg->pcode == pcode && pg->subpcode == subpcode)
			return pg;
	}
	return NULL;
}

int set_mode_page_changeable_mask(struct scsi_lu *lu, uint8_t pcode,
				  uint8_t subpcode, uint8_t *mask)
{
	struct mode_pg *pg = find_mode_page(lu, pcode, subpcode);

	if (pg) {
		memcpy(pg->mode_data + pg->pcode_size, mask, pg->pcode_size);
		return 0;
	}
	return 1;
}

/**
 * build_mode_page - static routine used by spc_mode_sense()
 * @data:	destination pointer
 * @m:		struct mode pointer (src of data)
 *
 * Description: Copy mode page data from list into SCSI data so it can
 * be returned to the initiator
 *
 * Returns number of bytes copied.
 * Returns remaining alloc length in out-param remain_len
 */
static int build_mode_page(uint8_t *data, struct mode_pg *pg,
			   uint32_t *avail_len, uint32_t *remain_len,
			   uint8_t pc)
{
	uint8_t hdr[4];
	uint32_t hdr_size, actual_len;
	uint8_t *p, *mode_data;

	if (!pg->subpcode) {
		hdr[0] = pg->pcode;
		hdr[1] = pg->pcode_size;
		hdr_size = 2;
	} else {
		hdr[0] = pg->pcode | 0x40;
		hdr[1] = pg->subpcode;
		hdr[2] = (pg->pcode_size >> 8) & 0xff;
		hdr[3] = pg->pcode_size & 0xff;
		hdr_size = 4;
	}
	actual_len = spc_memcpy(data, remain_len, hdr, hdr_size);
	*avail_len += hdr_size;

	p = &data[hdr_size];
	mode_data = pg->mode_data;
	if (pc == 1)
		mode_data += pg->pcode_size;
	actual_len += spc_memcpy(p, remain_len, mode_data, pg->pcode_size);
	*avail_len += pg->pcode_size;

	return actual_len;
}

/*
 * Set a byte at the given index within dst buffer to val,
 * not exceeding dst_len bytes available at dst.
 */
void set_byte_safe(uint8_t *dst, uint32_t index, uint32_t dst_len, int val)
{
	if (index < dst_len)
		dst[index] = (uint8_t)val;
}

/**
 * spc_mode_sense - Implement SCSI op MODE SENSE(6) and MODE SENSE(10)
 *
 * Reference : SPC4r11
 * 6.11 - MODE SENSE(6)
 * 6.12 - MODE SENSE(10)
 */
int spc_mode_sense(int host_no, struct scsi_cmd *cmd)
{
	uint8_t *data = NULL, *scb;
	uint8_t mode6, dbd, blk_desc_len, pcode, pctrl, subpcode;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint32_t alloc_len, avail_len, remain_len, actual_len;
	uint32_t hdr_len, mod_data_len;
	struct mode_pg *pg;

	scb = cmd->scb;
	mode6 = (scb[0] == 0x1a);
	dbd = scb[1] & 0x8; /* Disable Block Descriptors */
	blk_desc_len = dbd ? 0 : BLOCK_DESCRIPTOR_LEN;
	pcode = scb[2] & 0x3f;
	pctrl = (scb[2] & 0xc0) >> 6;
	subpcode = scb[3];

	if (pctrl == 3) {
		asc = ASC_SAVING_PARMS_UNSUP;
		goto sense;
	}

	data = scsi_get_in_buffer(cmd);

	if (mode6) {
		alloc_len = (uint32_t)scb[4];
		hdr_len = 4;
	} else {
		alloc_len = (uint32_t)get_unaligned_be16(&scb[7]);
		hdr_len = 8;
	}

	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;
	memset(data, 0, alloc_len);

	avail_len = hdr_len;
	actual_len = min_t(uint32_t, alloc_len, hdr_len);
	remain_len = alloc_len - actual_len;

	if (!dbd) {
		actual_len += spc_memcpy(data + actual_len,
					 &remain_len,
					 cmd->dev->mode_block_descriptor,
					 BLOCK_DESCRIPTOR_LEN);
		avail_len += BLOCK_DESCRIPTOR_LEN;
	}

	if (pcode == 0x3f) {
		list_for_each_entry(pg,
				    &cmd->dev->mode_pages,
				    mode_pg_siblings) {
			actual_len += build_mode_page(data + actual_len, pg,
						      &avail_len, &remain_len,
						      pctrl);
		}
	} else {
		pg = find_mode_page(cmd->dev, pcode, subpcode);
		if (!pg)
			goto sense;
		actual_len += build_mode_page(data + actual_len, pg,
					      &avail_len, &remain_len,
					      pctrl);
	}

	if (mode6) {
		mod_data_len = avail_len - 1;
		set_byte_safe(data, 0, alloc_len, mod_data_len & 0xff);
		set_byte_safe(data, 3, alloc_len, blk_desc_len & 0xff);
	} else {
		mod_data_len = avail_len - 2;
		set_byte_safe(data, 0, alloc_len, (mod_data_len >> 8) & 0xff);
		set_byte_safe(data, 1, alloc_len, mod_data_len & 0xff);
		set_byte_safe(data, 6, alloc_len, (blk_desc_len >> 8) & 0xff);
		set_byte_safe(data, 7, alloc_len, blk_desc_len & 0xff);
	}

	scsi_set_in_resid_by_actual(cmd, actual_len);
	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int report_opcodes_all(struct scsi_cmd *cmd, int rctd,
			      uint32_t alloc_len)
{
	uint8_t buf[2048], *data;
	struct device_type_operations *ops;
	struct service_action *service_action;
	int i;
	uint32_t avail_len, actual_len;
	int cdb_length;

	memset(buf, 0, sizeof(buf));
	data = &buf[4];

	ops = cmd->dev->dev_type_template.ops;
	for (i = 0; i < NR_SCSI_OPCODES; i++) {
		if (ops[i].cmd_perform == spc_illegal_op)
			continue;

		if (!is_bs_support_opcode(cmd->dev->bst, i))
			continue;

		/* this command does not take a service action, so just
		   report the opcode
		*/
		if (!ops[i].service_actions) {
			*data++ = i;

			/* reserved */
			data++;

			/* service action */
			data += 2;

			/* reserved */
			data++;

			/* flags : no service action, possibly timeout desc */
			*data++ = rctd ? 0x02 : 0x00;

			/* cdb length */
			cdb_length = get_scsi_command_size(i);
			*data++ = (cdb_length >> 8) & 0xff;
			*data++ = cdb_length & 0xff;

			/* timeout descriptor */
			if (rctd) {
				/* length == 0x0a */
				data[1] = 0x0a;

				data += 12;
			}
			continue;
		}

		for (service_action = ops[i].service_actions;
		     service_action->cmd_perform;
		     service_action++) {
			/* opcode */
			*data++ = i;

			/* reserved */
			data++;

			/* service action */
			*data++ = (service_action->service_action >> 8) & 0xff;
			*data++ = service_action->service_action & 0xff;

			/* reserved */
			data++;

			/* flags : service action, possibly timeout desc */
			*data++ = rctd ? 0x03 : 0x01;

			/* cdb length */
			cdb_length = get_scsi_command_size(i);
			*data++ = (cdb_length >> 8) & 0xff;
			*data++ = cdb_length & 0xff;

			/* timeout descriptor */
			if (rctd) {
				/* length == 0x0a */
				data[1] = 0x0a;

				data += 12;
			}
		}
	}

	avail_len = data - &buf[0];
	put_unaligned_be32(avail_len-4, &buf[0]);

	actual_len = spc_memcpy(scsi_get_in_buffer(cmd), &alloc_len,
				buf, avail_len);
	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
}

static int report_opcode_one(struct scsi_cmd *cmd, int rctd, uint8_t opcode,
	uint16_t sa, int is_service_action, uint32_t alloc_len)
{
	struct device_type_operations *ops;
	uint8_t buf[256], *data;
	uint32_t avail_len, actual_len;
	int cdb_length;

	ops = cmd->dev->dev_type_template.ops;

	if (!is_bs_support_opcode(cmd->dev->bst, opcode))
		return SAM_STAT_CHECK_CONDITION;

	if ((is_service_action && !ops[opcode].service_actions)
	||  (!is_service_action && ops[opcode].service_actions)) {
		return SAM_STAT_CHECK_CONDITION;
	}

	memset(buf, 0, sizeof(buf));
	data = &buf[0];

	/* reserved */
	data++;

	/* ctdp and support */
	*data++ = rctd ? 0x83 : 0x03;

	/* cdb length */
	cdb_length = get_scsi_command_size(opcode);
	*data++ = (cdb_length >> 8) & 0xff;
	*data++ = cdb_length & 0xff;

	/* cdb usage data */
	memcpy(data, get_scsi_cdb_usage_data(opcode, sa), cdb_length);
	data += cdb_length;

	/* timeout descriptor */
	if (rctd) {
		/* length == 0x0a */
		data[1] = 0x0a;
		data += 12;
	}


	avail_len = data - &buf[0];
	actual_len = spc_memcpy(scsi_get_in_buffer(cmd), &alloc_len,
				buf, avail_len);
	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
}

int spc_report_supported_opcodes(int host_no, struct scsi_cmd *cmd)
{
	uint8_t reporting_options;
	uint8_t opcode;
	uint16_t requested_service_action;
	uint32_t alloc_len;
	int rctd;
	int ret;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;

	reporting_options = cmd->scb[2] & 0x07;
	opcode = cmd->scb[3];
	requested_service_action = get_unaligned_be16(&cmd->scb[4]);

	alloc_len = get_unaligned_be32(&cmd->scb[6]);
	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	rctd = cmd->scb[2] & 0x80;

	switch (reporting_options) {
	case 0x00: /* report all */
		ret = report_opcodes_all(cmd, rctd, alloc_len);
		break;
	case 0x01: /* report one no service action*/
		ret = report_opcode_one(cmd, rctd, opcode,
			requested_service_action, 0, alloc_len);
		if (ret)
			goto sense;
		break;
	case 0x02: /* report one service action */
		ret = report_opcode_one(cmd, rctd, opcode,
			requested_service_action, 1, alloc_len);
		if (ret)
			goto sense;
		break;
	default:
		goto sense;
	}
	return ret;

sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

struct service_action maint_in_service_actions[] = {
	{0x0c, spc_report_supported_opcodes},
	{0, NULL}
};

struct service_action *
find_service_action(struct service_action *service_action, uint32_t action)
{
	while (service_action->cmd_perform) {
		if (service_action->service_action == action)
			return service_action;
		service_action++;
	}
	return NULL;
}

int spc_send_diagnostics(int host_no, struct scsi_cmd *cmd)
{
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;

	/* we only support SELF-TEST==1 */
	if (!(cmd->scb[1] & 0x04))
		goto sense;

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

/*
 * This is useful for the various commands using the SERVICE ACTION
 * format.
 */
int spc_service_action(int host_no, struct scsi_cmd *cmd)
{
	uint8_t action;
	unsigned char op = cmd->scb[0];
	struct service_action *service_action, *actions;

	action = cmd->scb[1] & 0x1f;
	actions = cmd->dev->dev_type_template.ops[op].service_actions;

	service_action = find_service_action(actions, action);

	if (!service_action) {
		scsi_set_in_resid_by_actual(cmd, 0);
		sense_data_build(cmd, ILLEGAL_REQUEST,
				ASC_INVALID_FIELD_IN_CDB);
		return SAM_STAT_CHECK_CONDITION;
	}

	return service_action->cmd_perform(host_no, cmd);
}

static int is_pr_holder(struct scsi_lu *lu, struct registration *reg)
{
	if (lu->pr_holder->pr_type == PR_TYPE_WRITE_EXCLUSIVE_ALLREG ||
	    lu->pr_holder->pr_type == PR_TYPE_EXCLUSIVE_ACCESS_ALLREG)
		return 1;

	if (lu->pr_holder == reg)
		return 1;

	return 0;
}

static int spc_pr_read_keys(int host_no, struct scsi_cmd *cmd)
{
	uint32_t alloc_len, avail_len, actual_len, remain_len;
	uint8_t *buf;
	struct registration *reg;
	uint64_t reg_key;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;

	alloc_len = (uint32_t)get_unaligned_be16(&cmd->scb[7]);
	if (alloc_len < 8)
		goto sense;

	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	buf = scsi_get_in_buffer(cmd);

	put_unaligned_be32(cmd->dev->prgeneration, &buf[0]);
	actual_len = 8;
	avail_len = 8;
	remain_len = alloc_len - 8;

	list_for_each_entry(reg, &cmd->dev->registration_list,
			    registration_siblings) {

		reg_key = __cpu_to_be64(reg->key);
		actual_len += spc_memcpy(&buf[actual_len], &remain_len,
					 (uint8_t *)&reg_key, 8);
		avail_len += 8;
	}

	put_unaligned_be32(avail_len - 8, &buf[4]); /* additional length */

	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_read_reservation(int host_no, struct scsi_cmd *cmd)
{
	uint32_t alloc_len, add_len, avail_len, actual_len;
	struct registration *reg;
	uint64_t res_key;
	uint8_t buf[32];
	uint8_t *data;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;

	alloc_len = (uint32_t)get_unaligned_be16(&cmd->scb[7]);
	if (alloc_len < 8)
		goto sense;

	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	reg = cmd->dev->pr_holder;
	add_len = (reg ? 16 : 0);
	avail_len = 8 + add_len;

	memset(buf, 0, avail_len);

	put_unaligned_be32(cmd->dev->prgeneration, &buf[0]);
	put_unaligned_be32(add_len, &buf[4]); /* additional length */

	if (reg) {
		if (reg->pr_type == PR_TYPE_WRITE_EXCLUSIVE_ALLREG ||
		    reg->pr_type == PR_TYPE_EXCLUSIVE_ACCESS_ALLREG)
			res_key = 0;
		else
			res_key = reg->key;

		put_unaligned_be64(res_key, &buf[8]);

		buf[21] = (reg->pr_scope << 4) & 0xf0;
		buf[21] |= reg->pr_type & 0x0f;
	}

	data = scsi_get_in_buffer(cmd);
	actual_len = spc_memcpy(data, &alloc_len, buf, avail_len);
	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_report_capabilities(int host_no, struct scsi_cmd *cmd)
{
	uint32_t alloc_len, avail_len, actual_len;
	uint8_t *data, buf[8];
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;

	alloc_len = (uint32_t)get_unaligned_be16(&cmd->scb[7]);
	if (alloc_len < 8)
		goto sense;

	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	memset(buf, 0, 8);
	avail_len = 8;

	put_unaligned_be16(avail_len, &buf[0]); /* length */

	/* we don't set any capability for now */

	/* Persistent Reservation Type Mask format */
	buf[3] |= 0x80; /* Type Mask Valid (TMV) */

	buf[4] |= 0x80; /* PR_TYPE_EXCLUSIVE_ACCESS_ALLREG */
	buf[4] |= 0x40; /* PR_TYPE_EXCLUSIVE_ACCESS_REGONLY */
	buf[4] |= 0x20; /* PR_TYPE_WRITE_EXCLUSIVE_REGONLY */
	buf[4] |= 0x08; /* PR_TYPE_EXCLUSIVE_ACCESS */
	buf[4] |= 0x02; /* PR_TYPE_WRITE_EXCLUSIVE */
	buf[5] |= 0x01; /* PR_TYPE_EXCLUSIVE_ACCESS_ALLREG */

	data = scsi_get_in_buffer(cmd);
	actual_len = spc_memcpy(data, &alloc_len, buf, avail_len);
	scsi_set_in_resid_by_actual(cmd, actual_len);

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

struct service_action persistent_reserve_in_actions[] = {
	{PR_IN_READ_KEYS, spc_pr_read_keys},
	{PR_IN_READ_RESERVATION, spc_pr_read_reservation},
	{PR_IN_REPORT_CAPABILITIES, spc_pr_report_capabilities},
	{0, NULL},
};

static struct registration *lookup_registration_by_nexus(struct scsi_lu *lu,
							 struct it_nexus *itn)
{
	struct registration *reg;

	list_for_each_entry(reg, &lu->registration_list, registration_siblings) {
		if (reg->nexus_id == itn->itn_id &&
		    reg->ctime == itn->ctime)
			return reg;
	}

	return NULL;
}

static int check_registration_key_exists(struct scsi_lu *lu, uint64_t key)
{
	struct registration *reg;

	list_for_each_entry(reg, &lu->registration_list,
				registration_siblings) {
		if (reg->key == key)
			return 0;
	}

	return 1;
}

static void __unregister(struct scsi_lu *lu, struct registration *reg)
{
	list_del(&reg->registration_siblings);
	free(reg);
}

static void __unregister_and_clean(struct scsi_cmd *cmd,
				   struct registration *reg)
{
	struct registration *holder, *registrant;


	/* if reservation owner goes away then so does reservation */
	list_del(&reg->registration_siblings);

	holder = cmd->dev->pr_holder;
	if (!holder) {
		free(reg);
		return;
	}

	if (!is_pr_holder(cmd->dev, reg)) {
		free(reg);
		return;
	}

	if (((holder->pr_type != PR_TYPE_WRITE_EXCLUSIVE_ALLREG) &&
	     (holder->pr_type != PR_TYPE_EXCLUSIVE_ACCESS_ALLREG)) ||
	    list_empty(&cmd->dev->registration_list)) {

		/* not all-registrants or no more registrants */

		holder->pr_scope = 0;
		holder->pr_type = 0;
		cmd->dev->pr_holder = NULL;

		/* tell other registrants the reservation went away */
		list_for_each_entry(registrant,
		    &cmd->dev->registration_list,
		    registration_siblings) {
			/* do not send UA to self */
			if (registrant == reg)
				continue;
			ua_sense_add_other_it_nexus(registrant->nexus_id,
			    cmd->dev,
			    ASC_RESERVATIONS_RELEASED);
		}
		cmd->dev->prgeneration++;
	} else {

		/* all-registrants and there are more registrants */

		list_for_each_entry(registrant,
		    &cmd->dev->registration_list,
		    registration_siblings) {
			if (registrant != reg) {
				/* give resvn to any sibling */
				cmd->dev->pr_holder = registrant;
				registrant->pr_scope = holder->pr_scope;
				registrant->pr_type = holder->pr_type;
				break;
			}
		}
	}

	free(reg);
}

static int check_pr_out_basic_parameter(struct scsi_cmd *cmd)
{
	uint32_t param_list_len;
	uint8_t *buf;
	uint8_t spec_i_pt, all_tg_pt, aptpl;

	param_list_len = get_unaligned_be32(&cmd->scb[5]);
	if (param_list_len != 24)
		return 1;

	if (scsi_get_out_length(cmd) < 24)
		return 1;

	buf = scsi_get_out_buffer(cmd);

	spec_i_pt = buf[20] & (1U << 3);
	all_tg_pt = buf[20] & (1U << 2);
	aptpl = buf[20] & (1U << 0);

	if (spec_i_pt | all_tg_pt | aptpl) {
		/*
		 * for now, we say that we don't support these bits
		 * via REPORT CAPABILITIES.
		 */
		return 1;
	}

	return 0;
}

static int spc_pr_register(int host_no, struct scsi_cmd *cmd)
{
	uint8_t force, key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint64_t res_key, sa_res_key;
	int ret;
	uint8_t *buf;
	struct registration *reg;

	force = ((cmd->scb[1] & 0x1f) == PR_OUT_REGISTER_AND_IGNORE_EXISTING_KEY);

	ret = check_pr_out_basic_parameter(cmd);
	if (ret)
		goto sense;

	buf = scsi_get_out_buffer(cmd);

	res_key = get_unaligned_be64(buf);
	sa_res_key = get_unaligned_be64(buf + 8);

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);
	if (reg) {
		if (force || reg->key == res_key) {
			if (sa_res_key)
				reg->key = sa_res_key;
			else
				__unregister_and_clean(cmd, reg);
		} else
			return SAM_STAT_RESERVATION_CONFLICT;
	} else {
		if (force || !res_key) {
			if (sa_res_key) {
				reg = zalloc(sizeof(*reg));
				if (!reg) {
					key = ILLEGAL_REQUEST;
					asc = ASC_INSUFFICENT_REGISTRATION_RESOURCES;
					goto sense;
				}

				reg->key = sa_res_key;
				reg->nexus_id = cmd->cmd_itn_id;
				reg->ctime = cmd->it_nexus->ctime;
				list_add_tail(&reg->registration_siblings,
					      &cmd->dev->registration_list);
			} else
				; /* do nothing */
		} else
			return SAM_STAT_RESERVATION_CONFLICT;
	}

	cmd->dev->prgeneration++;

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_reserve(int host_no, struct scsi_cmd *cmd)
{
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;
	uint8_t pr_scope, pr_type;
	int ret;
	struct registration *reg, *holder;

	ret = check_pr_out_basic_parameter(cmd);
	if (ret)
		goto sense;

	pr_scope = (cmd->scb[2] & 0xf0) >> 4;
	pr_type = cmd->scb[2] & 0x0f;

	switch (pr_type) {
	case PR_TYPE_WRITE_EXCLUSIVE:
	case PR_TYPE_EXCLUSIVE_ACCESS:
	case PR_TYPE_WRITE_EXCLUSIVE_REGONLY:
	case PR_TYPE_EXCLUSIVE_ACCESS_REGONLY:
	case PR_TYPE_WRITE_EXCLUSIVE_ALLREG:
	case PR_TYPE_EXCLUSIVE_ACCESS_ALLREG:
		break;
	default:
		goto sense;
	}

	if (pr_scope != PR_LU_SCOPE)
		goto sense;

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);
	if (!reg)
		return SAM_STAT_RESERVATION_CONFLICT;

	holder = cmd->dev->pr_holder;
	if (holder) {
		if (!is_pr_holder(cmd->dev, reg))
			return SAM_STAT_RESERVATION_CONFLICT;

		if (holder->pr_type != pr_type ||
		    holder->pr_scope != pr_scope)
			return SAM_STAT_RESERVATION_CONFLICT;

		return SAM_STAT_GOOD;
	}

	reg->pr_scope = pr_scope;
	reg->pr_type = pr_type;
	cmd->dev->pr_holder = reg;

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_release(int host_no, struct scsi_cmd *cmd)
{
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;
	uint8_t pr_scope, pr_type;
	uint8_t *buf;
	uint64_t res_key;
	int ret;
	struct registration *reg, *holder, *sibling;

	ret = check_pr_out_basic_parameter(cmd);
	if (ret)
		goto sense;

	pr_scope = (cmd->scb[2] & 0xf0) >> 4;
	pr_type = cmd->scb[2] & 0x0f;

	buf = scsi_get_out_buffer(cmd);

	res_key = get_unaligned_be64(buf);

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);
	if (!reg)
		return SAM_STAT_RESERVATION_CONFLICT;

	holder = cmd->dev->pr_holder;
	if (!holder)
		return SAM_STAT_GOOD;

	if (!is_pr_holder(cmd->dev, reg))
		return SAM_STAT_GOOD;

	if (res_key != reg->key)
		return SAM_STAT_RESERVATION_CONFLICT;

	if (holder->pr_scope != pr_scope || holder->pr_type != pr_type) {
		asc = ASC_INVALID_RELEASE_OF_PERSISTENT_RESERVATION;
		goto sense;
	}

	cmd->dev->pr_holder = NULL;
	reg->pr_scope = 0;
	reg->pr_type = 0;

	switch (pr_type) {
	case PR_TYPE_WRITE_EXCLUSIVE_REGONLY:
	case PR_TYPE_EXCLUSIVE_ACCESS_REGONLY:
	case PR_TYPE_WRITE_EXCLUSIVE_ALLREG:
	case PR_TYPE_EXCLUSIVE_ACCESS_ALLREG:
		return SAM_STAT_GOOD;
	default:
		;
	}

	list_for_each_entry(sibling, &cmd->dev->registration_list,
			    registration_siblings) {
		/* we don't send myself */
		if (sibling == reg)
			continue;

		ua_sense_add_other_it_nexus(sibling->nexus_id,
					    cmd->dev, ASC_RESERVATIONS_RELEASED);
	}

	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_clear(int host_no, struct scsi_cmd *cmd)
{
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;
	uint8_t *buf;
	uint64_t res_key;
	int ret;
	struct registration *reg, *holder, *sibling, *n;

	ret = check_pr_out_basic_parameter(cmd);
	if (ret)
		goto sense;

	buf = scsi_get_out_buffer(cmd);

	res_key = get_unaligned_be64(buf);

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);
	if (!reg)
		return SAM_STAT_RESERVATION_CONFLICT;

	if (reg->key != res_key)
		return SAM_STAT_RESERVATION_CONFLICT;

	holder = cmd->dev->pr_holder;
	if (holder) {
		holder->pr_scope = 0;
		holder->pr_type = 0;
		cmd->dev->pr_holder = NULL;
	}

	list_for_each_entry_safe(sibling, n, &cmd->dev->registration_list,
				 registration_siblings) {
		/* we don't send myself */
		if (sibling != reg)
			ua_sense_add_it_nexus(sibling->nexus_id,
				cmd->dev, ASC_RESERVATIONS_PREEMPTED);
		list_del(&sibling->registration_siblings);
		free(sibling);
	}

	cmd->dev->prgeneration++;
	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_preempt(int host_no, struct scsi_cmd *cmd)
{
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;
	int ret;
	int res_released = 0, remove_all_reg = 0;
	uint64_t res_key, sa_res_key;
	uint8_t pr_scope, pr_type;
	uint8_t *buf;
	struct registration *holder, *reg, *sibling, *n;

	ret = check_pr_out_basic_parameter(cmd);
	if (ret)
		goto sense;

	pr_scope = (cmd->scb[2] & 0xf0) >> 4;
	pr_type = cmd->scb[2] & 0x0f;

	buf = scsi_get_out_buffer(cmd);

	res_key = get_unaligned_be64(buf);
	sa_res_key = get_unaligned_be64(buf + 8);

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);
	if (!reg)
		return SAM_STAT_RESERVATION_CONFLICT;

	if (reg->key != res_key)
		return SAM_STAT_RESERVATION_CONFLICT;

	if (sa_res_key) {
		ret = check_registration_key_exists(cmd->dev, sa_res_key);
		if (ret)
			return SAM_STAT_RESERVATION_CONFLICT;
	}

	holder = cmd->dev->pr_holder;

	if (holder) {
		if (holder->pr_type == PR_TYPE_WRITE_EXCLUSIVE_ALLREG ||
			holder->pr_type == PR_TYPE_EXCLUSIVE_ACCESS_ALLREG) {

			if (!sa_res_key) {
				if (pr_type != holder->pr_type ||
					pr_scope != holder->pr_scope)
					res_released = 1;
				reg->pr_type = pr_type;
				reg->pr_scope = pr_scope;
				cmd->dev->pr_holder = reg;
				remove_all_reg = 1;
			}
		} else {
			if (holder->key == sa_res_key) {
				if ((pr_type != holder->pr_type) ||
					(pr_scope != holder->pr_scope))
					res_released = 1;
				reg->pr_type = pr_type;
				reg->pr_scope = pr_scope;
				cmd->dev->pr_holder = reg;
			} else {
				if (!sa_res_key)
					goto sense;
			}
		}
	}

	list_for_each_entry_safe(sibling, n, &cmd->dev->registration_list,
					registration_siblings) {
		if (sibling == reg)
			continue;

		if (sibling->key == sa_res_key || remove_all_reg) {
			ua_sense_add_it_nexus(sibling->nexus_id,
				cmd->dev, ASC_RESERVATIONS_PREEMPTED);
			__unregister(cmd->dev, sibling);
		} else {
			if (res_released)
				ua_sense_add_it_nexus(sibling->nexus_id,
				cmd->dev, ASC_RESERVATIONS_RELEASED);
		}
	}

	cmd->dev->prgeneration++;
	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static int spc_pr_register_and_move(int host_no, struct scsi_cmd *cmd)
{
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;
	uint8_t key = ILLEGAL_REQUEST;
	uint32_t param_list_len;
	char *buf;
	uint8_t unreg, aptpl;
	uint64_t res_key, sa_res_key;
	uint32_t tpid_data_len, idlen;
	struct registration *reg, *dst;
	int (*id)(int, uint64_t, char *, int);
	char tpid[300]; /* large enough? */

	param_list_len = get_unaligned_be32(&cmd->scb[5]);
	if (param_list_len < 24)
		goto sense;

	if (scsi_get_out_length(cmd) < param_list_len)
		goto sense;

	buf = scsi_get_out_buffer(cmd);

	aptpl = buf[17] & 0x01;
	if (aptpl) /* not reported in capabilities */
		goto sense;

	unreg = buf[17] & 0x02;

	res_key = get_unaligned_be64(buf);
	sa_res_key = get_unaligned_be64(buf + 8);

	tpid_data_len = get_unaligned_be32(&buf[20]);
	if (tpid_data_len < 24 || tpid_data_len % 4 != 0)
		goto sense;
	if (param_list_len - 24 < tpid_data_len)
		goto sense;

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);
	if (!reg) {
		if (cmd->dev->pr_holder)
			return SAM_STAT_RESERVATION_CONFLICT;
		else
			goto sense;
	}

	if (!is_pr_holder(cmd->dev, reg))
		return SAM_STAT_GOOD;

	if (reg->key != res_key)
		return SAM_STAT_RESERVATION_CONFLICT;

	if (!sa_res_key)
		return SAM_STAT_RESERVATION_CONFLICT;

	list_for_each_entry(dst, &cmd->dev->registration_list,
			    registration_siblings)
		if (dst->key == sa_res_key)
			goto found;

	/* we can't find the destination */
	goto sense;
found:
	id = tgt_drivers[cmd->c_target->lid]->transportid;
	if (id) {
		memset(tpid, 0, sizeof(tpid));
		idlen = id(cmd->dev->tgt->tid, dst->nexus_id, tpid, sizeof(tpid));
		if (tpid_data_len != idlen || memcmp(tpid, &buf[24], idlen))
			goto sense;
	}

	cmd->dev->pr_holder = dst;

	if (unreg)
		__unregister(cmd->dev, reg);

	cmd->dev->prgeneration++;
	return SAM_STAT_GOOD;
sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

struct service_action persistent_reserve_out_actions[] = {
	{PR_OUT_REGISTER, spc_pr_register},
	{PR_OUT_RESERVE, spc_pr_reserve},
	{PR_OUT_RELEASE, spc_pr_release},
	{PR_OUT_CLEAR, spc_pr_clear},
	{PR_OUT_PREEMPT, spc_pr_preempt},
/* 	{PR_OUT_PREEMPT_AND_ABORT, spc_pr_preempt}, */
	{PR_OUT_REGISTER_AND_IGNORE_EXISTING_KEY, spc_pr_register},
	{PR_OUT_REGISTER_AND_MOVE, spc_pr_register_and_move},
	{0, NULL},
};

int spc_access_check(struct scsi_cmd *cmd)
{
	unsigned char op = cmd->scb[0];
	uint8_t bits;
	uint8_t pr_type;
	struct registration *reg;
	int conflict = 0;

	if (!cmd->dev->pr_holder)
		return 0;

	reg = lookup_registration_by_nexus(cmd->dev, cmd->it_nexus);

	if (reg && is_pr_holder(cmd->dev, reg))
		return 0;

	pr_type = cmd->dev->pr_holder->pr_type;
	bits = cmd->dev->dev_type_template.ops[op].pr_conflict_bits;

	if (pr_type == PR_TYPE_EXCLUSIVE_ACCESS)
		conflict = bits & PR_EA_FA;
	else if (pr_type == PR_TYPE_WRITE_EXCLUSIVE)
		conflict = bits & PR_WE_FA;
	else {
		if (reg)
			conflict = bits & PR_RR_FR;
		else {
			if (pr_type == PR_TYPE_WRITE_EXCLUSIVE_REGONLY ||
			    pr_type == PR_TYPE_WRITE_EXCLUSIVE_ALLREG)
				conflict = bits & PR_WE_FN;
			else
				conflict = bits & PR_EA_FN;
		}
	}

	return conflict;
}

int spc_request_sense(int host_no, struct scsi_cmd *cmd)
{
	uint32_t alloc_len, actual_len;

	alloc_len = (uint32_t)cmd->scb[4];
	alloc_len = min_t(uint32_t, alloc_len, scsi_get_in_length(cmd));

	sense_data_build(cmd, NO_SENSE, NO_ADDITIONAL_SENSE);

	actual_len = spc_memcpy(scsi_get_in_buffer(cmd), &alloc_len,
				cmd->sense_buffer, cmd->sense_len);

	scsi_set_in_resid_by_actual(cmd, actual_len);

	/* reset sense buffer in cmnd */
	memset(cmd->sense_buffer, 0, sizeof(cmd->sense_buffer));
	cmd->sense_len = 0;

	return SAM_STAT_GOOD;
}

struct vpd *alloc_vpd(uint16_t size)
{
	struct vpd *vpd_pg;

	vpd_pg = zalloc(sizeof(struct vpd) + size);
	if (!vpd_pg)
		return NULL;

	vpd_pg->size = size;

	return vpd_pg;
}

static struct mode_pg *alloc_mode_pg(uint8_t pcode, uint8_t subpcode,
				     uint16_t size)
{
	struct mode_pg *pg;

	pg = zalloc(sizeof(*pg) + size * 2);
	if (!pg)
		return NULL;

	pg->pcode = pcode;
	pg->subpcode = subpcode;
	pg->pcode_size = size;

	return pg;
}

tgtadm_err add_mode_page(struct scsi_lu *lu, char *p)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;
	int i, tmp;
	uint8_t pcode, subpcode, *data;
	uint16_t size;
	struct mode_pg *pg;

	pcode = subpcode = i = size = 0;
	data = NULL;

	for (i = 0; p; i++) {
		switch (i) {
		case 0:
			pcode = strtol(p, NULL, 0);
			break;
		case 1:
			subpcode = strtol(p, NULL, 0);
			break;
		case 2:
			size = strtol(p, NULL, 0);

			pg = find_mode_page(lu, pcode, subpcode);
			if (pg) {
				list_del(&pg->mode_pg_siblings);
				free(pg);
			}

			pg = alloc_mode_pg(pcode, subpcode, size);
			if (!pg) {
				adm_err = TGTADM_NOMEM;
				goto exit;
			}

			list_add_tail(&pg->mode_pg_siblings, &lu->mode_pages);
			data = pg->mode_data;
			break;
		default:
			if (i < (size + 3)) {
				tmp = strtol(p, NULL, 0);
				if (tmp > UINT8_MAX)
					eprintf("Incorrect value %d "
						"Mode page %d (0x%02x), index: %d\n",
						tmp, pcode, subpcode, i - 3);
				data[i - 3] = (uint8_t)tmp;
			}
			break;
		}

		p = strchr(p, ':');
		if (p)
			p++;
	}

	if (i > size + 3) {
		adm_err = TGTADM_INVALID_REQUEST;
		eprintf("Mode Page %d (0x%02x): param_count %d > "
			"MODE PAGE size : %d\n", pcode, subpcode, i, size + 3);
	}
exit:
	return adm_err;
}

void dump_cdb(struct scsi_cmd *cmd)
{
	uint8_t *cdb = cmd->scb;

	switch(cmd->scb_len) {
	case 6:
		dprintf("SCSI CMD: %02x %02x %02x %02x %02d %02x\n",
			cdb[0], cdb[1], cdb[2], cdb[3], cdb[4], cdb[5]);
		break;
	case 10:
		dprintf("SCSI CMD: %02x %02x %02x %02x %02d %02x\n"
				" %02x %02x %02x %02x",
			cdb[0], cdb[1], cdb[2], cdb[3], cdb[4], cdb[5],
			cdb[6], cdb[7], cdb[8], cdb[9]);
		break;
	case 12:
		dprintf("SCSI CMD: %02x %02x %02x %02x %02d %02x"
				" %02x %02x %02x %02x %02x %02x\n",
			cdb[0], cdb[1], cdb[2], cdb[3], cdb[4], cdb[5],
			cdb[6], cdb[7], cdb[8], cdb[9], cdb[10], cdb[11]);
		break;
	case 16:
		dprintf("SCSI CMD: %02x %02x %02x %02x %02d %02x"
				" %02x %02x %02x %02x %02x %02x"
				" %02x %02x %02x %02x\n",
			cdb[0], cdb[1], cdb[2], cdb[3], cdb[4], cdb[5],
			cdb[6], cdb[7], cdb[8], cdb[9], cdb[10], cdb[11],
			cdb[12], cdb[13], cdb[14], cdb[15]);
		break;
	}
}

int spc_illegal_op(int host_no, struct scsi_cmd *cmd)
{
	dump_cdb(cmd);
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, ILLEGAL_REQUEST, ASC_INVALID_OP_CODE);
	return SAM_STAT_CHECK_CONDITION;
}

enum {
	Opt_scsi_id, Opt_scsi_sn,
	Opt_vendor_id, Opt_product_id,
	Opt_product_rev, Opt_sense_format,
	Opt_lbppbe, Opt_la_lba,
	Opt_optimal_xfer_gran, Opt_optimal_xfer_len,
	Opt_removable, Opt_readonly, Opt_online,
	Opt_mode_page,
	Opt_path, Opt_bsopts,
	Opt_bsoflags, Opt_thinprovisioning,
	Opt_err,
};

static match_table_t tokens = {
	{Opt_scsi_id, "scsi_id=%s"},
	{Opt_scsi_sn, "scsi_sn=%s"},
	{Opt_vendor_id, "vendor_id=%s"},
	{Opt_product_id, "product_id=%s"},
	{Opt_product_rev, "product_rev=%s"},
	{Opt_sense_format, "sense_format=%s"},
	{Opt_lbppbe, "lbppbe=%s"},
	{Opt_la_lba, "la_lba=%s"},
	{Opt_optimal_xfer_gran, "optimal_xfer_gran=%s"},
	{Opt_optimal_xfer_len, "optimal_xfer_len=%s"},
	{Opt_removable, "removable=%s"},
	{Opt_readonly, "readonly=%s"},
	{Opt_online, "online=%s"},
	{Opt_mode_page, "mode_page=%s"},
	{Opt_path, "path=%s"},
	{Opt_bsopts, "bsopts=%s"},
	{Opt_bsoflags, "bsoflags=%s"},
	{Opt_thinprovisioning, "thin_provisioning=%s"},
	{Opt_err, NULL},
};

tgtadm_err spc_lu_online(struct scsi_lu *lu)
{
	lu->attrs.online = 1;
	return TGTADM_SUCCESS;
}

tgtadm_err spc_lu_offline(struct scsi_lu *lu)
{
	if (lu_prevent_removal(lu))
		return TGTADM_PREVENT_REMOVAL;

	lu->attrs.online = 0;
	return TGTADM_SUCCESS;
}

tgtadm_err lu_config(struct scsi_lu *lu, char *params, match_fn_t *fn)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;
	char *p;
	char buf[1024];
	struct lu_phy_attr *attrs;
	struct vpd **lu_vpd;

	attrs = &lu->attrs;
	lu_vpd = attrs->lu_vpd;

	if (params && !strncmp("targetOps", params, 9))
		params = params + 10;

	while ((p = strsep(&params, ",")) != NULL) {
		substring_t args[MAX_OPT_ARGS];
		int token;

		if (!*p)
			continue;
		token = match_token(p, tokens, args);
		switch (token) {
		case Opt_scsi_id:
			match_strncpy(attrs->scsi_id, &args[0],
				      sizeof(attrs->scsi_id));
			lu_vpd[PCODE_OFFSET(0x83)]->vpd_update(lu, attrs->scsi_id);
			break;
		case Opt_scsi_sn:
			match_strncpy(attrs->scsi_sn, &args[0],
				      sizeof(attrs->scsi_sn));
			lu_vpd[PCODE_OFFSET(0x80)]->vpd_update(lu, attrs->scsi_sn);
			break;
		case Opt_vendor_id:
			match_strncpy(attrs->vendor_id, &args[0],
				      sizeof(attrs->vendor_id));
			break;
		case Opt_product_id:
			match_strncpy(attrs->product_id, &args[0],
				      sizeof(attrs->product_id));
			break;
		case Opt_product_rev:
			match_strncpy(attrs->product_rev, &args[0],
				      sizeof(attrs->product_rev));
			break;
		case Opt_sense_format:
			match_strncpy(buf, &args[0], sizeof(buf));
			attrs->sense_format = atoi(buf);
			break;
		case Opt_lbppbe:
			match_strncpy(buf, &args[0], sizeof(buf));
			attrs->lbppbe = atoi(buf);
			attrs->no_auto_lbppbe = 1;
			break;
		case Opt_la_lba:
			match_strncpy(buf, &args[0], sizeof(buf));
			attrs->la_lba = atoi(buf);
			break;
		case Opt_optimal_xfer_gran:
			match_strncpy(buf, &args[0], sizeof(buf));
			update_b0_opt_xfer_gran(lu, atoi(buf));
			break;
		case Opt_optimal_xfer_len:
			match_strncpy(buf, &args[0], sizeof(buf));
			update_b0_opt_xfer_len(lu, atoi(buf));
			break;
		case Opt_removable:
			match_strncpy(buf, &args[0], sizeof(buf));
			attrs->removable = atoi(buf);
			break;
		case Opt_readonly:
			match_strncpy(buf, &args[0], sizeof(buf));
			attrs->readonly = atoi(buf);
			break;
		case Opt_thinprovisioning:
			match_strncpy(buf, &args[0], sizeof(buf));
			attrs->thinprovisioning = atoi(buf);
			/* update the provisioning vpd page */
			lu_vpd[PCODE_OFFSET(0xb0)]->vpd_update(lu, NULL);
			lu_vpd[PCODE_OFFSET(0xb2)]->vpd_update(lu, NULL);
			break;
		case Opt_online:
			match_strncpy(buf, &args[0], sizeof(buf));
			if (atoi(buf))
				adm_err = lu->dev_type_template.lu_online(lu);
			else
				adm_err = lu->dev_type_template.lu_offline(lu);
			break;
		case Opt_mode_page:
			match_strncpy(buf, &args[0], sizeof(buf));
			adm_err = add_mode_page(lu, buf);
			break;
		case Opt_path:
			match_strncpy(buf, &args[0], sizeof(buf));
			adm_err = tgt_device_path_update(lu->tgt, lu, buf);
			break;
		default:
			adm_err = fn ? fn(lu, p) : TGTADM_INVALID_REQUEST;
		}
	}
	return adm_err;
}

tgtadm_err spc_lu_config(struct scsi_lu *lu, char *params)
{
	return lu_config(lu, params, NULL);
}

int spc_lu_init(struct scsi_lu *lu)
{
	struct vpd **lu_vpd = lu->attrs.lu_vpd;
	struct target *tgt = lu->tgt;
	int pg;

	lu->attrs.device_type = lu->dev_type_template.type;
	lu->attrs.qualifier = 0x0;
	lu->attrs.thinprovisioning = 0;
	lu->attrs.removable = 0;
	lu->attrs.readonly = 0;
	lu->attrs.swp = 0;
	lu->attrs.sense_format = 0;

	snprintf(lu->attrs.vendor_id, sizeof(lu->attrs.vendor_id),
		 "%-16s", VENDOR_ID);
	snprintf(lu->attrs.product_rev, sizeof(lu->attrs.product_rev),
		 "%s", "0001");
	snprintf(lu->attrs.scsi_id, sizeof(lu->attrs.scsi_id),
		 "IET     %04x%04" PRIx64, tgt->tid, lu->lun);
	snprintf(lu->attrs.scsi_sn, sizeof(lu->attrs.scsi_sn),
		 "beaf%d%" PRIu64, tgt->tid, lu->lun);
	lu->attrs.numeric_id = tgt->tid;
	lu->attrs.numeric_id <<= 32;
	lu->attrs.numeric_id |= lu->lun;

	/* VPD page 0x80 */
	pg = PCODE_OFFSET(0x80);
	lu_vpd[pg] = alloc_vpd(SCSI_SN_LEN);
	if (!lu_vpd[pg])
		return -ENOMEM;
	lu_vpd[pg]->vpd_update = update_vpd_80;
	lu_vpd[pg]->vpd_update(lu, lu->attrs.scsi_sn);

	/* VPD page 0x83 */
	pg = PCODE_OFFSET(0x83);
	lu_vpd[pg] = alloc_vpd(3*DESG_HDR_LEN + NAA_DESG_LEN + SCSI_ID_LEN + NAA_DESG_LEN_EXTD);
	if (!lu_vpd[pg])
		return -ENOMEM;
	lu_vpd[pg]->vpd_update = update_vpd_83;
	lu_vpd[pg]->vpd_update(lu, lu->attrs.scsi_id);

	/* VPD page 0xb0 */
	pg = PCODE_OFFSET(0xb0);
	lu_vpd[pg] = alloc_vpd(BLOCK_LIMITS_VPD_LEN);
	if (!lu_vpd[pg])
		return -ENOMEM;
	lu_vpd[pg]->vpd_update = update_vpd_b0;
	lu_vpd[pg]->vpd_update(lu, NULL);

	/* VPD page 0xb2 LOGICAL BLOCK PROVISIONING*/
	pg = PCODE_OFFSET(0xb2);
	lu_vpd[pg] = alloc_vpd(LBP_VPD_LEN);
	if (!lu_vpd[pg])
		return -ENOMEM;
	lu_vpd[pg]->vpd_update = update_vpd_b2;
	lu_vpd[pg]->vpd_update(lu, NULL);


	lu->dev_type_template.lu_offline(lu);

	return 0;
}

void spc_lu_exit(struct scsi_lu *lu)
{
	int i;
	struct vpd **lu_vpd = lu->attrs.lu_vpd;

	for (i = 0; i < ARRAY_SIZE(lu->attrs.lu_vpd); i++)
		if (lu_vpd[i])
			free(lu_vpd[i]);

	while (!list_empty(&lu->mode_pages)) {
		struct mode_pg *pg;
		pg = list_first_entry(&lu->mode_pages,
				       struct mode_pg,
				       mode_pg_siblings);
		list_del(&pg->mode_pg_siblings);
		free(pg);
	}
}
