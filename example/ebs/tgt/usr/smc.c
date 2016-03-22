/*
 * SCSI Medium Changer command processing
 * Based on smc3r06.pdf document from t10.org
 *
 * (C) 2004-2007 FUJITA Tomonori <tomof@acm.org>
 * (C) 2005-2007 Mike Christie <michaelc@cs.wisc.edu>
 * (C) 2007      Mark Harvey <markh794@gmail.com>
 * (C) 2012      Ronnie Sahlberg <ronniesahlberg@gmail.com>
 *
 * SCSI target emulation code is based on Ardis's iSCSI implementation.
 *   http://www.ardistech.com/iscsi/
 *   Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>,
 *   licensed under the terms of the GNU GPL v2.0,
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
#include <unistd.h>
#include <linux/fs.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "driver.h"
#include "tgtadm_error.h"
#include "scsi.h"
#include "spc.h"
#include "parser.h"
#include "smc.h"
#include "media.h"

static int check_slot_removable(struct slot *s)
{
	tgtadm_err adm_err = dtd_check_removable(s->drive_tid, s->drive_lun);
	return (adm_err == TGTADM_SUCCESS ? 0 : -EINVAL);
}

static int set_slot_full(struct slot *s, uint16_t src, char *path)
{
	int err = 0;

	if (path)
		err = dtd_load_unload(s->drive_tid, s->drive_lun, LOAD, path);
	if (err)
		return err;

	s->status |= 1;
	s->last_addr = src;

	return err;
}

static void set_slot_empty(struct slot *s)
{
	s->status &= 0xfe;
	s->last_addr = 0;
	memset(s->barcode, ' ', sizeof(s->barcode));
	if (s->element_type == ELEMENT_DATA_TRANSFER)
		dtd_load_unload(s->drive_tid, s->drive_lun, UNLOAD, NULL);
}

static int test_slot_full(struct slot *s)
{
	return s->status & 1;
}

/**
 * determine_element_sz  --  read element status
 * @dvcid	Device ID - true, return device ID information
 * @voltag	true, return Volume Tag (barcode)
 *
 * Ref: Working Draft SCSI Media Changer-3 (smc3r06.pdf), chapter 6.10
 *
 * The READ ELEMENT STATUS command request that the device server
 * report the status of its internal elements to the application
 * client.  Support for READ ELEMENT STATUS command is mandatory.
 */
static int determine_element_sz(uint8_t dvcid, uint8_t voltag)
{
	if (voltag)
		return (dvcid) ? 86 : 52;
	else
		return (dvcid) ? 50 : 16;
}

/**
 * element_status_data_hdr  --  Fill in Element Status Header
 * @data	uint8_t * - data pointer
 * @dvcid	Device ID - true, return device ID information
 * @voltag	true, return Volume Tag (barcode)
 * @start	Start searching from slot 'start'
 * @count	and return 'count' elements
 *
 * This builds the ELEMENT STATUS DATA header information built
 * from the params passed.
 *
 * DATA header is always 8 bytes long
 */
static int element_status_data_hdr(uint8_t *data, uint8_t dvcid,
				   uint8_t voltag, int start, int count)
{
	int element_sz;
	int size;

	element_sz = determine_element_sz(dvcid, voltag);

	/* First Element address reported */
	*(uint16_t *)(data) = __cpu_to_be16(start);

	/* Number of elements available */
	*(uint16_t *)(data + 2) = __cpu_to_be16(count);

	/* Byte count is the length required to return all valid data.
	 * Allocated length is how much data the initiator will accept */
	size = ((8 + (count * element_sz)) & 0xffffff);
	*(uint32_t *)(data + 4) = __cpu_to_be32(size);

	return size;
}

static int add_element_descriptor(uint8_t *data, struct slot *s,
				  uint8_t element_type, uint8_t dvcid,
				  uint8_t voltag)
{
	struct lu_phy_attr *attr = NULL;
	int i;	/* data[] index */

	*(uint16_t *)(data) = __cpu_to_be16(s->slot_addr);
	data[2] = s->status;
	data[3] = 0;	/* Reserved */
	data[4] = (s->asc >> 8) & 0xff;	/* Additional Sense Code */
	data[5] = s->asc & 0xff;	/* Additional Sense Code Qualifier */
	/* [6], [7] & [8] reserved */
	data[9] = (s->cart_type & 0xf);

	if (s->last_addr) {	/* Source address is valid ? */
		data[9] |= 0x80;
		*(uint16_t *)(data + 10) = __cpu_to_be16(s->last_addr);
	}

	i = 12;
	if (voltag) {
		if (s->barcode[0] == ' ')
			memset(&data[i], 0x20, 32);
		else
			snprintf((char *)&data[i], 32, "%-32s", s->barcode);

		/* Reserve additional 4 bytes if dvcid is set */
		i += (dvcid) ? 36 : 32;
	}

	if (element_type == ELEMENT_DATA_TRANSFER)
		attr = lu_attr_lookup(s->drive_tid, s->drive_lun);

	if (dvcid && attr) {
		data[i] = 2;	/* ASCII code set */
		data[i + 1] = attr->device_type;
		data[i + 2] = 0;	/* reserved */
		data[i + 3] = 34;	/* Length */
		snprintf((char *)&data[i + 4], 9, "%-8s", attr->vendor_id);
		snprintf((char *)&data[i + 12], 17, "%-16s", attr->product_id);
		snprintf((char *)&data[i + 28], 11, "%-10s", attr->scsi_sn);
	}

	return determine_element_sz(dvcid, voltag);
}

/**
 * build_element_descriptor  --  Fill in Element details
 * @data;	pointer
 * @head;	Slot struct head
 * @element_type; Slot type we are interested in.
 * @first:	Return address of first slot found
 * @start;	Start processing from this element #
 * @dvcid;	Device ID
 * @voltag;	Volume tag (barcode)
 *
 * Fill each Element Descriptor for slot *s
 * Return number of elements
 */
static int build_element_descriptors(uint8_t *data, struct list_head *head,
				     uint8_t elem_type, int *first,
				     uint16_t start, uint8_t dvcid,
				     uint8_t voltag)
{
	struct slot *s;
	int count = 0;
	int len = 8;
	int elem_sz = determine_element_sz(dvcid, voltag);

	list_for_each_entry(s, head, slot_siblings) {
		if (s->element_type == elem_type) {
			if (s->slot_addr >= start) {
				count++;
				len += add_element_descriptor(&data[len], s,
							      elem_type, dvcid,
							      voltag);
			}
		}
		if (count == 1)	/* Record first found slot Address */
			*first = s->slot_addr;
	}

	/* Fill in Element Status Page Header */
	data[0] = elem_type;
	data[1] = (voltag) ? 0x80 : 0;
	*(uint16_t *)(data + 2) = __cpu_to_be16(elem_sz);

	/* Total number of bytes in all element descriptors */
	*(uint32_t *)(data + 4) = __cpu_to_be32((elem_sz * count) & 0xffffff);

	return count;
}

/**
 * smc_initialize_element_status with range
 *                      - INITIALIZE ELEMENT STATUS WITH RANGE op code
 *
 * Support the SCSI op code INITIALIZE_ELEMENT_STATUS_WITH_RANGE
 * Ref: smc3r11, 6.5
 */
static int smc_initialize_element_status_range(int host_no, struct scsi_cmd *cmd)
{
	scsi_set_in_resid_by_actual(cmd, 0);

	if (device_reserved(cmd))
		return SAM_STAT_RESERVATION_CONFLICT;
	else
		return SAM_STAT_GOOD;
}

/**
 * smc_initialize_element_status - INITIALIZE ELEMENT STATUS op code
 *
 * Some backup libraries seem to require this.
 *
 * Support the SCSI op code INITIALIZE_ELEMENT_STATUS
 * Ref: smc3r10a, 6.2
 */
static int smc_initialize_element_status(int host_no, struct scsi_cmd *cmd)
{
	scsi_set_in_resid_by_actual(cmd, 0);

	if (device_reserved(cmd))
		return SAM_STAT_RESERVATION_CONFLICT;
	else
		return SAM_STAT_GOOD;
}

static int nr_slots(struct smc_info *smc)
{
	int count = 0;
	struct slot *s;

	list_for_each_entry(s, &smc->slots, slot_siblings)
		count++;

	return count;
}

/**
 * smc_read_element_status  -  READ ELEMENT STATUS op code
 *
 * Support the SCSI op code READ ELEMENT STATUS
 * Ref: smc3r06, 6.10
 */
static int smc_read_element_status(int host_no, struct scsi_cmd *cmd)
{
	struct smc_info *smc = dtype_priv(cmd->dev);
	uint8_t *data = NULL;
	uint8_t *scb;
	uint8_t element_type;
	uint8_t voltag;
	uint16_t req_start_elem;
	uint8_t dvcid;
	int alloc_len;
	uint16_t count = 0;
	int first = 0;		/* First valid slot location */
	int len = 8;
	int elementSize;
	int ret;
	unsigned char key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;

	scb = cmd->scb;
	element_type = scb[1] & 0x0f;
	voltag	= (scb[1] & 0x10) >> 4;
	dvcid = scb[6] & 0x01;
	req_start_elem = __be16_to_cpu(*(uint16_t *)(scb + 2));
	alloc_len = 0xffffff & __be32_to_cpu(*(uint32_t *)(scb + 6));

	if (scsi_get_in_length(cmd) < alloc_len)
		goto sense;

	elementSize = determine_element_sz(dvcid, voltag);

	scsi_set_in_resid_by_actual(cmd, 0);
	if (cmd->dev) {
		ret = device_reserved(cmd);
		if (ret) {
			dprintf("Reservation Conflict\n");
			return SAM_STAT_RESERVATION_CONFLICT;
		}
	}

	/* we allocate possible maximum data length */
	data = zalloc(8 + elementSize * nr_slots(smc));
	if (!data) {
		dprintf("Can't allocate enough memory for cmd\n");
		key = HARDWARE_ERROR;
		asc = ASC_INTERNAL_TGT_FAILURE;
		goto sense;
	}

	if (scb[11])	/* Reserved byte */
		goto sense;

	switch(element_type) {
	case ELEMENT_ANY:
		/* Return element in type order */
		count = build_element_descriptors(&data[len], &smc->slots,
						  ELEMENT_MEDIUM_TRANSPORT,
						  &first, req_start_elem,
						  dvcid, voltag);
		len = count * elementSize;
		count += build_element_descriptors(&data[len], &smc->slots,
						   ELEMENT_STORAGE,
						   &first, req_start_elem,
						   dvcid, voltag);
		len += count * elementSize;
		count += build_element_descriptors(&data[len], &smc->slots,
						   ELEMENT_MAP,
						   &first, req_start_elem,
						   dvcid, voltag);
		len += count * elementSize;
		count += build_element_descriptors(&data[len], &smc->slots,
						   ELEMENT_DATA_TRANSFER,
						   &first, req_start_elem,
						   dvcid, voltag);
		break;
	case ELEMENT_MEDIUM_TRANSPORT:
		count = build_element_descriptors(&data[len], &smc->slots,
						  ELEMENT_MEDIUM_TRANSPORT,
						  &first, req_start_elem,
						  dvcid, voltag);
		break;
	case ELEMENT_STORAGE:
		count = build_element_descriptors(&data[len], &smc->slots,
						  ELEMENT_STORAGE,
						  &first, req_start_elem,
						  dvcid, voltag);
		break;
	case ELEMENT_MAP:
		count = build_element_descriptors(&data[len], &smc->slots,
						  ELEMENT_MAP,
						  &first, req_start_elem,
						  dvcid, voltag);
		break;
	case ELEMENT_DATA_TRANSFER:
		count = build_element_descriptors(&data[len], &smc->slots,
						  ELEMENT_DATA_TRANSFER,
						  &first, req_start_elem,
						  dvcid, voltag);
		break;
	default:
		goto sense;
		break;
	}

	/* Lastly, fill in data header */
	len = element_status_data_hdr(data, dvcid, voltag, first, count);
	memcpy(scsi_get_in_buffer(cmd), data, min(len, alloc_len));
	scsi_set_in_resid_by_actual(cmd, len);
	free(data);
	return SAM_STAT_GOOD;
sense:
	if (data)
		free(data);
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

/**
 * smc_move_medium  -  MOVE MEDIUM op code
 *
 * Support the SCSI op code MOVE MEDIUM
 * Ref: smc3r06, 6.6
 */
static int smc_move_medium(int host_no, struct scsi_cmd *cmd)
{
	struct smc_info *smc = dtype_priv(cmd->dev);
	uint8_t *scb;
	uint16_t src;
	uint16_t dest;
	uint8_t invert;
	struct slot *src_slot = NULL;
	struct slot *dest_slot = NULL;
	struct slot *s;
	int key = ILLEGAL_REQUEST;
	uint16_t asc = ASC_INVALID_FIELD_IN_CDB;

	scb = cmd->scb;
	src = __be16_to_cpu(*(uint16_t *)(scb + 4));
	dest = __be16_to_cpu(*(uint16_t *)(scb + 6));
	invert = scb[10] & 1;

	list_for_each_entry(s, &smc->slots, slot_siblings) {
		if (s->slot_addr == src)
			src_slot = s;
		if (s->slot_addr == dest)
			dest_slot = s;
	}

	if (src_slot) {
		if (!test_slot_full(src_slot)) {
			asc = ASC_MEDIUM_SRC_EMPTY;
			goto sense;
		}
	} else	/* Could not find src slot - Error */
		goto sense;

	if (dest_slot) {
		if (test_slot_full(dest_slot)) {
			asc = ASC_MEDIUM_DEST_FULL;
			goto sense;
		}
	} else	/* Could not find dest slot - Error */
		goto sense;

	if (invert && (s->sides == 1))	/* Use default INVALID FIELD IN CDB */
		goto sense;

	if (src_slot->element_type == ELEMENT_DATA_TRANSFER) {
		if (check_slot_removable(src_slot)) {
			key = ILLEGAL_REQUEST;
			asc = ASC_MEDIUM_REMOVAL_PREVENTED;
			goto sense;
		}
	}

	memcpy(&dest_slot->barcode, &src_slot->barcode, sizeof(s->barcode));
	if (dest_slot->element_type == ELEMENT_DATA_TRANSFER) {
		char path[128];
		int sz;
		sz = snprintf(path, sizeof(path), "%s/%s",
					smc->media_home, dest_slot->barcode);
		if (sz >= sizeof(path)) {
			dprintf("Path too long: %s\n", path);
			key = ILLEGAL_REQUEST;
			asc = ASC_INTERNAL_TGT_FAILURE;
			memset(&dest_slot->barcode, ' ', sizeof(s->barcode));
			goto sense;
		}

		if (set_slot_full(dest_slot, src, path)) {
			key = HARDWARE_ERROR;
			asc = ASC_MECHANICAL_POSITIONING_ERROR;
			goto sense;
		}
	} else
		set_slot_full(dest_slot, src, NULL);

	set_slot_empty(src_slot);

	scsi_set_in_resid_by_actual(cmd, 0);
	return SAM_STAT_GOOD;

sense:
	scsi_set_in_resid_by_actual(cmd, 0);
	sense_data_build(cmd, key, asc);
	return SAM_STAT_CHECK_CONDITION;
}

static tgtadm_err smc_lu_init(struct scsi_lu *lu)
{
	struct smc_info *smc;
	tgtadm_err adm_err;

	smc = zalloc(sizeof(struct smc_info));
	if (smc)
		dtype_priv(lu) = smc;
	else
		return TGTADM_NOMEM;

	if (spc_lu_init(lu))
		return TGTADM_NOMEM;

	strncpy(lu->attrs.product_id, "VIRTUAL-CHANGER",
						sizeof(lu->attrs.product_id));
	lu->attrs.version_desc[0] = 0x0480; /* SMC-3 no version claimed */
	lu->attrs.version_desc[1] = 0x0960; /* iSCSI */
	lu->attrs.version_desc[2] = 0x0300; /* SPC-3 */

	/* Vendor uniq - However most apps seem to call for mode page 0*/
	add_mode_page(lu, "0:0:0");
	/* Control page */
	add_mode_page(lu, "0x0a:0:10:2:0:0:0:0:0:0:0:2:0");
	/* Control Extensions mode page:  TCMOS:1 */
	add_mode_page(lu, "0x0a:1:0x1c:0x04:0x00:0x00");
	/* Power Condition */
	add_mode_page(lu, "0x1a:0:10:8:0:0:0:0:0:0:0:0:0");
	/* Informational Exceptions Control page */
	add_mode_page(lu, "0x1c:0:10:8:0:0:0:0:0:0:0:0:0");
	/* Address Assignment */
	add_mode_page(lu, "0x1d:0:0x12:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0");
	/* Transport Geometry Params */
	add_mode_page(lu, "0x1e:0:2:0:0");
	/* Device Capabilities */
	add_mode_page(lu, "0x1f:0:0x12:15:7:15:15:15:15:0:0:0:0:15:15:15:15:0:0:0:0");

	INIT_LIST_HEAD(&smc->slots);

	adm_err = lu->dev_type_template.lu_online(lu); /* Library will now report as Online */
	lu->attrs.removable = 1; /* Default to removable media */

	return adm_err;
}

static void smc_lu_exit(struct scsi_lu *lu)
{
	struct smc_info *smc = dtype_priv(lu);

	dprintf("Medium Changer shutdown() called\n");

	free(smc);
}

static tgtadm_err slot_insert(struct list_head *head, int element_type, int address)
{
	struct slot *s;

	s = zalloc(sizeof(struct slot));
	if (!s)
		return TGTADM_NOMEM;

	s->slot_addr = address;
	s->element_type = element_type;
	s->sides = 1;
	if (element_type == ELEMENT_DATA_TRANSFER)	/* Drive */
		s->asc = ASC_INITIALIZING_REQUIRED;

	list_add_tail(&s->slot_siblings, head);

	return TGTADM_SUCCESS;
}

static void slot_remove(struct slot *s)
{
	list_del(&s->slot_siblings);
	free(s);
}

/**
 * slot_lookup  -- Find slot of type 'element_type' & address 'address'
 * @element_type;	Type of slot, 0 == Any type
 * @address;		Slot address, 0 == First found slot
 *
 * Return NULL if no match
 */
static struct slot *slot_lookup(struct list_head *head, int element_type,
				int address)
{
	struct slot *s;

	list_for_each_entry(s, head, slot_siblings) {
		if (element_type && address) {
			if ((s->slot_addr == address) &&
			    (s->element_type == element_type))
				return s;
		} else if (element_type) {
			if (s->element_type == element_type)
				return s;
		} else if (address) {
			if (s->slot_addr == address)
				return s;
		}
	}
	return NULL;
}

static void slot_dump(struct list_head *head)
{
	struct slot *s;

	list_for_each_entry(s, head, slot_siblings)
		if (s) {
			dprintf("Slot %d Information\n", s->slot_addr);
			dprintf("  Last Addr: %d\n", s->last_addr);
			dprintf("  Type: %d\n", s->element_type);
			dprintf("  Barcode: %s\n", s->barcode);
			if (s->drive_tid) {
				dprintf("  TID : %d\n", s->drive_tid);
				dprintf("  LUN : %" PRIu64 "\n", s->drive_lun);
			}
			dprintf("  ASC/ASCQ : %d\n\n", s->asc);
		}
}

static tgtadm_err add_slt(struct scsi_lu *lu, struct tmp_param *tmp)
{
	struct smc_info *smc = dtype_priv(lu);
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;
	struct mode_pg *pg;
	struct slot *s;
	uint16_t *element;
	int sv_addr;
	int qnty_save;
	int i;

	pg = find_mode_page(lu, 0x1d, 0);
	if (!pg) {
		dprintf("Failed to find Element Address Assignment mode pg\n");
		return TGTADM_UNKNOWN_ERR;
	}
	element = (uint16_t *)pg->mode_data;

	if (tmp->element_type && tmp->start_addr && tmp->quantity) {
		switch(tmp->element_type) {
		case ELEMENT_MEDIUM_TRANSPORT:
			break;
		case ELEMENT_MAP:
			element += 4;
			break;
		case ELEMENT_STORAGE:
			element += 2;
			break;
		case ELEMENT_DATA_TRANSFER:
			element += 6;
			break;
		default:
			goto dont_do_slots;
			break;
		}

		sv_addr = __be16_to_cpu(element[0]);
		qnty_save  = __be16_to_cpu(element[1]);

		if (sv_addr)
			element[0] = __cpu_to_be16(min_t(int, tmp->start_addr, sv_addr));
		else
			element[0] = __cpu_to_be16(tmp->start_addr);
		element[1] = __cpu_to_be16(tmp->quantity + qnty_save);

		s = slot_lookup(&smc->slots, tmp->element_type, tmp->start_addr);
		if (s)	// Opps... Found a slot at this address..
			goto dont_do_slots;

		adm_err = TGTADM_SUCCESS;
		for (i = tmp->start_addr; i < (tmp->start_addr + tmp->quantity); i++) {
			adm_err = slot_insert(&smc->slots, tmp->element_type, i);
			if (adm_err != TGTADM_SUCCESS) {
				int j;
				/* remove all slots added before error */
				for (j = tmp->start_addr; j < i; j++) {
					s = slot_lookup(&smc->slots, tmp->element_type, j);
					slot_remove(s);
				}
				break;
			}
		}
	}

dont_do_slots:
	return adm_err;
}

static tgtadm_err config_slot(struct scsi_lu *lu, struct tmp_param *tmp)
{
	struct smc_info *smc = dtype_priv(lu);
	struct mode_pg *m = NULL;
	struct slot *s = NULL;
	int adm_err = TGTADM_INVALID_REQUEST;

	switch(tmp->element_type) {
	case ELEMENT_MEDIUM_TRANSPORT:
		/* If medium has more than one side, set the 'rotate' bit */
		m = find_mode_page(lu, 0x1e, 0);
		if (m) {
			m->mode_data[0] = (tmp->sides > 1) ? 1 : 0;
			adm_err = TGTADM_SUCCESS;
		}
		break;
	case ELEMENT_STORAGE:
	case ELEMENT_MAP:
		s = slot_lookup(&smc->slots, tmp->element_type, tmp->address);
		if (!s)
			break;	// Slot not found..
		if (tmp->clear_slot) {
			set_slot_empty(s);
			adm_err = TGTADM_SUCCESS;
			break;
		}
		strncpy(s->barcode, tmp->barcode, sizeof(s->barcode));
		set_slot_full(s, 0, NULL);
		adm_err = TGTADM_SUCCESS;
		break;
	case ELEMENT_DATA_TRANSFER:
		if (!tmp->tid)
			break;	/* Fail if no TID specified */
		s = slot_lookup(&smc->slots, tmp->element_type, tmp->address);
		if (!s)
			break;	// Slot not found..
		s->asc  = NO_ADDITIONAL_SENSE;
		s->drive_tid = tmp->tid;
		s->drive_lun = tmp->lun;
		adm_err = TGTADM_SUCCESS;
		break;
	}
	return adm_err;
}

#define ADD	1
#define CONFIGURE 2

static tgtadm_err __smc_lu_config(struct scsi_lu *lu, char *params)
{
	struct smc_info *smc = dtype_priv(lu);
	tgtadm_err adm_err = TGTADM_SUCCESS;
	char *p;
	char buf[256];

	while (adm_err == TGTADM_SUCCESS && (p = strsep(&params, ",")) != NULL) {
		substring_t args[MAX_OPT_ARGS];
		int token;
		if (!*p)
			continue;
		token = match_token(p, tokens, args);
		switch (token) {
		case Opt_element_type:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.element_type = atoi(buf);
			break;
		case Opt_start_address:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.start_addr = atoi(buf);
			sv_param.operation = ADD;
			break;
		case Opt_quantity:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.quantity = atoi(buf);
			break;
		case Opt_sides:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.sides = atoi(buf);
			break;
		case Opt_clear_slot:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.clear_slot = atoi(buf);
			break;
		case Opt_address:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.address = atoi(buf);
			sv_param.operation = CONFIGURE;
			break;
		case Opt_barcode:
			match_strncpy(sv_param.barcode, &args[0],
				      sizeof(sv_param.barcode));
			break;
		case Opt_tid:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.tid = atoi(buf);
			break;
		case Opt_lun:
			match_strncpy(buf, &args[0], sizeof(buf));
			sv_param.lun = atoi(buf);
			break;
		case Opt_dump:
			slot_dump(&smc->slots);
			break;
		case Opt_media_home:
			if (smc->media_home)
				free(smc->media_home);
			match_strncpy(buf, &args[0], sizeof(buf));
			smc->media_home = strdup(buf);
			if (!smc->media_home)
				adm_err = TGTADM_NOMEM;
			break;
		default:
			adm_err = TGTADM_UNKNOWN_PARAM;
			break;
		}
	}
	return adm_err;
}

static tgtadm_err smc_lu_config(struct scsi_lu *lu, char *params)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;

	memset(&sv_param, 0, sizeof(struct tmp_param));

	adm_err = lu_config(lu, params, __smc_lu_config);
	if (adm_err != TGTADM_SUCCESS)
		return adm_err;

	switch(sv_param.operation) {
		case ADD:
			adm_err = add_slt(lu, &sv_param);
			break;
		case CONFIGURE:
			adm_err = config_slot(lu, &sv_param);
			break;
	}
	return adm_err;
}

struct device_type_template smc_template = {
	.type		= TYPE_MEDIUM_CHANGER,
	.lu_init	= smc_lu_init,
	.lu_exit 	= smc_lu_exit,
	.lu_online	= spc_lu_online,
	.lu_offline	= spc_lu_offline,
	.lu_config	= smc_lu_config,
	.ops	= {
		{spc_test_unit,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_request_sense,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{smc_initialize_element_status,},

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
		{spc_mode_sense,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0x20 ... 0x2f] = {spc_illegal_op,},

		/* 0x30 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{smc_initialize_element_status_range,},

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
		{spc_mode_sense,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0x60 ... 0x9f] = {spc_illegal_op,},

		/* 0xA0 */
		{spc_report_luns,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_service_action, maint_in_service_actions,},
		{spc_illegal_op,},
		{smc_move_medium,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_test_unit,},

		/* 0xB0 */
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		{smc_read_element_status,},	// Mandatory
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},
		{spc_illegal_op,},

		[0xc0 ... 0xff] = {spc_illegal_op},
	}
};

__attribute__((constructor)) static void smc_init(void)
{
	device_type_register(&smc_template);
}
