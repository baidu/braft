/*
 * SCSI Medium Changer Command
 *
 * Copyright (C) 2007 Mark Harvey <markh794@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#ifndef _SMC_H_
#define _SMC_H_

#define LOAD		1
#define UNLOAD		0

/**
 * Define for ELEMENT TYPE
 */
#define ELEMENT_ANY		0
#define	ELEMENT_MEDIUM_TRANSPORT	1
#define	ELEMENT_STORAGE		2
#define	ELEMENT_MAP		3
#define	ELEMENT_DATA_TRANSFER	4

/**
 * struct slot - Virtual Library element
 *
 * @slot_addr: Slot address - number between 0 & 65535
 * @last_addr: Last slot address where media came from
 * @asc: ASC/ASCQ for this slot
 * @element_type: 1 = Medium Transport, 2 = Storage, 3 = MAP, 4 = drive
 * @cart_type: Type of media in this slot
 * @status: Different bits used depending on element_type
 * @sides: If media is single or double sided
 * @barcode: Barcode of media ID
 * @drive_tid: TID of DATA TRANSFER DEVICE configured at this slot address.
 * @drive_lun: LUN of DATA TRANSFER DEVICE configured at this slot address.
 *
 * A linked list of slots describing each element within the virtual library
 */
struct slot {
	struct list_head slot_siblings;
	uint16_t slot_addr;
	uint16_t last_addr;
	uint16_t asc;
	uint8_t element_type;
	uint8_t cart_type;
	uint8_t status;
	uint8_t sides;
	char barcode[11];
	uint8_t drive_tid;
	uint64_t drive_lun;
};

/**
 * struct smc_info - Data structure for SMC device
 * @media_home: Home directory for virtual media
 * @slots: Linked list of 'slots' within the virtual library
 *
 */
struct smc_info {
	char *media_home;
	struct list_head slots;
};

enum {
	Opt_element_type, Opt_start_address,
	Opt_quantity, Opt_sides, Opt_clear_slot,
	Opt_address, Opt_barcode,
	Opt_tid, Opt_lun,
	Opt_type, Opt_dump,
	Opt_media_home,
	Opt_err,
};

static match_table_t tokens = {
	{Opt_element_type, "element_type=%s"},
	{Opt_start_address, "start_address=%s"},
	{Opt_quantity, "quantity=%s"},
	{Opt_sides, "sides=%s"},
	{Opt_clear_slot, "clear_slot=%s"},
	{Opt_address, "address=%s"},
	{Opt_barcode, "barcode=%s"},
	{Opt_tid, "tid=%s"},
	{Opt_lun, "lun=%s"},
	{Opt_type, "type=%s"},
	{Opt_dump, "dump=%s"},
	{Opt_media_home, "media_home=%s"},
	{Opt_err, NULL},
};

/**
 * struct tmp_param{} -- temporary storage of param values from user
 *
 * As param parsing is stateless, several params need to be collected
 * before we know if we are attempting to configure a slot or adding
 * a number of new slots. This is just a temporary holder for all
 * possible valid params before processing.
 */
struct tmp_param {
	int operation;
	int element_type;
	int start_addr;
	int quantity;
	int address;
	int tid;
	uint64_t lun;
	char barcode[20];
	int sides;
	int clear_slot;
} sv_param;

#endif // _SMC_H_
