#ifndef __BS_SSC_H
#define __BS_SSC_H

/*
 * structure of a 'poor mans double linked list' on disk.
 */

/**
 * Block type definitations
 *
 * @BLK_NOOP:	No Operation.. Dummy value
 * @BLK_UNCOMPRESS_DATA:	If true, data block is uncompressed
 * @BLK_ENCRYPTED_DATA:		If true, data block is encrypted
 * @BLK_FILEMARK:		Represents a filemark
 * @BLK_SETMARK:		Represents a setmark
 * @BLK_BOT:			Represents a Beginning of Tape marker
 * @BLK_EOD:			Represents an End of Data marker
 *
 * Defines for types of SSC data blocks
 */
#define	BLK_NOOP		0x00000000
#define	BLK_COMPRESSED_DATA	0x00000001
#define	BLK_UNCOMPRESS_DATA	0x00000002
#define	BLK_ENCRYPTED_DATA	0x00000004
#define	BLK_BOT			0x00000010
#define	BLK_EOD			0x00000020
#define	BLK_FILEMARK		0x00000040
#define	BLK_SETMARK		0x00000080

#define TGT_TAPE_VERSION	2

#define SSC_BLK_HDR_SIZE (sizeof(struct blk_header))

struct blk_header {
	uint8_t h_csum[4];
	uint32_t ondisk_sz;
	uint32_t blk_sz;
	uint32_t blk_type;
	uint64_t blk_num;
	uint64_t prev;
	uint64_t curr;
	uint64_t next;
};

/*
 * MAM (media access memory) structure based from IBM Ultrium SCSI
 * Reference WB1109-02
 */
struct MAM {
	uint32_t tape_fmt_version;
	uint32_t __pad1;

	uint64_t remaining_capacity;
	uint64_t max_capacity;
	uint64_t TapeAlert;
	uint64_t load_count;
	uint64_t MAM_space_remaining;

	uint8_t assigning_organization_1[8];
	uint8_t formatted_density_code;
	uint8_t __pad2[5];
	uint8_t initialization_count[2];
	uint8_t dev_make_serial_last_load[4][40];

	uint64_t written_in_medium_life;
	uint64_t read_in_medium_life;
	uint64_t written_in_last_load;
	uint64_t read_in_last_load;

	uint8_t medium_manufacturer[8];
	uint8_t medium_serial_number[32];
	uint32_t medium_length;
	uint32_t medium_width;
	uint8_t assigning_organization_2[8];
	uint8_t medium_density_code;
	uint8_t __pad3[7];
	uint8_t medium_manufacture_date[8];
	uint64_t MAM_capacity;
	uint8_t medium_type;
	uint8_t __pad4;
	uint16_t medium_type_information;
	uint8_t __pad5[4];

	uint8_t application_vendor[8];
	uint8_t application_name[32];
	uint8_t application_version[8];
	uint8_t user_medium_text_label[160];
	uint8_t date_time_last_written[12];
	uint8_t __pad6[3];
	uint8_t localization_identifier;
	uint8_t barcode[32];
	uint8_t owning_host_textual_name[80];
	uint8_t media_pool[160];

	uint8_t vendor_unique[256];

	uint8_t dirty;
	uint8_t __reserved[7];
};

#endif
