/*
 * SCSI Streaming specific header
 */

#ifndef _SSC_H_
#define _SSC_H_

struct blk_header_info {
	uint32_t ondisk_sz;
	uint32_t blk_sz;
	uint32_t blk_type;
	uint64_t blk_num;
	uint64_t prev;
	uint64_t curr;
	uint64_t next;
};

/*
 * MAM structure based from IBM Ultrium SCSI Reference WB1109-02
 */
struct MAM_info {
	uint32_t tape_fmt_version;

	uint64_t remaining_capacity;
	uint64_t max_capacity;
	uint64_t TapeAlert;
	uint64_t load_count;
	uint64_t MAM_space_remaining;

	uint8_t assigning_organization_1[8];
	uint8_t formatted_density_code;
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
	uint8_t medium_manufacture_date[8];
	uint64_t MAM_capacity;
	uint8_t medium_type;
	uint16_t medium_type_information;

	uint8_t application_vendor[8];
	uint8_t application_name[32];
	uint8_t application_version[8];
	uint8_t user_medium_text_label[160];
	uint8_t date_time_last_written[12];
	uint8_t localization_identifier;
	uint8_t barcode[32];
	uint8_t owning_host_textual_name[80];
	uint8_t media_pool[160];

	uint8_t vendor_unique[256];

	uint8_t dirty;
};

struct ssc_info {
	uint64_t bytes_read;	/* Bytes read this load */
	uint64_t bytes_written;	/* Bytes written this load */

	struct MAM_info mam;

	struct blk_header_info c_blk;	/* Current block header */
};

#endif

