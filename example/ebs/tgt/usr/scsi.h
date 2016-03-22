/*
 * taken from kernel.h
 *
 * better if we include kernel's one directly.
 */

#ifndef __SCSI_H
#define __SCSI_H

#define TEST_UNIT_READY       0x00
#define REZERO_UNIT           0x01
#define REQUEST_SENSE         0x03
#define FORMAT_UNIT           0x04
#define READ_BLOCK_LIMITS     0x05
#define REASSIGN_BLOCKS       0x07
#define INITIALIZE_ELEMENT_STATUS 0x07
#define READ_6                0x08
#define WRITE_6               0x0a
#define SEEK_6                0x0b
#define READ_REVERSE          0x0f
#define WRITE_FILEMARKS       0x10
#define SPACE                 0x11
#define INQUIRY               0x12
#define RECOVER_BUFFERED_DATA 0x14
#define MODE_SELECT           0x15
#define RESERVE               0x16
#define RELEASE               0x17
#define COPY                  0x18
#define ERASE                 0x19
#define MODE_SENSE            0x1a
#define START_STOP            0x1b
#define RECEIVE_DIAGNOSTIC    0x1c
#define SEND_DIAGNOSTIC       0x1d
#define ALLOW_MEDIUM_REMOVAL  0x1e

#define SET_WINDOW            0x24
#define READ_CAPACITY         0x25
#define READ_10               0x28
#define WRITE_10              0x2a
#define SEEK_10               0x2b
#define POSITION_TO_ELEMENT   0x2b
#define WRITE_VERIFY          0x2e
#define VERIFY_10             0x2f
#define SEARCH_HIGH           0x30
#define SEARCH_EQUAL          0x31
#define SEARCH_LOW            0x32
#define SET_LIMITS            0x33
#define PRE_FETCH_10          0x34
#define READ_POSITION         0x34
#define SYNCHRONIZE_CACHE     0x35
#define LOCK_UNLOCK_CACHE     0x36
#define READ_DEFECT_DATA      0x37
#define MEDIUM_SCAN           0x38
#define COMPARE               0x39
#define COPY_VERIFY           0x3a
#define WRITE_BUFFER          0x3b
#define READ_BUFFER           0x3c
#define UPDATE_BLOCK          0x3d
#define READ_LONG             0x3e
#define WRITE_LONG            0x3f
#define CHANGE_DEFINITION     0x40
#define WRITE_SAME            0x41
#define UNMAP		      0x42
#define READ_TOC              0x43
#define LOG_SELECT            0x4c
#define LOG_SENSE             0x4d
#define MODE_SELECT_10        0x55
#define RESERVE_10            0x56
#define RELEASE_10            0x57
#define MODE_SENSE_10         0x5a
#define PERSISTENT_RESERVE_IN 0x5e
#define PERSISTENT_RESERVE_OUT 0x5f
#define VARLEN_CDB            0x7f
#define READ_16               0x88
#define COMPARE_AND_WRITE     0x89
#define WRITE_16              0x8a
#define ORWRITE_16            0x8b
#define WRITE_VERIFY_16       0x8e
#define VERIFY_16	      0x8f
#define PRE_FETCH_16          0x90
#define SYNCHRONIZE_CACHE_16  0x91
#define WRITE_SAME_16         0x93
#define SERVICE_ACTION_IN     0x9e
#define	SAI_READ_CAPACITY_16  0x10
#define	SAI_GET_LBA_STATUS    0x12
#define REPORT_LUNS           0xa0
#define MAINT_PROTOCOL_IN     0xa3
#define MOVE_MEDIUM           0xa5
#define EXCHANGE_MEDIUM       0xa6
#define READ_12               0xa8
#define WRITE_12              0xaa
#define WRITE_VERIFY_12       0xae
#define VERIFY_12	      0xaf
#define SEARCH_HIGH_12        0xb0
#define SEARCH_EQUAL_12       0xb1
#define SEARCH_LOW_12         0xb2
#define READ_ELEMENT_STATUS   0xb8
#define SEND_VOLUME_TAG       0xb6
#define WRITE_LONG_2          0xea

/* Service actions for opcode 0xa3 */
#define MPI_REPORT_SUPPORTED_OPCODES 0x0c

#define SAM_STAT_GOOD            0x00
#define SAM_STAT_CHECK_CONDITION 0x02
#define SAM_STAT_CONDITION_MET   0x04
#define SAM_STAT_BUSY            0x08
#define SAM_STAT_INTERMEDIATE    0x10
#define SAM_STAT_INTERMEDIATE_CONDITION_MET 0x14
#define SAM_STAT_RESERVATION_CONFLICT 0x18
#define SAM_STAT_COMMAND_TERMINATED 0x22
#define SAM_STAT_TASK_SET_FULL   0x28
#define SAM_STAT_ACA_ACTIVE      0x30
#define SAM_STAT_TASK_ABORTED    0x40

#define NO_SENSE            0x00
#define RECOVERED_ERROR     0x01
#define NOT_READY           0x02
#define MEDIUM_ERROR        0x03
#define HARDWARE_ERROR      0x04
#define ILLEGAL_REQUEST     0x05
#define UNIT_ATTENTION      0x06
#define DATA_PROTECT        0x07
#define BLANK_CHECK         0x08
#define COPY_ABORTED        0x0a
#define ABORTED_COMMAND     0x0b
#define VOLUME_OVERFLOW     0x0d
#define MISCOMPARE          0x0e

#define TYPE_DISK           0x00
#define TYPE_TAPE           0x01
#define TYPE_PRINTER        0x02
#define TYPE_PROCESSOR      0x03
#define TYPE_WORM           0x04
#define TYPE_MMC            0x05
#define TYPE_SCANNER        0x06
#define TYPE_MOD            0x07

#define TYPE_MEDIUM_CHANGER 0x08
#define TYPE_COMM           0x09
#define TYPE_RAID           0x0c
#define TYPE_ENCLOSURE      0x0d
#define TYPE_RBC	    0x0e
#define TYPE_OSD	    0x11
#define TYPE_NO_LUN         0x7f

#define TYPE_PT	            0xff

#define	MSG_SIMPLE_TAG	0x20
#define	MSG_HEAD_TAG	0x21
#define	MSG_ORDERED_TAG	0x22

#define ABORT_TASK          0x0d
#define ABORT_TASK_SET      0x06
#define CLEAR_ACA           0x16
#define CLEAR_TASK_SET      0x0e
#define LOGICAL_UNIT_RESET  0x17
#define TASK_ABORTED         0x20
#define SAM_STAT_TASK_ABORTED    0x40

/* Key 0: No Sense Errors */
#define NO_ADDITIONAL_SENSE			0x0000
#define ASC_MARK				0x0001
#define ASC_EOM					0x0002
#define ASC_BOM					0x0004
#define ASC_END_OF_DATA				0x0005
#define ASC_OP_IN_PROGRESS			0x0016
#define ASC_DRIVE_REQUIRES_CLEANING		0x8282

/* Key 1: Recovered Errors */
#define ASC_WRITE_ERROR				0x0c00
#define ASC_READ_ERROR				0x1100
#define ASC_RECOVERED_WITH_RETRYS		0x1701
#define ASC_MEDIA_LOAD_EJECT_ERROR		0x5300
#define ASC_FAILURE_PREDICTION			0x5d00

/* Key 2: Not ready */
#define ASC_CAUSE_NOT_REPORTABLE		0x0400
#define ASC_BECOMING_READY			0x0401
#define ASC_INITIALIZING_REQUIRED		0x0402
#define ASC_CLEANING_CART_INSTALLED		0x3003
#define ASC_CLEANING_FAILURE			0x3007
#define ASC_MEDIUM_NOT_PRESENT			0x3a00
#define ASC_LOGICAL_UNIT_NOT_CONFIG		0x3e00

/* Key 3: Medium Errors */
#define ASC_WRITE_ERROR				0x0c00
#define ASC_UNRECOVERED_READ			0x1100
#define ASC_RECORDED_ENTITY_NOT_FOUND		0x1400
#define ASC_UNKNOWN_FORMAT			0x3001
#define ASC_IMCOMPATIBLE_FORMAT			0x3002
#define ASC_MEDIUM_FORMAT_CORRUPT		0x3100
#define ASC_SEQUENTIAL_POSITION_ERR		0x3b00
#define ASC_WRITE_APPEND_ERR			0x5000
#define ASC_CARTRIDGE_FAULT			0x5200
#define ASC_MEDIA_LOAD_OR_EJECT_FAILED		0x5300

/* Key 4: Hardware Failure */
#define ASC_COMPRESSION_CHECK			0x0c04
#define ASC_DECOMPRESSION_CRC			0x110d
#define ASC_MECHANICAL_POSITIONING_ERROR	0x1501
#define ASC_MANUAL_INTERVENTION_REQ		0x0403
#define ASC_HARDWARE_FAILURE			0x4000
#define ASC_INTERNAL_TGT_FAILURE		0x4400
#define ASC_ERASE_FAILURE			0x5100

/* Key 5: Illegal Request */
#define ASC_PARAMETER_LIST_LENGTH_ERR		0x1a00
#define ASC_INVALID_OP_CODE			0x2000
#define ASC_LBA_OUT_OF_RANGE			0x2100
#define ASC_INVALID_FIELD_IN_CDB		0x2400
#define ASC_LUN_NOT_SUPPORTED			0x2500
#define ASC_INVALID_FIELD_IN_PARMS		0x2600
#define ASC_INVALID_RELEASE_OF_PERSISTENT_RESERVATION	0x2604
#define ASC_INCOMPATIBLE_FORMAT			0x3005
#define ASC_SAVING_PARMS_UNSUP			0x3900
#define ASC_MEDIUM_DEST_FULL			0x3b0d
#define ASC_MEDIUM_SRC_EMPTY			0x3b0e
#define ASC_POSITION_PAST_BOM			0x3b0c
#define ASC_MEDIUM_REMOVAL_PREVENTED		0x5302
#define ASC_INSUFFICENT_REGISTRATION_RESOURCES	0x5504
#define ASC_BAD_MICROCODE_DETECTED		0x8283

/* Key 6: Unit Attention */
#define ASC_NOT_READY_TO_TRANSITION		0x2800
#define ASC_POWERON_RESET			0x2900
#define ASC_I_T_NEXUS_LOSS_OCCURRED		0x2907
#define ASC_MODE_PARAMETERS_CHANGED		0x2a01
#define ASC_RESERVATIONS_PREEMPTED		0x2a03
#define ASC_RESERVATIONS_RELEASED		0x2a04
#define ASC_INSUFFICIENT_TIME_FOR_OPERATION	0x2e00
#define ASC_CMDS_CLEARED_BY_ANOTHER_INI		0x2f00
#define ASC_MICROCODE_DOWNLOADED		0x3f01
#define ASC_INQUIRY_DATA_HAS_CHANGED		0x3f03
#define ASC_REPORTED_LUNS_DATA_HAS_CHANGED	0x3f0e
#define ASC_FAILURE_PREDICTION_FALSE		0x5dff

/* Data Protect */
#define ASC_WRITE_PROTECT			0x2700
#define ASC_MEDIUM_OVERWRITE_ATTEMPTED		0x300c

/* Miscompare */
#define ASC_MISCOMPARE_DURING_VERIFY_OPERATION  0x1d00


/* PERSISTENT_RESERVE_IN service action codes */
#define PR_IN_READ_KEYS				0x00
#define PR_IN_READ_RESERVATION			0x01
#define PR_IN_REPORT_CAPABILITIES		0x02
#define PR_IN_READ_FULL_STATUS			0x03

/* PERSISTENT_RESERVE_OUT service action codes */
#define PR_OUT_REGISTER				0x00
#define PR_OUT_RESERVE				0x01
#define PR_OUT_RELEASE				0x02
#define PR_OUT_CLEAR				0x03
#define PR_OUT_PREEMPT				0x04
#define PR_OUT_PREEMPT_AND_ABORT		0x05
#define PR_OUT_REGISTER_AND_IGNORE_EXISTING_KEY	0x06
#define PR_OUT_REGISTER_AND_MOVE		0x07

/* Persistent Reservation scope */
#define PR_LU_SCOPE				0x00

/* Persistent Reservation Type Mask format */
#define PR_TYPE_WRITE_EXCLUSIVE			0x01
#define PR_TYPE_EXCLUSIVE_ACCESS		0x03
#define PR_TYPE_WRITE_EXCLUSIVE_REGONLY		0x05
#define PR_TYPE_EXCLUSIVE_ACCESS_REGONLY	0x06
#define PR_TYPE_WRITE_EXCLUSIVE_ALLREG		0x07
#define PR_TYPE_EXCLUSIVE_ACCESS_ALLREG		0x08

#endif
