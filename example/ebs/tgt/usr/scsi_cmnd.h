struct target;
struct mgmt_req;

/* needs to move somewhere else */
#define SCSI_SENSE_BUFFERSIZE	252

enum data_direction {
	DATA_NONE = 0,
	DATA_WRITE = 1,
	DATA_READ = 2,
	DATA_BIDIRECTIONAL = 3,
};

struct scsi_data_buffer {
	uint64_t buffer;
	uint32_t length;
	uint32_t transfer_len;
	int32_t resid;
};

struct scsi_cmd {
	struct target *c_target;
	/* linked it_nexus->cmd_hash_list */
	struct list_head c_hlist;
	struct list_head qlist;

	uint64_t dev_id;

	struct scsi_lu *dev;
	unsigned long state;

	enum data_direction data_dir;
	struct scsi_data_buffer in_sdb;
	struct scsi_data_buffer out_sdb;

	uint64_t cmd_itn_id;
	uint64_t offset;
	uint32_t tl;
	uint8_t *scb;
	int scb_len;
	uint8_t lun[8];
	int attribute;
	uint64_t tag;
	int result;
	struct mgmt_req *mreq;

	unsigned char sense_buffer[SCSI_SENSE_BUFFERSIZE];
	int sense_len;

	struct list_head bs_list;

	struct it_nexus *it_nexus;
	struct it_nexus_lu_info *itn_lu_info;
};

#define scsi_cmnd_accessor(field, type)						\
static inline void scsi_set_##field(struct scsi_cmd *scmd, type val)		\
{										\
	scmd->field = val;							\
}										\
static inline type scsi_get_##field(struct scsi_cmd *scmd)			\
{										\
	return scmd->field;							\
}

scsi_cmnd_accessor(result, int);
scsi_cmnd_accessor(data_dir, enum data_direction);


#define scsi_data_buffer_accessor(field, type, set_cast, get_cast)		\
	scsi_data_buffer_function(in, field, type, set_cast, get_cast)		\
	scsi_data_buffer_function(out, field, type, set_cast, get_cast)

#define scsi_data_buffer_function(dir, field, type, set_cast, get_cast)		\
static inline void scsi_set_##dir##_##field(struct scsi_cmd *scmd, type val)	\
{										\
	scmd->dir##_sdb.field = set_cast (val);					\
}										\
static inline type scsi_get_##dir##_##field(struct scsi_cmd *scmd)		\
{										\
	return get_cast (scmd->dir##_sdb.field);				\
}

scsi_data_buffer_accessor(length, uint32_t, ,);
scsi_data_buffer_accessor(transfer_len, uint32_t, ,);
scsi_data_buffer_accessor(resid, int32_t, ,);
scsi_data_buffer_accessor(buffer, void *, (unsigned long), (void *)(unsigned long));

static inline void scsi_set_in_resid_by_actual(struct scsi_cmd *scmd,
					       uint32_t transfer_len)
{
	uint32_t expected_len = scsi_get_in_length(scmd);
	int32_t resid;

	if (transfer_len <= expected_len)
		resid = expected_len - transfer_len;
	else {
		resid = -(int32_t)(transfer_len - expected_len);
		transfer_len = expected_len;
	}
	scsi_set_in_transfer_len(scmd, transfer_len);
	scsi_set_in_resid(scmd, resid);
}

static inline void scsi_set_out_resid_by_actual(struct scsi_cmd *scmd,
						uint32_t transfer_len)
{
	uint32_t expected_len = scsi_get_out_length(scmd);
	int32_t resid;

	if (transfer_len <= expected_len)
		resid = expected_len - transfer_len;
	else {
		resid = -(int32_t)(transfer_len - expected_len);
		transfer_len = expected_len;
	}
	scsi_set_out_transfer_len(scmd, transfer_len);
	scsi_set_out_resid(scmd, resid);
}

enum {
	TGT_CMD_QUEUED,
	TGT_CMD_PROCESSED,
	TGT_CMD_ASYNC,
	TGT_CMD_NOT_LAST,
};

#define CMD_FNS(bit, name)						\
static inline void set_cmd_##name(struct scsi_cmd *c)			\
{									\
	(c)->state |= (1UL << TGT_CMD_##bit);				\
}									\
static inline void clear_cmd_##name(struct scsi_cmd *c)			\
{									\
	(c)->state &= ~(1UL << TGT_CMD_##bit);				\
}									\
static inline int cmd_##name(const struct scsi_cmd *c)			\
{									\
	return ((c)->state & (1UL << TGT_CMD_##bit));			\
}

CMD_FNS(QUEUED, queued)
CMD_FNS(PROCESSED, processed)
CMD_FNS(ASYNC, async)
CMD_FNS(NOT_LAST, not_last)
