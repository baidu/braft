#ifndef __DRIVER_H__
#define __DRIVER_H__

#include "tgtadm_error.h"

enum tgt_driver_state {
	DRIVER_REGD = 0, /* just registered */
	DRIVER_INIT, /* initialized ok */
	DRIVER_ERR,  /* failed to initialize */
	DRIVER_EXIT  /* exited */
};

struct tgt_driver {
	const char *name;
	enum tgt_driver_state drv_state;

	int (*init)(int, char *);
	void (*exit)(void);

	int (*target_create)(struct target *);
	void (*target_destroy)(int, int);

	int (*portal_create)(char *);
	int (*portal_destroy)(char *);

	int (*lu_create)(struct scsi_lu *);

	tgtadm_err (*update)(int, int, int ,uint64_t, uint64_t, uint32_t, char *);
	tgtadm_err (*show)(int, int, uint64_t, uint32_t, uint64_t, struct concat_buf *);
	tgtadm_err (*stat)(int, int, uint64_t, uint32_t, uint64_t, struct concat_buf *);

	uint64_t (*scsi_get_lun)(uint8_t *);

	int (*cmd_end_notify)(uint64_t nid, int result, struct scsi_cmd *);
	int (*mgmt_end_notify)(struct mgmt_req *);

	int (*transportid)(int, uint64_t, char *, int);

	const char *default_bst;

	struct list_head target_list;
};

extern struct tgt_driver *tgt_drivers[];
extern int get_driver_index(char *name);
extern int register_driver(struct tgt_driver *drv);
extern const char *driver_state_name(struct tgt_driver *drv);

#endif /* __DRIVER_H__ */
