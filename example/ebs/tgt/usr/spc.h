#ifndef __SPC_H
#define __SPC_H

extern struct service_action maint_in_service_actions[],
	persistent_reserve_in_actions[], persistent_reserve_out_actions[];

extern int spc_service_action(int host_no, struct scsi_cmd *cmd);
extern int spc_inquiry(int host_no, struct scsi_cmd *cmd);
extern int spc_report_luns(int host_no, struct scsi_cmd *cmd);
extern int spc_start_stop(int host_no, struct scsi_cmd *cmd);
extern int spc_test_unit(int host_no, struct scsi_cmd *cmd);
extern int spc_request_sense(int host_no, struct scsi_cmd *cmd);
extern int spc_prevent_allow_media_removal(int host_no, struct scsi_cmd *cmd);
extern int spc_illegal_op(int host_no, struct scsi_cmd *cmd);
extern int spc_lu_init(struct scsi_lu *lu);
extern int spc_send_diagnostics(int host_no, struct scsi_cmd *cmd);

typedef tgtadm_err (match_fn_t)(struct scsi_lu *lu, char *params);
extern tgtadm_err lu_config(struct scsi_lu *lu, char *params, match_fn_t *);
extern tgtadm_err spc_lu_config(struct scsi_lu *lu, char *params);
extern void spc_lu_exit(struct scsi_lu *lu);
extern void dump_cdb(struct scsi_cmd *cmd);
extern int spc_mode_sense(int host_no, struct scsi_cmd *cmd);
extern tgtadm_err add_mode_page(struct scsi_lu *lu, char *params);
extern int set_mode_page_changeable_mask(struct scsi_lu *lu, uint8_t pcode,
					 uint8_t subpcode, uint8_t *mask);
extern struct mode_pg *find_mode_page(struct scsi_lu *lu,
				      uint8_t pcode, uint8_t subpcode);
extern int spc_mode_select(int host_no, struct scsi_cmd *cmd,
			   int (*update)(struct scsi_cmd *, uint8_t *, int *));
extern struct vpd *alloc_vpd(uint16_t size);
extern tgtadm_err spc_lu_online(struct scsi_lu *lu);
extern tgtadm_err spc_lu_offline(struct scsi_lu *lu);

extern int spc_access_check(struct scsi_cmd *cmd);
#endif
