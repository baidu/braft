/*
 * Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>
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
#ifndef ISCSID_H
#define ISCSID_H

#include <stdint.h>
#include <inttypes.h>
#include <netdb.h>

#include "transport.h"
#include "list.h"
#include "param.h"
#include "log.h"
#include "tgtd.h"
#include "util.h"

#include "iscsi_proto.h"
#include "iscsi_if.h"

#define cpu_to_be16(x)	__cpu_to_be16(x)
#define cpu_to_be32(x)	__cpu_to_be32(x)
#define cpu_to_be64(x)	__cpu_to_be64(x)
#define be16_to_cpu(x)	__be16_to_cpu(x)
#define be32_to_cpu(x)	__be32_to_cpu(x)
#define be64_to_cpu(x)	__be64_to_cpu(x)

#define MAX_QUEUE_CMD_MIN	1
#define MAX_QUEUE_CMD_DEF	128
#define MAX_QUEUE_CMD_MAX	512

#define ISCSI_NAME_LEN 256

#define DIGEST_ALL		(DIGEST_NONE | DIGEST_CRC32C)
#define DIGEST_NONE		(1 << 0)
#define DIGEST_CRC32C           (1 << 1)

#define sid64(isid, tsih)					\
({								\
	(uint64_t) isid[0] <<  0 | (uint64_t) isid[1] <<  8 |	\
	(uint64_t) isid[2] << 16 | (uint64_t) isid[3] << 24 |	\
	(uint64_t) isid[4] << 32 | (uint64_t) isid[5] << 40 |	\
	(uint64_t) tsih << 48;					\
})

#define sid_to_tsih(sid) ((sid) >> 48)

struct iscsi_pdu {
	struct iscsi_hdr bhs;
	void *ahs;
	unsigned int ahssize;
	void *data;
	unsigned int datasize;
};

struct iscsi_session {
	int refcount;

	/* linked to target->sessions_list */
	struct list_head slist;

	/* linked to sessions_list */
	struct list_head hlist;

	char *initiator;
	char *initiator_alias;
	struct iscsi_target *target;
	uint8_t isid[6];
	uint16_t tsih;

	/* links all connections (conn->clist) */
	struct list_head conn_list;
	int conn_cnt;

	/* links all tasks (task->c_hlist) */
	struct list_head cmd_list;

	/* links pending tasks (task->c_list) */
	struct list_head pending_cmd_list;

	uint32_t exp_cmd_sn;
	uint32_t max_queue_cmd;

	struct param session_param[ISCSI_PARAM_MAX];

	char *info;

	/* if this session uses rdma connections */
	int rdma;
};

struct iscsi_task {
	struct iscsi_hdr req;
	struct iscsi_hdr rsp;

	uint64_t tag;
	struct iscsi_connection *conn;

	/* linked to session->cmd_list */
	struct list_head c_hlist;

	/* linked to conn->tx_clist or session->cmd_pending_list */
	struct list_head c_list;

	/* linked to conn->tx_clist or conn->task_list */
	struct list_head c_siblings;

	unsigned long flags;

	int result;
	int len;

	int offset;

	int r2t_count;
	int unsol_count;
	int exp_r2tsn;

	void *ahs;
	void *data;

	struct scsi_cmd scmd;

	unsigned long extdata[0];
};

struct iscsi_connection {
	int state;

	/* should be a new state */
	int closed;

	int rx_iostate;
	int tx_iostate;
	int refcount;

	struct list_head clist;
	struct iscsi_session *session;

	int tid;
	struct param session_param[ISCSI_PARAM_MAX];

	char *initiator;
	char *initiator_alias;
	uint8_t isid[6];
	uint16_t tsih;
	uint16_t cid;
	int session_type;
	int auth_method;

	uint32_t stat_sn;
	uint32_t exp_stat_sn;

	uint32_t cmd_sn;
	uint32_t exp_cmd_sn;
	uint32_t max_cmd_sn;

	struct iscsi_pdu req;
	void *req_buffer;
	struct iscsi_pdu rsp;
	void *rsp_buffer;
	int rsp_buffer_size;
	unsigned char *rx_buffer;
	unsigned char *tx_buffer;
	int rx_size;
	int tx_size;

	uint32_t ttt;
	int text_datasize;
	void *text_rsp_buffer;

	struct iscsi_task *rx_task;
	struct iscsi_task *tx_task;

	struct list_head tx_clist;

	struct list_head task_list;

	unsigned char rx_digest[4];
	unsigned char tx_digest[4];

	int auth_state;
	union {
		struct {
			int digest_alg;
			int id;
			int challenge_size;
			unsigned char *challenge;
		} chap;
	} auth;

	struct iscsi_transport *tp;

	struct iscsi_stats stats;
};

#define STATE_FREE		0
#define STATE_SECURITY		1
#define STATE_SECURITY_AUTH	2
#define STATE_SECURITY_DONE	3
#define STATE_SECURITY_LOGIN	4
#define STATE_SECURITY_FULL	5
#define STATE_LOGIN		6
#define STATE_LOGIN_FULL	7
#define STATE_FULL		8
#define STATE_KERNEL		9
#define STATE_CLOSE		10
#define STATE_EXIT		11
#define STATE_SCSI		12
#define STATE_INIT		13
#define STATE_START		14
#define STATE_READY		15

#define AUTH_STATE_START	0
#define AUTH_STATE_CHALLENGE	1

/* don't touch these */
#define AUTH_DIR_INCOMING       0
#define AUTH_DIR_OUTGOING       1

#define SESSION_NORMAL		0
#define SESSION_DISCOVERY	1
#define AUTH_UNKNOWN		-1
#define AUTH_NONE		0
#define AUTH_CHAP		1
#define DIGEST_UNKNOWN		-1

#define BHS_SIZE		sizeof(struct iscsi_hdr)

#define INCOMING_BUFSIZE	8192

extern int default_nop_interval;
extern int default_nop_count;

struct iscsi_target {
	struct list_head tlist;

	struct list_head sessions_list;

	struct param session_param[ISCSI_PARAM_MAX];

	int tid;
	char *alias;

	int max_nr_sessions;
	int nr_sessions;

	struct redirect_info {
		char addr[NI_MAXHOST + 1];
		char port[NI_MAXSERV + 1];
		uint8_t reason;
		char	*callback;
	} redirect_info;

	struct list_head isns_list;

	int rdma;
	int nop_interval;
	int nop_count;
};

enum task_flags {
	TASK_pending,
	TASK_in_scsi,
};

struct iscsi_portal {
	struct list_head iscsi_portal_siblings;
	char *addr;
	int port;
	int tpgt;
	int fd;
	int af;
};

extern struct list_head iscsi_portals_list;

extern char *portal_arguments;

#define set_task_pending(t)	((t)->flags |= (1 << TASK_pending))
#define clear_task_pending(t)	((t)->flags &= ~(1 << TASK_pending))
#define task_pending(t)		((t)->flags & (1 << TASK_pending))

#define set_task_in_scsi(t)     ((t)->flags |= (1 << TASK_in_scsi))
#define clear_task_in_scsi(t)	((t)->flags &= ~(1 << TASK_in_scsi))
#define task_in_scsi(t)		((t)->flags & (1 << TASK_in_scsi))

extern int lld_index;
extern struct list_head iscsi_targets_list;

/* chap.c */
extern int cmnd_exec_auth_chap(struct iscsi_connection *conn);

/* conn.c */
extern int conn_init(struct iscsi_connection *conn);
extern void conn_exit(struct iscsi_connection *conn);
extern void conn_close(struct iscsi_connection *conn);
extern void conn_put(struct iscsi_connection *conn);
extern int conn_get(struct iscsi_connection *conn);
extern struct iscsi_connection * conn_find(struct iscsi_session *session, uint32_t cid);
extern int conn_take_fd(struct iscsi_connection *conn);
extern void conn_add_to_session(struct iscsi_connection *conn, struct iscsi_session *session);
extern tgtadm_err conn_close_admin(uint32_t tid, uint64_t sid, uint32_t cid);

/* iscsid.c */
extern char *text_key_find(struct iscsi_connection *conn, char *searchKey);
extern void text_key_add(struct iscsi_connection *conn, char *key, char *value);
extern void conn_read_pdu(struct iscsi_connection *conn);
extern int iscsi_tx_handler(struct iscsi_connection *conn);
extern void iscsi_rx_handler(struct iscsi_connection *conn);
extern int iscsi_scsi_cmd_execute(struct iscsi_task *task);
extern int iscsi_transportid(int tid, uint64_t itn_id, char *buf, int size);
extern int iscsi_add_portal(char *addr, int port, int tpgt);
extern void iscsi_print_nop_settings(struct concat_buf *b, int tid);
extern int iscsi_update_target_nop_count(int tid, int count);
extern int iscsi_update_target_nop_interval(int tid, int interval);
extern void iscsi_set_nop_interval(int interval);
extern void iscsi_set_nop_count(int count);
extern int iscsi_delete_portal(char *addr, int port);
extern int iscsi_param_parse_portals(char *p, int do_add, int do_delete);
extern void iscsi_update_conn_stats_rx(struct iscsi_connection *conn, int size, int opcode);
extern void iscsi_update_conn_stats_tx(struct iscsi_connection *conn, int size, int opcode);
extern void iscsi_rsp_set_residual(struct iscsi_cmd_rsp *rsp, struct scsi_cmd *scmd);

/* iscsid.c iscsi_task */
extern void iscsi_free_task(struct iscsi_task *task);
extern void iscsi_free_cmd_task(struct iscsi_task *task);

/* session.c */
extern struct iscsi_session *session_find_name(int tid, const char *iname, uint8_t *isid);
extern struct iscsi_session *session_lookup_by_tsih(uint16_t tsih);
extern int session_create(struct iscsi_connection *conn);
extern void session_get(struct iscsi_session *session);
extern void session_put(struct iscsi_session *session);

/* target.c */
extern struct iscsi_target * target_find_by_name(const char *name);
extern struct iscsi_target * target_find_by_id(int tid);
extern void target_list_build(struct iscsi_connection *, char *, char *);
extern int ip_acl(int tid, struct iscsi_connection *conn);
extern int iqn_acl(int tid, struct iscsi_connection *conn);
extern int iscsi_target_create(struct target *);
extern void iscsi_target_destroy(int tid, int force);
extern tgtadm_err iscsi_target_show(int mode, int tid, uint64_t sid, uint32_t cid,
				    uint64_t lun, struct concat_buf *b);
extern tgtadm_err iscsi_stat(int mode, int tid, uint64_t sid, uint32_t cid,
			     uint64_t lun, struct concat_buf *b);
extern tgtadm_err iscsi_target_update(int mode, int op, int tid, uint64_t sid, uint64_t lun,
				      uint32_t cid, char *name);
extern int target_redirected(struct iscsi_target *target,
			     struct iscsi_connection *conn, char *buf, int *reason);

/* param.c */
extern int param_index_by_name(char *name, struct iscsi_key *keys);

/* transport.c */
extern int iscsi_init(int, char *);
extern void iscsi_exit(void);

/* isns.c */
extern int isns_init(void);
extern void isns_exit(void);
extern tgtadm_err isns_show(struct concat_buf *b);
extern tgtadm_err isns_update(char *params);
extern int isns_scn_access(int tid, char *name);
extern int isns_target_register(char *name);
extern int isns_target_deregister(char *name);

#endif	/* ISCSID_H */
