/*
 * Copyright (C) 2007 Dennis Dalessandro (dennis@osc.edu)
 * Copyright (C) 2007 Ananth Devulapalli (ananth@osc.edu)
 * Copyright (C) 2007 Pete Wyckoff (pw@osc.edu)
 * Copyright (C) 2010 Voltaire, Inc. All rights reserved.
 * Copyright (C) 2010 Alexander Nezhinsky (alexandern@voltaire.com)
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
#ifndef ISER_H
#define ISER_H

#include "iscsid.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

extern short control_port;

/*
 * The IB-extended version from the kernel.  Stags and VAs are in
 * big-endian format.
 */
struct iser_hdr {
	uint8_t   flags;
	uint8_t   rsvd[3];
	uint32_t  write_stag; /* write rkey */
	uint64_t  write_va;
	uint32_t  read_stag;  /* read rkey */
	uint64_t  read_va;
} __attribute__((packed));

#define ISER_WSV	(0x08)
#define ISER_RSV	(0x04)
#define ISCSI_CTRL      (0x10)
#define ISER_HELLO      (0x20)
#define ISER_HELLORPLY  (0x30)

struct iser_conn;

enum iser_ib_op_code {
	ISER_IB_RECV,
	ISER_IB_SEND,
	ISER_IB_RDMA_WRITE,
	ISER_IB_RDMA_READ,
};

#define ISER_HDRS_SZ (sizeof(struct iser_hdr) + sizeof(struct iscsi_hdr))

/*
 * Work requests are either posted Receives for control messages,
 * or Send and RDMA ops (also considered "send" by IB)
 * They have different work request descriptors.
 * During a flush, we need to know the type of op and the
 * task to which it is related.
 */
struct iser_work_req {
	struct list_head wr_list;
	struct iser_task *task;
	enum iser_ib_op_code iser_ib_op;
	struct ibv_sge sge;
	union {
		struct ibv_recv_wr recv_wr;
		struct ibv_send_wr send_wr;
	};
};

/*
 * Pre-registered memory.  Buffers are allocated by iscsi from us, handed
 * to device to fill, then iser can send them directly without registration.
 * Also for write path.
 */
struct iser_membuf {
	void *addr;
	unsigned size;
	unsigned offset; /* offset within task data */
	struct list_head task_list;
	int rdma;
	struct list_head pool_list;
};

struct iser_pdu {
	struct iser_hdr *iser_hdr;
	struct iscsi_hdr *bhs;
	unsigned int ahssize;
	void *ahs;
	/* pdu data only, original buffer is reflected in ibv_sge */
	struct iser_membuf membuf;
};

/*
 * Each SCSI command may have its own RDMA parameters.  These appear on
 * the connection then later are assigned to the particular task to be
 * used when the target responds.
 */
struct iser_task {
	struct iser_conn *conn;

	struct iser_pdu pdu;

	struct iser_work_req rxd;
	struct iser_work_req txd;
	struct iser_work_req rdmad;

	int opcode;
	int is_immediate;
	int is_read;
	int is_write;
	int unsolicited;

	uint64_t tag;
	uint32_t cmd_sn;

	unsigned long flags;

	int in_len;
	int out_len;

	int unsol_sz;
	int unsol_remains;
	int rdma_rd_sz;
	int rdma_rd_remains;
	/* int rdma_rd_offset; // ToDo: multiple RDMA-Write buffers */
	int rdma_wr_sz;
	int rdma_wr_remains;

	/* read and write from the initiator's point of view */
	uint32_t rem_read_stag, rem_write_stag;
	uint64_t rem_read_va, rem_write_va;

	struct list_head in_buf_list;
	int in_buf_num;

	struct list_head out_buf_list;
	int out_buf_num;

	struct list_head exec_list;
	struct list_head rdma_list;
	struct list_head tx_list;
	struct list_head recv_list;

	/* linked to session->cmd_list */
	struct list_head session_list;

	struct list_head dout_task_list;

	int result;
	struct scsi_cmd scmd;

	unsigned char *extdata;
};

struct iser_device;

enum iser_login_phase
{
	LOGIN_PHASE_INIT,
	LOGIN_PHASE_START,      /* keep 1 send spot and 1 recv posted */
	LOGIN_PHASE_LAST_SEND,  /* need 1 more send before ff */
	LOGIN_PHASE_FF, 	/* full feature */

	NUM_LOGIN_PHASE_VALS
};

/*
 * Parallels iscsi_connection.  Adds more fields for iser.
 */
struct iser_conn {
	struct iscsi_connection h;

	struct ibv_qp *qp_hndl;
	struct rdma_cm_id *cm_id;
	struct iser_device *dev;

	struct event_data sched_buf_alloc;
	struct list_head buf_alloc_list;

	struct event_data sched_rdma_rd;
	struct list_head rdma_rd_list;

	struct event_data sched_iosubmit;
	struct list_head iosubmit_list;

	struct event_data sched_tx;
	struct list_head resp_tx_list;

	struct list_head sent_list;

	struct event_data sched_post_recv;
	struct list_head post_recv_list;

	struct event_data sched_conn_free;

	struct sockaddr_storage peer_addr;  /* initiator address */
	char *peer_name;
	struct sockaddr_storage self_addr;  /* target address */
	char *self_name;

	unsigned int ssize, rsize, max_outst_pdu;

	/* FF resources */
	int ff_res_alloc;
	void *task_pool; /* iser_task structures */
	void *pdu_data_pool; /* memory for pdu, non-rdma send/recv */
	struct ibv_mr *pdu_data_mr;   /* memory registration for pdu data */
	struct iser_task *nop_in_task;
	struct iser_task *text_tx_task;

	enum iser_login_phase login_phase;

	/* login phase resources, freed at full-feature */
	int login_res_alloc;
	void *login_task_pool;
	void *login_data_pool;
	struct ibv_mr *login_data_mr;
	struct iser_task *login_rx_task;
	struct iser_task *login_tx_task;

	/* list of all iser conns */
	struct list_head conn_list;
};

static inline struct iser_conn *ISER_CONN(struct iscsi_connection *iscsi_conn)
{
       return container_of(iscsi_conn, struct iser_conn, h);
}

/*
 * Shared variables for a particular device.  The conn[] array will
 * have to be broken out when multiple device support is added, maybe with
 * a pointer into this "device" struct.
 */
struct iser_device {
	struct list_head list;
	struct ibv_context *ibv_ctxt;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_comp_channel *cq_channel;
	struct ibv_device_attr device_attr;

	/* membuf registered buffer, list area, handle */
	void *membuf_regbuf;
	void *membuf_listbuf;
	struct ibv_mr *membuf_mr;
	int waiting_for_mem;

	/* shared memory identifier */
	int rdma_hugetbl_shmid;

	struct event_data poll_sched;

	/* free and allocated membuf entries */
	struct list_head membuf_free, membuf_alloc;
};

void iser_login_exec(struct iscsi_connection *iscsi_conn,
		     struct iser_pdu *rx_pdu,
		     struct iser_pdu *tx_pdu);
int iser_login_complete(struct iscsi_connection *iscsi_conn);
int iser_text_exec(struct iscsi_connection *iscsi_conn,
		   struct iser_pdu *rx_pdu,
		   struct iser_pdu *tx_pdu);

void iser_conn_close(struct iser_conn *conn);

#endif  /* ISER_H */
