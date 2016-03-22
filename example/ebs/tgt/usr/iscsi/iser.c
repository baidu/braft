/*
 * iSCSI extensions for RDMA (iSER) data path
 *
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/ipc.h>
#include <sys/shm.h>

#include "util.h"
#include "iscsid.h"
#include "target.h"
#include "driver.h"
#include "scsi.h"
#include "work.h"
#include "iser.h"

#if defined(HAVE_VALGRIND) && !defined(NDEBUG)
#include <valgrind/memcheck.h>
#else
#define VALGRIND_MAKE_MEM_DEFINED(addr, len)
#endif

static struct iscsi_transport iscsi_iser;

/* global, across all devices */
static struct rdma_event_channel *rdma_evt_channel;
static struct rdma_cm_id *cma_listen_id;

/* accepted at RDMA layer, but not yet established */
static LIST_HEAD(temp_conn);

/* all devices */
static LIST_HEAD(iser_dev_list);

/* all iser connections */
static LIST_HEAD(iser_conn_list);

#define uint64_from_ptr(p) (uint64_t)(uintptr_t)(p)
#define ptr_from_int64(p) (void *)(unsigned long)(p)

#define ISER_LISTEN_PORT 3260

short int iser_listen_port = ISER_LISTEN_PORT;
char *iser_portal_addr;

/*
 * Crazy hard-coded linux iser settings need 128 * 8 slots + slop, plus
 * room for our rdmas and send requests.
 */
#define MAX_WQE 1800

#define RDMA_TRANSFER_SIZE  (512 * 1024)

#define MAX_POLL_WC 32

#define DEFAULT_POOL_SIZE_MB    1024
#define MAX_CQ_ENTRIES          (MAX_QUEUE_CMD_MAX * DEFAULT_POOL_SIZE_MB)

#define MASK_BY_BIT(b)  	((1UL << b) - 1)
#define ALIGN_TO_BIT(x, b)      ((((unsigned long)x) + MASK_BY_BIT(b)) & \
				 ~MASK_BY_BIT(b))
#define ALIGN_TO_32(x)  	ALIGN_TO_BIT(x, 5)

struct iscsi_sense_data {
	uint16_t length;
	uint8_t data[0];
} __packed;

static size_t buf_pool_sz_mb = DEFAULT_POOL_SIZE_MB;
static int cq_vector = -1;

static int membuf_num;
static size_t membuf_size = RDMA_TRANSFER_SIZE;

static int iser_conn_get(struct iser_conn *conn);
static int iser_conn_getn(struct iser_conn *conn, int n);
static void iser_conn_put(struct iser_conn *conn);

static void iser_free_login_resources(struct iser_conn *conn);
static void iser_free_ff_resources(struct iser_conn *conn);

static void iser_login_rx(struct iser_task *task);

static int iser_device_init(struct iser_device *dev);

static void iser_handle_cq_event(int fd __attribute__ ((unused)),
				 int events __attribute__ ((unused)),
				 void *data);

static void iser_handle_async_event(int fd __attribute__ ((unused)),
				    int events __attribute__ ((unused)),
				    void *data);

static void iser_sched_poll_cq(struct event_data *tev);
static void iser_sched_consume_cq(struct event_data *tev);
static void iser_sched_tx(struct event_data *evt);
static void iser_sched_post_recv(struct event_data *evt);
static void iser_sched_buf_alloc(struct event_data *evt);
static void iser_sched_iosubmit(struct event_data *evt);
static void iser_sched_rdma_rd(struct event_data *evt);

static inline void schedule_task_iosubmit(struct iser_task *task,
					  struct iser_conn *conn)
{
	list_add_tail(&task->exec_list, &conn->iosubmit_list);
	tgt_add_sched_event(&conn->sched_iosubmit);

	dprintf("task:%p tag:0x%04"PRIx64 " cmdsn:0x%x\n",
		task, task->tag, task->cmd_sn);
}

static inline void schedule_rdma_read(struct iser_task *task,
				      struct iser_conn *conn)
{
	list_add_tail(&task->rdma_list, &conn->rdma_rd_list);
	tgt_add_sched_event(&conn->sched_rdma_rd);

	dprintf("task:%p tag:0x%04"PRIx64 " cmdsn:0x%x\n",
		task, task->tag, task->cmd_sn);
}

static inline void schedule_resp_tx(struct iser_task *task,
				    struct iser_conn *conn)
{
	list_add_tail(&task->tx_list, &conn->resp_tx_list);
	tgt_add_sched_event(&conn->sched_tx);

	dprintf("task:%p tag:0x%04"PRIx64 " cmdsn:0x%x\n",
		task, task->tag, task->cmd_sn);
}

static inline void schedule_post_recv(struct iser_task *task,
				      struct iser_conn *conn)
{
	list_add_tail(&task->recv_list, &conn->post_recv_list);
	tgt_add_sched_event(&conn->sched_post_recv);

	dprintf("task:%p tag:0x%04"PRIx64 " cmdsn:0x%x\n",
		task, task->tag, task->cmd_sn);
}

static char *iser_ib_op_to_str(enum iser_ib_op_code iser_ib_op)
{
	char *op_str;

	switch (iser_ib_op) {
	case ISER_IB_RECV:
		op_str = "recv";
		break;
	case ISER_IB_SEND:
		op_str = "send";
		break;
	case ISER_IB_RDMA_WRITE:
		op_str = "rdma_wr";
		break;
	case ISER_IB_RDMA_READ:
		op_str = "rdma_rd";
		break;
	default:
		op_str = "Unknown";
		break;
	}
	return op_str;
}

static void iser_pdu_init(struct iser_pdu *pdu, void *buf)
{
	pdu->iser_hdr = (struct iser_hdr *) buf;
	buf += sizeof(struct iser_hdr);

	pdu->bhs = (struct iscsi_hdr *) buf;
	buf += sizeof(struct iscsi_hdr);

	pdu->ahs = buf;
	pdu->ahssize = 0;

	pdu->membuf.addr = buf;
	pdu->membuf.size = 0;
}

static void iser_rxd_init(struct iser_work_req *rxd,
			  struct iser_task *task,
			  void *buf, unsigned size,
			  struct ibv_mr *srmr)
{
	rxd->task = task;
	rxd->iser_ib_op = ISER_IB_RECV;

	rxd->sge.addr = uint64_from_ptr(buf);
	rxd->sge.length = size;
	rxd->sge.lkey = srmr->lkey;

	rxd->recv_wr.wr_id = uint64_from_ptr(rxd);
	rxd->recv_wr.sg_list = &rxd->sge;
	rxd->recv_wr.num_sge = 1;
	rxd->recv_wr.next = NULL;
}

static void iser_txd_init(struct iser_work_req *txd,
			  struct iser_task *task,
			  void *buf, unsigned size,
			  struct ibv_mr *srmr)
{
	txd->task = task;
	txd->iser_ib_op = ISER_IB_SEND;

	txd->sge.addr = uint64_from_ptr(buf);
	txd->sge.length = size;
	txd->sge.lkey = srmr->lkey;

	txd->send_wr.wr_id = uint64_from_ptr(txd);
	txd->send_wr.next = NULL;
	txd->send_wr.sg_list = &txd->sge;
	txd->send_wr.num_sge = 1;
	txd->send_wr.opcode = IBV_WR_SEND;
	txd->send_wr.send_flags = IBV_SEND_SIGNALED;

	INIT_LIST_HEAD(&txd->wr_list);
}

static void iser_rdmad_init(struct iser_work_req *rdmad,
			    struct iser_task *task,
			    struct ibv_mr *srmr)
{
	rdmad->task = task;

	rdmad->sge.lkey = srmr->lkey;

	rdmad->send_wr.wr_id = uint64_from_ptr(rdmad);
	rdmad->send_wr.sg_list = &rdmad->sge;
	rdmad->send_wr.num_sge = 1;
	rdmad->send_wr.next = NULL;
	rdmad->send_wr.send_flags = IBV_SEND_SIGNALED;

	/* to be set before posting:
	   rdmad->iser_ib_op, rdmad->send_wr.opcode
	   rdmad->sge.addr, rdmad->sge.length
	   rdmad->send_wr.wr.rdma.(remote_addr,rkey) */

	INIT_LIST_HEAD(&rdmad->wr_list);
}

static void iser_task_init(struct iser_task *task,
			   struct iser_conn *conn,
			   void *pdu_buf,
			   unsigned long buf_size,
			   struct ibv_mr *srmr)
{
	task->conn = conn;
	task->unsolicited = 0;

	iser_pdu_init(&task->pdu, pdu_buf);

	iser_rxd_init(&task->rxd, task, pdu_buf, buf_size, srmr);
	iser_txd_init(&task->txd, task, pdu_buf, buf_size, srmr);
	iser_rdmad_init(&task->rdmad, task, conn->dev->membuf_mr);

	INIT_LIST_HEAD(&task->in_buf_list);
	INIT_LIST_HEAD(&task->out_buf_list);

	INIT_LIST_HEAD(&task->exec_list);
	INIT_LIST_HEAD(&task->rdma_list);
	INIT_LIST_HEAD(&task->tx_list);
	INIT_LIST_HEAD(&task->recv_list);

	INIT_LIST_HEAD(&task->session_list);
	INIT_LIST_HEAD(&task->dout_task_list);
}

static void iser_unsolicited_task_init(struct iser_task *task,
				       struct iser_conn *conn,
				       void *pdu_data_buf,
				       unsigned long buf_size,
				       struct ibv_mr *srmr)
{
	task->conn = conn;
	task->unsolicited = 1;

	iser_txd_init(&task->txd, task, pdu_data_buf, buf_size, srmr);
	iser_pdu_init(&task->pdu, pdu_data_buf);
}

static void iser_task_add_out_pdu_buf(struct iser_task *task,
				      struct iser_membuf *data_buf,
				      unsigned int offset)
{
	data_buf->offset = offset;
	task->out_buf_num++;
	if (!list_empty(&task->out_buf_list)) {
		struct iser_membuf *cur_buf;
		list_for_each_entry(cur_buf, &task->out_buf_list, task_list) {
			if (offset < cur_buf->offset) {
				dprintf("task:%p offset:%d size:%d data_buf:%p add before:%p\n",
					task, offset, data_buf->size, data_buf->addr, cur_buf->addr);
				list_add_tail(&data_buf->task_list, &cur_buf->task_list);
				return;
			}
		}
	}
	dprintf("task:%p offset:%d size:%d data_buf:%p add last\n",
		task, offset, data_buf->size, data_buf->addr);
	list_add_tail(&data_buf->task_list, &task->out_buf_list);
}

static void iser_task_add_out_rdma_buf(struct iser_task *task,
				       struct iser_membuf *data_buf,
				       unsigned int offset)
{
	data_buf->offset = offset;
	task->out_buf_num++;
	dprintf("task:%p offset:%d size:%d data_buf:%p add last\n",
		task, offset, data_buf->size, data_buf->addr);
	list_add_tail(&data_buf->task_list, &task->out_buf_list);
}

static void iser_task_del_out_buf(struct iser_task *task,
				  struct iser_membuf *data_buf)
{
	dprintf("task:%p offset:%d size:%d data_buf:%p\n",
		task, data_buf->offset, data_buf->size, data_buf->addr);
	list_del(&data_buf->task_list);
	task->out_buf_num--;
}

static void iser_task_add_in_rdma_buf(struct iser_task *task,
				      struct iser_membuf *data_buf,
				      unsigned int offset)
{
	dprintf("task:%p offset:0x%d size:%d data_buf:%p add last\n",
		task, offset, data_buf->size, data_buf->addr);
	data_buf->offset = offset;
	task->in_buf_num++;
	list_add_tail(&data_buf->task_list, &task->in_buf_list);
}

static void iser_task_del_in_buf(struct iser_task *task,
				 struct iser_membuf *data_buf)
{
	dprintf("task:%p offset:%d size:%d data_buf:%p\n",
		task, data_buf->offset, data_buf->size, data_buf->addr);
	list_del(&data_buf->task_list);
	task->in_buf_num--;
}

static void iser_prep_resp_send_req(struct iser_task *task,
				    struct iser_work_req *next_wr,
				    int signaled)
{
	struct iser_pdu *pdu = &task->pdu;
	struct iser_hdr *iser_hdr = pdu->iser_hdr;
	struct iscsi_hdr *bhs = pdu->bhs;
	struct iser_work_req *txd = &task->txd;

	bhs->hlength = pdu->ahssize / 4;
	hton24(bhs->dlength, pdu->membuf.size);

	txd->sge.length = ISER_HDRS_SZ;
	txd->sge.length += pdu->ahssize;
	txd->sge.length += pdu->membuf.size;

	memset(iser_hdr, 0, sizeof(*iser_hdr));
	iser_hdr->flags = ISCSI_CTRL;

	txd->send_wr.next = (next_wr ? &next_wr->send_wr : NULL);
	txd->send_wr.send_flags = (signaled ? IBV_SEND_SIGNALED : 0);

	dprintf("task:%p wr_id:0x%"PRIx64 " tag:0x%04"PRIx64 " dtbuf:%p "
		"dtsz:%u ahs_sz:%u stat:0x%x statsn:0x%x expcmdsn:0x%x\n",
		task, txd->send_wr.wr_id, task->tag,
		pdu->membuf.addr, pdu->membuf.size, pdu->ahssize,
		bhs->rsvd2[1], ntohl(bhs->statsn), ntohl(bhs->exp_statsn));
}

static void iser_prep_rdma_wr_send_req(struct iser_task *task,
				       struct iser_work_req *next_wr,
				       int signaled)
{
	struct iser_work_req *rdmad = &task->rdmad;
	struct iser_membuf *rdma_buf;
	uint64_t offset = 0; /* ToDo: multiple RDMA-Write buffers */

	rdmad->iser_ib_op = ISER_IB_RDMA_WRITE;

	/* RDMA-Write buffer is the only one on the list */
	/* ToDo: multiple RDMA-Write buffers, use rdma_buf->offset */
	rdma_buf = list_first_entry(&task->in_buf_list, struct iser_membuf, task_list);
	rdmad->sge.addr = uint64_from_ptr(rdma_buf->addr);

	if (likely(task->rdma_wr_remains <= rdma_buf->size)) {
		rdmad->sge.length = task->rdma_wr_remains;
		task->rdma_wr_remains = 0;
	} else {
		rdmad->sge.length = rdma_buf->size;
		task->rdma_wr_remains -= rdmad->sge.length;
	}

	rdmad->send_wr.next = (next_wr ? &next_wr->send_wr : NULL);
	rdmad->send_wr.opcode = IBV_WR_RDMA_WRITE;
	rdmad->send_wr.send_flags = (signaled ? IBV_SEND_SIGNALED : 0);

	rdmad->send_wr.wr.rdma.remote_addr = task->rem_read_va + offset;
	/* offset += rdmad->sge.length // ToDo: multiple RDMA-Write buffers */
	rdmad->send_wr.wr.rdma.rkey = task->rem_read_stag;

	dprintf(" task:%p tag:0x%04"PRIx64 " wr_id:0x%"PRIx64 " daddr:0x%"PRIx64 " dsz:%u "
		"bufsz:%u rdma:%d lkey:%x raddr:%"PRIx64 " rkey:%x rems:%u\n",
		task, task->tag, rdmad->send_wr.wr_id, rdmad->sge.addr,
		rdmad->sge.length, rdma_buf->size, rdma_buf->rdma,
		rdmad->sge.lkey, rdmad->send_wr.wr.rdma.remote_addr,
		rdmad->send_wr.wr.rdma.rkey, task->rdma_wr_remains);
}

static void iser_prep_rdma_rd_send_req(struct iser_task *task,
				       struct iser_work_req *next_wr,
				       int signaled)
{
	struct iser_work_req *rdmad = &task->rdmad;
	struct iser_membuf *rdma_buf;
	uint64_t offset;
	int cur_req_sz;

	rdmad->iser_ib_op = ISER_IB_RDMA_READ;

	/* RDMA-Read buffer is always put at the list's tail */
	/* ToDo: multiple RDMA-Write buffers */
	rdma_buf = container_of(task->out_buf_list.prev,
				struct iser_membuf, task_list);
	offset = rdma_buf->offset;
	rdmad->sge.addr = uint64_from_ptr(rdma_buf->addr) + offset;

	if (task->rdma_rd_sz <= rdma_buf->size)
		cur_req_sz = task->rdma_rd_sz;
	else
		cur_req_sz = rdma_buf->size;

	/* task->rdma_rd_offset += cur_req_sz; // ToDo: multiple RDMA-Write buffers */

	rdmad->sge.length = cur_req_sz;

	rdmad->send_wr.next = (next_wr ? &next_wr->send_wr : NULL);
	rdmad->send_wr.opcode = IBV_WR_RDMA_READ;
	rdmad->send_wr.send_flags = (signaled ? IBV_SEND_SIGNALED : 0);
	rdmad->send_wr.wr.rdma.remote_addr =
		(uint64_t) task->rem_write_va;
		/* + (offset - task->unsol_sz); // ToDo: multiple RDMA-Write buffers */
	rdmad->send_wr.wr.rdma.rkey =
		(uint32_t) task->rem_write_stag;

	dprintf("task:%p wr_id:0x%"PRIx64 " tag:0x%04"PRIx64 " daddr:0x%"PRIx64 " dsz:%u "
		"bufsz:%u rdma:%d lkey:%x raddr:%"PRIx64 " rkey:%x rems:%u\n",
		task, rdmad->send_wr.wr_id, task->tag, rdmad->sge.addr,
		rdmad->sge.length, rdma_buf->size, rdma_buf->rdma,
		rdmad->sge.lkey, rdmad->send_wr.wr.rdma.remote_addr,
		rdmad->send_wr.wr.rdma.rkey, task->rdma_rd_sz);
}

static int iser_post_send(struct iser_conn *conn,
			  struct iser_work_req *iser_send,
			  int num_send_reqs)
{
	struct ibv_send_wr *bad_wr;
	int err, nr_posted;

	err = ibv_post_send(conn->qp_hndl, &iser_send->send_wr, &bad_wr);
	if (likely(!err)) {
		nr_posted = num_send_reqs;
		dprintf("conn:%p posted:%d 1st wr:%p wr_id:0x%lx sge_sz:%u\n",
			&conn->h, nr_posted, iser_send,
			(unsigned long)iser_send->send_wr.wr_id,
			iser_send->sge.length);
	} else {
		struct ibv_send_wr *wr;

		nr_posted = 0;
		for (wr = &iser_send->send_wr; wr != bad_wr; wr = wr->next)
			nr_posted++;

		eprintf("%m, conn:%p posted:%d/%d 1st wr:%p wr_id:0x%lx sge_sz:%u\n",
			&conn->h, nr_posted, num_send_reqs, iser_send,
			(unsigned long)iser_send->send_wr.wr_id,
			iser_send->sge.length);
	}

	iser_conn_getn(conn, nr_posted);

	return err;
}

static int iser_post_recv(struct iser_conn *conn,
			  struct iser_task *task,
			  int num_recv_bufs)
{
	struct ibv_recv_wr *bad_wr;
	int err, nr_posted;

	err = ibv_post_recv(conn->qp_hndl, &task->rxd.recv_wr, &bad_wr);
	if (likely(!err)) {
		nr_posted = num_recv_bufs;
		dprintf("conn:%p posted:%d 1st task:%p "
			"wr_id:0x%lx sge_sz:%u\n",
			&conn->h, nr_posted, task,
			(unsigned long)task->rxd.recv_wr.wr_id,
			task->rxd.sge.length);
	} else {
		struct ibv_recv_wr *wr;

		nr_posted = 0;
		for (wr = &task->rxd.recv_wr; wr != bad_wr; wr = wr->next)
			nr_posted++;

		eprintf("%m, conn:%p posted:%d/%d 1st task:%p "
			"wr_id:0x%lx sge_sz:%u\n",
			&conn->h, nr_posted, num_recv_bufs, task,
			(unsigned long)task->rxd.recv_wr.wr_id,
			task->rxd.sge.length);
	}

	iser_conn_getn(conn, nr_posted);

	return err;
}

static inline void iser_set_rsp_stat_sn(struct iscsi_session *session,
					struct iscsi_hdr *rsp)
{
	if (session) {
		rsp->exp_statsn = cpu_to_be32(session->exp_cmd_sn);
		rsp->max_statsn = cpu_to_be32(session->exp_cmd_sn +
					      session->max_queue_cmd);
	}
}

static uint8_t* iser_alloc_pool(size_t pool_size, int *shmid)
{
	int shmemid;
	uint8_t *buf;

	/* allocate memory */
	shmemid = shmget(IPC_PRIVATE, pool_size,
			SHM_HUGETLB | IPC_CREAT | SHM_R | SHM_W);

	if (shmemid < 0) {
		eprintf("shmget rdma pool sz:%zu failed\n", pool_size);
		goto failed_huge_page;
	}

	/* get pointer to allocated memory */
	buf = shmat(shmemid, NULL, 0);

	if (buf == (void*)-1) {
		eprintf("Shared memory attach failure (errno=%d %m)", errno);
		shmctl(shmemid, IPC_RMID, NULL);
		goto failed_huge_page;
	}

	/* mark 'to be destroyed' when process detaches from shmem segment
	   this will clear the HugePage resources even if process if killed not nicely.
	   From checking shmctl man page it is unlikely that it will fail here. */
	if (shmctl(shmemid, IPC_RMID, NULL)) {
		eprintf("Shared memory contrl mark 'to be destroyed' failed (errno=%d %m)", errno);
	}

	dprintf("Allocated huge page sz:%zu\n", pool_size);
	*shmid = shmemid;
	return buf;

 failed_huge_page:
	*shmid = -1;
	return valloc(pool_size);
}

static void iser_free_pool(uint8_t *pool_buf, int shmid) {
	if (shmid >= 0) {
		if (shmdt(pool_buf) != 0) {
			eprintf("shmem detach failure (errno=%d %m)", errno);
		}
	} else {
		free(pool_buf);
	}
}

static int iser_init_rdma_buf_pool(struct iser_device *dev)
{
	uint8_t *pool_buf, *list_buf;
	size_t pool_size, list_size;
	struct iser_membuf *rdma_buf;
	int shmid;
	int i;

	/* The membuf size is rounded up at initialization time to the hardware
	   page size so that allocations for direct IO devices are aligned. */
	membuf_size = roundup(membuf_size, pagesize);
	pool_size = buf_pool_sz_mb * 1024 * 1024;
	membuf_num = pool_size / membuf_size;
	pool_size = membuf_num * membuf_size; /* reflect possible round-down */
	pool_buf = iser_alloc_pool(pool_size, &shmid);
	if (!pool_buf) {
		eprintf("malloc rdma pool sz:%zu failed\n", pool_size);
		return -ENOMEM;
	}

	list_size = membuf_num * sizeof(*rdma_buf);
	list_buf = malloc(list_size);
	if (!list_buf) {
		eprintf("malloc list_buf sz:%zu failed\n", list_size);
		iser_free_pool(pool_buf, shmid);
		return -ENOMEM;
	}

	/* One pool of registered memory per PD */
	dev->membuf_mr = ibv_reg_mr(dev->pd, pool_buf, pool_size,
				    IBV_ACCESS_LOCAL_WRITE);
	if (!dev->membuf_mr) {
		eprintf("ibv_reg_mr failed, %m\n");
		iser_free_pool(pool_buf, shmid);
		free(list_buf);
		return -1;
	}
	dprintf("pool buf:%p list:%p mr:%p lkey:0x%x\n",
		pool_buf, list_buf, dev->membuf_mr, dev->membuf_mr->lkey);

	dev->rdma_hugetbl_shmid = shmid;
	dev->membuf_regbuf = pool_buf;
	dev->membuf_listbuf = list_buf;
	INIT_LIST_HEAD(&dev->membuf_free);
	INIT_LIST_HEAD(&dev->membuf_alloc);

	for (i = 0; i < membuf_num; i++) {
		rdma_buf = (void *) list_buf;
		list_buf += sizeof(*rdma_buf);

		list_add_tail(&rdma_buf->pool_list, &dev->membuf_free);
		INIT_LIST_HEAD(&rdma_buf->task_list);
		rdma_buf->addr = pool_buf;
		rdma_buf->size = membuf_size;
		rdma_buf->rdma = 1;

		pool_buf += membuf_size;
	}

	return 0;
}

static void iser_destroy_rdma_buf_pool(struct iser_device *dev)
{
        int err;

	assert(list_empty(&dev->membuf_alloc));

	err = ibv_dereg_mr(dev->membuf_mr);
	if (err)
		eprintf("ibv_dereg_mr failed: (errno=%d %m)\n", errno);

	iser_free_pool(dev->membuf_regbuf, dev->rdma_hugetbl_shmid);
	free(dev->membuf_listbuf);

	dev->membuf_mr = NULL;
	dev->membuf_regbuf = NULL;
	dev->membuf_listbuf = NULL;
}

static struct iser_membuf *iser_dev_alloc_rdma_buf(struct iser_device *dev)
{
	struct iser_membuf *rdma_buf;

	if (unlikely(list_empty(&dev->membuf_free)))
		return NULL;

	rdma_buf = list_first_entry(&dev->membuf_free, struct iser_membuf,
				    pool_list);
	list_del(&rdma_buf->pool_list);
	list_add_tail(&rdma_buf->pool_list, &dev->membuf_alloc);

	dprintf("alloc:%p\n", rdma_buf);
	return rdma_buf;
}

static void iser_dev_free_rdma_buf(struct iser_device *dev, struct iser_membuf *rdma_buf)
{


	if (unlikely(!rdma_buf || !rdma_buf->rdma))
		return;

	dprintf("free %p\n", rdma_buf);

	/* add to the free list head to reuse recently used buffers first */
	list_del(&rdma_buf->pool_list);
	list_add(&rdma_buf->pool_list, &dev->membuf_free);
}

static int iser_task_alloc_rdma_bufs(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iser_membuf *rdma_wr_buf = NULL, *rdma_rd_buf = NULL;

	if (task->is_write && task->rdma_rd_sz > 0) {
		/* ToDo: multiple RDMA-Read buffers */
		if (unlikely(task->rdma_rd_sz > membuf_size)) {
			eprintf("conn:%p task:%p tag:0x%04"PRIx64 ", "
				"rdma-rd size:%u too big\n",
				&conn->h, task, task->tag, task->rdma_rd_sz);
			return -E2BIG;
		}
		rdma_rd_buf = iser_dev_alloc_rdma_buf(conn->dev);
		if (unlikely(rdma_rd_buf == NULL))
			goto no_mem_err;

		/* if this is a bidir task, allocation of the rdma_wr buffer
		   may still fail, thus	don't add the buffer to the task yet */
	}

	if (task->is_read && task->rdma_wr_sz > 0) {
		/* ToDo: multiple RDMA-Read buffers */
		if (unlikely(task->rdma_wr_sz > membuf_size)) {
			eprintf("conn:%p task:%p tag:0x%04"PRIx64 ", "
				"rdma-wr size:%u too big\n",
				&conn->h, task, task->tag, task->rdma_wr_sz);
			return -E2BIG;
		}
		rdma_wr_buf = iser_dev_alloc_rdma_buf(conn->dev);
		if (unlikely(rdma_wr_buf == NULL))
			goto no_mem_err;

		iser_task_add_in_rdma_buf(task, rdma_wr_buf, 0);
		dprintf("conn:%p task:%p tag:0x%04"PRIx64 ", "
			"rdma-wr buf:%p sz:%u\n",
			&conn->h, task, task->tag,
			rdma_wr_buf->addr, rdma_wr_buf->size);
	}

	/* With a write or bidir task there may be rdma portion and data-outs,
	   independently. If data-outs remain, wait until they all arrive */
	if (task->is_write) {
		if (rdma_rd_buf) {
			/* ToDo: multiple RDMA-Read buffers */
			iser_task_add_out_rdma_buf(task, rdma_rd_buf,
						   task->unsol_sz);
			dprintf("conn:%p task:%p tag:0x%04"PRIx64 ", "
				"rdma-rd buf:%p sz:%u\n",
				&conn->h, task, task->tag,
				rdma_rd_buf->addr, rdma_rd_buf->size);
			schedule_rdma_read(task, conn);
			return 0;
		}
		if (task->unsol_remains > 0)
			return 0;
	}

	schedule_task_iosubmit(task, conn);
	return 0;

no_mem_err:
	if (rdma_rd_buf)
		iser_dev_free_rdma_buf(conn->dev, rdma_rd_buf);
	conn->dev->waiting_for_mem = 1;

	eprintf("conn:%p task:%p tag:0x%04"PRIx64 ", free list empty\n",
		&conn->h, task, task->tag);
	return -ENOMEM;
}

static void iser_task_free_rdma_buf(struct iser_task *task, struct iser_membuf *rdma_buf)
{
	struct iser_conn *conn = task->conn;
	struct iser_device *dev = conn->dev;

	iser_dev_free_rdma_buf(dev, rdma_buf);
	if (unlikely(dev->waiting_for_mem)) {
		dev->waiting_for_mem = 0;
		tgt_add_sched_event(&conn->sched_buf_alloc);
	}
}

static void iser_task_free_out_bufs(struct iser_task *task)
{
	struct iser_membuf *membuf, *mbnext;

	list_for_each_entry_safe(membuf, mbnext, &task->out_buf_list, task_list) {
		iser_task_del_out_buf(task, membuf);
		if (membuf->rdma)
			iser_task_free_rdma_buf(task, membuf);
	}
	assert(task->out_buf_num == 0);
}

static void iser_task_free_in_bufs(struct iser_task *task)
{
	struct iser_membuf *membuf, *mbnext;

	list_for_each_entry_safe(membuf, mbnext, &task->in_buf_list, task_list) {
		iser_task_del_in_buf(task, membuf);
		iser_task_free_rdma_buf(task, membuf);
	}
	assert(task->in_buf_num == 0);
}

static void iser_complete_task(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;

	if (unlikely(task->opcode != ISCSI_OP_SCSI_CMD)) {
		dprintf("task:%p, non-cmd\n", task);
		return;
	}

	list_del(&task->session_list);
	if (task->is_read)
		iser_task_free_in_bufs(task);
	if (task->is_write) {
		iser_task_free_out_bufs(task);
		/* iser_task_free_dout_tasks(task); // ToDo: multiple out buffers */
	}

	/* we are completing scsi cmd task, returning from target */
	if (likely(task_in_scsi(task))) {
		target_cmd_done(&task->scmd);
		clear_task_in_scsi(task);
		iser_conn_put(conn);
	}
	if (task->extdata) {
		free(task->extdata);
		task->extdata = NULL;
	}
}

static char *iser_conn_login_phase_name(enum iser_login_phase phase)
{
	switch (phase) {
	case LOGIN_PHASE_INIT:
		return "INIT";
	case LOGIN_PHASE_START:
		return "START";
	case LOGIN_PHASE_LAST_SEND:
		return "LAST_SEND";
	case LOGIN_PHASE_FF:
		return "FF";
	default:
		return "Illegal";
	}
}

static void iser_conn_login_phase_set(struct iser_conn *conn,
				      enum iser_login_phase phase)
{
	dprintf("conn:%p from:%s to:%s\n", &conn->h,
		iser_conn_login_phase_name(conn->login_phase),
		iser_conn_login_phase_name(phase));
	conn->login_phase = phase;
}

/*
 * Called at accept time, builds resources just for login phase.
 */

#define NUM_LOGIN_TASKS 	2 /* one posted for req rx, one for reply tx */

static int iser_alloc_login_resources(struct iser_conn *conn)
{
	unsigned long buf_size = ALIGN_TO_32(conn->rsize);
	unsigned long pool_size = NUM_LOGIN_TASKS * buf_size;
	struct iser_task *login_task[NUM_LOGIN_TASKS];
	uint8_t *pdu_data_buf, *task_buf;
	unsigned int i;
	int err = 0;

	dprintf("conn:%p login tasks num:%u, buf_sz:%lu (rx_sz:%u tx_sz:%u)\n",
		&conn->h, NUM_LOGIN_TASKS, buf_size, conn->rsize, conn->ssize);

	conn->login_data_pool = malloc(pool_size);
	if (!conn->login_data_pool) {
		eprintf("conn:%p malloc login_data_pool sz:%lu failed\n",
			&conn->h, pool_size);
		err = -1;
		goto out;
	}

	conn->login_data_mr = ibv_reg_mr(conn->dev->pd, conn->login_data_pool,
					 pool_size, IBV_ACCESS_LOCAL_WRITE);
	if (!conn->login_data_mr) {
		eprintf("conn:%p ibv_reg_mr login pool failed, %m\n", &conn->h);
		free(conn->login_data_pool);
		conn->login_data_pool = NULL;
		err = -1;
		goto out;
	}

	pool_size = NUM_LOGIN_TASKS * sizeof(struct iser_task);
	conn->login_task_pool = malloc(pool_size);
	if (!conn->login_task_pool) {
		eprintf("conn:%p malloc login_task_pool sz:%lu failed\n",
			&conn->h, pool_size);
		ibv_dereg_mr(conn->login_data_mr);
		conn->login_data_mr = NULL;
		free(conn->login_data_pool);
		conn->login_data_pool = NULL;
		goto out;
	}
	memset(conn->login_task_pool, 0, pool_size);

	conn->login_res_alloc = 1;

	pdu_data_buf = conn->login_data_pool;
	task_buf = conn->login_task_pool;
	for (i = 0; i < NUM_LOGIN_TASKS; i++) {
		login_task[i] = (struct iser_task *) task_buf;

		iser_task_init(login_task[i], conn,
			       pdu_data_buf, buf_size,
			       conn->login_data_mr);

		task_buf += sizeof(struct iser_task);
		pdu_data_buf += buf_size;
	}

	dprintf("post_recv login rx task:%p\n", login_task[0]);
	err = iser_post_recv(conn, login_task[0], 1);
	if (err) {
		eprintf("conn:%p post_recv login rx-task failed\n", &conn->h);
		iser_free_login_resources(conn);
		goto out;
	}
	dprintf("saved login tx-task:%p\n", login_task[1]);
	conn->login_tx_task = login_task[1];
out:
	return err;
}

/*
 * When ready for full-feature mode, free login-phase resources.
 */
static void iser_free_login_resources(struct iser_conn *conn)
{
	int err;

	if (!conn->login_res_alloc)
		return;

	dprintf("conn:%p, login phase:%s\n", &conn->h,
		iser_conn_login_phase_name(conn->login_phase));

	/* release mr and free the lists */
	if (conn->login_data_mr) {
		err = ibv_dereg_mr(conn->login_data_mr);
		if (err)
			eprintf("conn:%p ibv_dereg_mr failed, %m\n", &conn->h);
	}
	if (conn->login_data_pool)
		free(conn->login_data_pool);
	if (conn->login_task_pool)
		free(conn->login_task_pool);
	conn->login_tx_task = NULL;

	conn->login_res_alloc = 0;
}

/*
 * Ready for full feature, allocate resources.
 */
static int iser_alloc_ff_resources(struct iser_conn *conn)
{
	/* ToDo: need to fix the ISCSI_PARAM_INITIATOR_RDSL bug in initiator */
	/* buf_size = ALIGN_TO_32(MAX(conn->rsize, conn->ssize)); */
	unsigned long buf_size = ALIGN_TO_32(conn->rsize); /* includes headers */
	unsigned long num_tasks = conn->max_outst_pdu + 2; /* all rx, text tx, nop-in*/
	unsigned long alloc_sz;
	uint8_t *pdu_data_buf, *task_buf;
	struct iser_task *task;
	unsigned int i;
	int err = 0;

	dprintf("conn:%p max_outst:%u buf_sz:%lu (ssize:%u rsize:%u)\n",
		&conn->h, conn->max_outst_pdu, buf_size,
		conn->ssize, conn->rsize);

	alloc_sz = num_tasks * buf_size;
	conn->pdu_data_pool = malloc(alloc_sz);
	if (!conn->pdu_data_pool) {
		eprintf("conn:%p malloc pdu_data_buf sz:%lu failed\n",
			&conn->h, alloc_sz);
		err = -1;
		goto out;
	}

	conn->pdu_data_mr = ibv_reg_mr(conn->dev->pd, conn->pdu_data_pool,
				       alloc_sz, IBV_ACCESS_LOCAL_WRITE);
	if (!conn->pdu_data_mr) {
		eprintf("conn:%p ibv_reg_mr pdu_data_pool failed, %m\n",
			&conn->h);
		free(conn->pdu_data_pool);
		conn->pdu_data_pool = NULL;
		err = -1;
		goto out;
	}

	alloc_sz = num_tasks * sizeof(struct iser_task);
	conn->task_pool = malloc(alloc_sz);
	if (!conn->task_pool) {
		eprintf("conn:%p malloc task_pool sz:%lu failed\n",
			&conn->h, alloc_sz);
		ibv_dereg_mr(conn->pdu_data_mr);
		conn->pdu_data_mr = NULL;
		free(conn->pdu_data_pool);
		conn->pdu_data_pool = NULL;
		err = -1;
		goto out;
	}
	memset(conn->task_pool, 0, alloc_sz);

	conn->ff_res_alloc = 1;

	pdu_data_buf = conn->pdu_data_pool;
	task_buf = conn->task_pool;
	for (i = 0; i < conn->max_outst_pdu; i++) {
		task = (void *) task_buf;

		iser_task_init(task, conn,
			       pdu_data_buf, buf_size,
			       conn->pdu_data_mr);

		task_buf += sizeof(*task);
		/* ToDo: need to fix the ISCSI_PARAM_INITIATOR_RDSL bug in initiator */
		/* pdu_data_buf += conn->rsize + conn->ssize; */
		pdu_data_buf += buf_size;

		err = iser_post_recv(conn, task, 1);
		if (err) {
			eprintf("conn:%p post_recv (%d/%d) failed\n",
				&conn->h, i, conn->max_outst_pdu);
			iser_free_ff_resources(conn);
			err = -1;
			goto out;
		}
	}

	/* initialize unsolicited tx task: nop-in */
	task = (void *) task_buf;
	iser_unsolicited_task_init(task, conn, pdu_data_buf,
				   buf_size, conn->pdu_data_mr);
	conn->nop_in_task = task;
	task_buf += sizeof(*task);
	pdu_data_buf += buf_size;

	/* initialize tx task: text/login */
	task = (void *) task_buf;
	iser_unsolicited_task_init(task, conn, pdu_data_buf,
				   buf_size, conn->pdu_data_mr);
	conn->text_tx_task = task;

out:
	return err;
}

/*
 * On connection shutdown.
 */
static void iser_free_ff_resources(struct iser_conn *conn)
{
	int err;

	if (!conn->ff_res_alloc)
		return;

	dprintf("conn:%p pdu_mr:%p pdu_pool:%p task_pool:%p\n",
		&conn->h, conn->pdu_data_mr,
		conn->pdu_data_pool, conn->task_pool);

	/* release mr and free the lists */
	if (conn->pdu_data_mr) {
		err = ibv_dereg_mr(conn->pdu_data_mr);
		if (err)
			eprintf("conn:%p ibv_dereg_mr failed, %m\n", &conn->h);
	}
	if (conn->pdu_data_pool)
		free(conn->pdu_data_pool);
	if (conn->task_pool)
		free(conn->task_pool);
	conn->nop_in_task = NULL;
	conn->text_tx_task = NULL;

	conn->ff_res_alloc = 0;
}

/*
 * Allocate resources for this new connection.  Called after login, when
 * final negotiated transfer parameters are known.
 */
int iser_login_complete(struct iscsi_connection *iscsi_conn)
{
	struct iser_conn *conn = ISER_CONN(iscsi_conn);
	unsigned int trdsl;
	/* unsigned int irdsl; */
	unsigned int outst_pdu, hdrsz;
	uint32_t max_q_cmd;
	int err = -1;

	dprintf("entry\n");

	/* one more send, then done; login resources are left until then */
	iser_conn_login_phase_set(conn, LOGIN_PHASE_LAST_SEND);
	/* irdsl = iscsi_conn->session_param[ISCSI_PARAM_INITIATOR_RDSL].val; */
	trdsl = iscsi_conn->session_param[ISCSI_PARAM_TARGET_RDSL].val;

	/* ToDo: outstanding pdus num */
	/* outst_pdu =
		iscsi_conn->session_param[ISCSI_PARAM_MAX_OUTST_PDU].val; */

	/* hack, ib/ulp/iser does not have this param, but reading the code
	 * shows their formula for max tx dtos outstanding
	 *    = cmds_max * (1 + dataouts) + rx_misc + tx_misc
	 */
#define ISER_INFLIGHT_DATAOUTS  0
#define ISER_MAX_RX_MISC_PDUS   4
#define ISER_MAX_TX_MISC_PDUS   6

	max_q_cmd = iscsi_conn->session_param[ISCSI_PARAM_MAX_QUEUE_CMD].val;
	/* if (outst_pdu == 0) // ToDo: outstanding pdus num */
	outst_pdu =
		3 * max_q_cmd * (1 + ISER_INFLIGHT_DATAOUTS) +
		ISER_MAX_RX_MISC_PDUS + ISER_MAX_TX_MISC_PDUS;

	/* RDSLs do not include headers. */
	hdrsz = sizeof(struct iser_hdr) +
		sizeof(struct iscsi_hdr) +
		sizeof(struct iscsi_ecdb_ahdr) +
		sizeof(struct iscsi_rlength_ahdr);

	if (trdsl < 1024)
		trdsl = 1024;
	conn->rsize = hdrsz + trdsl;
	/* ToDo: need to fix the ISCSI_PARAM_INITIATOR_RDSL bug in initiator */
	/* conn->ssize = hdrsz + irdsl; */
	conn->ssize = conn->rsize;

	conn->max_outst_pdu = outst_pdu;
	err = iser_alloc_ff_resources(conn);
	if (err)
		goto out;

	/* How much data to grab in an RDMA operation, read or write */
	/* ToDo: fix iscsi login code, broken handling of MAX_XMIT_DL */
	iscsi_conn->session_param[ISCSI_PARAM_MAX_XMIT_DLENGTH].val =
		RDMA_TRANSFER_SIZE;
out:
	return err;
}

static int iser_ib_clear_iosubmit_list(struct iser_conn *conn)
{
	struct iser_task *task;

	dprintf("start\n");
	while (!list_empty(&conn->iosubmit_list)) {
		task = list_first_entry(&conn->iosubmit_list,
					struct iser_task, exec_list);
		list_del(&task->exec_list);
		iser_complete_task(task); /* must free, task keeps rdma buffer */
	}
	return 0;
}

static int iser_ib_clear_rdma_rd_list(struct iser_conn *conn)
{
	struct iser_task *task;

	dprintf("start\n");
	while (!list_empty(&conn->rdma_rd_list)) {
		task = list_first_entry(&conn->rdma_rd_list,
					struct iser_task, rdma_list);
		list_del(&task->rdma_list);
		iser_complete_task(task);  /* must free, task keeps rdma buffer */
	}
	return 0;
}

static int iser_ib_clear_tx_list(struct iser_conn *conn)
{
	struct iser_task *task;

	dprintf("start\n");
	while (!list_empty(&conn->resp_tx_list)) {
		task = list_first_entry(&conn->resp_tx_list,
					struct iser_task, tx_list);
		list_del(&task->tx_list);
		iser_complete_task(task);  /* must free, task keeps rdma buffer */
	}
	return 0;
}

static int iser_ib_clear_sent_list(struct iser_conn *conn)
{
	struct iser_task *task;

	dprintf("start\n");
	while (!list_empty(&conn->sent_list)) {
		task = list_first_entry(&conn->sent_list,
					struct iser_task, tx_list);
		list_del(&task->tx_list); /* don't free, future completion guaranteed */
	}
	return 0;
}

static int iser_ib_clear_post_recv_list(struct iser_conn *conn)
{
	struct iser_task *task;

	dprintf("start\n");
	while (!list_empty(&conn->post_recv_list)) {
		task = list_first_entry(&conn->post_recv_list,
					struct iser_task, recv_list);
		list_del(&task->recv_list); /* don't free, future completion guaranteed */
	}
	return 0;
}

int iser_conn_init(struct iser_conn *conn)
{
	conn->h.refcount = 0;
	conn->h.state = STATE_INIT;
	param_set_defaults(conn->h.session_param, session_keys);

	INIT_LIST_HEAD(&conn->h.clist);

	INIT_LIST_HEAD(&conn->buf_alloc_list);
	INIT_LIST_HEAD(&conn->rdma_rd_list);
	/* INIT_LIST_HEAD(&conn->rdma_wr_list); */
	INIT_LIST_HEAD(&conn->iosubmit_list);
	INIT_LIST_HEAD(&conn->resp_tx_list);
	INIT_LIST_HEAD(&conn->sent_list);
	INIT_LIST_HEAD(&conn->post_recv_list);

	return 0;
}

/*
 * Start closing connection. Transfer IB QP to error state.
 * This will be followed by WC error and buffers flush events.
 * We also should expect DISCONNECTED and TIMEWAIT_EXIT events.
 * Only after the draining is over we are sure to have reclaimed
 * all buffers (and tasks). After the RDMA CM events are collected,
 * the connection QP may be destroyed, and its number may be recycled.
 */
void iser_conn_close(struct iser_conn *conn)
{
	int err;

	if (conn->h.state == STATE_CLOSE)
		return;

	dprintf("rdma_disconnect conn:%p\n", &conn->h);
	err = rdma_disconnect(conn->cm_id);
	if (err)
		eprintf("conn:%p rdma_disconnect failed, %m\n", &conn->h);

	list_del(&conn->conn_list);

	tgt_remove_sched_event(&conn->sched_buf_alloc);
	tgt_remove_sched_event(&conn->sched_rdma_rd);
	tgt_remove_sched_event(&conn->sched_iosubmit);
	tgt_remove_sched_event(&conn->sched_tx);
	tgt_remove_sched_event(&conn->sched_post_recv);

	conn->h.state = STATE_CLOSE;
	eprintf("conn:%p cm_id:0x%p state: CLOSE, refcnt:%d\n",
		&conn->h, conn->cm_id, conn->h.refcount);
}

static void iser_conn_force_close(struct iscsi_connection *iscsi_conn)
{
	struct iser_conn *conn = ISER_CONN(iscsi_conn);

	eprintf("conn:%p\n", &conn->h);
	conn->h.closed = 1;
	iser_conn_close(conn);
	iser_conn_put(conn);
}

/*
 * Called when the connection is freed, from iscsi, but won't do anything until
 * all posted WRs have gone away.  So also called again from RX progress when
 * it notices this happens.
 */
void iser_conn_free(struct iser_conn *conn)
{
	int err;

	dprintf("conn:%p refcnt:%d qp:%p cm_id:%p\n",
		&conn->h, conn->h.refcount, conn->qp_hndl, conn->cm_id);

	assert(conn->h.refcount == 0);

	iser_ib_clear_iosubmit_list(conn);
	iser_ib_clear_rdma_rd_list(conn);
	iser_ib_clear_tx_list(conn);
	iser_ib_clear_sent_list(conn);
	iser_ib_clear_post_recv_list(conn);

	/* try to free unconditionally, resources freed only if necessary */
	iser_free_login_resources(conn);
	iser_free_ff_resources(conn);

	if (conn->qp_hndl) {
		err = ibv_destroy_qp(conn->qp_hndl);
		if (err)
			eprintf("conn:%p ibv_destroy_qp failed, %m\n", &conn->h);
	}
	if (conn->cm_id) {
		err = rdma_destroy_id(conn->cm_id);
		if (err)
			eprintf("conn:%p rdma_destroy_id failed, %m\n", &conn->h);
	}

	/* delete from session; was put there by conn_add_to_session() */
	list_del(&conn->h.clist);

	if (conn->h.initiator)
		free(conn->h.initiator);

	if (conn->h.session)
		session_put(conn->h.session);

	if (conn->peer_name)
		free(conn->peer_name);
	if (conn->self_name)
		free(conn->self_name);

	conn->h.state = STATE_INIT;
	free(conn);
	dprintf("conn:%p freed\n", &conn->h);
}

static void iser_sched_conn_free(struct event_data *evt)
{
	struct iser_conn *conn = (struct iser_conn *) evt->data;

	iser_conn_free(conn);
}

int iser_conn_get(struct iser_conn *conn)
{
	/* TODO: check state */
	conn->h.refcount++;
	dprintf("refcnt:%d\n", conn->h.refcount);
	return 0;
}

int iser_conn_getn(struct iser_conn *conn, int n)
{
	int new_count = conn->h.refcount + n;
	dprintf("refcnt:%d + %d = %d\n", conn->h.refcount, n, new_count);
	conn->h.refcount = new_count;
	return 0;
}

void iser_conn_put(struct iser_conn *conn)
{
	conn->h.refcount--;
	dprintf("refcnt:%d\n", conn->h.refcount);
	if (unlikely(conn->h.refcount == 0)) {
		assert(conn->h.state == STATE_CLOSE);
		tgt_add_sched_event(&conn->sched_conn_free);
	}
}

static int iser_get_host_name(struct sockaddr_storage *addr, char **name)
{
	int err;
	char host[NI_MAXHOST];

	if (name == NULL)
		return -EINVAL;

	err = getnameinfo((struct sockaddr *) addr, sizeof(*addr),
			  host, sizeof(host), NULL, 0, NI_NUMERICHOST);
	if (!err) {
		*name = strdup(host);
		if (*name == NULL)
			err = -ENOMEM;
	} else
		eprintf("getnameinfo failed, %m\n");

	return err;
}

static int iser_show(struct iscsi_connection *iscsi_conn, char *buf,
		     int rest)
{
	struct iser_conn *conn = ISER_CONN(iscsi_conn);
	int len;

	len = snprintf(buf, rest, "RDMA IP Address: %s", conn->peer_name);
	return len;
}

static int iser_getsockname(struct iscsi_connection *iscsi_conn,
			    struct sockaddr *sa, socklen_t *len)
{
	struct iser_conn *conn = ISER_CONN(iscsi_conn);

	if (*len > sizeof(conn->self_addr))
		*len = sizeof(conn->self_addr);
	memcpy(sa, &conn->self_addr, *len);
	return 0;
}

static int iser_getpeername(struct iscsi_connection *iscsi_conn,
			    struct sockaddr *sa, socklen_t *len)
{
	struct iser_conn *conn = ISER_CONN(iscsi_conn);

	if (*len > sizeof(conn->peer_addr))
		*len = sizeof(conn->peer_addr);
	memcpy(sa, &conn->peer_addr, *len);
	return 0;
}

static void iser_cm_connect_request(struct rdma_cm_event *ev)
{
	struct rdma_cm_id *cm_id = ev->id;
	struct ibv_qp_init_attr qp_init_attr;
	struct iser_conn *conn;
	struct iser_device *dev;
	int err, dev_found;

	struct rdma_conn_param conn_param = {
		.responder_resources = 1,
		.initiator_depth = 1,
		.retry_count = 5,
	};

	/* find device */
	dev_found = 0;
	list_for_each_entry(dev, &iser_dev_list, list) {
		if (dev->ibv_ctxt == cm_id->verbs) {
			dev_found = 1;
			break;
		}
	}
	if (!dev_found) {
		dev = malloc(sizeof(*dev));
		if (dev == NULL) {
			eprintf("cm_id:%p malloc dev failed\n", cm_id);
			goto reject;
		}
		dev->ibv_ctxt = cm_id->verbs;
		err = iser_device_init(dev);
		if (err) {
			free(dev);
			goto reject;
		}
	}

	/* build a new connection structure */
	conn = zalloc(sizeof(*conn));
	if (!conn) {
		eprintf("cm_id:%p malloc conn failed\n", cm_id);
		goto reject;
	}

	err = iser_conn_init(conn);
	if (err) {
		free(conn);
		goto reject;
	}
	dprintf("alloc conn:%p cm_id:%p\n", &conn->h, cm_id);
	list_add(&conn->conn_list, &iser_conn_list);

	/* relate iser and rdma connections */
	conn->cm_id = cm_id;
	cm_id->context = conn;

	conn->dev = dev;

	iser_conn_login_phase_set(conn, LOGIN_PHASE_START);

	tgt_init_sched_event(&conn->sched_buf_alloc, iser_sched_buf_alloc,
			     conn);
	tgt_init_sched_event(&conn->sched_iosubmit, iser_sched_iosubmit,
			     conn);
	tgt_init_sched_event(&conn->sched_rdma_rd, iser_sched_rdma_rd,
			     conn);
	tgt_init_sched_event(&conn->sched_tx, iser_sched_tx,
			     conn);
	tgt_init_sched_event(&conn->sched_post_recv, iser_sched_post_recv,
			     conn);
	tgt_init_sched_event(&conn->sched_conn_free, iser_sched_conn_free,
			     conn);

	/* initiator is dst, target is src */
	memcpy(&conn->peer_addr, &cm_id->route.addr.dst_addr,
	       sizeof(conn->peer_addr));
	err = iser_get_host_name(&conn->peer_addr, &conn->peer_name);
	if (err)
		conn->peer_name = strdup("Unresolved");

	memcpy(&conn->self_addr, &cm_id->route.addr.src_addr,
	       sizeof(conn->self_addr));
	err = iser_get_host_name(&conn->self_addr, &conn->self_name);
	if (err)
		conn->self_name = strdup("Unresolved");

	/* create qp */
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.qp_context = conn;
	/* both send and recv to the same CQ */
	qp_init_attr.send_cq = dev->cq;
	qp_init_attr.recv_cq = dev->cq;
	qp_init_attr.cap.max_send_wr = MAX_WQE;
	qp_init_attr.cap.max_recv_wr = MAX_WQE;
	qp_init_attr.cap.max_send_sge = 1;      /* scatter/gather entries */
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.qp_type = IBV_QPT_RC;
	/* only generate completion queue entries if requested */
	qp_init_attr.sq_sig_all = 0;

	err = rdma_create_qp(cm_id, dev->pd, &qp_init_attr);
	if (err) {
		eprintf("conn:%p cm_id:%p rdma_create_qp failed, %m\n",
			&conn->h, cm_id);
		goto free_conn;
	}
	conn->qp_hndl = cm_id->qp;
	VALGRIND_MAKE_MEM_DEFINED(conn->qp_hndl, sizeof(*conn->qp_hndl));
	dprintf("conn:%p cm_id:%p, created qp:%p\n", &conn->h, cm_id,
		conn->qp_hndl);

	/*
	 * Post buffers for the login phase, only.
	 */
	conn->rsize =
		sizeof(struct iser_hdr) +
		sizeof(struct iscsi_hdr) +
		sizeof(struct iscsi_ecdb_ahdr) +
		sizeof(struct iscsi_rlength_ahdr) + 8192;
	conn->ssize = conn->rsize;
	err = iser_alloc_login_resources(conn);
	if (err)
		goto free_conn;

	conn_param.initiator_depth = dev->device_attr.max_qp_init_rd_atom;
	if (conn_param.initiator_depth > ev->param.conn.initiator_depth)
		conn_param.initiator_depth = ev->param.conn.initiator_depth;

	/* Increment reference count to be able to wait for TIMEWAIT_EXIT
	   when finalizing the disconnect process */
	iser_conn_get(conn);

	/* now we can actually accept the connection */
	err = rdma_accept(conn->cm_id, &conn_param);
	if (err) {
		eprintf("conn:%p cm_id:%p rdma_accept failed, %m\n",
			&conn->h, cm_id);
		goto free_conn;
	}

	conn->h.tp = &iscsi_iser;
	conn->h.state = STATE_START;
	dprintf("conn:%p cm_id:%p, %s -> %s, accepted\n",
		&conn->h, cm_id, conn->peer_name, conn->self_name);
	return;

free_conn:
	iser_conn_free(conn);

reject:
	err = rdma_reject(cm_id, NULL, 0);
	if (err)
		eprintf("cm_id:%p rdma_reject failed, %m\n", cm_id);
}

/*
 * Finish putting the connection together, now that the other side
 * has ACKed our acceptance.  Moves it from the temp_conn to the
 * iser_conn_list.
 *
 * Release the temporary conn_info and glue it into iser_conn_list.
 */
static void iser_cm_conn_established(struct rdma_cm_event *ev)
{
	struct rdma_cm_id *cm_id = ev->id;
	struct iser_conn *conn = cm_id->context;

	if (conn->h.state == STATE_START) {
		conn->h.state = STATE_READY;
		eprintf("conn:%p cm_id:%p, %s -> %s, established\n",
			&conn->h, cm_id, conn->peer_name, conn->self_name);
	} else if (conn->h.state == STATE_READY) {
		eprintf("conn:%p cm_id:%p, %s -> %s, "
			"execute delayed login_rx now\n",
			&conn->h, cm_id, conn->peer_name, conn->self_name);
		iser_login_rx(conn->login_rx_task);
	}
}

/*
 * Handle RDMA_CM_EVENT_DISCONNECTED or an equivalent event.
 * Start closing the target's side connection.
*/
static void iser_cm_disconnected(struct rdma_cm_event *ev)
{
	struct rdma_cm_id *cm_id = ev->id;
	struct iser_conn *conn = cm_id->context;
	enum rdma_cm_event_type ev_type = ev->event;

	eprintf("conn:%p cm_id:%p event:%d, %s\n", &conn->h, cm_id,
		ev_type, rdma_event_str(ev_type));
	iser_conn_close(conn);
}

/*
 * Handle RDMA_CM_EVENT_TIMEWAIT_EXIT which is expected to be the last
 * event during the lifecycle of a connection, when it had been shut down
 * and the network has cleared from the remaining in-flight messages.
*/
static void iser_cm_timewait_exit(struct rdma_cm_event *ev)
{
	struct rdma_cm_id *cm_id = ev->id;
	struct iser_conn *conn = cm_id->context;

	eprintf("conn:%p cm_id:%p\n", &conn->h, cm_id);

	/* Refcount was incremented just before accepting the connection,
	   typically this is the last decrement and the connection will be
	   released instantly */
	iser_conn_put(conn);
}

/*
 * Handle RDMA CM events.
 */
static void iser_handle_rdmacm(int fd __attribute__ ((unused)),
			       int events __attribute__ ((unused)),
			       void *data __attribute__ ((unused)))
{
	struct rdma_cm_event *ev;
	enum rdma_cm_event_type ev_type;
	int err;

	err = rdma_get_cm_event(rdma_evt_channel, &ev);
	if (err) {
		eprintf("rdma_get_cm_event failed, %m\n");
		return;
	}

	VALGRIND_MAKE_MEM_DEFINED(ev, sizeof(*ev));

	ev_type = ev->event;
	switch (ev_type) {
	case RDMA_CM_EVENT_CONNECT_REQUEST:
		iser_cm_connect_request(ev);
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
		iser_cm_conn_established(ev);
		break;

	case RDMA_CM_EVENT_CONNECT_ERROR:
	case RDMA_CM_EVENT_REJECTED:
	case RDMA_CM_EVENT_ADDR_CHANGE:
	case RDMA_CM_EVENT_DISCONNECTED:
		iser_cm_disconnected(ev);
		break;

	case RDMA_CM_EVENT_TIMEWAIT_EXIT:
		iser_cm_timewait_exit(ev);
		break;

	case RDMA_CM_EVENT_MULTICAST_JOIN:
	case RDMA_CM_EVENT_MULTICAST_ERROR:
		eprintf("UD-related event:%d, %s - ignored\n", ev_type,
			rdma_event_str(ev_type));
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		eprintf("Unsupported event:%d, %s - ignored\n", ev_type,
			rdma_event_str(ev_type));
		break;

	case RDMA_CM_EVENT_ADDR_RESOLVED:
	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_ROUTE_RESOLVED:
	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_RESPONSE:
	case RDMA_CM_EVENT_UNREACHABLE:
		eprintf("Active side event:%d, %s - ignored\n", ev_type,
			rdma_event_str(ev_type));
		break;

	default:
		eprintf("Illegal event:%d - ignored\n", ev_type);
		break;
	}

	err = rdma_ack_cm_event(ev);
	if (err)
		eprintf("ack cm event failed, %s\n", rdma_event_str(ev_type));
}

static int iser_logout_exec(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iscsi_logout_rsp *rsp_bhs =
		(struct iscsi_logout_rsp *) task->pdu.bhs;

	memset(rsp_bhs, 0, BHS_SIZE);
	rsp_bhs->opcode = ISCSI_OP_LOGOUT_RSP;
	rsp_bhs->flags = ISCSI_FLAG_CMD_FINAL;
	rsp_bhs->response = ISCSI_LOGOUT_SUCCESS;
	rsp_bhs->itt = task->tag;
	rsp_bhs->statsn = cpu_to_be32(conn->h.stat_sn++);

	if (session->exp_cmd_sn == task->cmd_sn && !task->is_immediate)
		session->exp_cmd_sn++;
	iser_set_rsp_stat_sn(session, task->pdu.bhs);

	task->pdu.ahssize = 0;
	task->pdu.membuf.size = 0;

	dprintf("rsp task:%p op:%0x itt:%0x statsn:%0x\n",
		task, (unsigned int)rsp_bhs->opcode,
		(unsigned int)rsp_bhs->itt,
		(unsigned int)rsp_bhs->statsn);

	/* add the logout resp to tx list, and force all scheduled tx now */
	list_add_tail(&task->tx_list, &conn->resp_tx_list);
	dprintf("add to tx list, logout resp task:%p tag:0x%04"PRIx64 " cmdsn:0x%x\n",
		task, task->tag, task->cmd_sn);
	tgt_remove_sched_event(&conn->sched_tx);
	iser_sched_tx(&conn->sched_tx);

	return 0;
}

static int iser_nop_out_exec(struct iser_task *task)
{
	struct iscsi_nopin *rsp_bhs = (struct iscsi_nopin *) task->pdu.bhs;
	struct iser_conn *conn = task->conn;

	rsp_bhs->opcode = ISCSI_OP_NOOP_IN;
	rsp_bhs->flags = ISCSI_FLAG_CMD_FINAL;
	rsp_bhs->rsvd2 = 0;
	rsp_bhs->rsvd3 = 0;
	memset(rsp_bhs->lun, 0, sizeof(rsp_bhs->lun));
	rsp_bhs->itt = task->tag;
	rsp_bhs->ttt = ISCSI_RESERVED_TAG;
	rsp_bhs->statsn = cpu_to_be32(conn->h.stat_sn);
	if (task->tag != ISCSI_RESERVED_TAG)
		conn->h.stat_sn++;

	iser_set_rsp_stat_sn(conn->h.session, task->pdu.bhs);

	memset(rsp_bhs->rsvd4, 0, sizeof(rsp_bhs->rsvd4));
	task->pdu.ahssize = 0;
	task->pdu.membuf.size = task->out_len; /* ping back nop-out data */

	schedule_resp_tx(task, conn);
	return 0;
}

static int iser_send_ping_nop_in(struct iser_task *task)
{
	struct iscsi_nopin *rsp_bhs = (struct iscsi_nopin *) task->pdu.bhs;
	struct iser_conn *conn = task->conn;

	task->opcode = ISCSI_OP_NOOP_IN;
	task->tag = ISCSI_RESERVED_TAG;

	rsp_bhs->opcode = ISCSI_OP_NOOP_IN;
	rsp_bhs->flags = ISCSI_FLAG_CMD_FINAL;
	rsp_bhs->rsvd2 = 0;
	rsp_bhs->rsvd3 = 0;
	memset(rsp_bhs->lun, 0, sizeof(rsp_bhs->lun));
	rsp_bhs->itt = ISCSI_RESERVED_TAG;
	rsp_bhs->ttt = ISCSI_RESERVED_TAG;
	rsp_bhs->statsn = cpu_to_be32(conn->h.stat_sn);
	iser_set_rsp_stat_sn(conn->h.session, task->pdu.bhs);

	memset(rsp_bhs->rsvd4, 0, sizeof(rsp_bhs->rsvd4));
	task->pdu.ahssize = 0;
	task->pdu.membuf.size = 0;

	dprintf("task:%p conn:%p\n", task, &conn->h);

	schedule_resp_tx(task, conn);
	return 0;
}

/*
static int iser_send_reject(struct iser_task *task, uint8 reason)
{
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iscsi_hdr *req = (struct iscsi_hdr *) task->req.bhs;
	struct iscsi_reject *rsp = (struct iscsi_reject *) task->rsp.bhs;

	memset(rsp, 0, BHS_SIZE);
	rsp->opcode = ISCSI_OP_REJECT;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	rsp->reason = reason;
	hton24(rsp->dlength, sizeof(struct iser_hdr));
	rsp->ffffffff = 0xffffffff;
	rsp->itt = req->itt;
	rsp->statsn = cpu_to_be32(conn->h.stat_sn++);
	iser_set_rsp_stat_sn(session, task->rsp.bhs);

	task->rsp.ahssize = 0;
	task->rsp.membuf.addr = task->req.bhs;
	task->rsp.membuf.size = sizeof(struct iser_hdr);

	schedule_resp_tx(task, conn);
	return 0;
}
*/

static int iser_tm_exec(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iscsi_tm *req_bhs = (struct iscsi_tm *) task->pdu.bhs;
	int fn = 0;
	int err = 0;

	eprintf("conn:%p TM itt:0x%x cmdsn:0x%x "
		"ref.itt:0x%x ref.cmdsn:0x%x "
		"exp.cmdsn:0x%x "
		"lun:0x%02x%02x%02x%02x%02x%02x%02x%02x\n",
		&conn->h,
		be32_to_cpu(req_bhs->itt), be32_to_cpu(req_bhs->cmdsn),
		be32_to_cpu(req_bhs->rtt), be32_to_cpu(req_bhs->refcmdsn),
		be32_to_cpu(req_bhs->exp_statsn),
		req_bhs->lun[0], req_bhs->lun[1], req_bhs->lun[2], req_bhs->lun[3],
		req_bhs->lun[4], req_bhs->lun[5], req_bhs->lun[6], req_bhs->lun[7]);

	switch (req_bhs->flags & ISCSI_FLAG_TM_FUNC_MASK) {
	case ISCSI_TM_FUNC_ABORT_TASK:
		fn = ABORT_TASK;
		break;
	case ISCSI_TM_FUNC_ABORT_TASK_SET:
		fn = ABORT_TASK_SET;
		break;
	case ISCSI_TM_FUNC_CLEAR_ACA:
		fn = CLEAR_TASK_SET;
		break;
	case ISCSI_TM_FUNC_CLEAR_TASK_SET:
		fn = CLEAR_ACA;
		break;
	case ISCSI_TM_FUNC_LOGICAL_UNIT_RESET:
		fn = LOGICAL_UNIT_RESET;
		break;
	case ISCSI_TM_FUNC_TARGET_WARM_RESET:
	case ISCSI_TM_FUNC_TARGET_COLD_RESET:
	case ISCSI_TM_FUNC_TASK_REASSIGN:
		err = ISCSI_TMF_RSP_NOT_SUPPORTED;
		eprintf("unsupported TMF %d\n",
			req_bhs->flags & ISCSI_FLAG_TM_FUNC_MASK);
		break;
	default:
		err = ISCSI_TMF_RSP_REJECTED;
		eprintf("unknown TMF %d\n",
			req_bhs->flags & ISCSI_FLAG_TM_FUNC_MASK);
	}

	if (err)
		task->result = err;
	else {
		int ret;
		ret = target_mgmt_request(session->target->tid, session->tsih,
					  (unsigned long) task, fn, req_bhs->lun,
					  req_bhs->rtt, 0);
		set_task_in_scsi(task);
		iser_conn_get(conn);

		switch (ret) {
		case MGMT_REQ_QUEUED:
			break;
		case MGMT_REQ_FAILED:
		case MGMT_REQ_DONE:
			clear_task_in_scsi(task);
			iser_conn_put(conn);
			break;
		}
	}
	return err;
}

static int iser_tm_done(struct mgmt_req *mreq)
{
	struct iser_task *task = (struct iser_task *) (unsigned long) mreq->mid;
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iscsi_tm_rsp *rsp_bhs =
		(struct iscsi_tm_rsp *) task->pdu.bhs;

	memset(rsp_bhs, 0, sizeof(*rsp_bhs));
	rsp_bhs->opcode = ISCSI_OP_SCSI_TMFUNC_RSP;
	rsp_bhs->flags = ISCSI_FLAG_CMD_FINAL;
	rsp_bhs->itt = task->tag;
	switch (mreq->result) {
	case 0:
		rsp_bhs->response = ISCSI_TMF_RSP_COMPLETE;
		break;
	case -EINVAL:
		rsp_bhs->response = ISCSI_TMF_RSP_NOT_SUPPORTED;
		break;
	case -EEXIST:
		/*
		 * the command completed or we could not find it so
		 * we retrun  no task here
		 */
		rsp_bhs->response = ISCSI_TMF_RSP_NO_TASK;
		break;
	default:
		rsp_bhs->response = ISCSI_TMF_RSP_REJECTED;
		break;
	}

	rsp_bhs->statsn = cpu_to_be32(conn->h.stat_sn++);
	iser_set_rsp_stat_sn(session, task->pdu.bhs);

	/* Must be zero according to 10.6.3 */
	task->pdu.ahssize = 0;
	task->pdu.membuf.size = 0;

	schedule_resp_tx(task, conn);

	return 0;
}

static int scsi_cmd_attr(unsigned int flags)
{
	int attr;

	switch (flags & ISCSI_FLAG_CMD_ATTR_MASK) {
	case ISCSI_ATTR_UNTAGGED:
	case ISCSI_ATTR_SIMPLE:
		attr = MSG_SIMPLE_TAG;
		break;
	case ISCSI_ATTR_HEAD_OF_QUEUE:
		attr = MSG_HEAD_TAG;
		break;
	case ISCSI_ATTR_ORDERED:
	default:
		attr = MSG_ORDERED_TAG;
	}
	return attr;
}

static void iser_task_free_dout_tasks(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iser_task *dout_task, *tnext;

	list_for_each_entry_safe(dout_task, tnext, &task->dout_task_list, dout_task_list) {
		list_del(&dout_task->dout_task_list);
		iser_task_del_out_buf(task, &dout_task->pdu.membuf);
		schedule_post_recv(dout_task, conn);
	}
}

static void iser_scsi_cmd_iosubmit(struct iser_task *task, int not_last)
{
	struct iscsi_cmd *req_bhs = (struct iscsi_cmd *) task->pdu.bhs;
	struct scsi_cmd *scmd = &task->scmd;
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iser_membuf *data_buf;

	scmd->state = 0;
	if (not_last)
		set_cmd_not_last(scmd);
	scsi_set_in_length(scmd, 0);
	scsi_set_out_length(scmd, 0);

	if (task->is_write) {
		/* It's either the last buffer, which is RDMA,
		   or the only buffer */
		data_buf = list_entry(task->out_buf_list.prev,
				      struct iser_membuf, task_list);
		/* ToDo: multiple RDMA-Write buffers */
		if (task->out_buf_num > 1) {
			unsigned char *ptr = data_buf->addr;
			struct iser_membuf *cur_buf;

			list_for_each_entry(cur_buf, &task->out_buf_list, task_list) {
				if (cur_buf->rdma)
					break;
				memcpy(ptr, cur_buf->addr, cur_buf->size);
				ptr += cur_buf->size;
			}
			iser_task_free_dout_tasks(task);
		}

		scsi_set_out_buffer(scmd, data_buf->addr);
		scsi_set_out_length(scmd, task->out_len);
	}
	if (task->is_read) {
		/* ToDo: multiple RDMA-Read buffers */
		data_buf = list_entry(task->in_buf_list.next,
				      struct iser_membuf, task_list);
		scsi_set_in_buffer(scmd, data_buf->addr);
		scsi_set_in_length(scmd, task->in_len);
	}

	scmd->cmd_itn_id = session->tsih;
	scmd->scb = req_bhs->cdb;
	scmd->scb_len = sizeof(req_bhs->cdb);
	memcpy(scmd->lun, req_bhs->lun, sizeof(scmd->lun));
	scmd->attribute = scsi_cmd_attr(req_bhs->flags);
	scmd->tag = task->tag;
	scmd->result = 0;
	scmd->mreq = NULL;
	scmd->sense_len = 0;

	dprintf("task:%p tag:0x%04"PRIx64 "\n", task, task->tag);

	set_task_in_scsi(task);
	iser_conn_get(conn);

	target_cmd_queue(session->target->tid, scmd);
}

static int iser_scsi_cmd_done(uint64_t nid, int result,
			      struct scsi_cmd *scmd)
{
	struct iser_task *task = container_of(scmd, struct iser_task, scmd);
	struct iscsi_cmd_rsp *rsp_bhs =
		(struct iscsi_cmd_rsp *) task->pdu.bhs;
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	unsigned char sense_len = scmd->sense_len;

	assert(nid == scmd->cmd_itn_id);

	if (unlikely(conn->h.state != STATE_FULL)) {
		/* Connection is closed, but its resources are not released
		   to allow receiving completion of such late tasks.
		   When all tasks are released, and connection refcnt
		   drops to zero, then all the resources can be freed. */
		iser_complete_task(task);
		return 0;
	}

	rsp_bhs->opcode = ISCSI_OP_SCSI_CMD_RSP;
	rsp_bhs->flags = ISCSI_FLAG_CMD_FINAL;
	rsp_bhs->response = ISCSI_STATUS_CMD_COMPLETED;
	rsp_bhs->cmd_status = scsi_get_result(scmd);
	*((uint64_t *) rsp_bhs->rsvd) = (uint64_t) 0;
	rsp_bhs->itt = task->tag;
	rsp_bhs->rsvd1 = 0;
	rsp_bhs->statsn = cpu_to_be32(conn->h.stat_sn++);
	iser_set_rsp_stat_sn(session, task->pdu.bhs);
	rsp_bhs->exp_datasn = 0;

	iscsi_rsp_set_residual(rsp_bhs, scmd);
	if (task->is_read) {
		task->rdma_wr_remains = scsi_get_in_transfer_len(scmd);
		task->rdma_wr_sz = scsi_get_in_transfer_len(scmd);
	}

	task->pdu.ahssize = 0;
	task->pdu.membuf.size = 0;

	if (unlikely(sense_len > 0)) {
		struct iscsi_sense_data *sense =
		    (struct iscsi_sense_data *) task->pdu.membuf.addr;
		sense->length = cpu_to_be16(sense_len);

		/* ToDo: need to fix the ISCSI_PARAM_INITIATOR_RDSL bug in initiator */
		assert(sense_len + 2 <= conn->rsize);

		memcpy(sense->data, scmd->sense_buffer, sense_len);
		task->pdu.membuf.size = sense_len + sizeof(*sense);
	}
	dprintf("task:%p tag:0x%04"PRIx64 " status:%x statsn:%d flags:0x%x "
		"in_len:%d out_len:%d rsd:%d birsd:%d\n",
		task, task->tag, rsp_bhs->cmd_status,
		conn->h.stat_sn-1, rsp_bhs->flags,
		scsi_get_in_length(scmd), scsi_get_out_length(scmd),
		ntohl(rsp_bhs->residual_count),
		ntohl(rsp_bhs->bi_residual_count));

	schedule_resp_tx(task, conn);

	return 0;
}

static void iser_sched_buf_alloc(struct event_data *evt)
{
	struct iser_conn *conn = (struct iser_conn *) evt->data;
	struct iser_task *task;
	int err;

	while (!list_empty(&conn->buf_alloc_list)) {
		task = list_first_entry(&conn->buf_alloc_list,
					struct iser_task,
					exec_list);
		err = iser_task_alloc_rdma_bufs(task);
		if (likely(!err))
			list_del(&task->exec_list);
		else {
			if (err != -ENOMEM)
				iser_conn_close(conn);
			break;
		}
	}
}

static inline int task_can_batch(struct iser_task *task)
{
	struct iscsi_cmd *req_bhs = (struct iscsi_cmd *) task->pdu.bhs;

	switch (req_bhs->cdb[0]) {
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		return 1;
	default:
		return 0;
	}
}

static void iser_sched_iosubmit(struct event_data *evt)
{
	struct iser_conn *conn = (struct iser_conn *) evt->data;
	struct iser_task *task, *next_task;
	int last_in_batch;

	list_for_each_entry_safe(task, next_task, &conn->iosubmit_list, exec_list) {
		if (&next_task->exec_list == &conn->iosubmit_list)
			last_in_batch = 1; /* end of list */
		else
			last_in_batch = (task_can_batch(task) &&
					 task_can_batch(next_task)) ? 0 : 1;
		list_del(&task->exec_list);
		iser_scsi_cmd_iosubmit(task, !last_in_batch);
	}
}

static void iser_sched_rdma_rd(struct event_data *evt)
{
	struct iser_conn *conn = (struct iser_conn *) evt->data;
	struct iser_work_req *first_wr = NULL;
	struct iser_task *prev_task = NULL;
	struct iser_task *task = NULL;
	int num_reqs = 0;
	int err;

	if (unlikely(conn->h.state != STATE_FULL)) {
		dprintf("conn:%p closing, ignoring rdma_rd\n", conn);
		/* ToDo: free all tasks and buffers */
		return;
	}

	while (!list_empty(&conn->rdma_rd_list)) {
		task = list_first_entry(&conn->rdma_rd_list,
					struct iser_task, rdma_list);
		list_del(&task->rdma_list);

		iser_prep_rdma_rd_send_req(task, NULL, 1);
		if (first_wr == NULL)
			first_wr = &task->rdmad;
		else
			prev_task->rdmad.send_wr.next = &task->rdmad.send_wr;
		prev_task = task;
		num_reqs++;
	}
	if (prev_task) {
		prev_task->rdmad.send_wr.next = NULL;
		/* submit the chain of rdma-rd requests, start from the first */
		err = iser_post_send(conn, first_wr, num_reqs);

		/* ToDo: error handling */
	}
}

static void iser_sched_tx(struct event_data *evt)
{
	struct iser_conn *conn = (struct iser_conn *) evt->data;
	struct iser_work_req *first_wr = NULL;
	struct iser_task *prev_task = NULL;
	struct iser_task *task;
	struct iser_work_req *cur_send_wr;
	int num_reqs = 0;
	int err;

	if (unlikely(conn->h.state == STATE_CLOSE)) {
		dprintf("conn:%p closing, ignoring tx\n", conn);
		return;
	}

	while (!list_empty(&conn->resp_tx_list)) {
		task = list_first_entry(&conn->resp_tx_list,
					struct iser_task,
					tx_list);
		list_del(&task->tx_list);
		list_add_tail(&task->tx_list, &conn->sent_list);

		if (task->is_read && task->rdma_wr_remains) {
			iser_prep_rdma_wr_send_req(task, &task->txd, 1);
			cur_send_wr = &task->rdmad;
			num_reqs++;
		} else
			cur_send_wr = &task->txd;

		if (prev_task == NULL)
			first_wr = cur_send_wr;
		else
			prev_task->txd.send_wr.next = &cur_send_wr->send_wr;

		iser_prep_resp_send_req(task, NULL, 1);
		prev_task = task;
		num_reqs++;
	}
	if (prev_task) {
		prev_task->txd.send_wr.next = NULL;
		/* submit the chain of rdma-wr & tx reqs, start from the first */
		err = iser_post_send(conn, first_wr, num_reqs);

		/* ToDo: error handling */
	}
}

static void iser_sched_post_recv(struct event_data *evt)
{
	struct iser_conn *conn = (struct iser_conn *) evt->data;
	struct iser_task *first_task = NULL;
	struct iser_task *prev_task = NULL;
	struct iser_task *task;
	int num_recv_bufs = 0;
	int err;

	if (unlikely(conn->h.state != STATE_FULL)) {
		dprintf("conn:%p closing, ignoring post recv\n", conn);
		return;
	}

	while (!list_empty(&conn->post_recv_list)) {
		task = list_first_entry(&conn->post_recv_list,
					struct iser_task,
					recv_list);
		list_del(&task->recv_list);

		if (prev_task == NULL)
			first_task = task;
		else
			prev_task->rxd.recv_wr.next = &task->rxd.recv_wr;

		prev_task = task;
		num_recv_bufs++;
	}
	if (prev_task) {
		prev_task->rxd.recv_wr.next = NULL;
		/* post the chain of recv buffers, start from the first */
		err = iser_post_recv(conn, first_task, num_recv_bufs);

		/* ToDo: error handling */
	}
}

static int iser_task_handle_ahs(struct iser_task *task)
{
	struct iscsi_connection *conn = &task->conn->h;
	struct iscsi_cmd *req_bhs = (struct iscsi_cmd *) task->pdu.bhs;
	int ahslen = task->pdu.ahssize;
	void *ahs = task->pdu.ahs;
	struct scsi_cmd *scmd = &task->scmd;
	enum data_direction dir = scsi_get_data_dir(scmd);

	if (ahslen >= 4) {
		struct iscsi_ecdb_ahdr *ahs_extcdb = ahs;

		if (ahs_extcdb->ahstype == ISCSI_AHSTYPE_CDB) {
			int extcdb_len = ntohs(ahs_extcdb->ahslength) - 1;
			int total_cdb_len = sizeof(req_bhs->cdb) + extcdb_len;
			unsigned char *p;

			if (4 + extcdb_len > ahslen) {
				eprintf("AHS len:%d too short for extcdb %d\n",
					ahslen, extcdb_len);
				return -EINVAL;
			}
			if (total_cdb_len > 260) {
				eprintf("invalid extcdb len:%d\n", extcdb_len);

				return -EINVAL;
			}
			p = malloc(total_cdb_len);
			if (!p) {
				eprintf("failed to allocate extdata len:%d\n", total_cdb_len);
				return -ENOMEM;
			}
			memcpy(p, req_bhs->cdb, sizeof(req_bhs->cdb));
			p += sizeof(req_bhs->cdb);
			memcpy(p, ahs_extcdb->ecdb, extcdb_len);
			task->extdata = p;

			scmd->scb = ahs;
			scmd->scb_len = total_cdb_len;

			ahs += 4 + extcdb_len;
			ahslen -= 4 + extcdb_len;
		}
	}

	if (dir == DATA_BIDIRECTIONAL && ahslen >= 8) {
		struct iscsi_rlength_ahdr *ahs_bidi = ahs;
		if (ahs_bidi->ahstype == ISCSI_AHSTYPE_RLENGTH) {
			uint32_t in_length = ntohl(ahs_bidi->read_length);

			if (in_length)
				task->in_len = roundup(in_length,
						       conn->tp->data_padding);
			dprintf("bidi read len:%u, padded:%u\n",
				in_length, task->in_len);
		}
	}

	return 0;
}

static int iser_scsi_cmd_rx(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iscsi_cmd *req_bhs = (struct iscsi_cmd *) task->pdu.bhs;
	unsigned int flags = req_bhs->flags;
	uint32_t imm_data_sz = ntoh24(req_bhs->dlength);
	uint32_t xfer_sz = ntohl(req_bhs->data_length);
	int err = 0;

	task->is_read = flags & ISCSI_FLAG_CMD_READ;
	task->is_write = flags & ISCSI_FLAG_CMD_WRITE;

	if (task->is_write) {
		task->out_len = xfer_sz;
		if (!task->is_read) {
			scsi_set_data_dir(&task->scmd, DATA_WRITE);
			/* reset irrelevant fields */
			task->in_len = 0;
			task->rdma_wr_sz = 0;
			task->rdma_wr_remains = 0;
		} else
			scsi_set_data_dir(&task->scmd, DATA_BIDIRECTIONAL);
	} else {
		if (likely(task->is_read)) {
			task->in_len = xfer_sz;
			scsi_set_data_dir(&task->scmd, DATA_READ);
			/* reset irrelevant fields */
			task->out_len = 0;
			task->unsol_sz = 0;
			task->unsol_remains = 0;
			task->rdma_rd_sz = 0;
			task->rdma_rd_remains = 0;
		} else {
			scsi_set_data_dir(&task->scmd, DATA_NONE);
			task->out_len = 0;
			task->in_len = 0;
		}
	}

	if (task->pdu.ahssize) {
		err = iser_task_handle_ahs(task); /* may set task->in_len */
		if (err)
			goto out;
	}

	if (task->is_write) {
		/* add immediate data to the task */
		if (!conn->h.session_param[ISCSI_PARAM_INITIAL_R2T_EN].val) {
			task->unsol_sz = conn->h.session_param[ISCSI_PARAM_FIRST_BURST].val;
			if (task->out_len < task->unsol_sz)
				task->unsol_sz = task->out_len;
		} else
			task->unsol_sz = 0;

		if (conn->h.session_param[ISCSI_PARAM_IMM_DATA_EN].val) {
			if (imm_data_sz > 0) {
				if (task->unsol_sz == 0)
					task->unsol_sz = imm_data_sz;
				iser_task_add_out_pdu_buf(task, &task->pdu.membuf, 0);
			}
		} else if (imm_data_sz > 0) {
			eprintf("ImmediateData disabled but received\n");
			err = -EINVAL;
			goto out;
		}

		/* immediate data is the first chunk of the unsolicited data */
		task->unsol_remains = task->unsol_sz - imm_data_sz;
		/* rdma-reads cover the entire solicited data */
		task->rdma_rd_sz = task->out_len - task->unsol_sz;
		task->rdma_rd_remains = task->rdma_rd_sz;

		/* ToDo: multiple RDMA-Write buffers */
		/* task->rdma_rd_offset = task->unsol_sz; */
	}

	if (task->is_read) {
		task->rdma_wr_sz = task->in_len;
		task->rdma_wr_remains = task->in_len;
	}

	list_add_tail(&task->session_list, &session->cmd_list);
out:
	dprintf("task:%p tag:0x%04"PRIx64 " scsi_op:0x%x %s%s in_len:%d out_len:%d "
		"imm_sz:%d unsol_sz:%d cmdsn:0x%x expcmdsn:0x%x\n",
		task, task->tag, req_bhs->cdb[0],
		task->is_read ? "rd" : "", task->is_write ? "wr" : "",
		task->in_len, task->out_len, imm_data_sz, task->unsol_sz,
		task->cmd_sn, session->exp_cmd_sn);

	return err;
}

static int iser_data_out_rx(struct iser_task *dout_task)
{
	struct iser_conn *conn = dout_task->conn;
	struct iscsi_session *session = conn->h.session;
	struct iscsi_data *req_bhs =
	    (struct iscsi_data *) dout_task->pdu.bhs;
	struct iser_task *task;
	int err = 0;

	list_for_each_entry(task, &session->cmd_list, session_list) {
		if (task->tag == req_bhs->itt)
			goto found;
	}
	return -EINVAL;

found:
	iser_task_add_out_pdu_buf(task, &dout_task->pdu.membuf,
				  be32_to_cpu(req_bhs->offset));
	list_add_tail(&dout_task->dout_task_list, &task->dout_task_list);

	/* ToDo: BUG!!! add an indication that it's data-out task so that
		it can be released when the buffer is released */

	task->unsol_remains -= ntoh24(req_bhs->dlength);

	dprintf("task:%p tag:0x%04"PRIx64 ", dout taskptr:%p out_len:%d "
		"uns_rem:%d rdma_rd_rem:%d expcmdsn:0x%x\n",
		task, task->tag, dout_task, task->out_len,
		task->unsol_remains, task->rdma_rd_remains,
		session->exp_cmd_sn);

	/* ToDo: look at the task counters !!! */
	if (req_bhs->ttt == cpu_to_be32(ISCSI_RESERVED_TAG)) {
		if (req_bhs->flags & ISCSI_FLAG_CMD_FINAL) {
			if (!task_pending(task)) {
				/* ToDo: this condition is weird ... */
				if (task->rdma_rd_remains == 0 && task->unsol_remains == 0)
					schedule_task_iosubmit(task, conn);
			}
		}
	} else {
		if (!(req_bhs->flags & ISCSI_FLAG_CMD_FINAL))
			return err;

		if (task->rdma_rd_remains == 0 && task->unsol_remains == 0)
			schedule_task_iosubmit(task, conn);
	}
	return err;
}

/* usually rx_task is passed directly from the rx handler,
   but due to the connection establishment event-data race
   the first login task may be saved and injected afterwards.
 */
static void iser_login_rx(struct iser_task *rx_task)
{
	struct iser_conn *conn = rx_task->conn;
	struct iser_task *tx_task = conn->login_tx_task;
	struct iscsi_login_rsp *rsp_bhs =
		(struct iscsi_login_rsp *)tx_task->pdu.bhs;

	if (conn->h.state == STATE_START) {
		eprintf("conn:%p, not established yet, delaying login_rx\n",
			&conn->h);
		conn->h.state = STATE_READY;
		conn->login_rx_task = rx_task;
		return;
	}

	iser_login_exec(&conn->h, &rx_task->pdu, &tx_task->pdu);
	if (rsp_bhs->status_class) {
		eprintf("conn:%p, login failed, class:%0x detail:%0x\n",
			&conn->h, rsp_bhs->status_class, rsp_bhs->status_detail);
	}

	if (conn->login_phase == LOGIN_PHASE_LAST_SEND)
		dprintf("transitioning to full-feature, no repost\n");
	else
		iser_post_recv(conn, rx_task, 1);

	schedule_resp_tx(tx_task, conn);
}

static int iser_nop_out_rx(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iscsi_nopout *req_bhs =
		(struct iscsi_nopout *) task->pdu.bhs;
	int err = 0;

	if (req_bhs->ttt != cpu_to_be32(ISCSI_RESERVED_TAG)) {
		/*
		 * When sending NOP-In, we don't request a NOP-Out      			       .
		 * by sending a valid Target Tranfer Tag.
		 * See iser_send_ping_nop_in() and 10.18.2 in the draft 20.
		 */
		eprintf("conn:%p task:%p initiator bug, ttt not reserved\n",
			&conn->h, task);
		err = -ISCSI_REASON_PROTOCOL_ERROR;
		goto reject;
	}
	if (req_bhs->itt == cpu_to_be32(ISCSI_RESERVED_TAG)) {
		if (req_bhs->opcode & ISCSI_OP_IMMEDIATE) {
			dprintf("No response to Nop-Out\n");
			iser_post_recv(conn, task, 1);
			return -EAGAIN; /* ToDo: fix the ret codes */
		} else {
			eprintf("conn:%p task:%p initiator bug, "
				"itt reserved with no imm. flag\n", &conn->h, task);
			err = -ISCSI_REASON_PROTOCOL_ERROR;
			goto reject;
		}
	}

	task->out_len = ntoh24(req_bhs->dlength);
	dprintf("conn:%p nop-out task:%p cmdsn:0x%x data_sz:%d\n",
		&conn->h, task, task->cmd_sn, task->out_len);

	return 0;

reject: /* ToDo: prepare and send reject pdu */
	/* iser_send_reject(task, ISCSI_REASON_INVALID_PDU_FIELD); */
	return err;
}

static int iser_task_delivery(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	int err;

	switch (task->opcode) {
	case ISCSI_OP_SCSI_CMD:
		err = iser_task_alloc_rdma_bufs(task);
		if (unlikely(err == -ENOMEM))
			list_add_tail(&task->exec_list,
				      &task->conn->buf_alloc_list);
		break;
	case ISCSI_OP_NOOP_OUT:
		err = iser_nop_out_exec(task);
		break;
	case ISCSI_OP_LOGOUT:
		err = iser_logout_exec(task);
		break;
	case ISCSI_OP_SCSI_TMFUNC:
		err = iser_tm_exec(task);
		break;
	case ISCSI_OP_TEXT:
		err = iser_text_exec(&conn->h, &task->pdu, &conn->text_tx_task->pdu);
		schedule_resp_tx(conn->text_tx_task, conn);
		break;

	default:
		eprintf("Internal error: Unexpected op:0x%x\n", task->opcode);
		err = -EINVAL;
		break;
	}
	return err;
}

static inline int cmdsn_cmp(uint32_t sn1, uint32_t sn2)
{
	if (sn1 == sn2)
		return 0;

	return ((int32_t)sn1 - (int32_t)sn2) > 0 ? 1 : -1;
}

/* queues the task according to cmd-sn, no exec here */
static int iser_queue_task(struct iscsi_session *session,
			   struct iser_task *task)
{
	uint32_t cmd_sn = task->cmd_sn;
	struct list_head *cmp_entry;
	int err;

	if (unlikely(task->is_immediate)) {
		dprintf("exec imm task task:%p tag:0x%0"PRIx64 " cmd_sn:0x%x\n",
			task, task->tag, cmd_sn);
		err = iser_task_delivery(task);
		if (likely(!err || err == -ENOMEM))
			return 0;
		else
			return err;
	}

	/* if the current command is the expected one, exec it
	   and all others possibly acumulated on the queue */
	while (session->exp_cmd_sn == cmd_sn) {
		session->exp_cmd_sn++;
		dprintf("exec task:%p cmd_sn:0x%x\n", task, cmd_sn);
		err = iser_task_delivery(task);
		if (unlikely(err && err != -ENOMEM)) {
			/* when no free buffers remains, the task will wait
			   on queue, so it is not a real error, but we should
			   not attempt to start other tasks until more
			   memory becomes available */
			return err;
			/* ToDo: what if there are more tasks in case of error */
		}

		if (list_empty(&session->pending_cmd_list))
			return 0;

		task = list_first_entry(&session->pending_cmd_list,
					struct iser_task, exec_list);
		list_del(&task->exec_list);
		clear_task_pending(task);
		cmd_sn = be32_to_cpu(task->pdu.bhs->statsn);
	}

	/* cmd_sn > (exp_cmd_sn+max_queue_cmd), i.e. beyond allowed window */
	if (cmdsn_cmp(cmd_sn,
		      session->exp_cmd_sn+session->max_queue_cmd) == 1) {
		eprintf("unexpected cmd_sn:0x%x, max:0x%x\n",
			cmd_sn, session->exp_cmd_sn+session->max_queue_cmd);
		return -EINVAL;
	}

	/* insert the current task, ordered by cmd_sn */
	list_for_each_prev(cmp_entry, &session->pending_cmd_list) {
		struct iser_task *cmp_task;
		uint32_t cmp_cmd_sn;
		int cmp_res;

		cmp_task = list_entry(cmp_entry, struct iser_task, exec_list);
		cmp_cmd_sn = cmp_task->cmd_sn;

		cmp_res = cmdsn_cmp(cmd_sn, cmp_cmd_sn);
		if (cmp_res == 1) { /* cmd_sn > cmp_cmd_sn */
			dprintf("inserted cmdsn:0x%x after cmdsn:0x%x\n",
				cmd_sn, cmp_cmd_sn);
			break;
		} else if (cmp_res == -1) { /* cmd_sn < cmp_cmd_sn */
			dprintf("inserting cmdsn:0x%x skip cmdsn:0x%x\n",
				cmd_sn, cmp_cmd_sn);
			continue;
		} else { /* cmd_sn == cmp_cmd_sn */
			eprintf("duplicate cmd_sn:0x%x, exp:%u\n",
				cmd_sn, session->exp_cmd_sn);
			return -EINVAL;
		}
	}
	list_add(&task->exec_list, cmp_entry);
	set_task_pending(task);
	return 0;
}

static int iser_parse_req_headers(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	struct iser_hdr *iser_hdr = task->pdu.iser_hdr;
	struct iscsi_hdr *iscsi_hdr = task->pdu.bhs;
	unsigned pdu_dlength = ntoh24(iscsi_hdr->dlength);
	unsigned pdu_len = pdu_dlength + sizeof(struct iscsi_hdr);
	int err = -1;

	switch (iser_hdr->flags & 0xF0) {
	case ISCSI_CTRL:
		if (iser_hdr->flags & ISER_RSV) {
			task->rem_read_stag =
			    be32_to_cpu(iser_hdr->read_stag);
			task->rem_read_va = be64_to_cpu(iser_hdr->read_va);
			dprintf("task:%p rstag:0x%x va:0x%"PRIx64 "\n", task,
				task->rem_read_stag, task->rem_read_va);
		}
		if (iser_hdr->flags & ISER_WSV) {
			task->rem_write_stag =
			    be32_to_cpu(iser_hdr->write_stag);
			task->rem_write_va =
			    be64_to_cpu(iser_hdr->write_va);
			dprintf("task:%p wstag:0x%x va:0x%"PRIx64 "\n", task,
				task->rem_write_stag, task->rem_write_va);
		}
		err = 0;
		break;
	case ISER_HELLO:
		dprintf("iSER Hello message??\n");
		break;
	default:
		eprintf("malformed iser iser_hdr, flags 0x%02x\n",
			iser_hdr->flags);
		break;
	}

	task->opcode = iscsi_hdr->opcode & ISCSI_OPCODE_MASK;
	task->is_immediate = iscsi_hdr->opcode & ISCSI_OP_IMMEDIATE ? 1 : 0;
	task->is_read = 0; /* valid for cmds only */
	task->is_write = 0; /* valid for cmds only */

	task->pdu.ahssize = iscsi_hdr->hlength * 4;
	task->pdu.membuf.addr += task->pdu.ahssize;
	pdu_len += task->pdu.ahssize;
	task->pdu.membuf.size = pdu_dlength;
	task->pdu.membuf.rdma = 0;

	task->tag = iscsi_hdr->itt;
	task->cmd_sn = be32_to_cpu(iscsi_hdr->statsn);
	conn->h.exp_stat_sn = be32_to_cpu(iscsi_hdr->exp_statsn);
	iscsi_update_conn_stats_rx(&conn->h, pdu_len, task->opcode);

	return err;
}

static int iser_rx_handler_non_ff(struct iser_task *task)
{
	struct iser_conn *conn = task->conn;
	int err = 0;

	switch (conn->h.state) {
	case STATE_START:
	case STATE_READY:
	case STATE_SECURITY:
	case STATE_SECURITY_AUTH:
	case STATE_SECURITY_DONE:
	case STATE_SECURITY_LOGIN:
	case STATE_SECURITY_FULL:
	case STATE_LOGIN:
	case STATE_LOGIN_FULL:
		if (task->opcode == ISCSI_OP_LOGIN) {
			dprintf("login rx, conn:%p\n", conn);
			iser_login_rx(task);
		} else {
			eprintf("non-login pdu during login phase, "
				"conn:%p opcode:0x%0x\n",
				conn, task->opcode);
			err = -EINVAL;
		}
		break;
	case STATE_EXIT:
	case STATE_CLOSE:
		dprintf("ignored rx, while conn:%p closing\n", conn);
		break;
	default:
		dprintf("ignored rx, conn:%p unexpected state:%d\n",
			conn, conn->h.state);
		break;
	}
	return 0;
}

static void iser_rx_handler(struct iser_work_req *rxd)
{
	struct iser_task *task = rxd->task;
	struct iser_conn *conn = task->conn;
	int queue_task = 1;
	int err = 0;

	iser_conn_put(conn);

	err = iser_parse_req_headers(task);
	if (unlikely(err))
		goto out;

	if (unlikely(conn->h.state != STATE_FULL)) {
		err = iser_rx_handler_non_ff(task);
		goto out;
	}

	INIT_LIST_HEAD(&task->in_buf_list);
	task->in_buf_num = 0;
	INIT_LIST_HEAD(&task->out_buf_list);
	task->out_buf_num = 0;

	if (likely(task->opcode == ISCSI_OP_SCSI_CMD))
		err = iser_scsi_cmd_rx(task);
	else {
		switch (task->opcode) {
		case ISCSI_OP_SCSI_DATA_OUT:
			err = iser_data_out_rx(task);
			queue_task = 0;
			break;
		case ISCSI_OP_NOOP_OUT:
			err = iser_nop_out_rx(task);
			break;
		case ISCSI_OP_LOGOUT:
			dprintf("logout rx\n");
			break;
		case ISCSI_OP_SCSI_TMFUNC:
			dprintf("tmfunc rx\n");
			break;
		case ISCSI_OP_TEXT:
			dprintf("text rx\n");
			err = iser_task_delivery(task);
			queue_task = 0;
			break;
		case ISCSI_OP_SNACK:
			eprintf("Cannot handle SNACK yet\n");
			err = -EINVAL;
			break;
		default:
			eprintf("Unknown op 0x%x\n", task->opcode);
			err = -EINVAL;
			break;
		}
	}
	if (likely(!err && queue_task))
		err = iser_queue_task(conn->h.session, task);
out:
	if (unlikely(err)) {
		eprintf("conn:%p task:%p err:%d, closing\n",
			&conn->h, task, err);
		iser_conn_close(conn);
	}
}

static void iser_login_tx_complete(struct iser_conn *conn)
{
	switch (conn->h.state) {
	case STATE_SECURITY_LOGIN:
		conn->h.state = STATE_LOGIN;
		break;
	case STATE_SECURITY_FULL: /* fall through */
	case STATE_LOGIN_FULL:
		dprintf("conn:%p, last login send completed\n", conn);
		conn->h.state = STATE_FULL;
		iser_conn_login_phase_set(conn, LOGIN_PHASE_FF);
		iser_free_login_resources(conn);
		break;
	}
}

static void iser_tx_complete_handler(struct iser_work_req *txd)
{
	struct iser_task *task = txd->task;
	struct iser_conn *conn = task->conn;
	int opcode = task->pdu.bhs->opcode & ISCSI_OPCODE_MASK;

	iscsi_update_conn_stats_tx(&conn->h, txd->sge.length, opcode);
	dprintf("conn:%p task:%p tag:0x%04"PRIx64 " opcode:0x%x\n",
		&conn->h, task, task->tag, opcode);
	iser_conn_put(conn);

	list_del(&task->tx_list); /* remove from conn->sent_list */

	if (unlikely(task->unsolicited)) {
		conn->nop_in_task = task;
		return;
	}

	iser_complete_task(task);

	if (unlikely(conn->h.state != STATE_FULL))
		iser_login_tx_complete(conn);
	else
		schedule_post_recv(task, conn);
}

static void iser_rdma_wr_complete_handler(struct iser_work_req *rdmad)
{
	struct iser_task *task = rdmad->task;
	struct iser_conn *conn = task->conn;

	iscsi_update_conn_stats_tx(&conn->h, rdmad->sge.length, ISCSI_OP_SCSI_DATA_IN);
	dprintf("conn:%p task:%p tag:0x%04"PRIx64 "\n",
		&conn->h, task, task->tag);
	iser_conn_put(conn);

	/* no need to remove from conn->sent_list, it is done in
	   iser_tx_complete_handler(), as rdma-wr is followed by tx */
}

static void iser_rdma_rd_complete_handler(struct iser_work_req *rdmad)
{
	struct iser_task *task = rdmad->task;
	struct iser_conn *conn = task->conn;

	iscsi_update_conn_stats_rx(&conn->h, rdmad->sge.length, ISCSI_OP_SCSI_DATA_OUT);
	task->rdma_rd_remains -= rdmad->sge.length;
	dprintf("conn:%p task:%p tag:0x%04"PRIx64 ", rems rdma:%d unsol:%d\n",
		&conn->h, task, task->tag, task->rdma_rd_remains,
		task->unsol_remains);
	iser_conn_put(conn);

	/* no need to remove from a list, was removed before rdma-rd */

	if (unlikely(conn->h.state != STATE_FULL))
		return;

	if (task->rdma_rd_remains == 0 && task->unsol_remains == 0)
		schedule_task_iosubmit(task, conn);
}

/*
 * Deal with just one work completion.
 */
static void handle_wc(struct ibv_wc *wc)
{
	struct iser_work_req *req = ptr_from_int64(wc->wr_id);
	struct iser_task *task;
	struct iser_conn *conn;

	dprintf("%s complete, wr_id:%p len:%d\n",
		iser_ib_op_to_str(req->iser_ib_op), req, wc->byte_len);

	switch (req->iser_ib_op) {
	case ISER_IB_RECV:
		iser_rx_handler(req);
		break;
	case ISER_IB_SEND:
		iser_tx_complete_handler(req);
		break;
	case ISER_IB_RDMA_WRITE:
		iser_rdma_wr_complete_handler(req);
		break;
	case ISER_IB_RDMA_READ:
		iser_rdma_rd_complete_handler(req);
		break;
	default:
		task = req->task;
		conn = (task ? task->conn : NULL);
		eprintf("unexpected req op:%d, wc op%d, wc::%p "
			"wr_id:%p task:%p conn:%p\n",
			req->iser_ib_op, wc->opcode, wc, req, task, conn);
		if (conn)
			iser_conn_close(conn);
		break;
	}
}

static void handle_wc_error(struct ibv_wc *wc)
{
	struct iser_work_req *req = ptr_from_int64(wc->wr_id);
	struct iser_task *task = req->task;
	struct iser_conn *conn = task ? task->conn : NULL;

	if (wc->status != IBV_WC_WR_FLUSH_ERR)
		eprintf("conn:%p task:%p tag:0x%04"PRIx64 " wr_id:0x%p op:%s "
			"err:%s vendor_err:0x%0x\n",
			&conn->h, task, task->tag, req,
			iser_ib_op_to_str(req->iser_ib_op),
			ibv_wc_status_str(wc->status), wc->vendor_err);
	else
		dprintf("conn:%p task:%p tag:0x%04"PRIx64 " wr_id:0x%p op:%s "
			"err:%s vendor_err:0x%0x\n",
			&conn->h, task, task->tag, req,
			iser_ib_op_to_str(req->iser_ib_op),
			ibv_wc_status_str(wc->status), wc->vendor_err);

	switch (req->iser_ib_op) {
	case ISER_IB_SEND:
		/* in both read and write tasks SEND is last,
		   the task should be completed now */
	case ISER_IB_RDMA_READ:
		/* RDMA-RD is sent separately, and Response
		   is to be SENT after its completion, so if RDMA-RD fails,
		   task to be completed now */
		iser_complete_task(task);
		break;
	case ISER_IB_RECV:
		/* this should be the Flush, no task has been created yet */
	case ISER_IB_RDMA_WRITE:
		/* RDMA-WR and SEND response of a READ task
		   are sent together, so when receiving RDMA-WR error,
		   wait until SEND error arrives to complete the task */
		break;
	default:
		eprintf("unexpected opcode %d, "
			"wc:%p wr_id:%p task:%p conn:%p\n",
			wc->opcode, wc, req, task, conn);
		break;
	}

	if (conn) {
		iser_conn_put(conn);
		iser_conn_close(conn);
	}
}

/*
 * Could read as many entries as possible without blocking, but
 * that just fills up a list of tasks.  Instead pop out of here
 * so that tx progress, like issuing rdma reads and writes, can
 * happen periodically.
 */
static int iser_poll_cq(struct iser_device *dev, int max_wc)
{
	int err = 0, numwc = 0;
	struct ibv_wc wc;

	for (;;) {
		err = ibv_poll_cq(dev->cq, 1, &wc);
		if (err == 0) /* no completions retrieved */
			break;

		if (unlikely(err < 0)) {
			eprintf("ibv_poll_cq failed\n");
			break;
		}

		VALGRIND_MAKE_MEM_DEFINED(&wc, sizeof(wc));
		if (likely(wc.status == IBV_WC_SUCCESS))
			handle_wc(&wc);
		else
			handle_wc_error(&wc);

		if (++numwc == max_wc) {
			err = 1;
			break;
		}
	}
	return err;
}

static int num_delayed_arm;
#define MAX_NUM_DELAYED_ARM 16

static void iser_rearm_completions(struct iser_device *dev)
{
	int err;

	err = ibv_req_notify_cq(dev->cq, 0);
	if (unlikely(err))
		eprintf("ibv_req_notify_cq failed\n");

	dev->poll_sched.sched_handler = iser_sched_consume_cq;
	tgt_add_sched_event(&dev->poll_sched);

	num_delayed_arm = 0;
}

static void iser_poll_cq_armable(struct iser_device *dev)
{
	int err;

	err = iser_poll_cq(dev, MAX_POLL_WC);
	if (unlikely(err < 0)) {
		iser_rearm_completions(dev);
		return;
	}

	if (err == 0 && (++num_delayed_arm == MAX_NUM_DELAYED_ARM))
		/* no more completions on cq, give up and arm the interrupts */
		iser_rearm_completions(dev);
	else {
		dev->poll_sched.sched_handler = iser_sched_poll_cq;
		tgt_add_sched_event(&dev->poll_sched);
	}
}

/* iser_sched_consume_cq() is scheduled to consume completion events that
   could arrive after the cq had been seen empty, but just before
   the interrupts were re-armed.
   Intended to consume those remaining completions only, the function
   does not re-arm interrupts, but polls the cq until it's empty.
   As we always limit the number of completions polled at a time, we may
   need to schedule this functions few times.
   It may happen that during this process new completions occur, and
   we get an interrupt about that. Some of the "new" completions may be
   processed by the self-scheduling iser_sched_consume_cq(), which is
   a good thing, because we don't need to wait for the interrupt event.
   When the interrupt notification arrives, its handler will remove the
   scheduled event, and call iser_poll_cq_armable(), so that the polling
   cycle resumes normally.
*/
static void iser_sched_consume_cq(struct event_data *tev)
{
	struct iser_device *dev = tev->data;
	int err;

	err = iser_poll_cq(dev, MAX_POLL_WC);
	if (err > 0) {
		dev->poll_sched.sched_handler = iser_sched_consume_cq;
		tgt_add_sched_event(&dev->poll_sched);
	}
}

/* Scheduled to poll cq after a completion event has been
   received and acknowledged, if no more completions are found
   the interrupts are re-armed */
static void iser_sched_poll_cq(struct event_data *tev)
{
	struct iser_device *dev = tev->data;
	iser_poll_cq_armable(dev);
}

/*
 * Called from main event loop when a CQ notification is available.
 */
static void iser_handle_cq_event(int fd __attribute__ ((unused)),
				 int events __attribute__ ((unused)),
				 void *data)
{
	struct iser_device *dev = data;
	void *cq_context;
	int err;

	err = ibv_get_cq_event(dev->cq_channel, &dev->cq, &cq_context);
	if (unlikely(err != 0)) {
		/* Just print the log message, if that was a serious problem,
		   it will express itself elsewhere */
		eprintf("failed to retrieve CQ event, cq:%p\n", dev->cq);
		return;
	}

	ibv_ack_cq_events(dev->cq, 1);

	/* if a poll was previosuly scheduled, remove it,
	   as it will be scheduled when necessary */
	if (dev->poll_sched.scheduled)
		tgt_remove_sched_event(&dev->poll_sched);

	iser_poll_cq_armable(dev);
}

/*
 * Called from main event loop when async event is available.
 */
static void iser_handle_async_event(int fd __attribute__ ((unused)),
				    int events __attribute__ ((unused)),
				    void *data)
{
	struct iser_device *dev = data;
	char *dev_name = dev->ibv_ctxt->device->name;
	struct ibv_async_event async_event;
	struct iser_conn *conn;

	if (ibv_get_async_event(dev->ibv_ctxt, &async_event)) {
		eprintf("ibv_get_async_event failed\n");
		return;
	}

	switch (async_event.event_type) {
	case IBV_EVENT_COMM_EST:
		conn = async_event.element.qp->qp_context;
		eprintf("conn:0x%p cm_id:0x%p dev:%s, QP evt: %s\n",
			&conn->h, conn->cm_id, dev_name,
			ibv_event_type_str(IBV_EVENT_COMM_EST));
		/* force "connection established" event */
		rdma_notify(conn->cm_id, IBV_EVENT_COMM_EST);
		break;

	/* rest of QP-related events */
	case IBV_EVENT_QP_FATAL:
	case IBV_EVENT_QP_REQ_ERR:
	case IBV_EVENT_QP_ACCESS_ERR:
	case IBV_EVENT_SQ_DRAINED:
	case IBV_EVENT_PATH_MIG:
	case IBV_EVENT_PATH_MIG_ERR:
	case IBV_EVENT_QP_LAST_WQE_REACHED:
		conn = async_event.element.qp->qp_context;
		eprintf("conn:0x%p cm_id:0x%p dev:%s, QP evt: %s\n",
			&conn->h, conn->cm_id, dev_name,
			ibv_event_type_str(async_event.event_type));
		break;

	/* CQ-related events */
	case IBV_EVENT_CQ_ERR:
		eprintf("dev:%s CQ evt: %s\n", dev_name,
			ibv_event_type_str(async_event.event_type));
		break;

	/* SRQ events */
	case IBV_EVENT_SRQ_ERR:
	case IBV_EVENT_SRQ_LIMIT_REACHED:
		eprintf("dev:%s SRQ evt: %s\n", dev_name,
			ibv_event_type_str(async_event.event_type));
		break;

	/* Port events */
	case IBV_EVENT_PORT_ACTIVE:
	case IBV_EVENT_PORT_ERR:
	case IBV_EVENT_LID_CHANGE:
	case IBV_EVENT_PKEY_CHANGE:
	case IBV_EVENT_SM_CHANGE:
	case IBV_EVENT_CLIENT_REREGISTER:
		eprintf("dev:%s port:%d evt: %s\n",
			dev_name, async_event.element.port_num,
			ibv_event_type_str(async_event.event_type));
		break;

	/* HCA events */
	case IBV_EVENT_DEVICE_FATAL:
		eprintf("dev:%s HCA evt: %s\n", dev_name,
			ibv_event_type_str(async_event.event_type));
		break;

	default:
		eprintf("dev:%s evt: %s\n", dev_name,
			ibv_event_type_str(async_event.event_type));
		break;
	}

	ibv_ack_async_event(&async_event);
}

/*
 * First time a new connection is received on an RDMA device, record
 * it and build a PD and static memory.
 */
static int iser_device_init(struct iser_device *dev)
{
	int cqe_num;
	int err = -1;

	dprintf("dev %p\n", dev);
	dev->pd = ibv_alloc_pd(dev->ibv_ctxt);
	if (dev->pd == NULL) {
		eprintf("ibv_alloc_pd failed\n");
		goto out;
	}

	err = iser_init_rdma_buf_pool(dev);
	if (err) {
		eprintf("iser_init_rdma_buf_pool failed\n");
		goto out;
	}

	err = ibv_query_device(dev->ibv_ctxt, &dev->device_attr);
	if (err < 0) {
		eprintf("ibv_query_device failed, %m\n");
		goto out;
	}
	cqe_num = min(dev->device_attr.max_cqe, MAX_CQ_ENTRIES);
	dprintf("max %d CQEs\n", cqe_num);

	err = -1;
	dev->cq_channel = ibv_create_comp_channel(dev->ibv_ctxt);
	if (dev->cq_channel == NULL) {
		eprintf("ibv_create_comp_channel failed\n");
		goto out;
	}

	/* verify cq_vector */
	if (cq_vector < 0)
		cq_vector = control_port % dev->ibv_ctxt->num_comp_vectors;
	else if (cq_vector >= dev->ibv_ctxt->num_comp_vectors) {
		eprintf("Bad CQ vector. max: %d\n",
			dev->ibv_ctxt->num_comp_vectors);
		goto out;
	}
	dprintf("CQ vector: %d\n", cq_vector);

	dev->cq = ibv_create_cq(dev->ibv_ctxt, cqe_num, NULL,
				dev->cq_channel, cq_vector);
	if (dev->cq == NULL) {
		eprintf("ibv_create_cq failed\n");
		goto out;
	}
	dprintf("dev->cq:%p\n", dev->cq);

	tgt_init_sched_event(&dev->poll_sched, iser_sched_poll_cq, dev);

	err = ibv_req_notify_cq(dev->cq, 0);
	if (err) {
		eprintf("ibv_req_notify failed, %m\n");
		goto out;
	}

	err = tgt_event_add(dev->cq_channel->fd, EPOLLIN,
			    iser_handle_cq_event, dev);
	if (err) {
		eprintf("tgt_event_add failed, %m\n");
		goto out;

	}

	err = tgt_event_add(dev->ibv_ctxt->async_fd, EPOLLIN,
			    iser_handle_async_event, dev);
	if (err)
		return err;

	list_add_tail(&dev->list, &iser_dev_list);

out:
	return err;
}

static void iser_device_release(struct iser_device *dev)
{
	int err;

	list_del(&dev->list);

	tgt_event_del(dev->ibv_ctxt->async_fd);
	tgt_event_del(dev->cq_channel->fd);
	tgt_remove_sched_event(&dev->poll_sched);

	err = ibv_destroy_cq(dev->cq);
	if (err)
		eprintf("ibv_destroy_cq failed: (errno=%d %m)\n", errno);

	err = ibv_destroy_comp_channel(dev->cq_channel);
	if (err)
		eprintf("ibv_destroy_comp_channel failed: (errno=%d %m)\n", errno);

	iser_destroy_rdma_buf_pool(dev);

	err = ibv_dealloc_pd(dev->pd);
	if (err)
		eprintf("ibv_dealloc_pd failed: (errno=%d %m)\n", errno);
}

/*
 * Init entire iscsi transport.  Begin listening for connections.
 */
static int iser_ib_init(void)
{
	int err;
	struct sockaddr_in sock_addr;
	short int port = iser_listen_port;

	rdma_evt_channel = rdma_create_event_channel();
	if (!rdma_evt_channel) {
		eprintf("Failed to initialize RDMA; load kernel modules?\n");
		return -1;
	}

	err = rdma_create_id(rdma_evt_channel, &cma_listen_id, NULL,
			     RDMA_PS_TCP);
	if (err) {
		eprintf("rdma_create_id failed, %m\n");
		return -1;
	}

	memset(&sock_addr, 0, sizeof(sock_addr));
	sock_addr.sin_family = AF_INET;
	sock_addr.sin_port = htons(port);
	sock_addr.sin_addr.s_addr = INADDR_ANY;
	err =
	    rdma_bind_addr(cma_listen_id, (struct sockaddr *) &sock_addr);
	if (err) {
		if (err == -1)
			eprintf("rdma_bind_addr -1: %m\n");
		else
			eprintf("rdma_bind_addr: %s\n", strerror(-err));
		return -1;
	}

	/* 0 == maximum backlog */
	err = rdma_listen(cma_listen_id, 0);
	if (err) {
		if (err == -1)
			eprintf("rdma_listen -1: %m\n");
		else
			eprintf("rdma_listen: %s\n", strerror(-err));
		return -1;
	}

	dprintf("listening for iser connections on port %d\n", port);
	err = tgt_event_add(cma_listen_id->channel->fd, EPOLLIN,
			    iser_handle_rdmacm, NULL);
	if (err)
		return err;

	return err;
}

static void iser_ib_release(void)
{
	int err;
	struct iser_device *dev, *tdev;

	assert(list_empty(&iser_conn_list));

	list_for_each_entry_safe(dev, tdev, &iser_dev_list, list) {
	        iser_device_release(dev);
		free(dev);
	}

	if (cma_listen_id) {
		tgt_event_del(cma_listen_id->channel->fd);

		err = rdma_destroy_id(cma_listen_id);
		if (err)
			eprintf("rdma_destroy_id failed: (errno=%d %m)\n", errno);

		rdma_destroy_event_channel(rdma_evt_channel);
	}
}

static int iser_send_nop = 1;
static struct tgt_work nop_work;

#define ISER_TIMER_INT_SEC      5

static void iser_nop_work_handler(void *data)
{
	struct iser_conn *conn;
	struct iser_task *task;

	list_for_each_entry(conn, &iser_conn_list, conn_list) {
		if (conn->h.state != STATE_FULL)
			continue;
		task = conn->nop_in_task;
		if (!task)
			continue;
		conn->nop_in_task = NULL;
		iser_send_ping_nop_in(task);
	}

	add_work(&nop_work, ISER_TIMER_INT_SEC);
}

static int iser_init(int index, char *args)
{
	int err;

	err = iser_ib_init();
	if (err) {
		iser_send_nop = 0;
		return err;
	}

	if (iser_send_nop) {
		nop_work.func = iser_nop_work_handler;
		nop_work.data = &nop_work;

		add_work(&nop_work, ISER_TIMER_INT_SEC);
	}

	return 0;
}

static void iser_exit(void)
{
	if (iser_send_nop)
		del_work(&nop_work);

	iser_ib_release();
}

static int iser_target_create(struct target *t)
{
	struct iscsi_target *target;
	int err;

	err = iscsi_target_create(t);
	if (err)
		return err;

	target = target_find_by_id(t->tid);
	assert(target != NULL);

	target->rdma = 1;
	target->session_param[ISCSI_PARAM_INITIAL_R2T_EN].val = 1;
	target->session_param[ISCSI_PARAM_IMM_DATA_EN].val = 0;

	return 0;
}

static const char *lld_param_port = "port";
static const char *lld_param_nop = "nop";
static const char *lld_param_on = "on";
static const char *lld_param_off = "off";
static const char *lld_param_pool_sz_mb = "pool_sz_mb";
static const char *lld_param_cq_vector = "cq_vector";

static int iser_param_parser(char *p)
{
	int err = 0;
	char *q;

	while (*p) {
		if (!strncmp(p, lld_param_port, strlen(lld_param_port))) {
			short int port_val;

			q = p + strlen(lld_param_port) + 1;
			port_val = strtol(q, NULL, 10);
			if (!errno)
				iser_listen_port = port_val;
			else {
				eprintf("invalid port supplied\n");
				err = -EINVAL;
				break;
			}
		} else if (!strncmp(p, lld_param_nop, strlen(lld_param_nop))) {
			q = p + strlen(lld_param_nop) + 1;
			if (!strncmp(q, lld_param_on, strlen(lld_param_on)))
				iser_send_nop = 1;
			else
			if (!strncmp(q, lld_param_off, strlen(lld_param_off)))
				iser_send_nop = 0;
			else {
				eprintf("unsupported value for param:%s\n",
					lld_param_nop);
				err = -EINVAL;
				break;
			}
		} else if (!strncmp(p, lld_param_pool_sz_mb,
				    strlen(lld_param_pool_sz_mb))) {
			q = p + strlen(lld_param_pool_sz_mb) + 1;
			buf_pool_sz_mb = atoi(q);
			if (buf_pool_sz_mb < 128)
				buf_pool_sz_mb = 128;
		} else if (!strncmp(p, lld_param_cq_vector,
				    strlen(lld_param_cq_vector))) {
			q = p + strlen(lld_param_cq_vector) + 1;
			cq_vector = atoi(q);
			if (cq_vector < 0) {
				eprintf("unsupported value for param: %s\n",
					lld_param_cq_vector);
				err = -EINVAL;
				break;
			}
		} else {
			dprintf("unsupported param:%s\n", p);
			err = -EINVAL;
			break;
		}

		p += strcspn(p, ",");
		if (*p == ',')
			++p;
	}
	return err;
}

static struct iscsi_transport iscsi_iser = {
	.name   		= "iser",
	.rdma   		= 1,
	.data_padding   	= 1,
	.ep_show		= iser_show,
	.ep_force_close		= iser_conn_force_close,
	.ep_getsockname 	= iser_getsockname,
	.ep_getpeername 	= iser_getpeername,
};

static struct tgt_driver iser = {
	.name   		= "iser",
	.init   		= iser_init,
	.exit   		= iser_exit,
	.target_create  	= iser_target_create,
	.target_destroy 	= iscsi_target_destroy,

	.update 		= iscsi_target_update,
	.show 			= iscsi_target_show,
	.stat                   = iscsi_stat,
	.cmd_end_notify 	= iser_scsi_cmd_done,
	.mgmt_end_notify	= iser_tm_done,
	.transportid    	= iscsi_transportid,
	.default_bst    	= "rdwr",
};

__attribute__ ((constructor))
static void iser_driver_constructor(void)
{
	register_driver(&iser);

	setup_param("iser", iser_param_parser);
}

