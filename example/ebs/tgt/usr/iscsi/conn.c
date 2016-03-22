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
#include <ctype.h>
#include <dirent.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/epoll.h>

#include "iscsid.h"
#include "tgtd.h"
#include "util.h"
#include "tgtadm_error.h"

void conn_add_to_session(struct iscsi_connection *conn, struct iscsi_session *session)
{
	if (!list_empty(&conn->clist)) {
		eprintf("%" PRIx64 " %u\n",
			sid64(session->isid, session->tsih), conn->cid);
		exit(0);
	}

	/* release in conn_free */
	session_get(session);
	conn->session = session;
	list_add(&conn->clist, &session->conn_list);
}

int conn_init(struct iscsi_connection *conn)
{
	conn->req_buffer = malloc(INCOMING_BUFSIZE);
	if (!conn->req_buffer)
		return -ENOMEM;

	conn->rsp_buffer = malloc(INCOMING_BUFSIZE);
	if (!conn->rsp_buffer) {
		free(conn->req_buffer);
		return -ENOMEM;
	}
	conn->rsp_buffer_size = INCOMING_BUFSIZE;

	conn->refcount = 1;
	conn->state = STATE_FREE;
	param_set_defaults(conn->session_param, session_keys);

	INIT_LIST_HEAD(&conn->clist);
	INIT_LIST_HEAD(&conn->tx_clist);
	INIT_LIST_HEAD(&conn->task_list);

	return 0;
}

void conn_exit(struct iscsi_connection *conn)
{
	struct iscsi_session *session = conn->session;

	list_del(&conn->clist);
	free(conn->req_buffer);
	free(conn->rsp_buffer);
	free(conn->initiator);
	if (conn->initiator_alias)
		free(conn->initiator_alias);

	if (session)
		session_put(session);
}

void conn_close(struct iscsi_connection *conn)
{
	struct iscsi_task *task, *tmp;
	int ret;

	if (conn->closed) {
		eprintf("already closed %p %u\n", conn, conn->refcount);
		return;
	}

	conn->closed = 1;

	ret = conn->tp->ep_close(conn);
	if (ret)
		eprintf("failed to close a connection, %p %u %s\n",
			conn, conn->refcount, strerror(errno));
	else
		eprintf("connection closed, %p %u\n", conn, conn->refcount);

	/* may not have been in FFP yet */
	if (!conn->session)
		goto done;

	dprintf("session %p %d\n", conn->session, conn->session->refcount);

	/*
	 * We just closed the ep so we are not going to send/recv anything.
	 * Just free these up since they are not going to complete.
	 */
	list_for_each_entry_safe(task, tmp, &conn->session->pending_cmd_list,
				 c_list) {
		if (task->conn != conn)
			continue;

		eprintf("Forcing release of pending task %p %" PRIx64 "\n",
			task, task->tag);
		list_del(&task->c_list);
		iscsi_free_task(task);
	}

	if (conn->tx_task) {
		eprintf("Add current tx task to the tx list for removal "
			"%p %" PRIx64 "\n",
			conn->tx_task, conn->tx_task->tag);
		list_add(&conn->tx_task->c_list, &conn->tx_clist);
		conn->tx_task = NULL;
	}

	list_for_each_entry_safe(task, tmp, &conn->tx_clist, c_list) {
		uint8_t op;

		op = task->req.opcode & ISCSI_OPCODE_MASK;

		eprintf("Forcing release of tx task %p %" PRIx64 " %x\n",
			task, task->tag, op);
		switch (op) {
		case ISCSI_OP_SCSI_CMD:
			/*
			 * We can't call iscsi_free_cmd_task for a
			 * command waiting for SCSI_DATA_OUT. There
			 * would be a better way to see
			 * task->scmd.c_target though.
			 */
			if (task->scmd.c_target)
				iscsi_free_cmd_task(task);
			else
				iscsi_free_task(task);
			break;
		case ISCSI_OP_NOOP_OUT:
		case ISCSI_OP_LOGOUT:
		case ISCSI_OP_SCSI_TMFUNC:
			iscsi_free_task(task);
			break;
		default:
			eprintf("%x\n", op);
			break;
		}
	}

	if (conn->rx_task) {
		eprintf("Forcing release of rx task %p %" PRIx64 "\n",
			conn->rx_task, conn->rx_task->tag);
		iscsi_free_task(conn->rx_task);
	}
	conn->rx_task = NULL;

	/* cleaning up commands waiting for SCSI_DATA_OUT */
	list_for_each_entry_safe(task, tmp, &conn->task_list, c_siblings) {
		/*
		 * This task is in SCSI. We need to wait for I/O
		 * completion.
		 */
		if (task_in_scsi(task))
			continue;
		iscsi_free_task(task);
	}
done:
	conn_put(conn);
}

void conn_put(struct iscsi_connection *conn)
{
	conn->refcount--;
	if (!conn->refcount)
		conn->tp->ep_release(conn);
}

int conn_get(struct iscsi_connection *conn)
{
	/* TODO: check state */
	conn->refcount++;
	return 0;
}

struct iscsi_connection *conn_find(struct iscsi_session *session, uint32_t cid)
{
	struct iscsi_connection *conn;

	list_for_each_entry(conn, &session->conn_list, clist) {
		if (conn->cid == cid)
			return conn;
	}

	return NULL;
}

int conn_take_fd(struct iscsi_connection *conn)
{
	dprintf("%u %u %u %" PRIx64 "\n", conn->cid, conn->stat_sn,
		conn->exp_stat_sn, sid64(conn->isid, conn->tsih));
	conn->session->conn_cnt++;
	return 0;
}

/* called by tgtadm */
tgtadm_err conn_close_admin(uint32_t tid, uint64_t sid, uint32_t cid)
{
	struct iscsi_target* target = NULL;
	struct iscsi_session *session;
	struct iscsi_connection *conn;
	int sess_found = 0;

	target = target_find_by_id(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	list_for_each_entry(session, &target->sessions_list, slist) {
		if (session->tsih == sid) {
			sess_found = 1;
			list_for_each_entry(conn, &session->conn_list, clist) {
				if (conn->cid == cid) {
					eprintf("close %" PRIx64 " %u\n", sid, cid);
					conn->tp->ep_force_close(conn);
					return TGTADM_SUCCESS;
				}
			}
		}
	}

	return sess_found ? TGTADM_NO_CONNECTION : TGTADM_NO_SESSION;
}

void iscsi_update_conn_stats_rx(struct iscsi_connection *conn, int size, int opcode)
{
	conn->stats.rxdata_octets += (uint64_t)size;

	if (unlikely(opcode < 0))
		return;

	if (opcode == ISCSI_OP_SCSI_CMD)
		conn->stats.scsicmd_pdus++;
	else if (opcode == ISCSI_OP_SCSI_DATA_OUT)
		conn->stats.dataout_pdus++;
}

void iscsi_update_conn_stats_tx(struct iscsi_connection *conn, int size, int opcode)
{
	conn->stats.txdata_octets += (uint64_t)size;

	if (unlikely(opcode < 0))
		return;

	if (opcode == ISCSI_OP_SCSI_DATA_IN)
		conn->stats.datain_pdus++;
	else if (opcode == ISCSI_OP_SCSI_CMD_RSP)
		conn->stats.scsirsp_pdus++;
}
