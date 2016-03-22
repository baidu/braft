/*
 * SCSI target management functions
 *
 * Copyright (C) 2005-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2005-2007 Mike Christie <michaelc@cs.wisc.edu>
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
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#include "list.h"
#include "tgtd.h"
#include "log.h"
#include "tgtadm.h"
#include "driver.h"
#include "util.h"

enum mgmt_task_state {
	MTASK_STATE_HDR_RECV,
	MTASK_STATE_PDU_RECV,
	MTASK_STATE_HDR_SEND,
	MTASK_STATE_PDU_SEND,
};

struct mgmt_task {
	enum mgmt_task_state mtask_state;
	int retry;
	int done;
	struct tgtadm_req req;
	char *req_buf;
	int req_bsize;
	struct tgtadm_rsp rsp;
	struct concat_buf rsp_concat;
/* 	struct tgt_work work; */
};

#define MAX_MGT_BUFSIZE	(8*1024) /* limit incoming mgmt request data size */

static int ipc_fd, ipc_lock_fd;
char mgmt_path[256];
char mgmt_lock_path[256];

static struct mgmt_task *mtask_alloc(void);
static void mtask_free(struct mgmt_task *mtask);

static tgtadm_err errno2tgtadm(int err)
{
	if (err >= 0)
		return TGTADM_SUCCESS;
	else if (err == -ENOMEM)
		return TGTADM_NOMEM;
	else
		return TGTADM_UNKNOWN_ERR;
}

static void set_mtask_result(struct mgmt_task *mtask, tgtadm_err adm_err)
{
	if (adm_err == TGTADM_SUCCESS && mtask->rsp_concat.err)
		adm_err = errno2tgtadm(mtask->rsp_concat.err);

	mtask->rsp.len = sizeof(mtask->rsp);
	if (adm_err == TGTADM_SUCCESS) {
		mtask->rsp.len += mtask->rsp_concat.size;
		mtask->rsp.err = 0;
	}
	else
		mtask->rsp.err = (uint32_t)adm_err;
}

static tgtadm_err target_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	switch (req->op) {
	case OP_NEW:
		adm_err = tgt_target_create(lld_no, req->tid, mtask->req_buf);
		break;
	case OP_DELETE:
		adm_err = tgt_target_destroy(lld_no, req->tid, req->force);
		break;
	case OP_BIND:
		/* FIXME */
		if (req->len == sizeof(*req))
			adm_err = tgt_bind_host_to_target(req->tid, req->host_no);
		else {
			char *p;

			p = strstr(mtask->req_buf, "initiator-address=");
			if (p) {
				p += strlen("initiator-address=");
				adm_err = acl_add(req->tid, p);
				if (adm_err != TGTADM_SUCCESS) {
					eprintf("Failed to bind by address: %s\n", p);
					break;
				}
			}

			p = strstr(mtask->req_buf, "initiator-name=");
			if (p) {
				p += strlen("initiator-name=");
				adm_err = iqn_acl_add(req->tid, p);
				if (adm_err != TGTADM_SUCCESS) {
					eprintf("Failed to bind by name: %s\n", p);
					break;
				}
			}
		}
		break;
	case OP_UNBIND:
		if (req->len == sizeof(*req))
			adm_err = tgt_unbind_host_to_target(req->tid, req->host_no);
		else {
			char *p;

			p = strstr(mtask->req_buf, "initiator-address=");
			if (p) {
				p += strlen("initiator-address=");
				adm_err = acl_del(req->tid, p);
				if (adm_err != TGTADM_SUCCESS) {
					eprintf("Failed to unbind by address: %s\n", p);
					break;
				}
			}

			p = strstr(mtask->req_buf, "initiator-name=");
			if (p) {
				p += strlen("initiator-name=");
				adm_err = iqn_acl_del(req->tid, p + strlen("initiator-name="));
				if (adm_err != TGTADM_SUCCESS) {
					eprintf("Failed to unbind by name: %s\n", p);
					break;
				}
			}
		}
		break;
	case OP_UPDATE:
	{
		char *p;
		adm_err = TGTADM_UNSUPPORTED_OPERATION;

		p = strchr(mtask->req_buf, '=');
		if (!p)
			break;
		*p++ = '\0';

		if (!strcmp(mtask->req_buf, "state")) {
			adm_err = tgt_set_target_state(req->tid, p);
		} else if (tgt_drivers[lld_no]->update)
			adm_err = tgt_drivers[lld_no]->update(req->mode, req->op, req->tid,
							  req->sid, req->lun,
							  req->cid, mtask->req_buf);
		break;
	}
	case OP_SHOW:
	{
		concat_buf_init(&mtask->rsp_concat);
		if (req->tid < 0)
			adm_err = tgt_target_show_all(&mtask->rsp_concat);
		else if (tgt_drivers[lld_no]->show)
			adm_err = tgt_drivers[lld_no]->show(req->mode,
							req->tid,
							req->sid,
							req->cid, req->lun,
							&mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	}
	case OP_STATS:
	{
		concat_buf_init(&mtask->rsp_concat);
		adm_err = tgt_stat_target_by_id(req->tid, &mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	}
	default:
		break;
	}

	return adm_err;
}

static tgtadm_err portal_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	switch (req->op) {
	case OP_SHOW:
		if (tgt_drivers[lld_no]->show) {
			concat_buf_init(&mtask->rsp_concat);
			adm_err = tgt_drivers[lld_no]->show(req->mode,
							req->tid, req->sid,
							req->cid, req->lun,
							&mtask->rsp_concat);
			concat_buf_finish(&mtask->rsp_concat);
		}
		break;
	case OP_NEW:
		adm_err = tgt_portal_create(lld_no, mtask->req_buf);
		break;
	case OP_DELETE:
		adm_err = tgt_portal_destroy(lld_no, mtask->req_buf);
		break;
	default:
		break;
	}

	return adm_err;
}

static tgtadm_err device_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	char *params = mtask->req_buf;
	tgtadm_err adm_err = TGTADM_UNSUPPORTED_OPERATION;

	switch (req->op) {
	case OP_NEW:
		eprintf("sz:%d params:%s\n",mtask->req_bsize,params);
		adm_err = tgt_device_create(req->tid, req->device_type, req->lun,
					    params, 1);
		break;
	case OP_DELETE:
		adm_err = tgt_device_destroy(req->tid, req->lun, 0);
		break;
	case OP_UPDATE:
		adm_err = tgt_device_update(req->tid, req->lun, params);
		break;
	case OP_STATS:
		concat_buf_init(&mtask->rsp_concat);
		if (!req->sid)
			adm_err = tgt_stat_device_by_id(req->tid, req->lun,
							&mtask->rsp_concat);
		else if (tgt_drivers[lld_no]->stat)
			adm_err = tgt_drivers[lld_no]->stat(req->mode, req->tid,
							    req->sid, req->cid, req->lun,
							    &mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	default:
		break;
	}

	return adm_err;
}

static tgtadm_err account_mgmt(int lld_no,  struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	char *user, *password;
	tgtadm_err adm_err = TGTADM_UNSUPPORTED_OPERATION;

	switch (req->op) {
	case OP_NEW:
	case OP_DELETE:
	case OP_BIND:
	case OP_UNBIND:
		user = strstr(mtask->req_buf, "user=");
		if (!user)
			return TGTADM_INVALID_REQUEST;
		user += 5;

		if (req->op == OP_NEW) {
			password = strchr(user, ',');
			if (!password)
				return TGTADM_INVALID_REQUEST;

			*password++ = '\0';
			password += strlen("password=");

			adm_err = account_add(user, password);
		} else {
			if (req->op == OP_DELETE) {
				adm_err = account_del(user);
			} else
				adm_err = account_ctl(req->tid, req->ac_dir,
						      user, req->op == OP_BIND);
		}
		break;
	case OP_SHOW:
		concat_buf_init(&mtask->rsp_concat);
		adm_err = account_show(&mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	default:
		break;
	}

	return adm_err;
}

static tgtadm_err sys_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	switch (req->op) {
	case OP_UPDATE:
		if (!strncmp(mtask->req_buf, "debug=", 6)) {
			if (!strncmp(mtask->req_buf+6, "on", 2)) {
				is_debug = 1;
				adm_err = TGTADM_SUCCESS;
			} else if (!strncmp(mtask->req_buf+6, "off", 3)) {
				is_debug = 0;
				adm_err = TGTADM_SUCCESS;
			}
			if (adm_err == TGTADM_SUCCESS)
				eprintf("set debug to: %d\n", is_debug);
		} else if (tgt_drivers[lld_no]->update)
			adm_err = tgt_drivers[lld_no]->update(req->mode, req->op,
							  req->tid,
							  req->sid, req->lun,
							  req->cid, mtask->req_buf);

		break;
	case OP_SHOW:
		concat_buf_init(&mtask->rsp_concat);
		adm_err = system_show(req->mode, &mtask->rsp_concat);
		if (tgt_drivers[lld_no]->show)
			adm_err = tgt_drivers[lld_no]->show(req->mode,
							req->tid, req->sid,
							req->cid, req->lun,
							&mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	case OP_STATS:
		concat_buf_init(&mtask->rsp_concat);
		adm_err = tgt_stat_system(&mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	case OP_DELETE:
		if (is_system_inactive())
			adm_err = TGTADM_SUCCESS;
		break;
	default:
		break;
	}

	return adm_err;
}

static tgtadm_err session_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	int adm_err = TGTADM_INVALID_REQUEST;

	switch (req->op) {
	case OP_STATS:
		if (tgt_drivers[lld_no]->stat) {
			concat_buf_init(&mtask->rsp_concat);
			adm_err = tgt_drivers[lld_no]->stat(req->mode,
							    req->tid, req->sid,
							    req->cid, req->lun,
							    &mtask->rsp_concat);
			concat_buf_finish(&mtask->rsp_concat);
		}
		break;
	default:
		if (tgt_drivers[lld_no]->update)
			adm_err = tgt_drivers[lld_no]->update(req->mode, req->op,
							      req->tid,
							      req->sid, req->lun,
							      req->cid, mtask->req_buf);
		break;
	}

	return adm_err;
}

static tgtadm_err connection_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	switch (req->op) {
	case OP_SHOW:
		if (tgt_drivers[lld_no]->show) {
			concat_buf_init(&mtask->rsp_concat);
			adm_err = tgt_drivers[lld_no]->show(req->mode,
							    req->tid, req->sid,
							    req->cid, req->lun,
							    &mtask->rsp_concat);
			concat_buf_finish(&mtask->rsp_concat);
			break;
		}
		break;
	case OP_STATS:
		if (tgt_drivers[lld_no]->stat) {
			concat_buf_init(&mtask->rsp_concat);
			adm_err = tgt_drivers[lld_no]->stat(req->mode,
							    req->tid, req->sid,
							    req->cid, req->lun,
							    &mtask->rsp_concat);
			concat_buf_finish(&mtask->rsp_concat);
		}
		break;
	default:
		if (tgt_drivers[lld_no]->update)
			adm_err = tgt_drivers[lld_no]->update(req->mode, req->op,
							      req->tid,
							      req->sid, req->lun,
							      req->cid, mtask->req_buf);
		break;
	}

	return adm_err;
}

static tgtadm_err lld_mgmt(int lld_no, struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	switch (req->op) {
	case OP_START:
		if (tgt_drivers[lld_no]->drv_state != DRIVER_INIT) {
			if (!lld_init_one(lld_no))
				adm_err = TGTADM_SUCCESS;
			else
				adm_err = TGTADM_UNKNOWN_ERR;
		} else
			adm_err = TGTADM_SUCCESS;
		break;
	case OP_STOP:
		if (tgt_drivers[lld_no]->drv_state == DRIVER_INIT) {
			if (list_empty(&tgt_drivers[lld_no]->target_list)) {
				if (tgt_drivers[lld_no]->exit) {
					tgt_drivers[lld_no]->exit();
					tgt_drivers[lld_no]->drv_state = DRIVER_EXIT;
				}
				adm_err = TGTADM_SUCCESS;
			} else
				adm_err = TGTADM_DRIVER_ACTIVE;
		} else
			adm_err = TGTADM_SUCCESS;
		break;
	case OP_SHOW:
		concat_buf_init(&mtask->rsp_concat);
		adm_err = lld_show(&mtask->rsp_concat);
		concat_buf_finish(&mtask->rsp_concat);
		break;
	default:
		break;
	}

	return adm_err;
}

static tgtadm_err mtask_execute(struct mgmt_task *mtask)
{
	struct tgtadm_req *req = &mtask->req;
	int lld_no;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	req->lld[TGT_LLD_NAME_LEN - 1] = '\0';

	if (!strlen(req->lld))
		lld_no = 0;
	else {
		lld_no = get_driver_index(req->lld);
		if (lld_no < 0 ||
		   (tgt_drivers[lld_no]->drv_state != DRIVER_INIT &&
		    req->mode != MODE_LLD))
		{
			if (lld_no < 0)
				eprintf("can't find the driver %s\n", req->lld);
			else
				eprintf("driver %s is in state: %s\n",
					req->lld, driver_state_name(tgt_drivers[lld_no]));
			return TGTADM_NO_DRIVER;
		}
	}

	dprintf("%d %d %d %d %d %" PRIx64 " %" PRIx64 " %s %d\n",
		req->len, lld_no, req->mode, req->op,
		req->tid, req->sid, req->lun, mtask->req_buf, getpid());

	switch (req->mode) {
	case MODE_SYSTEM:
		adm_err = sys_mgmt(lld_no, mtask);
		break;
	case MODE_TARGET:
		adm_err = target_mgmt(lld_no, mtask);
		break;
	case MODE_PORTAL:
		adm_err = portal_mgmt(lld_no, mtask);
		break;
	case MODE_DEVICE:
		adm_err = device_mgmt(lld_no, mtask);
		break;
	case MODE_ACCOUNT:
		adm_err = account_mgmt(lld_no, mtask);
		break;
	case MODE_SESSION:
		adm_err = session_mgmt(lld_no, mtask);
		break;
	case MODE_CONNECTION:
		adm_err = connection_mgmt(lld_no, mtask);
		break;
	case MODE_LLD:
		adm_err = lld_mgmt(lld_no, mtask);
		break;
	default:
		if (req->op == OP_SHOW && tgt_drivers[lld_no]->show) {
			concat_buf_init(&mtask->rsp_concat);
			adm_err = tgt_drivers[lld_no]->show(req->mode,
							    req->tid, req->sid,
							    req->cid, req->lun,
							    &mtask->rsp_concat);
			concat_buf_finish(&mtask->rsp_concat);
		} else
			eprintf("unsupported mode: %d\n", req->mode);
		break;
	}

	return adm_err;
}

static int ipc_accept(int accept_fd)
{
	struct sockaddr addr;
	socklen_t len;
	int fd;

	len = sizeof(addr);
	fd = accept(accept_fd, (struct sockaddr *) &addr, &len);
	if (fd < 0)
		eprintf("can't accept a new connection, %m\n");
	return fd;
}

static int ipc_perm(int fd)
{
	struct ucred cred;
	socklen_t len;
	int err;

	len = sizeof(cred);
	err = getsockopt(fd, SOL_SOCKET, SO_PEERCRED, (void *) &cred, &len);
	if (err) {
		eprintf("can't get sockopt, %m\n");
		return -1;
	}

	if (cred.uid || cred.gid)
		return -EPERM;

	return 0;
}

static struct mgmt_task *mtask_alloc(void)
{
	struct mgmt_task *mtask;

	mtask = zalloc(sizeof(*mtask));
	if (!mtask) {
		eprintf("can't allocate mtask\n");
		return NULL;
	}
	mtask->mtask_state = MTASK_STATE_HDR_RECV;

	dprintf("mtask:%p\n", mtask);
	return mtask;
}

static void mtask_free(struct mgmt_task *mtask)
{
	dprintf("mtask:%p\n", mtask);

	if (mtask->req_buf)
		free(mtask->req_buf);
	concat_buf_release(&mtask->rsp_concat);
	free(mtask);
}

static int mtask_received(struct mgmt_task *mtask, int fd)
{
	tgtadm_err adm_err;
	int err;

	adm_err = mtask_execute(mtask);
	set_mtask_result(mtask, adm_err);

	/* whatever the result of mtask execution, a response is sent */
	mtask->mtask_state = MTASK_STATE_HDR_SEND;
	mtask->done = 0;
	err = tgt_event_modify(fd, EPOLLOUT);
	if (err)
		eprintf("failed to modify mgmt task event out\n");
	return err;
}

static void mtask_recv_send_handler(int fd, int events, void *data)
{
	int err, len;
	char *p;
	struct mgmt_task *mtask = data;
	struct tgtadm_req *req = &mtask->req;
	struct tgtadm_rsp *rsp = &mtask->rsp;

	switch (mtask->mtask_state) {
	case MTASK_STATE_HDR_RECV:
		len = sizeof(*req) - mtask->done;
		err = read(fd, (char *)req + mtask->done, len);
		if (err > 0) {
			mtask->done += err;
			if (mtask->done == sizeof(*req)) {
				mtask->req_bsize = req->len - sizeof(*req);
				if (!mtask->req_bsize) {
					err = mtask_received(mtask, fd);
					if (err)
						goto out;
				} else {
					/* the pdu exists */
					if (mtask->req_bsize > MAX_MGT_BUFSIZE) {
						eprintf("mtask buffer len: %d too large\n",
							mtask->req_bsize);
						mtask->req_bsize = 0;
						goto out;
					}
					mtask->req_buf = zalloc(mtask->req_bsize);
					if (!mtask->req_buf) {
						eprintf("can't allocate mtask buffer len: %d\n",
							mtask->req_bsize);
						mtask->req_bsize = 0;
						goto out;
					}
					mtask->mtask_state = MTASK_STATE_PDU_RECV;
					mtask->done = 0;
				}
			}
		} else
			if (errno != EAGAIN)
				goto out;

		break;
	case MTASK_STATE_PDU_RECV:
		len = mtask->req_bsize - mtask->done;
		err = read(fd, mtask->req_buf + mtask->done, len);
		if (err > 0) {
			mtask->done += err;
			if (mtask->done == mtask->req_bsize) {
				err = mtask_received(mtask, fd);
				if (err)
					goto out;
			}
		} else
			if (errno != EAGAIN)
				goto out;

		break;
	case MTASK_STATE_HDR_SEND:
		p = (char *)rsp + mtask->done;
		len = sizeof(*rsp) - mtask->done;

		err = write(fd, p, len);
		if (err > 0) {
			mtask->done += err;
			if (mtask->done == sizeof(*rsp)) {
				if (rsp->len == sizeof(*rsp))
					goto out;
				mtask->done = 0;
				mtask->mtask_state = MTASK_STATE_PDU_SEND;
			}
		} else
			if (errno != EAGAIN)
				goto out;

		break;
	case MTASK_STATE_PDU_SEND:
		err = concat_write(&mtask->rsp_concat, fd, mtask->done);
		if (err >= 0) {
			mtask->done += err;
			if (mtask->done == (rsp->len - sizeof(*rsp)))
				goto out;
		} else
			if (errno != EAGAIN)
				goto out;

		break;
	default:
		eprintf("unknown state %d\n", mtask->mtask_state);
	}

	return;
out:
	if (req->mode == MODE_SYSTEM && req->op == OP_DELETE && !rsp->err)
		system_active = 0;
	tgt_event_del(fd);
	close(fd);
	mtask_free(mtask);
}

static void mgmt_event_handler(int accept_fd, int events, void *data)
{
	int fd, err;
	struct mgmt_task *mtask;

	fd = ipc_accept(accept_fd);
	if (fd < 0) {
		eprintf("failed to accept a socket\n");
		return;
	}

	err = ipc_perm(fd);
	if (err < 0) {
		eprintf("permission error\n");
		goto out;
	}

	err = set_non_blocking(fd);
	if (err) {
		eprintf("failed to set a socket non-blocking\n");
		goto out;
	}

	mtask = mtask_alloc();
	if (!mtask)
		goto out;

	err = tgt_event_add(fd, EPOLLIN, mtask_recv_send_handler, mtask);
	if (err) {
		eprintf("failed to add a socket to epoll %d\n", fd);
		mtask_free(mtask);
		goto out;
	}

	return;
out:
	if (fd > 0)
		close(fd);

	return;
}

int ipc_init(void)
{
	extern short control_port;
	int fd = 0, err;
	struct sockaddr_un addr;

	sprintf(mgmt_lock_path, "%s.%d.lock", TGT_IPC_NAMESPACE, control_port);
	ipc_lock_fd = open(mgmt_lock_path, O_WRONLY | O_CREAT,
			   S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	if (ipc_lock_fd < 0) {
		eprintf("failed to open lock file for management IPC\n");
		return -1;
	}

	if (lockf(ipc_lock_fd, F_TLOCK, 1) < 0) {
		if (errno == EACCES || errno == EAGAIN)
			eprintf("another tgtd is using %s\n", mgmt_lock_path);
		else
			eprintf("unable to get lock of management IPC: %s"\
				" (errno: %m)\n", mgmt_lock_path);
		goto close_lock_fd;
	}

	fd = socket(AF_LOCAL, SOCK_STREAM, 0);
	if (fd < 0) {
		eprintf("can't open a socket, %m\n");
		goto close_lock_fd;
	}

	sprintf(mgmt_path, "%s.%d", TGT_IPC_NAMESPACE, control_port);
	unlink(mgmt_path);
	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_LOCAL;
	strncpy(addr.sun_path, mgmt_path, sizeof(addr.sun_path));

	err = bind(fd, (struct sockaddr *) &addr, sizeof(addr));
	if (err) {
		eprintf("can't bind a socket, %m\n");
		goto close_ipc_fd;
	}

	err = listen(fd, 32);
	if (err) {
		eprintf("can't listen a socket, %m\n");
		goto close_ipc_fd;
	}

	err = tgt_event_add(fd, EPOLLIN, mgmt_event_handler, NULL);
	if (err)
		goto close_ipc_fd;

	ipc_fd = fd;

	return 0;

close_ipc_fd:
	close(fd);
close_lock_fd:
	close(ipc_lock_fd);
	return -1;
}

void ipc_exit(void)
{
	tgt_event_del(ipc_fd);
	close(ipc_fd);
	close(ipc_lock_fd);
}
