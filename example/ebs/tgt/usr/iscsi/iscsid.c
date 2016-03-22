/*
 * Software iSCSI target protocol routines
 *
 * Copyright (C) 2005-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2005-2007 Mike Christie <michaelc@cs.wisc.edu>
 * Copyright (C) 2007 Pete Wyckoff <pw@osc.edu>
 *
 * This code is based on Ardis's iSCSI implementation.
 *   http://www.ardistech.com/iscsi/
 *   Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>
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
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>

#include "iscsid.h"
#include "tgtd.h"
#include "util.h"
#include "driver.h"
#include "scsi.h"
#include "tgtadm.h"
#include "crc32c.h"

int default_nop_interval;
int default_nop_count;

LIST_HEAD(iscsi_portals_list);

char *portal_arguments;

enum {
	IOSTATE_FREE,

	IOSTATE_RX_BHS,
	IOSTATE_RX_INIT_AHS,
	IOSTATE_RX_AHS,
	IOSTATE_RX_INIT_HDIGEST,
	IOSTATE_RX_HDIGEST,
	IOSTATE_RX_CHECK_HDIGEST,
	IOSTATE_RX_INIT_DATA,
	IOSTATE_RX_DATA,
	IOSTATE_RX_INIT_DDIGEST,
	IOSTATE_RX_DDIGEST,
	IOSTATE_RX_CHECK_DDIGEST,
	IOSTATE_RX_END,

	IOSTATE_TX_BHS,
	IOSTATE_TX_INIT_AHS,
	IOSTATE_TX_AHS,
	IOSTATE_TX_INIT_HDIGEST,
	IOSTATE_TX_HDIGEST,
	IOSTATE_TX_INIT_DATA,
	IOSTATE_TX_DATA,
	IOSTATE_TX_INIT_DDIGEST,
	IOSTATE_TX_DDIGEST,
	IOSTATE_TX_END,
};

void conn_read_pdu(struct iscsi_connection *conn)
{
	conn->rx_iostate = IOSTATE_RX_BHS;
	conn->rx_buffer = (void *)&conn->req.bhs;
	conn->rx_size = BHS_SIZE;
}

static void conn_write_pdu(struct iscsi_connection *conn)
{
	conn->tx_iostate = IOSTATE_TX_BHS;
	memset(&conn->rsp, 0, sizeof(conn->rsp));
	conn->tx_buffer = (void *)&conn->rsp.bhs;
	conn->tx_size = BHS_SIZE;
}

static struct iscsi_key login_keys[] = {
	{"InitiatorName",},
	{"InitiatorAlias",},
	{"SessionType",},
	{"TargetName",},
	{NULL, 0, 0, 0, NULL},
};

char *text_key_find(struct iscsi_connection *conn, char *searchKey)
{
	char *data, *key, *value;
	int keylen, datasize;

	keylen = strlen(searchKey);
	data = conn->req.data;
	datasize = conn->req.datasize;

	while (1) {
		for (key = data; datasize > 0 && *data != '='; data++, datasize--)
			;
		if (!datasize)
			return NULL;
		data++;
		datasize--;

		for (value = data; datasize > 0 && *data != 0; data++, datasize--)
			;
		if (!datasize)
			return NULL;
		data++;
		datasize--;

		if (keylen == value - key - 1
		     && !strncmp(key, searchKey, keylen))
			return value;
	}
}

static char *next_key(char **data, int *datasize, char **value)
{
	char *key, *p, *q;
	int size = *datasize;

	key = p = *data;
	for (; size > 0 && *p != '='; p++, size--)
		;
	if (!size)
		return NULL;
	*p++ = 0;
	size--;

	for (q = p; size > 0 && *p != 0; p++, size--)
		;
	if (!size)
		return NULL;
	p++;
	size--;

	*data = p;
	*value = q;
	*datasize = size;

	return key;
}

void text_key_add(struct iscsi_connection *conn, char *key, char *value)
{
	int keylen = strlen(key);
	int valuelen = strlen(value);
	int len = keylen + valuelen + 2;
	char *buffer;
	int max_len;

	if (conn->state == STATE_FULL)
		max_len = conn->session_param[ISCSI_PARAM_MAX_XMIT_DLENGTH].val;
	else
		max_len = conn->rsp_buffer_size;

	if (!conn->rsp.datasize)
		conn->rsp.data = conn->rsp_buffer;

	buffer = conn->rsp_buffer;

	if (conn->rsp.datasize + len > max_len &&
	    (conn->req.bhs.opcode & ISCSI_OPCODE_MASK) != ISCSI_OP_TEXT)
		goto drop;

	if (conn->rsp.datasize + len > conn->rsp_buffer_size) {
		buffer = realloc(buffer, conn->rsp.datasize + len);
		if (buffer) {
			conn->rsp_buffer = buffer;
			conn->rsp.data = conn->rsp_buffer;
			conn->rsp_buffer_size = conn->rsp.datasize + len;
		} else
			goto drop;
	}

	buffer += conn->rsp.datasize;
	conn->rsp.datasize += len;

	strcpy(buffer, key);
	buffer += keylen;
	*buffer++ = '=';
	strcpy(buffer, value);
	return;
drop:
	log_warning("Dropping key (%s=%s)", key, value);
	return;
}

static void text_key_add_reject(struct iscsi_connection *conn, char *key)
{
	text_key_add(conn, key, "Reject");
}

static void text_scan_security(struct iscsi_connection *conn)
{
	struct iscsi_login_rsp *rsp = (struct iscsi_login_rsp *)&conn->rsp.bhs;
	char *key, *value, *data, *nextValue;
	int datasize;

	data = conn->req.data;
	datasize = conn->req.datasize;

	while ((key = next_key(&data, &datasize, &value))) {
		if (!(param_index_by_name(key, login_keys) < 0))
			;
		else if (!strcmp(key, "AuthMethod")) {
			do {
				nextValue = strchr(value, ',');
				if (nextValue)
					*nextValue++ = 0;

				if (!strcmp(value, "None")) {
					if (account_available(conn->tid, AUTH_DIR_INCOMING))
						continue;
					conn->auth_method = AUTH_NONE;
					text_key_add(conn, key, "None");
					break;
				} else if (!strcmp(value, "CHAP")) {
					if (!account_available(conn->tid, AUTH_DIR_INCOMING))
						continue;
					conn->auth_method = AUTH_CHAP;
					text_key_add(conn, key, "CHAP");
					break;
				}
			} while ((value = nextValue));

			if (conn->auth_method == AUTH_UNKNOWN)
				text_key_add_reject(conn, key);
		} else
			text_key_add(conn, key, "NotUnderstood");
	}
	if (conn->auth_method == AUTH_UNKNOWN) {
		rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp->status_detail = ISCSI_LOGIN_STATUS_AUTH_FAILED;
		conn->state = STATE_EXIT;
	}
}

static void login_security_done(struct iscsi_connection *conn)
{
	struct iscsi_login *req = (struct iscsi_login *)&conn->req.bhs;
	struct iscsi_login_rsp *rsp = (struct iscsi_login_rsp *) &conn->rsp.bhs;
	struct iscsi_session *session;

	if (!conn->tid)
		return;

	session = session_find_name(conn->tid, conn->initiator, req->isid);
	if (session) {
		if (!req->tsih) {
			struct iscsi_connection *ent, *next;

			/* do session reinstatement */

			list_for_each_entry_safe(ent, next, &session->conn_list,
						 clist) {
				conn_close(ent);
			}

			session = NULL;
		} else if (req->tsih != session->tsih) {
			/* fail the login */
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			conn->state = STATE_EXIT;
			return;
		} else if (conn_find(session, conn->cid)) {
			/* do connection reinstatement */
		}

		/* add a new connection to the session */
		if (session)
			conn_add_to_session(conn, session);
	} else {
		if (req->tsih) {
			/* fail the login */
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_NO_SESSION;
			conn->state = STATE_EXIT;
			return;
		}
		/*
		 * We do nothing here and instantiate a new session
		 * later at login_finish().
		 */
	}
}

static void text_scan_login(struct iscsi_connection *conn)
{
	char *key, *value, *data;
	int datasize, idx, is_rdma = 0;
	struct iscsi_login_rsp *rsp = (struct iscsi_login_rsp *)&conn->rsp.bhs;

	data = conn->req.data;
	datasize = conn->req.datasize;

	while ((key = next_key(&data, &datasize, &value))) {
		if (!(param_index_by_name(key, login_keys) < 0))
			;
		else if (!strcmp(key, "AuthMethod"))
			;
		else if (!((idx = param_index_by_name(key, session_keys)) < 0)) {
			int err;
			unsigned int val;
			char buf[32];

			if (idx == ISCSI_PARAM_MAX_RECV_DLENGTH)
				idx = ISCSI_PARAM_MAX_XMIT_DLENGTH;

			if (idx == ISCSI_PARAM_RDMA_EXTENSIONS)
				is_rdma = 1;

			if (param_str_to_val(session_keys, idx, value, &val) < 0) {
				if (conn->session_param[idx].state
				    == KEY_STATE_START) {
					text_key_add_reject(conn, key);
					continue;
				} else {
					rsp->status_class =
						ISCSI_STATUS_CLS_INITIATOR_ERR;
					rsp->status_detail =
						ISCSI_LOGIN_STATUS_INIT_ERR;
					conn->state = STATE_EXIT;
					goto out;
				}
			}

			err = param_check_val(session_keys, idx, &val);
			if (err) {
				text_key_add_reject(conn, key);
				continue;
			}
			if (idx >= ISCSI_PARAM_FIRST_LOCAL)
				conn->session_param[idx].val = val;
			else
				param_set_val(session_keys,
					      conn->session_param,
					      idx, &val);

			switch (conn->session_param[idx].state) {
			case KEY_STATE_START:
				if (idx >= ISCSI_PARAM_FIRST_LOCAL)
					break;
				memset(buf, 0, sizeof(buf));
				param_val_to_str(session_keys, idx, val, buf);
				text_key_add(conn, key, buf);
				break;
			case KEY_STATE_REQUEST:
				if (val != conn->session_param[idx].val) {
					rsp->status_class =
						ISCSI_STATUS_CLS_INITIATOR_ERR;
					rsp->status_detail =
						ISCSI_LOGIN_STATUS_INIT_ERR;
					conn->state = STATE_EXIT;
					log_warning("%s %u %u\n", key,
					val, conn->session_param[idx].val);
					goto out;
				}
				break;
			case KEY_STATE_DONE:
				break;
			}
			conn->session_param[idx].state = KEY_STATE_DONE;
		} else
			text_key_add(conn, key, "NotUnderstood");
	}

	if (is_rdma) {
		/* do not try to do digests, not supported in iser */
		conn->session_param[ISCSI_PARAM_HDRDGST_EN].val = DIGEST_NONE;
		conn->session_param[ISCSI_PARAM_DATADGST_EN].val = DIGEST_NONE;
	} else {
		/* do not offer RDMA, initiator must explicitly request */
		conn->session_param[ISCSI_PARAM_RDMA_EXTENSIONS].val = 0;
	}

out:
	return;
}

static int text_check_param(struct iscsi_connection *conn)
{
	struct param *p = conn->session_param;
	char buf[32];
	int i, cnt;

	for (i = 0, cnt = 0; session_keys[i].name; i++) {
		if (p[i].state == KEY_STATE_START && p[i].val != session_keys[i].def) {
			if (conn->state == STATE_LOGIN) {
				if (i >= ISCSI_PARAM_FIRST_LOCAL) {
					if (p[i].val > session_keys[i].def)
						p[i].val = session_keys[i].def;
					p[i].state = KEY_STATE_DONE;
					continue;
				}
				if (p[ISCSI_PARAM_RDMA_EXTENSIONS].val == 1) {
					if (i == ISCSI_PARAM_MAX_RECV_DLENGTH)
						continue;
				} else {
					if (i >= ISCSI_PARAM_RDMA_EXTENSIONS)
						continue;
				}
				memset(buf, 0, sizeof(buf));
				param_val_to_str(session_keys, i, p[i].val,
						 buf);
				text_key_add(conn, session_keys[i].name, buf);
				p[i].state = KEY_STATE_REQUEST;
			}
			cnt++;
		}
	}

	return cnt;
}

static void login_start(struct iscsi_connection *conn)
{
	struct iscsi_login *req = (struct iscsi_login *)&conn->req.bhs;
	struct iscsi_login_rsp *rsp = (struct iscsi_login_rsp *)&conn->rsp.bhs;
	char *name, *alias, *session_type, *target_name;
	struct iscsi_target *target;
	char buf[NI_MAXHOST + NI_MAXSERV + 4];
	int reason, redir;

	conn->cid = be16_to_cpu(req->cid);
	memcpy(conn->isid, req->isid, sizeof(req->isid));
	conn->tsih = req->tsih;

	if (!sid64(conn->isid, conn->tsih)) {
		rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp->status_detail = ISCSI_LOGIN_STATUS_MISSING_FIELDS;
		conn->state = STATE_EXIT;
		return;
	}

	name = text_key_find(conn, "InitiatorName");
	if (!name) {
		rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp->status_detail = ISCSI_LOGIN_STATUS_MISSING_FIELDS;
		conn->state = STATE_EXIT;
		return;
	}
	conn->initiator = strdup(name);

	alias = text_key_find(conn, "InitiatorAlias");
	if (alias)
		conn->initiator_alias = strdup(alias);

	session_type = text_key_find(conn, "SessionType");
	target_name = text_key_find(conn, "TargetName");

	conn->auth_method = -1;
	conn->session_type = SESSION_NORMAL;

	if (session_type) {
		if (!strcmp(session_type, "Discovery"))
			conn->session_type = SESSION_DISCOVERY;
		else if (strcmp(session_type, "Normal")) {
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_NO_SESSION_TYPE;
			conn->state = STATE_EXIT;
			return;
		}
	}

	if (conn->session_type == SESSION_DISCOVERY)
		conn->tid = GLOBAL_TID;

	if (conn->session_type == SESSION_NORMAL) {
		if (!target_name) {
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_MISSING_FIELDS;
			conn->state = STATE_EXIT;
			return;
		}

		target = target_find_by_name(target_name);
		if (!target) {
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			conn->state = STATE_EXIT;
			return;
		}
		if (target->rdma) {
			eprintf("Target %s is RDMA, but conn cid:%d from %s is TCP\n",
				target_name, conn->cid, conn->initiator);
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			conn->state = STATE_EXIT;
			return;
		}

		conn->tid = target->tid;

		redir = target_redirected(target, conn, buf, &reason);
		if (redir < 0) {
			rsp->status_class = ISCSI_STATUS_CLS_TARGET_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TARGET_ERROR;
			conn->state = STATE_EXIT;
			return;
		}
		else if (redir) {
			text_key_add(conn, "TargetAddress", buf);
			rsp->status_class = ISCSI_STATUS_CLS_REDIRECT;
			rsp->status_detail = reason;
			conn->state = STATE_EXIT;
			return;
		}

		if (tgt_get_target_state(target->tid) != SCSI_TARGET_READY) {
			rsp->status_class = ISCSI_STATUS_CLS_TARGET_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TARGET_ERROR;
			conn->state = STATE_EXIT;
			return;
		}

		if (ip_acl(conn->tid, conn)) {
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			conn->state = STATE_EXIT;
			return;
		}

		if (iqn_acl(conn->tid, conn)) {
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			conn->state = STATE_EXIT;
			return;
		}

		if (isns_scn_access(conn->tid, name)) {
			rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			conn->state = STATE_EXIT;
			return;
		}

/* 		if (conn->target->max_sessions && */
/* 		    (++conn->target->session_cnt > conn->target->max_sessions)) { */
/* 			conn->target->session_cnt--; */
/* 			rsp->status_class = ISCSI_STATUS_INITIATOR_ERR; */
/* 			rsp->status_detail = ISCSI_STATUS_TOO_MANY_CONN; */
/* 			conn->state = STATE_EXIT; */
/* 			return; */
/* 		} */

		memcpy(conn->session_param, target->session_param,
		       sizeof(conn->session_param));
	}

	conn->exp_cmd_sn = be32_to_cpu(req->cmdsn);
	conn->max_cmd_sn = conn->exp_cmd_sn + 1;
	dprintf("exp_cmd_sn: %d,%d\n", conn->exp_cmd_sn, req->cmdsn);

	text_key_add(conn, "TargetPortalGroupTag", "1");
}

static void login_finish(struct iscsi_connection *conn)
{
	struct iscsi_login_rsp *rsp = (struct iscsi_login_rsp *) &conn->rsp.bhs;
	int ret;
	uint8_t class, detail;

	switch (conn->session_type) {
	case SESSION_NORMAL:
		/*
		 * Allocate transport resources for this connection.
		 */
		ret = conn->tp->ep_login_complete(conn);
		if (ret) {
			class = ISCSI_STATUS_CLS_TARGET_ERR;
			detail = ISCSI_LOGIN_STATUS_NO_RESOURCES;
			goto fail;
		}
		if (!conn->session) {
			ret = session_create(conn);
			if (ret) {
				class = ISCSI_STATUS_CLS_TARGET_ERR;
				detail = ISCSI_LOGIN_STATUS_TARGET_ERROR;
				goto fail;
			}
		} else {
			if (conn->tp->rdma ^ conn->session->rdma) {
				eprintf("new conn rdma %d, but session %d\n",
					conn->tp->rdma, conn->session->rdma);

				class = ISCSI_STATUS_CLS_INITIATOR_ERR;
				detail =ISCSI_LOGIN_STATUS_INVALID_REQUEST;
				goto fail;
			}
		}
		memcpy(conn->isid, conn->session->isid, sizeof(conn->isid));
		conn->tsih = conn->session->tsih;
		break;
	case SESSION_DISCOVERY:
		/* set a dummy tsih value */
		conn->tsih = 1;
		break;
	}

	return;
fail:
	rsp->flags = 0;
	rsp->status_class = class;
	rsp->status_detail = detail;
	conn->state = STATE_EXIT;
	return;
}

static int cmnd_exec_auth(struct iscsi_connection *conn)
{
       int res;

        switch (conn->auth_method) {
        case AUTH_CHAP:
                res = cmnd_exec_auth_chap(conn);
                break;
        case AUTH_NONE:
                res = 0;
                break;
        default:
                eprintf("Unknown auth. method %d\n", conn->auth_method);
                res = -3;
        }

        return res;
}

static void cmnd_reject(struct iscsi_connection *conn, uint8_t reason)
{
	struct iscsi_reject *rsp = (struct iscsi_reject *)&conn->rsp.bhs;

	memset(rsp, 0, BHS_SIZE);

	rsp->opcode = ISCSI_OP_REJECT;
	rsp->reason = reason;
	rsp->ffffffff = ISCSI_RESERVED_TAG;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;

	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->max_cmd_sn);

	conn->rsp.data = conn->rsp_buffer;
	conn->rsp.datasize = BHS_SIZE;
	memcpy(conn->rsp.data, &conn->req.bhs, BHS_SIZE);
}

static void cmnd_exec_login(struct iscsi_connection *conn)
{
	struct iscsi_login *req = (struct iscsi_login *)&conn->req.bhs;
	struct iscsi_login_rsp *rsp = (struct iscsi_login_rsp *)&conn->rsp.bhs;
	int stay = 0, nsg_disagree = 0;

	memset(rsp, 0, BHS_SIZE);
	if ((req->opcode & ISCSI_OPCODE_MASK) != ISCSI_OP_LOGIN ||
	    !(req->opcode & ISCSI_OP_IMMEDIATE)) {
		cmnd_reject(conn, ISCSI_REASON_PROTOCOL_ERROR);
		return;
	}

	rsp->opcode = ISCSI_OP_LOGIN_RSP;
	rsp->max_version = ISCSI_DRAFT20_VERSION;
	rsp->active_version = ISCSI_DRAFT20_VERSION;
	rsp->itt = req->itt;

	if (/* req->max_version < ISCSI_VERSION || */
	    req->min_version > ISCSI_DRAFT20_VERSION) {
		rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp->status_detail = ISCSI_LOGIN_STATUS_NO_VERSION;
		conn->state = STATE_EXIT;
		return;
	}

	switch (ISCSI_LOGIN_CURRENT_STAGE(req->flags)) {
	case ISCSI_SECURITY_NEGOTIATION_STAGE:
		dprintf("Login request (security negotiation): %d\n",
			conn->state);
		rsp->flags = ISCSI_SECURITY_NEGOTIATION_STAGE << 2;

		switch (conn->state) {
		case STATE_FREE:
			conn->state = STATE_SECURITY;
			login_start(conn);
			if (rsp->status_class)
				return;
			/* fall through */
		case STATE_SECURITY:
			text_scan_security(conn);
			if (rsp->status_class)
				return;
			if (conn->auth_method != AUTH_NONE) {
				conn->state = STATE_SECURITY_AUTH;
				conn->auth_state = AUTH_STATE_START;
			}
			break;
		case STATE_SECURITY_AUTH:
			switch (cmnd_exec_auth(conn)) {
			case 0:
				break;
			default:
			case -1:
				goto init_err;
			case -2:
				goto auth_err;
			}
			break;
		default:
			goto init_err;
		}

		break;
	case ISCSI_OP_PARMS_NEGOTIATION_STAGE:
		dprintf("Login request (operational negotiation): %d\n",
			conn->state);
		rsp->flags = ISCSI_OP_PARMS_NEGOTIATION_STAGE << 2;

		switch (conn->state) {
		case STATE_FREE:
			conn->state = STATE_LOGIN;

			login_start(conn);
			if (account_available(conn->tid, AUTH_DIR_INCOMING))
				goto auth_err;
			if (rsp->status_class)
				return;
			text_scan_login(conn);
			if (rsp->status_class)
				return;
			stay = text_check_param(conn);
			break;
		case STATE_LOGIN:
			text_scan_login(conn);
			if (rsp->status_class)
				return;
			stay = text_check_param(conn);
			break;
		default:
			goto init_err;
		}
		break;
	default:
		goto init_err;
	}

	if (rsp->status_class)
		return;
	if (conn->state != STATE_SECURITY_AUTH &&
	    req->flags & ISCSI_FLAG_LOGIN_TRANSIT) {
		int nsg = ISCSI_LOGIN_NEXT_STAGE(req->flags);

		switch (nsg) {
		case ISCSI_OP_PARMS_NEGOTIATION_STAGE:
			switch (conn->state) {
			case STATE_SECURITY:
			case STATE_SECURITY_DONE:
				conn->state = STATE_SECURITY_LOGIN;
				login_security_done(conn);
				break;
			default:
				goto init_err;
			}
			break;
		case ISCSI_FULL_FEATURE_PHASE:
			switch (conn->state) {
			case STATE_SECURITY:
			case STATE_SECURITY_DONE:
				if ((nsg_disagree = text_check_param(conn))) {
					conn->state = STATE_LOGIN;
					nsg = ISCSI_OP_PARMS_NEGOTIATION_STAGE;
					break;
				}
				conn->state = STATE_SECURITY_FULL;
				login_security_done(conn);
				break;
			case STATE_LOGIN:
				if (stay)
					nsg = ISCSI_OP_PARMS_NEGOTIATION_STAGE;
				else
					conn->state = STATE_LOGIN_FULL;
				break;
			default:
				goto init_err;
			}
			if (!stay && !nsg_disagree) {
				login_finish(conn);
				if (rsp->status_class)
					return;
			}
			break;
		default:
			goto init_err;
		}
		rsp->flags |= nsg | (stay ? 0 : ISCSI_FLAG_LOGIN_TRANSIT);
	}

	memcpy(rsp->isid, conn->isid, sizeof(rsp->isid));
	rsp->tsih = conn->tsih;
	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->max_cmd_sn);
	return;
init_err:
	rsp->flags = 0;
	rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
	rsp->status_detail = ISCSI_LOGIN_STATUS_INIT_ERR;
	conn->state = STATE_EXIT;
	return;
auth_err:
	rsp->flags = 0;
	rsp->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
	rsp->status_detail = ISCSI_LOGIN_STATUS_AUTH_FAILED;
	conn->state = STATE_EXIT;
	return;
}

static void text_scan_text(struct iscsi_connection *conn)
{
	char *key, *value, *data;
	int datasize;

	data = conn->req.data;
	datasize = conn->req.datasize;

	while ((key = next_key(&data, &datasize, &value))) {
		if (!strcmp(key, "SendTargets")) {
			struct sockaddr_storage ss;
			socklen_t slen, blen;
			char *p, buf[NI_MAXHOST + 128];
			int ret, port;

			if (value[0] == 0)
				continue;

			p = buf;
			blen = sizeof(buf);

			slen = sizeof(ss);
			ret = conn->tp->ep_getsockname(conn,
						       (struct sockaddr *)&ss,
						       &slen);
			if (ret) {
				eprintf("getsockname failed\n");
				continue;
			}

			if (ss.ss_family == AF_INET6) {
				*p++ = '[';
				blen--;
			}

			ret = getnameinfo((struct sockaddr *)&ss, slen, p, blen,
					  NULL, 0, NI_NUMERICHOST);
			if (ret) {
				eprintf("getnameinfo failed, %m\n");
				continue;
			}

			/* strip zone id */
			if (ss.ss_family == AF_INET6)
				(void) strsep(&p, "%");

			p = buf + strlen(buf);

			if (ss.ss_family == AF_INET6)
				 *p++ = ']';

			if (ss.ss_family == AF_INET6)
				port = ntohs(((struct sockaddr_in6 *)
						&ss)->sin6_port);
			else
				port = ntohs(((struct sockaddr_in *)
						&ss)->sin_port);

			sprintf(p, ":%d,1", port);
			target_list_build(conn, buf,
					  strcmp(value, "All") ? value : NULL);
		} else
			text_key_add(conn, key, "NotUnderstood");
	}
}

static void cmnd_exec_text(struct iscsi_connection *conn)
{
	struct iscsi_text *req = (struct iscsi_text *)&conn->req.bhs;
	struct iscsi_text_rsp *rsp = (struct iscsi_text_rsp *)&conn->rsp.bhs;
	int max_len = conn->session_param[ISCSI_PARAM_MAX_XMIT_DLENGTH].val;

	memset(rsp, 0, BHS_SIZE);

	rsp->opcode = ISCSI_OP_TEXT_RSP;
	rsp->itt = req->itt;
	conn->exp_cmd_sn = be32_to_cpu(req->cmdsn);
	if (!(req->opcode & ISCSI_OP_IMMEDIATE))
		conn->exp_cmd_sn++;

	if (be32_to_cpu(req->ttt) == ISCSI_RESERVED_TAG) {
		conn->text_datasize = 0;

		text_scan_text(conn);

		conn->text_rsp_buffer = conn->rsp_buffer;
		conn->text_datasize = conn->rsp.datasize;

		if (conn->text_datasize > max_len) {
			conn->ttt++;
			if (conn->ttt == ISCSI_RESERVED_TAG)
				conn->ttt++;
		} else
			conn->ttt = ISCSI_RESERVED_TAG;
	} else if (!conn->text_datasize || conn->ttt != be32_to_cpu(req->ttt)) {
		cmnd_reject(conn, ISCSI_REASON_INVALID_PDU_FIELD);
		return;
	}

	if (conn->text_datasize <= max_len) {
		rsp->flags = ISCSI_FLAG_CMD_FINAL;
		conn->ttt = ISCSI_RESERVED_TAG;
	}

	conn->rsp.datasize = min(max_len, conn->text_datasize);
	conn->rsp.data = conn->text_rsp_buffer;
	conn->text_rsp_buffer += conn->rsp.datasize;
	conn->text_datasize -= conn->rsp.datasize;

	rsp->ttt = cpu_to_be32(conn->ttt);

	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->max_cmd_sn);
}

static void cmnd_exec_logout(struct iscsi_connection *conn)
{
	struct iscsi_logout *req = (struct iscsi_logout *)&conn->req.bhs;
	struct iscsi_logout_rsp *rsp = (struct iscsi_logout_rsp *)&conn->rsp.bhs;

	memset(rsp, 0, BHS_SIZE);
	rsp->opcode = ISCSI_OP_LOGOUT_RSP;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	rsp->itt = req->itt;
	conn->exp_cmd_sn = be32_to_cpu(req->cmdsn);
	if (!(req->opcode & ISCSI_OP_IMMEDIATE))
		conn->exp_cmd_sn++;

	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->max_cmd_sn);
}

static int cmnd_execute(struct iscsi_connection *conn)
{
	int res = 0;

	switch (conn->req.bhs.opcode & ISCSI_OPCODE_MASK) {
	case ISCSI_OP_LOGIN:
		cmnd_exec_login(conn);
		conn->rsp.bhs.hlength = conn->rsp.ahssize / 4;
		hton24(conn->rsp.bhs.dlength, conn->rsp.datasize);
		break;
	case ISCSI_OP_TEXT:
		cmnd_exec_text(conn);
		conn->rsp.bhs.hlength = conn->rsp.ahssize / 4;
		hton24(conn->rsp.bhs.dlength, conn->rsp.datasize);
		break;
	case ISCSI_OP_LOGOUT:
		cmnd_exec_logout(conn);
		conn->rsp.bhs.hlength = conn->rsp.ahssize / 4;
		hton24(conn->rsp.bhs.dlength, conn->rsp.datasize);
		break;
	default:
		cmnd_reject(conn, ISCSI_REASON_CMD_NOT_SUPPORTED);
		res = 1;
		break;
	}

	return res;
}

static void cmnd_finish(struct iscsi_connection *conn)
{
	switch (conn->state) {
	case STATE_EXIT:
		conn->state = STATE_CLOSE;
		break;
	case STATE_SECURITY_LOGIN:
		conn->state = STATE_LOGIN;
		break;
	case STATE_SECURITY_FULL:
		/* fall through */
	case STATE_LOGIN_FULL:
		if (conn->session_type == SESSION_NORMAL)
			conn->state = STATE_KERNEL;
		else
			conn->state = STATE_FULL;
		break;
	}
}

void iscsi_set_data_rsp_residual(struct iscsi_data_rsp *data_in, struct scsi_cmd *scmd)
{
	int32_t resid = scsi_get_in_resid(scmd);

	if (likely(!resid))
		data_in->residual_count = 0;
	else if (resid > 0) {
		data_in->flags |= ISCSI_FLAG_CMD_UNDERFLOW;
		data_in->residual_count = cpu_to_be32((uint32_t)resid);
	} else {
		data_in->flags |= ISCSI_FLAG_CMD_OVERFLOW;
		data_in->residual_count = cpu_to_be32((uint32_t)-resid);
	}
}

static inline void iscsi_rsp_set_resid(struct iscsi_cmd_rsp *rsp,
				       int32_t resid)
{
	if (likely(!resid))
		rsp->residual_count = 0;
	else if (resid > 0) {
		rsp->flags |= ISCSI_FLAG_CMD_UNDERFLOW;
		rsp->residual_count = cpu_to_be32((uint32_t)resid);
	} else {
		rsp->flags |= ISCSI_FLAG_CMD_OVERFLOW;
		rsp->residual_count = cpu_to_be32((uint32_t)-resid);
	}
}

static inline void iscsi_rsp_set_bidir_resid(struct iscsi_cmd_rsp *rsp,
					     int32_t resid)
{
	if (likely(!resid))
		rsp->bi_residual_count = 0;
	else if (resid > 0) {
		rsp->flags |= ISCSI_FLAG_CMD_BIDI_UNDERFLOW;
		rsp->bi_residual_count = cpu_to_be32((uint32_t)resid);
	} else {
		rsp->flags |= ISCSI_FLAG_CMD_BIDI_OVERFLOW;
		rsp->bi_residual_count = cpu_to_be32((uint32_t)-resid);
	}
}

void iscsi_rsp_set_residual(struct iscsi_cmd_rsp *rsp, struct scsi_cmd *scmd)
{
	rsp->bi_residual_count = 0;
	if (scsi_get_data_dir(scmd) == DATA_READ)
		iscsi_rsp_set_resid(rsp, scsi_get_in_resid(scmd));
	else if (scsi_get_data_dir(scmd) == DATA_WRITE)
		iscsi_rsp_set_resid(rsp, scsi_get_out_resid(scmd));
	else if (scsi_get_data_dir(scmd) == DATA_BIDIRECTIONAL) {
		iscsi_rsp_set_bidir_resid(rsp, scsi_get_in_resid(scmd));
		iscsi_rsp_set_resid(rsp, scsi_get_out_resid(scmd));
	} else
		rsp->residual_count = 0;
}

struct iscsi_sense_data {
	uint16_t length;
	uint8_t  data[0];
} __packed;

static int iscsi_cmd_rsp_build(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_cmd_rsp *rsp = (struct iscsi_cmd_rsp *) &conn->rsp.bhs;
	struct iscsi_sense_data *sense;
	unsigned char sense_len;

	memset(rsp, 0, sizeof(*rsp));
	rsp->opcode = ISCSI_OP_SCSI_CMD_RSP;
	rsp->itt = task->tag;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	rsp->response = ISCSI_STATUS_CMD_COMPLETED;
	rsp->cmd_status = scsi_get_result(&task->scmd);
	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn +
				     conn->session->max_queue_cmd);

	iscsi_rsp_set_residual(rsp, &task->scmd);

	sense_len = task->scmd.sense_len;
	if (sense_len) {
		sense = (struct iscsi_sense_data *)task->scmd.sense_buffer;

		memmove(sense->data, sense, sense_len);
		sense->length = cpu_to_be16(sense_len);

		conn->rsp.datasize = sense_len + sizeof(*sense);
		hton24(rsp->dlength, sense_len + sizeof(*sense));
		conn->rsp.data = sense;
	}

	return 0;
}

static int iscsi_data_rsp_build(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_data_rsp *rsp = (struct iscsi_data_rsp *) &conn->rsp.bhs;
	int datalen, maxdatalen;
	int result = scsi_get_result(&task->scmd);

	memset(rsp, 0, sizeof(*rsp));
	rsp->opcode = ISCSI_OP_SCSI_DATA_IN;
	rsp->itt = task->tag;
	rsp->ttt = cpu_to_be32(ISCSI_RESERVED_TAG);

	rsp->offset = cpu_to_be32(task->offset);
	rsp->datasn = cpu_to_be32(task->exp_r2tsn++);

	datalen = scsi_get_in_transfer_len(&task->scmd) - task->offset;

	maxdatalen = conn->tp->rdma ?
		conn->session_param[ISCSI_PARAM_MAX_BURST].val :
		conn->session_param[ISCSI_PARAM_MAX_XMIT_DLENGTH].val;

	dprintf("%d %d %d %" PRIu32 "%x\n", datalen,
		scsi_get_in_transfer_len(&task->scmd), task->offset, maxdatalen,
		rsp->itt);

	if (datalen <= maxdatalen) {
		rsp->flags = ISCSI_FLAG_CMD_FINAL;

		/* collapse status into final packet if successful */
		if (result == SAM_STAT_GOOD &&
		    scsi_get_data_dir(&task->scmd) != DATA_BIDIRECTIONAL &&
		    !conn->tp->rdma) {
			rsp->flags |= ISCSI_FLAG_DATA_STATUS;
			rsp->cmd_status = result;
			rsp->statsn = cpu_to_be32(conn->stat_sn++);
			iscsi_set_data_rsp_residual(rsp, &task->scmd);
		}
	} else
		datalen = maxdatalen;

	rsp->exp_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn +
				     conn->session->max_queue_cmd);

	conn->rsp.datasize = datalen;
	hton24(rsp->dlength, datalen);
	conn->rsp.data = scsi_get_in_buffer(&task->scmd);
	conn->rsp.data += task->offset;

	task->offset += datalen;

	return 0;
}

static int iscsi_r2t_build(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_r2t_rsp *rsp = (struct iscsi_r2t_rsp *) &conn->rsp.bhs;
	uint32_t length;

	memset(rsp, 0, sizeof(*rsp));

	rsp->opcode = ISCSI_OP_R2T;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	memcpy(rsp->lun, task->req.lun, sizeof(rsp->lun));

	rsp->itt = task->req.itt;
	rsp->r2tsn = cpu_to_be32(task->exp_r2tsn++);
	rsp->data_offset = cpu_to_be32(task->offset);
	/* return next statsn for this conn w/o advancing it */
	rsp->statsn = cpu_to_be32(conn->stat_sn);
	rsp->ttt = (unsigned long) task;
	length = min_t(uint32_t, task->r2t_count,
		       conn->session_param[ISCSI_PARAM_MAX_BURST].val);
	rsp->data_length = cpu_to_be32(length);

	return 0;
}

static struct iscsi_task *iscsi_alloc_task(struct iscsi_connection *conn,
					   int ext_len, int data_len)
{
	struct iscsi_hdr *req = (struct iscsi_hdr *) &conn->req.bhs;
	struct iscsi_task *task;
	void *buf;

	task = conn->tp->alloc_task(conn, ext_len);
	if (!task)
		return NULL;

	if (data_len) {
		buf = conn->tp->alloc_data_buf(conn, data_len);
		if (!buf) {
			conn->tp->free_task(task);
			return NULL;
		}
		task->data = buf;
	}

	memcpy(&task->req, req, sizeof(*req));
	task->conn = conn;
	INIT_LIST_HEAD(&task->c_hlist);
	INIT_LIST_HEAD(&task->c_list);
	list_add(&task->c_siblings, &conn->task_list);
	conn_get(conn);
	return task;
}

void iscsi_free_task(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;

	list_del(&task->c_hlist);
	list_del(&task->c_siblings);

	conn->tp->free_data_buf(conn, scsi_get_in_buffer(&task->scmd));
	conn->tp->free_data_buf(conn, scsi_get_out_buffer(&task->scmd));

	/*
	 * If freeing task before in/out buffers are set, make sure to free
	 * task->data or it leaks.
	 */
	if ((task->data != scsi_get_in_buffer(&task->scmd)) &&
	    (task->data != scsi_get_out_buffer(&task->scmd)))
		conn->tp->free_data_buf(conn, task->data);

	conn->tp->free_task(task);
	conn_put(conn);
}

static inline struct iscsi_task *ITASK(struct scsi_cmd *scmd)
{
	return container_of(scmd, struct iscsi_task, scmd);
}

void iscsi_free_cmd_task(struct iscsi_task *task)
{
	target_cmd_done(&task->scmd);

	iscsi_free_task(task);
}

static int iscsi_scsi_cmd_done(uint64_t nid, int result, struct scsi_cmd *scmd)
{
	struct iscsi_task *task = ITASK(scmd);

	/*
	 * Since the connection is closed we just free the task.
	 * We could delay the closing of the conn in some cases and send
	 * the response with a little extra code or we can check if this
	 * task got reassinged to another connection.
	 */
	if (task->conn->state == STATE_CLOSE) {
		iscsi_free_cmd_task(task);
		return 0;
	}

	list_add_tail(&task->c_list, &task->conn->tx_clist);
	task->conn->tp->ep_event_modify(task->conn, EPOLLIN | EPOLLOUT);

	return 0;
}

static int cmd_attr(struct iscsi_task *task)
{
	int attr;
	struct iscsi_cmd *req = (struct iscsi_cmd *) &task->req;

	switch (req->flags & ISCSI_FLAG_CMD_ATTR_MASK) {
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

static int iscsi_target_cmd_queue(struct iscsi_task *task)
{
	struct scsi_cmd *scmd = &task->scmd;
	struct iscsi_connection *conn = task->conn;
	struct iscsi_cmd *req = (struct iscsi_cmd *) &task->req;
	uint32_t data_len;
	uint8_t *ahs;
	int ahslen;
	int err;
	enum data_direction dir = scsi_get_data_dir(scmd);

	scmd->cmd_itn_id = conn->session->tsih;
	scmd->scb = req->cdb;
	scmd->scb_len = sizeof(req->cdb);

	ahs = task->ahs;
	ahslen = req->hlength * 4;
	if (ahslen >= 4) {
		struct iscsi_ecdb_ahdr *ahs_extcdb = (void *) ahs;

		if (ahs_extcdb->ahstype == ISCSI_AHSTYPE_CDB) {
			int extcdb_len = ntohs(ahs_extcdb->ahslength) - 1;
			unsigned char *p = (void *)task->extdata;

			if (4 + extcdb_len > ahslen) {
				eprintf("AHS len %d too short for extcdb %d\n",
					ahslen, extcdb_len);
				return -EINVAL;
			}
			if (extcdb_len + sizeof(req->cdb) > 260) {
				eprintf("invalid extcdb len %d\n", extcdb_len);

				return -EINVAL;
			}

			memcpy(p, req->cdb, sizeof(req->cdb));
			memmove(p + sizeof(req->cdb), ahs_extcdb->ecdb,
				extcdb_len);

			scmd->scb = p;
			scmd->scb_len = sizeof(req->cdb) + extcdb_len;

			ahs += 4 + extcdb_len;
			ahslen -= 4 + extcdb_len;
		}
	}

	data_len = ntohl(req->data_length);
	/* figure out incoming (write) and outgoing (read) sizes */
	if (dir == DATA_WRITE || dir == DATA_BIDIRECTIONAL) {
		scsi_set_out_length(scmd, data_len);
		scsi_set_out_buffer(scmd, task->data);
	} else if (dir == DATA_READ) {
		scsi_set_in_length(scmd, data_len);
		scsi_set_in_buffer(scmd, task->data);
	}

	if (dir == DATA_BIDIRECTIONAL && ahslen >= 8) {
		struct iscsi_rlength_ahdr *ahs_bidi = (void *) ahs;
		if (ahs_bidi->ahstype == ISCSI_AHSTYPE_RLENGTH) {
			uint32_t in_length = ntohl(ahs_bidi->read_length);

			dprintf("bidi read len %u\n", in_length);

			if (in_length) {
				uint32_t len;
				void *buf;

				len = roundup(in_length,
					      conn->tp->data_padding);
				buf = conn->tp->alloc_data_buf(conn, len);
				if (!buf)
					return -ENOMEM;

				scsi_set_in_buffer(scmd, buf);
				scsi_set_in_length(scmd, in_length);
			}
		}
	}

	memcpy(scmd->lun, task->req.lun, sizeof(scmd->lun));
	scmd->attribute = cmd_attr(task);
	scmd->tag = req->itt;
	set_task_in_scsi(task);

	err = target_cmd_queue(conn->session->target->tid, scmd);
	if (err)
		clear_task_in_scsi(task);

	return err;
}

int iscsi_scsi_cmd_execute(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_cmd *req = (struct iscsi_cmd *) &task->req;
	int ret = 0;

	if ((req->flags & ISCSI_FLAG_CMD_WRITE) && task->r2t_count) {
		if (!task->unsol_count)
			list_add_tail(&task->c_list, &task->conn->tx_clist);
		goto no_queuing;
	}

	task->offset = 0;  /* for use as transmit pointer for data-ins */
	ret = iscsi_target_cmd_queue(task);
no_queuing:
	conn->tp->ep_event_modify(conn, EPOLLIN | EPOLLOUT);
	return ret;
}

static int iscsi_tm_done(struct mgmt_req *mreq)
{
	struct iscsi_task *task;

	task = (struct iscsi_task *) (unsigned long) mreq->mid;

	switch (mreq->result) {
	case 0:
		task->result = ISCSI_TMF_RSP_COMPLETE;
		break;
	case -EINVAL:
		task->result = ISCSI_TMF_RSP_NOT_SUPPORTED;
		break;
	case -EEXIST:
		/*
		 * the command completed or we could not find it so
		 * we retrun  no task here
		 */
		task->result = ISCSI_TMF_RSP_NO_TASK;
		break;
	default:
		task->result = ISCSI_TMF_RSP_REJECTED;
		break;
	}

	if (task->conn->state == STATE_CLOSE) {
		iscsi_free_task(task);
		return 0;
	}
	list_add_tail(&task->c_list, &task->conn->tx_clist);
	task->conn->tp->ep_event_modify(task->conn, EPOLLIN | EPOLLOUT);
	return 0;
}

static int iscsi_tm_execute(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_tm *req = (struct iscsi_tm *) &task->req;
	int fn = 0, err = 0;

	switch (req->flags & ISCSI_FLAG_TM_FUNC_MASK) {
	case ISCSI_TM_FUNC_ABORT_TASK:
		fn = ABORT_TASK;
		break;
	case ISCSI_TM_FUNC_ABORT_TASK_SET:
		fn = ABORT_TASK_SET;
		break;
	case ISCSI_TM_FUNC_CLEAR_ACA:
		err = ISCSI_TMF_RSP_NOT_SUPPORTED;
		break;
	case ISCSI_TM_FUNC_CLEAR_TASK_SET:
		err = ISCSI_TMF_RSP_NOT_SUPPORTED;
		break;
	case ISCSI_TM_FUNC_LOGICAL_UNIT_RESET:
		fn = LOGICAL_UNIT_RESET;
		break;
	case ISCSI_TM_FUNC_TARGET_WARM_RESET:
	case ISCSI_TM_FUNC_TARGET_COLD_RESET:
	case ISCSI_TM_FUNC_TASK_REASSIGN:
		err = ISCSI_TMF_RSP_NOT_SUPPORTED;
		break;
	default:
		err = ISCSI_TMF_RSP_REJECTED;

		eprintf("unknown task management function %d\n",
			req->flags & ISCSI_FLAG_TM_FUNC_MASK);
	}

	if (err)
		task->result = err;
	else {
		int ret;
		ret = target_mgmt_request(conn->session->target->tid,
					  conn->session->tsih,
					  (unsigned long)task, fn, req->lun,
					  req->rtt, 0);
		set_task_in_scsi(task);
		switch (ret) {
		case MGMT_REQ_QUEUED:
			break;
		case MGMT_REQ_FAILED:
		case MGMT_REQ_DONE:
			clear_task_in_scsi(task);
			break;
		}
	}

	return err;
}

static int iscsi_task_execute(struct iscsi_task *task)
{
	struct iscsi_hdr *hdr = (struct iscsi_hdr *) &task->req;
	uint8_t op = hdr->opcode & ISCSI_OPCODE_MASK;
	int err;

	switch (op) {
	case ISCSI_OP_NOOP_OUT:
	case ISCSI_OP_LOGOUT:
		list_add_tail(&task->c_list, &task->conn->tx_clist);
		task->conn->tp->ep_event_modify(task->conn, EPOLLIN | EPOLLOUT);
		break;
	case ISCSI_OP_SCSI_CMD:
		/* convenient directionality for our internal use */
		if (hdr->flags & ISCSI_FLAG_CMD_READ) {
			if (hdr->flags & ISCSI_FLAG_CMD_WRITE)
				scsi_set_data_dir(&task->scmd, DATA_BIDIRECTIONAL);
			else
				scsi_set_data_dir(&task->scmd, DATA_READ);
		} else if (hdr->flags & ISCSI_FLAG_CMD_WRITE) {
			scsi_set_data_dir(&task->scmd, DATA_WRITE);
		} else
			scsi_set_data_dir(&task->scmd, DATA_NONE);

		err = iscsi_scsi_cmd_execute(task);
		break;
	case ISCSI_OP_SCSI_TMFUNC:
		err = iscsi_tm_execute(task);
		if (err) {
			list_add_tail(&task->c_list, &task->conn->tx_clist);
			task->conn->tp->ep_event_modify(task->conn,
							EPOLLIN | EPOLLOUT);
		}
		break;
	case ISCSI_OP_TEXT:
	case ISCSI_OP_SNACK:
		break;
	default:
		break;
	}

	return 0;
}

static int iscsi_data_out_rx_done(struct iscsi_task *task)
{
	struct iscsi_hdr *hdr = &task->conn->req.bhs;
	int err = 0;

	if (hdr->ttt == cpu_to_be32(ISCSI_RESERVED_TAG)) {
		if (hdr->flags & ISCSI_FLAG_CMD_FINAL) {
			task->unsol_count = 0;
			if (!task_pending(task))
				err = iscsi_scsi_cmd_execute(task);
		}
	} else {
		if (!(hdr->flags & ISCSI_FLAG_CMD_FINAL))
			return err;

		err = iscsi_scsi_cmd_execute(task);
	}

	return err;
}

static int iscsi_data_out_rx_start(struct iscsi_connection *conn)
{
	struct iscsi_task *task;
	struct iscsi_data *req = (struct iscsi_data *) &conn->req.bhs;

	list_for_each_entry(task, &conn->session->cmd_list, c_hlist) {
		if (task->tag == req->itt)
			goto found;
	}
	return -EINVAL;
found:
	dprintf("found a task %" PRIx64 " %u %u %u %u %u\n", task->tag,
		ntohl(((struct iscsi_cmd *) (&task->req))->data_length),
		task->offset,
		task->r2t_count,
		ntoh24(req->dlength), be32_to_cpu(req->offset));

	conn->req.data = task->data + be32_to_cpu(req->offset);

	task->offset += ntoh24(req->dlength);
	task->r2t_count -= ntoh24(req->dlength);

	conn->rx_task = task;

	return 0;
}

static int iscsi_task_queue(struct iscsi_task *task)
{
	struct iscsi_session *session = task->conn->session;
	struct iscsi_hdr *req = (struct iscsi_hdr *) &task->req;
	uint32_t cmd_sn;
	struct iscsi_task *ent;
	int err;

	dprintf("%x %x %x\n", be32_to_cpu(req->statsn), session->exp_cmd_sn,
		req->opcode);

	if (req->opcode & ISCSI_OP_IMMEDIATE)
		return iscsi_task_execute(task);

	cmd_sn = be32_to_cpu(req->statsn);
	if (cmd_sn == session->exp_cmd_sn) {
	retry:
		session->exp_cmd_sn = ++cmd_sn;

		/* Should we close the connection... */
		err = iscsi_task_execute(task);

		if (list_empty(&session->pending_cmd_list))
			return 0;
		task = list_first_entry(&session->pending_cmd_list,
					struct iscsi_task, c_list);
		if (be32_to_cpu(task->req.statsn) != cmd_sn)
			return 0;

		list_del(&task->c_list);
		clear_task_pending(task);
		goto retry;
	} else {
		if (before(cmd_sn, session->exp_cmd_sn)) {
			eprintf("unexpected cmd_sn (%u,%u)\n",
				cmd_sn, session->exp_cmd_sn);
			return -EINVAL;
		}

		/* TODO: check max cmd_sn */

		list_for_each_entry(ent, &session->pending_cmd_list, c_list) {
			if (before(cmd_sn, be32_to_cpu(ent->req.statsn)))
				break;
		}

		list_add_tail(&task->c_list, &ent->c_list);
		set_task_pending(task);
	}
	return 0;
}

static int iscsi_scsi_cmd_rx_start(struct iscsi_connection *conn)
{
	struct iscsi_cmd *req = (struct iscsi_cmd *) &conn->req.bhs;
	struct iscsi_task *task;
	int ahs_len, imm_len, data_len, ext_len;

	ahs_len = req->hlength * 4;
	imm_len = roundup(ntoh24(req->dlength), conn->tp->data_padding);
	data_len = roundup(ntohl(req->data_length), conn->tp->data_padding);

	dprintf("%u %x %d %d %d %x %x\n", conn->session->tsih,
		req->cdb[0], ahs_len, imm_len, data_len,
		req->flags & ISCSI_FLAG_CMD_ATTR_MASK, req->itt);

	ext_len = ahs_len ? sizeof(req->cdb) + ahs_len : 0;

	task = iscsi_alloc_task(conn, ext_len, max(imm_len, data_len));
	if (task)
		conn->rx_task = task;
	else
		return -ENOMEM;

	task->tag = req->itt;

	if (ahs_len) {
		task->ahs = (uint8_t *) task->extdata + sizeof(req->cdb);
		conn->req.ahs = task->ahs;
		conn->req.data = task->data;
	} else if (data_len)
		conn->req.data = task->data;

	if (req->flags & ISCSI_FLAG_CMD_WRITE) {
		task->offset = ntoh24(req->dlength);
		task->r2t_count = ntohl(req->data_length) - task->offset;
		task->unsol_count = !(req->flags & ISCSI_FLAG_CMD_FINAL);

		dprintf("%d %d %d %d\n", conn->rx_size, task->r2t_count,
			task->unsol_count, task->offset);
	}

	list_add(&task->c_hlist, &conn->session->cmd_list);
	return 0;
}

static int iscsi_noop_out_rx_start(struct iscsi_connection *conn)
{
	struct iscsi_hdr *req = (struct iscsi_hdr *) &conn->req.bhs;
	struct iscsi_task *task;
	int len, err = 0;

	dprintf("%x %x %u\n", req->ttt, req->itt, ntoh24(req->dlength));
	if (req->ttt != cpu_to_be32(ISCSI_RESERVED_TAG)) {
		if ((req->opcode & ISCSI_OPCODE_MASK) == ISCSI_OP_NOOP_OUT) {
			goto good;
		}
	}

	if (req->itt == cpu_to_be32(ISCSI_RESERVED_TAG)) {
		if (!(req->opcode & ISCSI_OP_IMMEDIATE)) {
			eprintf("initiator bug\n");
			err = -ISCSI_REASON_PROTOCOL_ERROR;
			goto out;
		}
	}

good:
	conn->exp_stat_sn = be32_to_cpu(req->exp_statsn);

	len = ntoh24(req->dlength);
	task = iscsi_alloc_task(conn, 0, len);
	if (task)
		conn->rx_task = task;
	else {
		err = -ENOMEM;
		goto out;
	}

	if (len) {
		task->len = len;
		conn->req.data = task->data;
	}
out:
	return err;
}

static int iscsi_task_rx_done(struct iscsi_connection *conn)
{
	struct iscsi_hdr *hdr = &conn->req.bhs;
	struct iscsi_task *task = conn->rx_task;
	uint8_t op;
	int err = 0;

	op = hdr->opcode & ISCSI_OPCODE_MASK;
	switch (op) {
	case ISCSI_OP_SCSI_CMD:
	case ISCSI_OP_NOOP_OUT:
	case ISCSI_OP_SCSI_TMFUNC:
	case ISCSI_OP_LOGOUT:
		err = iscsi_task_queue(task);
		break;
	case ISCSI_OP_SCSI_DATA_OUT:
		err = iscsi_data_out_rx_done(task);
		break;
	case ISCSI_OP_TEXT:
	case ISCSI_OP_SNACK:
	default:
		eprintf("Cannot handle yet %x\n", op);
		break;
	}

	conn->rx_task = NULL;
	return err;
}

static int iscsi_task_rx_start(struct iscsi_connection *conn)
{
	struct iscsi_hdr *hdr = &conn->req.bhs;
	struct iscsi_task *task;
	uint8_t op;
	int err = 0;

	op = hdr->opcode & ISCSI_OPCODE_MASK;
	switch (op) {
	case ISCSI_OP_SCSI_CMD:
		err = iscsi_scsi_cmd_rx_start(conn);
		if (!err)
			conn->exp_stat_sn = be32_to_cpu(hdr->exp_statsn);
		break;
	case ISCSI_OP_SCSI_DATA_OUT:
		err = iscsi_data_out_rx_start(conn);
		if (!err)
			conn->exp_stat_sn = be32_to_cpu(hdr->exp_statsn);
		break;
	case ISCSI_OP_NOOP_OUT:
		err = iscsi_noop_out_rx_start(conn);
		break;
	case ISCSI_OP_SCSI_TMFUNC:
	case ISCSI_OP_LOGOUT:
		task = iscsi_alloc_task(conn, 0, 0);
		if (task)
			conn->rx_task = task;
		else
			err = -ENOMEM;
		break;
	case ISCSI_OP_TEXT:
	case ISCSI_OP_SNACK:
		eprintf("Cannot handle yet %x\n", op);
		err = -EINVAL;
		break;
	default:
		eprintf("Unknown op %x\n", op);
		err = -EINVAL;
		break;
	}

	return err;
}

static int iscsi_scsi_cmd_tx_start(struct iscsi_task *task)
{
	enum data_direction data_dir = scsi_get_data_dir(&task->scmd);
	int err = 0;

	switch (data_dir) {
	case DATA_NONE:
		err = iscsi_cmd_rsp_build(task);
		break;
	case DATA_READ:
		if (task->offset < scsi_get_in_transfer_len(&task->scmd))
			err = iscsi_data_rsp_build(task);
		else
			err = iscsi_cmd_rsp_build(task);
		break;
	case DATA_WRITE:
		if (task->r2t_count)
			err = iscsi_r2t_build(task);
		else
			err = iscsi_cmd_rsp_build(task);
		break;
	case DATA_BIDIRECTIONAL:
		if (task->r2t_count)
			err = iscsi_r2t_build(task);
		else if (task->offset < scsi_get_in_transfer_len(&task->scmd))
			err = iscsi_data_rsp_build(task);
		else
			err = iscsi_cmd_rsp_build(task);
		break;
	default:
		eprintf("Unexpected data_dir %d task %p\n", data_dir, task);
		exit(-1);
	}

	return err;
}

static int iscsi_logout_tx_start(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_logout_rsp *rsp =
		(struct iscsi_logout_rsp *) &conn->rsp.bhs;

	rsp->opcode = ISCSI_OP_LOGOUT_RSP;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	rsp->itt = task->req.itt;
	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn +
				     conn->session->max_queue_cmd);

	return 0;
}

static int iscsi_noop_in_tx_start(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_data_rsp *rsp = (struct iscsi_data_rsp *) &conn->rsp.bhs;

	memset(rsp, 0, sizeof(*rsp));
	rsp->opcode = ISCSI_OP_NOOP_IN;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	rsp->itt = task->req.itt;
	rsp->ttt = task->req.ttt;
	rsp->statsn = cpu_to_be32(conn->stat_sn);
	rsp->exp_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn +
				     conn->session->max_queue_cmd);

	/* TODO: honor max_burst */
	conn->rsp.datasize = task->len;
	hton24(rsp->dlength, task->len);
	conn->rsp.data = task->data;

	return 0;
}

static int iscsi_noop_out_tx_start(struct iscsi_task *task, int *is_rsp)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_data_rsp *rsp = (struct iscsi_data_rsp *) &conn->rsp.bhs;

	if (task->req.itt == cpu_to_be32(ISCSI_RESERVED_TAG)) {
		*is_rsp = 0;

		if (conn->tp->ep_nop_reply)
			conn->tp->ep_nop_reply(be32_to_cpu(task->req.ttt));

		iscsi_free_task(task);
	} else {
		*is_rsp = 1;

		memset(rsp, 0, sizeof(*rsp));
		rsp->opcode = ISCSI_OP_NOOP_IN;
		rsp->flags = ISCSI_FLAG_CMD_FINAL;
		rsp->itt = task->req.itt;
		rsp->ttt = cpu_to_be32(ISCSI_RESERVED_TAG);
		rsp->statsn = cpu_to_be32(conn->stat_sn++);
		rsp->exp_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn);
		rsp->max_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn +
					     conn->session->max_queue_cmd);

		/* TODO: honor max_burst */
		conn->rsp.datasize = task->len;
		hton24(rsp->dlength, task->len);
		conn->rsp.data = task->data;
	}

	return 0;
}

static int iscsi_tm_tx_start(struct iscsi_task *task)
{
	struct iscsi_connection *conn = task->conn;
	struct iscsi_tm_rsp *rsp = (struct iscsi_tm_rsp *) &conn->rsp.bhs;

	memset(rsp, 0, sizeof(*rsp));
	rsp->opcode = ISCSI_OP_SCSI_TMFUNC_RSP;
	rsp->flags = ISCSI_FLAG_CMD_FINAL;
	rsp->itt = task->req.itt;
	rsp->response = task->result;

	rsp->statsn = cpu_to_be32(conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(conn->session->exp_cmd_sn +
				     conn->session->max_queue_cmd);

	return 0;
}

static int iscsi_scsi_cmd_tx_done(struct iscsi_connection *conn)
{
	struct iscsi_hdr *hdr = &conn->rsp.bhs;
	struct iscsi_task *task = conn->tx_task;

	switch (hdr->opcode & ISCSI_OPCODE_MASK) {
	case ISCSI_OP_R2T:
		break;
	case ISCSI_OP_SCSI_DATA_IN:
		if (task->offset < scsi_get_in_transfer_len(&task->scmd) ||
		    scsi_get_result(&task->scmd) != SAM_STAT_GOOD ||
		    scsi_get_data_dir(&task->scmd) == DATA_BIDIRECTIONAL) {
			dprintf("more data or sense or bidir %x\n", hdr->itt);
			list_add(&task->c_list, &task->conn->tx_clist);
			return 0;
		}
	case ISCSI_OP_SCSI_CMD_RSP:
		iscsi_free_cmd_task(task);
		break;
	default:
		eprintf("target bug %x\n", hdr->opcode & ISCSI_OPCODE_MASK);
	}

	return 0;
}

static int iscsi_task_tx_done(struct iscsi_connection *conn)
{
	struct iscsi_task *task = conn->tx_task;
	int err;
	uint8_t op;

	op = task->req.opcode & ISCSI_OPCODE_MASK;
	switch (op) {
	case ISCSI_OP_SCSI_CMD:
		err = iscsi_scsi_cmd_tx_done(conn);
		break;
	case ISCSI_OP_NOOP_OUT:
	case ISCSI_OP_LOGOUT:
	case ISCSI_OP_SCSI_TMFUNC:
		iscsi_free_task(task);

		if (op == ISCSI_OP_LOGOUT)
			conn->state = STATE_CLOSE;
	}

	conn->tx_task = NULL;
	return 0;
}

static int iscsi_task_tx_start(struct iscsi_connection *conn)
{
	struct iscsi_task *task;
	int is_rsp, err = 0;

	if (list_empty(&conn->tx_clist))
		goto nodata;

	conn_write_pdu(conn);

	task = list_first_entry(&conn->tx_clist, struct iscsi_task, c_list);
	dprintf("found a task %" PRIx64 " %u %u %u\n", task->tag,
		ntohl(((struct iscsi_cmd *) (&task->req))->data_length),
		task->offset,
		task->r2t_count);

	list_del(&task->c_list);

	switch (task->req.opcode & ISCSI_OPCODE_MASK) {
	case ISCSI_OP_SCSI_CMD:
		err = iscsi_scsi_cmd_tx_start(task);
		break;
	case ISCSI_OP_NOOP_IN:
		err = iscsi_noop_in_tx_start(task);
		break;
	case ISCSI_OP_NOOP_OUT:
		err = iscsi_noop_out_tx_start(task, &is_rsp);
		if (!is_rsp)
			goto nodata;
		break;
	case ISCSI_OP_LOGOUT:
		err = iscsi_logout_tx_start(task);
		break;
	case ISCSI_OP_SCSI_TMFUNC:
		err = iscsi_tm_tx_start(task);
		break;
	}

	conn->tx_task = task;
	return err;

nodata:
	dprintf("no more data\n");
	conn->tp->ep_event_modify(conn, EPOLLIN);
	return -EAGAIN;
}

static int do_recv(struct iscsi_connection *conn, int next_state)
{
	int ret, opcode;

	ret = conn->tp->ep_read(conn, conn->rx_buffer, conn->rx_size);
	if (!ret) {
		conn->state = STATE_CLOSE;
		return 0;
	} else if (ret < 0) {
		if (errno == EINTR || errno == EAGAIN)
			return 0;
		else
			return -EIO;
	}

	conn->rx_size -= ret;
	conn->rx_buffer += ret;

	opcode = (conn->rx_iostate == IOSTATE_RX_BHS) ?
		(conn->req.bhs.opcode & ISCSI_OPCODE_MASK) : -1;
	iscsi_update_conn_stats_rx(conn, ret, opcode);

	if (!conn->rx_size)
		conn->rx_iostate = next_state;

	return ret;
}

void iscsi_rx_handler(struct iscsi_connection *conn)
{
	int ret = 0, hdigest, ddigest;
	uint32_t crc;


	if (conn->state == STATE_SCSI) {
		struct param *p = conn->session_param;
		hdigest = p[ISCSI_PARAM_HDRDGST_EN].val & DIGEST_CRC32C;
		ddigest = p[ISCSI_PARAM_DATADGST_EN].val & DIGEST_CRC32C;
	} else
		hdigest = ddigest = 0;
again:
	switch (conn->rx_iostate) {
	case IOSTATE_RX_BHS:
		ret = do_recv(conn, IOSTATE_RX_INIT_AHS);
		if (ret <= 0 || conn->rx_iostate != IOSTATE_RX_INIT_AHS)
			break;
	case IOSTATE_RX_INIT_AHS:
		if (conn->state == STATE_SCSI) {
			ret = iscsi_task_rx_start(conn);
			if (ret) {
				conn->state = STATE_CLOSE;
				break;
			}
		} else {
			conn->rx_buffer = conn->req_buffer;
			conn->req.ahs = conn->rx_buffer;
			conn->req.data = conn->rx_buffer
				+ conn->req.bhs.hlength * 4;
		}
		conn->req.ahssize = conn->req.bhs.hlength * 4;
		conn->req.datasize = ntoh24(conn->req.bhs.dlength);
		conn->rx_size = conn->req.ahssize;

		if (conn->state != STATE_SCSI &&
		    conn->req.ahssize > INCOMING_BUFSIZE) {
			conn->state = STATE_CLOSE;
			return;
		}

		if (conn->rx_size) {
			conn->rx_buffer = conn->req.ahs;
			conn->rx_iostate = IOSTATE_RX_AHS;
		} else
			conn->rx_iostate = hdigest ?
				IOSTATE_RX_INIT_HDIGEST : IOSTATE_RX_INIT_DATA;

		/*
		 * if the datasize is zero, we must go to
		 * IOSTATE_RX_END via IOSTATE_RX_INIT_DATA now. Note
		 * iscsi_rx_handler will not called since tgtd doesn't
		 * have data to read.
		 */
		if (conn->rx_iostate == IOSTATE_RX_INIT_DATA)
			goto again;
		else if (conn->rx_iostate != IOSTATE_RX_AHS)
			break;
	case IOSTATE_RX_AHS:
		ret = do_recv(conn, hdigest ?
			      IOSTATE_RX_INIT_HDIGEST : IOSTATE_RX_INIT_DATA);
		if (ret <= 0)
			break;
		if (conn->rx_iostate == IOSTATE_RX_INIT_DATA)
			goto again;
		if (conn->rx_iostate != IOSTATE_RX_INIT_HDIGEST)
			break;
	case IOSTATE_RX_INIT_HDIGEST:
		conn->rx_buffer = conn->rx_digest;
		conn->rx_size = sizeof(conn->rx_digest);
		conn->rx_iostate = IOSTATE_RX_HDIGEST;
	case IOSTATE_RX_HDIGEST:
		ret = do_recv(conn, IOSTATE_RX_CHECK_HDIGEST);
		if (ret <= 0 || conn->rx_iostate != IOSTATE_RX_CHECK_HDIGEST)
			break;
	case IOSTATE_RX_CHECK_HDIGEST:
		crc = ~0;
		crc = crc32c(crc, &conn->req.bhs, BHS_SIZE);
		if (conn->req.ahssize)
			crc = crc32c(crc, conn->req.ahs, conn->req.ahssize);
		crc = ~crc;
		if (*((uint32_t *)conn->rx_digest) != crc) {
			eprintf("rx hdr digest error 0x%x calc 0x%x\n",
				*((uint32_t *)conn->rx_digest), crc);
			conn->state = STATE_CLOSE;
		}
		conn->rx_iostate = IOSTATE_RX_INIT_DATA;
	case IOSTATE_RX_INIT_DATA:
		conn->rx_size = roundup(conn->req.datasize,
					conn->tp->data_padding);
		if (conn->rx_size) {
			conn->rx_iostate = IOSTATE_RX_DATA;
			conn->rx_buffer = conn->req.data;

			if (conn->state != STATE_SCSI) {
				if (conn->req.ahssize + conn->rx_size >
				    INCOMING_BUFSIZE) {
					conn->state = STATE_CLOSE;
					return;
				}
			}
		} else {
			conn->rx_iostate = IOSTATE_RX_END;
			break;
		}
	case IOSTATE_RX_DATA:
		ret = do_recv(conn, ddigest ?
			      IOSTATE_RX_INIT_DDIGEST : IOSTATE_RX_END);
		if (ret <= 0 || conn->rx_iostate != IOSTATE_RX_INIT_DDIGEST)
			break;
	case IOSTATE_RX_INIT_DDIGEST:
		conn->rx_buffer = conn->rx_digest;
		conn->rx_size = sizeof(conn->rx_digest);
		conn->rx_iostate = IOSTATE_RX_DDIGEST;
	case IOSTATE_RX_DDIGEST:
		ret = do_recv(conn, IOSTATE_RX_CHECK_DDIGEST);
		if (ret <= 0 || conn->rx_iostate != IOSTATE_RX_CHECK_DDIGEST)
			break;
	case IOSTATE_RX_CHECK_DDIGEST:
		crc = ~0;
		crc = crc32c(crc, conn->req.data,
			     roundup(conn->req.datasize,
				     conn->tp->data_padding));
		crc = ~crc;
		conn->rx_iostate = IOSTATE_RX_END;
		if (*((uint32_t *)conn->rx_digest) != crc) {
			eprintf("rx hdr digest error 0x%x calc 0x%x\n",
				*((uint32_t *)conn->rx_digest), crc);
			conn->state = STATE_CLOSE;
		}
		break;
	default:
		eprintf("error %d %d\n", conn->state, conn->rx_iostate);
		exit(1);
	}

	if (ret < 0 ||
	    conn->rx_iostate != IOSTATE_RX_END ||
	    conn->state == STATE_CLOSE)
		return;

	if (conn->rx_size) {
		eprintf("error %d %d %d\n", conn->state, conn->rx_iostate,
			conn->rx_size);
		exit(1);
	}

	if (conn->state == STATE_SCSI) {
		ret = iscsi_task_rx_done(conn);
		if (ret)
			conn->state = STATE_CLOSE;
		else
			conn_read_pdu(conn);
	} else {
		conn_write_pdu(conn);
		conn->tp->ep_event_modify(conn, EPOLLOUT);
		ret = cmnd_execute(conn);
		if (ret)
			conn->state = STATE_CLOSE;
	}
}

static int do_send(struct iscsi_connection *conn, int next_state)
{
	int ret, opcode;
again:
	ret = conn->tp->ep_write_begin(conn, conn->tx_buffer, conn->tx_size);
	if (ret < 0) {
		if (errno != EINTR && errno != EAGAIN)
			conn->state = STATE_CLOSE;
		else if (errno == EINTR || errno == EAGAIN)
			goto again;

		return -EIO;
	}

	conn->tx_size -= ret;
	conn->tx_buffer += ret;

	opcode = (conn->tx_iostate == IOSTATE_TX_BHS) ?
			(conn->req.bhs.opcode & ISCSI_OPCODE_MASK) : -1;
	iscsi_update_conn_stats_tx(conn, ret, opcode);

	if (conn->tx_size)
		goto again;
	conn->tx_iostate = next_state;

	return 0;
}

int iscsi_tx_handler(struct iscsi_connection *conn)
{
	int ret = 0, hdigest, ddigest;
	uint32_t crc;

	if (conn->state == STATE_SCSI) {
		struct param *p = conn->session_param;
		hdigest = p[ISCSI_PARAM_HDRDGST_EN].val & DIGEST_CRC32C;
		ddigest = p[ISCSI_PARAM_DATADGST_EN].val & DIGEST_CRC32C;
	} else
		hdigest = ddigest = 0;

	if (conn->state == STATE_SCSI && !conn->tx_task) {
		ret = iscsi_task_tx_start(conn);
		if (ret)
			goto out;
	}

	/*
	 * For rdma, grab the data-in or r2t packet and covert to
	 * an RDMA operation.
	 */
	if (conn->tp->rdma && conn->state == STATE_SCSI) {
		switch (conn->rsp.bhs.opcode) {
		case ISCSI_OP_R2T:
			ret = conn->tp->ep_rdma_read(conn);
			if (ret < 0)  /* wait for free slot */
				goto out;
			goto finish;

		case ISCSI_OP_SCSI_DATA_IN:
			ret = conn->tp->ep_rdma_write(conn);
			if (ret < 0)
				goto out;
			goto finish;

		default:
			break;
		}
	}

again:
	switch (conn->tx_iostate) {
	case IOSTATE_TX_BHS:
		ret = do_send(conn, IOSTATE_TX_INIT_AHS);
		if (ret < 0)
			break;
	case IOSTATE_TX_INIT_AHS:
		if (conn->rsp.ahssize) {
			conn->tx_iostate = IOSTATE_TX_AHS;
			conn->tx_buffer = conn->rsp.ahs;
			conn->tx_size = conn->rsp.ahssize;

			conn->tx_iostate = IOSTATE_TX_AHS;
		} else
			conn->tx_iostate = hdigest ?
				IOSTATE_TX_INIT_HDIGEST : IOSTATE_TX_INIT_DATA;

		if (conn->tx_iostate != IOSTATE_TX_AHS)
			break;
	case IOSTATE_TX_AHS:
		conn->tx_iostate = hdigest ?
			IOSTATE_TX_INIT_HDIGEST : IOSTATE_TX_INIT_DATA;
		if (conn->tx_iostate != IOSTATE_TX_INIT_HDIGEST)
			break;
	case IOSTATE_TX_INIT_HDIGEST:
		crc = ~0;
		crc = crc32c(crc, &conn->rsp.bhs, BHS_SIZE);
		*(uint32_t *)conn->tx_digest = ~crc;
		conn->tx_iostate = IOSTATE_TX_HDIGEST;
		conn->tx_buffer = conn->tx_digest;
		conn->tx_size = sizeof(conn->tx_digest);
	case IOSTATE_TX_HDIGEST:
		ret = do_send(conn, IOSTATE_TX_INIT_DATA);
		if (ret < 0)
			break;
	case IOSTATE_TX_INIT_DATA:
		if (conn->rsp.datasize) {
			int pad;

			conn->tx_iostate = IOSTATE_TX_DATA;
			conn->tx_buffer = conn->rsp.data;
			conn->tx_size = conn->rsp.datasize;
			pad = conn->tx_size & (conn->tp->data_padding - 1);
			if (pad) {
				pad = PAD_WORD_LEN - pad;
				memset(conn->tx_buffer + conn->tx_size, 0, pad);
				conn->tx_size += pad;
			}
		} else
			conn->tx_iostate = IOSTATE_TX_END;
		if (conn->tx_iostate != IOSTATE_TX_DATA)
			break;
	case IOSTATE_TX_DATA:
		ret = do_send(conn, ddigest ?
			      IOSTATE_TX_INIT_DDIGEST : IOSTATE_TX_END);
		if (ret < 0)
			goto out;
		if (conn->tx_iostate != IOSTATE_TX_INIT_DDIGEST)
			break;
	case IOSTATE_TX_INIT_DDIGEST:
		crc = ~0;
		crc = crc32c(crc, conn->rsp.data,
			     roundup(conn->rsp.datasize,
				     conn->tp->data_padding));
		*(uint32_t *)conn->tx_digest = ~crc;
		conn->tx_iostate = IOSTATE_TX_DDIGEST;
		conn->tx_buffer = conn->tx_digest;
		conn->tx_size = sizeof(conn->tx_digest);
	case IOSTATE_TX_DDIGEST:
		ret = do_send(conn, IOSTATE_TX_END);
		break;
	default:
		eprintf("error %d %d\n", conn->state, conn->tx_iostate);
		exit(1);
	}

	if (ret < 0 || conn->state == STATE_CLOSE)
		goto out;

	if (conn->tx_iostate != IOSTATE_TX_END) {
		if (conn->tp->rdma)
			goto again;  /* avoid event loop, just push */
		goto out;
	}

	if (conn->tx_size) {
		eprintf("error %d %d %d\n", conn->state, conn->tx_iostate,
			conn->tx_size);
		exit(1);
	}

	conn->tp->ep_write_end(conn);

finish:
	cmnd_finish(conn);

	switch (conn->state) {
	case STATE_KERNEL:
		ret = conn_take_fd(conn);
		if (ret)
			conn->state = STATE_CLOSE;
		else {
			conn->state = STATE_SCSI;
			conn_read_pdu(conn);
			conn->tp->ep_event_modify(conn, EPOLLIN);
		}
		break;
	case STATE_EXIT:
	case STATE_CLOSE:
		break;
	case STATE_SCSI:
		iscsi_task_tx_done(conn);
		break;
	default:
		conn_read_pdu(conn);
		conn->tp->ep_event_modify(conn, EPOLLIN);
		break;
	}

out:
	return ret;
}

int iscsi_transportid(int tid, uint64_t itn_id, char *buf, int size)
{
	struct iscsi_session *session;
	char *p;
	uint16_t len;

	session = session_lookup_by_tsih(itn_id);
	if (!session)
		return 0;

	len = 4;
	len += strlen(session->initiator) + 1;
	len += 5; /* separator */
	len += 7; /* isid + '\0' */

	len = ALIGN(len, 4);

	if (len > size)
		return len;

	memset(buf, 0, size);

	buf[0] = 0x05;
	buf[0] |= 0x40;

	put_unaligned_be16(len - 4, buf + 2);

	sprintf(buf + 4, "%s", session->initiator);

	p = buf + (4 + strlen(session->initiator) + 1);

	p += sprintf(p, ",i,0x");

	memcpy(p, session->isid, sizeof(session->isid));

	return len;
}

int iscsi_param_parse_portals(char *p, int do_add,
			int do_delete)
{
	while (*p) {
		if (!strncmp(p, "portal", 6)) {
			char *addr, *q;
			int len = 0, port = 0;

			addr = p + 7;

			if (addr[0] == '[') {
				addr++;
				q = strchr(addr, ']');
				if (!q) {
					eprintf("malformed string when parsing "
						"portal (%s). mismatched ipv6 "
						"'[' ']'\n", p);
					return -1;
				}
				q++;
				len = q - addr -1;
				if (*q != ':')
					q = NULL;
			} else
				q = strchr(addr, ':');

			if (q)
				port = atoi(q + 1);
			else
				q = strchr(addr, ',');

			if (!len) {
				if (q)
					len = q - addr;
				else
					len = strlen(addr);
			}

			if (len) {
				char *tmp;
				tmp = zalloc(len + 1);
				memcpy(tmp, addr, len);
				if (do_add && iscsi_add_portal(tmp,
							port, 1)) {
					free(tmp);
					return -1;
				}
				if (do_delete && iscsi_delete_portal(tmp,
							port)) {
					free(tmp);
					return -1;
				}
			}
		} else if (!strncmp(p, "nop_interval", 12)) {
			iscsi_set_nop_interval(atoi(p+13));
		} else if (!strncmp(p, "nop_count", 9)) {
			iscsi_set_nop_count(atoi(p+10));
		}

		p += strcspn(p, ",");
		if (*p == ',')
			++p;
	}

	return 0;
}

static int iscsi_param_parser(char *p)
{
	portal_arguments = p;
	return 0;
}

static int iscsi_portal_create(char *p)
{
	return iscsi_param_parse_portals(p, 1, 0);
}

static int iscsi_portal_destroy(char *p)
{
	return iscsi_param_parse_portals(p, 0, 1);
}

static struct tgt_driver iscsi = {
	.name			= "iscsi",
	.init			= iscsi_init,
	.exit			= iscsi_exit,
	.target_create		= iscsi_target_create,
	.target_destroy		= iscsi_target_destroy,

	.portal_create		= iscsi_portal_create,
	.portal_destroy		= iscsi_portal_destroy,

	.update			= iscsi_target_update,
	.show			= iscsi_target_show,
	.stat			= iscsi_stat,
	.cmd_end_notify		= iscsi_scsi_cmd_done,
	.mgmt_end_notify	= iscsi_tm_done,
	.transportid		= iscsi_transportid,
	.default_bst		= "rdwr",
};

__attribute__((constructor)) static void iscsi_driver_constructor(void)
{
	register_driver(&iscsi);

	setup_param("iscsi", iscsi_param_parser);
}
