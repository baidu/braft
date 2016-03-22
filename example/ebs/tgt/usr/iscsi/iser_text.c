/*
 * iSCSI extensions for RDMA (iSER)
 * LOGIN and TEXT related code
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

#include "util.h"
#include "iscsid.h"
#include "iser.h"

#if defined(HAVE_VALGRIND) && !defined(NDEBUG)
	#include <valgrind/memcheck.h>
#else
	#define VALGRIND_MAKE_MEM_DEFINED(addr, len)
#endif


static struct iscsi_key login_keys[] = {
	{"InitiatorName",},
	{"InitiatorAlias",},
	{"SessionType",},
	{"TargetName",},
	{NULL, 0, 0, 0, NULL},
};

static char *iser_text_next_key(char **data, int *datasize, char **value)
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

static char *iser_text_key_find(char *data, int datasize, char *searchKey)
{
	int keylen = strlen(searchKey);
	char *key, *value;

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

static void iser_text_key_add(struct iscsi_connection *iscsi_conn,
			      struct iser_pdu *pdu,
			      char *key, char *value)
{
	struct iser_conn *iser_conn = ISER_CONN(iscsi_conn);
	int keylen = strlen(key);
	int valuelen = strlen(value);
	int len = keylen + valuelen + 2;
	char *buffer;

	if (pdu->membuf.size + len > iser_conn->ssize) {
		log_warning("Dropping key: %s=%s, pdu_sz:%d, key_sz:%d, ssize:%d\n",
			    key, value, pdu->membuf.size, len, iser_conn->ssize);
		return;
	}

	dprintf("%s=%s, offset:%d\n", key, value, pdu->membuf.size);

	buffer = pdu->membuf.addr + pdu->membuf.size;
	pdu->membuf.size += len;

	strcpy(buffer, key);
	buffer += keylen;
	*buffer++ = '=';
	strcpy(buffer, value);
}

static void iser_text_key_add_reject(struct iscsi_connection *iscsi_conn,
				     struct iser_pdu *pdu,
				     char *key)
{
	iser_text_key_add(iscsi_conn, pdu, key, "Reject");
}

static void iser_login_security_scan(struct iscsi_connection *iscsi_conn,
				     struct iser_pdu *rx_pdu,
				     struct iser_pdu *tx_pdu)
{
	struct iscsi_login_rsp *rsp_bhs = (struct iscsi_login_rsp *)tx_pdu->bhs;
	char *key, *value, *data, *nextValue;
	int datasize;

	data = rx_pdu->membuf.addr;
	datasize = rx_pdu->membuf.size;

	while ((key = iser_text_next_key(&data, &datasize, &value))) {
		if (!(param_index_by_name(key, login_keys) < 0))
			;
		else if (!strcmp(key, "AuthMethod")) {
			do {
				nextValue = strchr(value, ',');
				if (nextValue)
					*nextValue++ = 0;

				if (!strcmp(value, "None")) {
					if (account_available(iscsi_conn->tid, AUTH_DIR_INCOMING))
						continue;
					iscsi_conn->auth_method = AUTH_NONE;
					iser_text_key_add(iscsi_conn, tx_pdu, key, "None");
					break;
				} else if (!strcmp(value, "CHAP")) {
					if (!account_available(iscsi_conn->tid, AUTH_DIR_INCOMING))
						continue;
					iscsi_conn->auth_method = AUTH_CHAP;
					iser_text_key_add(iscsi_conn, tx_pdu, key, "CHAP");
					break;
				}
			} while ((value = nextValue));

			if (iscsi_conn->auth_method == AUTH_UNKNOWN)
				iser_text_key_add_reject(iscsi_conn, tx_pdu, key);
		} else
			iser_text_key_add(iscsi_conn, tx_pdu, key, "NotUnderstood");
	}
	if (iscsi_conn->auth_method == AUTH_UNKNOWN) {
		rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_AUTH_FAILED;
		iscsi_conn->state = STATE_EXIT;
	}
}

static void iser_login_security_done(struct iscsi_connection *iscsi_conn,
				     struct iser_pdu *rx_pdu,
				     struct iser_pdu *tx_pdu)
{
	struct iscsi_login *req_bhs = (struct iscsi_login *)rx_pdu->bhs;
	struct iscsi_login_rsp *rsp_bhs = (struct iscsi_login_rsp *)tx_pdu->bhs;
	struct iscsi_session *session;

	if (!iscsi_conn->tid)
		return;

	session = session_find_name(iscsi_conn->tid, iscsi_conn->initiator, req_bhs->isid);
	if (session) {
		if (!req_bhs->tsih) {
			struct iscsi_connection *ent, *next;
			struct iser_conn *c;

			/* do session reinstatement */
			list_for_each_entry_safe(ent, next, &session->conn_list,
						 clist) {
				c = container_of(ent, struct iser_conn, h);
				iser_conn_close(c);
			}

			session = NULL;
		} else if (req_bhs->tsih != session->tsih) {
			/* fail the login */
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			iscsi_conn->state = STATE_EXIT;
			return;
		} else if (conn_find(session, iscsi_conn->cid)) {
			/* do connection reinstatement */
		}

		/* add a new connection to the session */
		if (session)
			conn_add_to_session(iscsi_conn, session);
	} else {
		if (req_bhs->tsih) {
			/* fail the login */
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_NO_SESSION;
			iscsi_conn->state = STATE_EXIT;
			return;
		}
		/*
		 * We do nothing here and instantiate a new session
		 * later at login_finish().
		 */
	}
}

static void iser_login_oper_scan(struct iscsi_connection *iscsi_conn,
				 struct iser_pdu *rx_pdu,
				 struct iser_pdu *tx_pdu)
{
	struct iscsi_login_rsp *rsp_bhs = (struct iscsi_login_rsp *)tx_pdu->bhs;
	char *key, *value, *data;
	int datasize, idx, is_rdma = 0;

	data = rx_pdu->membuf.addr;
	datasize = rx_pdu->membuf.size;

	while ((key = iser_text_next_key(&data, &datasize, &value))) {
		dprintf("%s=%s\n", key, value);
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
				if (iscsi_conn->session_param[idx].state
				    == KEY_STATE_START) {
					iser_text_key_add_reject(iscsi_conn, tx_pdu, key);
					continue;
				} else {
					rsp_bhs->status_class =
						ISCSI_STATUS_CLS_INITIATOR_ERR;
					rsp_bhs->status_detail =
						ISCSI_LOGIN_STATUS_INIT_ERR;
					iscsi_conn->state = STATE_EXIT;
					goto out;
				}
			}

			err = param_check_val(session_keys, idx, &val);
			if (err) {
				iser_text_key_add_reject(iscsi_conn, tx_pdu, key);
				continue;
			}

			param_set_val(session_keys, iscsi_conn->session_param, idx, &val);

			switch (iscsi_conn->session_param[idx].state) {
			case KEY_STATE_START:
				if (idx >= ISCSI_PARAM_FIRST_LOCAL)
					break;
				memset(buf, 0, sizeof(buf));
				param_val_to_str(session_keys, idx, val, buf);
				iser_text_key_add(iscsi_conn, tx_pdu, key, buf);
				break;
			case KEY_STATE_REQUEST:
				if (val != iscsi_conn->session_param[idx].val) {
					rsp_bhs->status_class =
						ISCSI_STATUS_CLS_INITIATOR_ERR;
					rsp_bhs->status_detail =
						ISCSI_LOGIN_STATUS_INIT_ERR;
					iscsi_conn->state = STATE_EXIT;
					log_warning("%s %u %u\n",
						    key, val,
						    iscsi_conn->session_param[idx].val);
					goto out;
				}
				break;
			case KEY_STATE_DONE:
				break;
			}
			iscsi_conn->session_param[idx].state = KEY_STATE_DONE;
		} else
			iser_text_key_add(iscsi_conn, tx_pdu, key, "NotUnderstood");
	}

	if (is_rdma) {
		/* do not try to do digests, not supported in iser */
		iscsi_conn->session_param[ISCSI_PARAM_HDRDGST_EN].val = DIGEST_NONE;
		iscsi_conn->session_param[ISCSI_PARAM_DATADGST_EN].val = DIGEST_NONE;
	} else {
		/* do not offer RDMA, initiator must explicitly request */
		iscsi_conn->session_param[ISCSI_PARAM_RDMA_EXTENSIONS].val = 0;
	}

out:
	return;
}

static int iser_login_check_params(struct iscsi_connection *iscsi_conn,
				   struct iser_pdu *pdu)
{
	struct param *p = iscsi_conn->session_param;
	char buf[32];
	int i, cnt;

	for (i = 0, cnt = 0; session_keys[i].name; i++) {
		if (p[i].state == KEY_STATE_START && p[i].val != session_keys[i].def) {
			if (iscsi_conn->state == STATE_LOGIN) {
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
				param_val_to_str(session_keys, i, p[i].val, buf);
				iser_text_key_add(iscsi_conn, pdu, session_keys[i].name, buf);
				p[i].state = KEY_STATE_REQUEST;
			}
			cnt++;
		}
	}

	return cnt;
}

static int iser_login_auth_exec(struct iscsi_connection *iscsi_conn,
				struct iser_pdu *rx_pdu,
				struct iser_pdu *tx_pdu)
{
	struct iscsi_connection fake_iscsi_conn;
	int res;

	switch (iscsi_conn->auth_method) {
	case AUTH_CHAP:
		/* ToDo: need to implement buffer-based chap functions */
		memset(&fake_iscsi_conn, 0, sizeof(fake_iscsi_conn));
		memcpy(&fake_iscsi_conn, iscsi_conn, sizeof(*iscsi_conn));

		fake_iscsi_conn.req_buffer = rx_pdu->membuf.addr;
		fake_iscsi_conn.req.data = rx_pdu->membuf.addr;
		fake_iscsi_conn.req.datasize = rx_pdu->membuf.size;

		fake_iscsi_conn.rsp.datasize = 0; // here the new size will be returned
		fake_iscsi_conn.rsp.data = 0;
		fake_iscsi_conn.rsp_buffer = tx_pdu->membuf.addr;
		fake_iscsi_conn.rsp_buffer_size = 8192; // max buffer capacity

		res = cmnd_exec_auth_chap(&fake_iscsi_conn);
		if (!res) {
		       memcpy(iscsi_conn, &fake_iscsi_conn, sizeof(*iscsi_conn));
		       tx_pdu->membuf.size = fake_iscsi_conn.rsp.datasize;
		}
		break;
	case AUTH_NONE:
		res = 0;
		break;
	default:
		eprintf("Unknown auth. method %d\n", iscsi_conn->auth_method);
		res = -3;
	}

	return res;
}

static void iser_login_start(struct iscsi_connection *iscsi_conn,
			     struct iser_pdu *rx_pdu,
			     struct iser_pdu *tx_pdu)
{
	struct iscsi_login *req_bhs = (struct iscsi_login *)rx_pdu->bhs;
	struct iscsi_login_rsp *rsp_bhs = (struct iscsi_login_rsp *)tx_pdu->bhs;
	char *req_data = rx_pdu->membuf.addr;
	int req_datasize = rx_pdu->membuf.size;
	char *name, *alias, *session_type, *target_name;
	struct iscsi_target *target;
	char buf[NI_MAXHOST + NI_MAXSERV + 4];
	int reason;

	iscsi_conn->cid = be16_to_cpu(req_bhs->cid);
	memcpy(iscsi_conn->isid, req_bhs->isid, sizeof(req_bhs->isid));
	iscsi_conn->tsih = req_bhs->tsih;

	if (!sid64(iscsi_conn->isid, iscsi_conn->tsih)) {
		rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_MISSING_FIELDS;
		iscsi_conn->state = STATE_EXIT;
		return;
	}

	name = iser_text_key_find(req_data, req_datasize, "InitiatorName");
	if (!name) {
		rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_MISSING_FIELDS;
		iscsi_conn->state = STATE_EXIT;
		return;
	}
	iscsi_conn->initiator = strdup(name);

	alias = iser_text_key_find(req_data, req_datasize, "InitiatorAlias");
	if (alias)
		iscsi_conn->initiator_alias = strdup(alias);

	session_type = iser_text_key_find(req_data, req_datasize, "SessionType");
	target_name = iser_text_key_find(req_data, req_datasize, "TargetName");

	iscsi_conn->auth_method = -1;
	iscsi_conn->session_type = SESSION_NORMAL;

	if (session_type) {
		if (!strcmp(session_type, "Discovery"))
			iscsi_conn->session_type = SESSION_DISCOVERY;
		else if (strcmp(session_type, "Normal")) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_NO_SESSION_TYPE;
			iscsi_conn->state = STATE_EXIT;
			return;
		}
	}

	if (iscsi_conn->session_type == SESSION_NORMAL) {
		if (!target_name) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_MISSING_FIELDS;
			iscsi_conn->state = STATE_EXIT;
			return;
		}

		target = target_find_by_name(target_name);
		if (!target) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			iscsi_conn->state = STATE_EXIT;
			return;
		}
		if (!target->rdma) {
			eprintf("Target %s is TCP, but conn cid:%d from %s is RDMA\n",
				target_name, iscsi_conn->cid, iscsi_conn->initiator);
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			iscsi_conn->state = STATE_EXIT;
			return;
		}
		iscsi_conn->tid = target->tid;

		if (target_redirected(target, iscsi_conn, buf, &reason)) {
			iser_text_key_add(iscsi_conn, tx_pdu, "TargetAddress", buf);
			rsp_bhs->status_class = ISCSI_STATUS_CLS_REDIRECT;
			rsp_bhs->status_detail = reason;
			iscsi_conn->state = STATE_EXIT;
			return;
		}

		if (tgt_get_target_state(target->tid) != SCSI_TARGET_READY) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_TARGET_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TARGET_ERROR;
			iscsi_conn->state = STATE_EXIT;
			return;
		}

		if (ip_acl(iscsi_conn->tid, iscsi_conn)) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			iscsi_conn->state = STATE_EXIT;
			return;
		}

		if (iqn_acl(iscsi_conn->tid, iscsi_conn)) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			iscsi_conn->state = STATE_EXIT;
			return;
		}

		if (isns_scn_access(iscsi_conn->tid, name)) {
			rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
			rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_TGT_NOT_FOUND;
			iscsi_conn->state = STATE_EXIT;
			return;
		}

/*      	if (iscsi_conn->target->max_sessions && */
/*      	    (++iscsi_conn->target->session_cnt > iscsi_conn->target->max_sessions)) { */
/*      		iscsi_conn->target->session_cnt--; */
/*      		rsp_bhs->status_class = ISCSI_STATUS_INITIATOR_ERR; */
/*      		rsp_bhs->status_detail = ISCSI_STATUS_TOO_MANY_CONN; */
/*      		iscsi_conn->state = STATE_EXIT; */
/*      		return; */
/*      	} */

		memcpy(iscsi_conn->session_param, target->session_param,
		       sizeof(iscsi_conn->session_param));
		iscsi_conn->exp_cmd_sn = be32_to_cpu(req_bhs->cmdsn);
		iscsi_conn->max_cmd_sn = iscsi_conn->exp_cmd_sn;
		dprintf("set exp_cmdsn:0x%0x\n", iscsi_conn->exp_cmd_sn);
	}
	iser_text_key_add(iscsi_conn, tx_pdu, "TargetPortalGroupTag", "1");
}

static void iser_login_finish(struct iscsi_connection *iscsi_conn,
			      struct iser_pdu *tx_pdu)
{
	struct iscsi_login_rsp *rsp_bhs = (struct iscsi_login_rsp *)tx_pdu->bhs;
	int err;
	uint8_t class, detail;

	switch (iscsi_conn->session_type) {
	case SESSION_NORMAL:
		/*
		 * update based on negotiations (but ep_login_complete
		 * could override)
		 */
		//iscsi_conn->data_inout_max_length =
		//iscsi_conn->session_param[ISCSI_PARAM_MAX_XMIT_DLENGTH].val;

		/*
		 * Allocate transport resources for this connection.
		 */
		err = iser_login_complete(iscsi_conn);
		if (err) {
			class = ISCSI_STATUS_CLS_TARGET_ERR;
			detail = ISCSI_LOGIN_STATUS_NO_RESOURCES;
			goto fail;
		}
		if (!iscsi_conn->session) {
			err = session_create(iscsi_conn);
			if (err) {
				class = ISCSI_STATUS_CLS_TARGET_ERR;
				detail = ISCSI_LOGIN_STATUS_TARGET_ERROR;
				goto fail;
			}
		} else {
			/*
			if (iscsi_conn->rdma ^ iscsi_conn->session->rdma) {
				eprintf("new iscsi_conn rdma %d, but session %d\n",
					iscsi_conn->rdma, iscsi_conn->session->rdma);

				class = ISCSI_STATUS_CLS_INITIATOR_ERR;
				detail =ISCSI_LOGIN_STATUS_INVALID_REQUEST;
				goto fail;
			}
			*/
		}
		memcpy(iscsi_conn->isid, iscsi_conn->session->isid, sizeof(iscsi_conn->isid));
		iscsi_conn->tsih = iscsi_conn->session->tsih;
		break;
	case SESSION_DISCOVERY:
		err = iser_login_complete(iscsi_conn);
		if (err) {
			class = ISCSI_STATUS_CLS_TARGET_ERR;
			detail = ISCSI_LOGIN_STATUS_NO_RESOURCES;
			goto fail;
		}
		/* set a dummy tsih value */
		iscsi_conn->tsih = 1;
		break;
	}

	return;
fail:
	rsp_bhs->flags = 0;
	rsp_bhs->status_class = class;
	rsp_bhs->status_detail = detail;
	iscsi_conn->state = STATE_EXIT;
	return;
}

void iser_login_exec(struct iscsi_connection *iscsi_conn,
		     struct iser_pdu *rx_pdu,
		     struct iser_pdu *tx_pdu)
{
	struct iscsi_login *req_bhs = (struct iscsi_login *)rx_pdu->bhs;
	struct iscsi_login_rsp *rsp_bhs = (struct iscsi_login_rsp *)tx_pdu->bhs;
	int stay = 0, nsg_disagree = 0;

	tx_pdu->membuf.size = 0;

	memset(rsp_bhs, 0, BHS_SIZE);
	if ((req_bhs->opcode & ISCSI_OPCODE_MASK) != ISCSI_OP_LOGIN ||
	    !(req_bhs->opcode & ISCSI_OP_IMMEDIATE)) {
		/* reject */
	}

	rsp_bhs->opcode = ISCSI_OP_LOGIN_RSP;
	rsp_bhs->max_version = ISCSI_DRAFT20_VERSION;
	rsp_bhs->active_version = ISCSI_DRAFT20_VERSION;
	rsp_bhs->itt = req_bhs->itt;

	if (/* req_bhs->max_version < ISCSI_VERSION || */
	    req_bhs->min_version > ISCSI_DRAFT20_VERSION) {
		rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
		rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_NO_VERSION;
		iscsi_conn->state = STATE_EXIT;
		return;
	}

	iscsi_conn->exp_cmd_sn = iscsi_conn->max_cmd_sn = ntohl(req_bhs->cmdsn);

	switch (ISCSI_LOGIN_CURRENT_STAGE(req_bhs->flags)) {
	case ISCSI_SECURITY_NEGOTIATION_STAGE:
		dprintf("conn:%p Login request (security negotiation): %d\n",
			iscsi_conn, iscsi_conn->state);

		rsp_bhs->flags = ISCSI_SECURITY_NEGOTIATION_STAGE << 2;

		switch (iscsi_conn->state) {
		case STATE_READY:
			iscsi_conn->state = STATE_SECURITY;
			iser_login_start(iscsi_conn, rx_pdu, tx_pdu);

			if (rsp_bhs->status_class)
				return;
			/* fall through */
		case STATE_SECURITY:
			iser_login_security_scan(iscsi_conn, rx_pdu, tx_pdu);
			if (rsp_bhs->status_class)
				return;
			if (iscsi_conn->auth_method != AUTH_NONE) {
				iscsi_conn->state = STATE_SECURITY_AUTH;
				iscsi_conn->auth_state = AUTH_STATE_START;
			}
			break;
		case STATE_SECURITY_AUTH:
			switch (iser_login_auth_exec(iscsi_conn, rx_pdu, tx_pdu)) {
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
		dprintf("conn:%p Login request (operational negotiation): %d\n",
			iscsi_conn, iscsi_conn->state);
		rsp_bhs->flags = ISCSI_OP_PARMS_NEGOTIATION_STAGE << 2;

		switch (iscsi_conn->state) {
		case STATE_READY:
			iscsi_conn->state = STATE_LOGIN;
			iser_login_start(iscsi_conn, rx_pdu, tx_pdu);

			if (account_available(iscsi_conn->tid, AUTH_DIR_INCOMING))
				goto auth_err;
			if (rsp_bhs->status_class)
				return;
			iser_login_oper_scan(iscsi_conn, rx_pdu, tx_pdu);
			if (rsp_bhs->status_class)
				return;
			stay = iser_login_check_params(iscsi_conn, tx_pdu);
			break;
		case STATE_LOGIN:
			iser_login_oper_scan(iscsi_conn, rx_pdu, tx_pdu);
			if (rsp_bhs->status_class)
				return;
			stay = iser_login_check_params(iscsi_conn, tx_pdu);
			break;
		default:
			goto init_err;
		}
		break;
	default:
		goto init_err;
	}

	if (rsp_bhs->status_class)
		return;
	if (iscsi_conn->state != STATE_SECURITY_AUTH &&
	    req_bhs->flags & ISCSI_FLAG_LOGIN_TRANSIT) {
		int nsg = ISCSI_LOGIN_NEXT_STAGE(req_bhs->flags);

		switch (nsg) {
		case ISCSI_OP_PARMS_NEGOTIATION_STAGE:
			switch (iscsi_conn->state) {
			case STATE_SECURITY:
			case STATE_SECURITY_DONE:
				iscsi_conn->state = STATE_SECURITY_LOGIN;
				iser_login_security_done(iscsi_conn, rx_pdu, tx_pdu);
				break;
			default:
				goto init_err;
			}
			break;
		case ISCSI_FULL_FEATURE_PHASE:
			switch (iscsi_conn->state) {
			case STATE_SECURITY:
			case STATE_SECURITY_DONE:
				if ((nsg_disagree = iser_login_check_params(iscsi_conn, tx_pdu))) {
					iscsi_conn->state = STATE_LOGIN;
					nsg = ISCSI_OP_PARMS_NEGOTIATION_STAGE;
					break;
				}
				iscsi_conn->state = STATE_SECURITY_FULL;
				iser_login_security_done(iscsi_conn, rx_pdu, tx_pdu);
				break;
			case STATE_LOGIN:
				if (stay)
					nsg = ISCSI_OP_PARMS_NEGOTIATION_STAGE;
				else
					iscsi_conn->state = STATE_LOGIN_FULL;
				break;
			default:
				goto init_err;
			}
			if (!stay && !nsg_disagree) {
				iser_login_finish(iscsi_conn, tx_pdu);

				if (rsp_bhs->status_class)
					return;
			}
			break;
		default:
			goto init_err;
		}
		rsp_bhs->flags |= nsg | (stay ? 0 : ISCSI_FLAG_LOGIN_TRANSIT);
	}

	if (iscsi_conn->exp_cmd_sn == ntohl(req_bhs->cmdsn))
		iscsi_conn->exp_cmd_sn++;

	memcpy(rsp_bhs->isid, iscsi_conn->isid, sizeof(rsp_bhs->isid));
	rsp_bhs->tsih = iscsi_conn->tsih;
	rsp_bhs->statsn = cpu_to_be32(iscsi_conn->stat_sn++);
	rsp_bhs->exp_cmdsn = cpu_to_be32(iscsi_conn->exp_cmd_sn);
	rsp_bhs->max_cmdsn = cpu_to_be32(iscsi_conn->max_cmd_sn);
	return;

init_err:
	eprintf("conn:%p, Initiator error\n", iscsi_conn);
	rsp_bhs->flags = 0;
	rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
	rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_INIT_ERR;
	iscsi_conn->state = STATE_EXIT;
	return;

auth_err:
	eprintf("conn:%p, Authentication error\n", iscsi_conn);
	rsp_bhs->flags = 0;
	rsp_bhs->status_class = ISCSI_STATUS_CLS_INITIATOR_ERR;
	rsp_bhs->status_detail = ISCSI_LOGIN_STATUS_AUTH_FAILED;
	iscsi_conn->state = STATE_EXIT;
	return;
}


void iser_target_list_build(struct iscsi_connection *conn, struct iser_pdu *tx_pdu, char *addr, char *name)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (name && strcmp(tgt_targetname(target->tid), name))
			continue;

		if (!target->rdma)
			continue;

		if (ip_acl(target->tid, conn))
			continue;

		if (iqn_acl(target->tid, conn))
			continue;

		if (isns_scn_access(target->tid, conn->initiator))
			continue;

		iser_text_key_add(conn, tx_pdu, "TargetName", tgt_targetname(target->tid));
		iser_text_key_add(conn, tx_pdu, "TargetAddress", addr);
	}
}

static void iser_text_scan(struct iscsi_connection *iscsi_conn,
			   struct iser_pdu *rx_pdu,
			   struct iser_pdu *tx_pdu)
{
	char *key, *value, *data;
	int datasize;

	data = rx_pdu->membuf.addr;
	datasize = rx_pdu->membuf.size;

	while ((key = iser_text_next_key(&data, &datasize, &value))) {
		if (!strcmp(key, "SendTargets")) {
			struct sockaddr_storage ss;
			socklen_t slen, blen;
			char *p, buf[NI_MAXHOST + 128];
			int port;

			if (value[0] == 0)
				continue;

			p = buf;
			blen = sizeof(buf);

			slen = sizeof(ss);
			iscsi_conn->tp->ep_getsockname(iscsi_conn,
						       (struct sockaddr *) &ss,
						       &slen);
			if (ss.ss_family == AF_INET6) {
				*p++ = '[';
				blen--;
			}

			slen = sizeof(ss);
			getnameinfo((struct sockaddr *) &ss, slen, p, blen,
				    NULL, 0, NI_NUMERICHOST);

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
			iser_target_list_build(iscsi_conn, tx_pdu, buf,
					  strcmp(value, "All") ? value : NULL);
		} else
			iser_text_key_add(iscsi_conn, tx_pdu, key, "NotUnderstood");
	}
}

int iser_text_exec(struct iscsi_connection *iscsi_conn,
		   struct iser_pdu *rx_pdu,
		   struct iser_pdu *tx_pdu)
{
	struct iscsi_text *req = (struct iscsi_text *)rx_pdu->bhs;
	struct iscsi_text_rsp *rsp = (struct iscsi_text_rsp *)tx_pdu->bhs;

	memset(rsp, 0, BHS_SIZE);

	if (be32_to_cpu(req->ttt) != 0xffffffff) {
		/* reject */;
	}
	rsp->opcode = ISCSI_OP_TEXT_RSP;
	rsp->itt = req->itt;
	/* rsp->ttt = rsp->ttt; */
	rsp->ttt = 0xffffffff;
	iscsi_conn->exp_cmd_sn = be32_to_cpu(req->cmdsn);
	if (!(req->opcode & ISCSI_OP_IMMEDIATE))
		iscsi_conn->exp_cmd_sn++;

	dprintf("Text request: %d\n", iscsi_conn->state);
	iser_text_scan(iscsi_conn, rx_pdu, tx_pdu);

	if (tx_pdu->membuf.size > MAX_KEY_VALUE_PAIRS) {
		eprintf("Text pdu size %d too big, can't send at once\n",
			tx_pdu->membuf.size);
		tx_pdu->membuf.size = 0;
	}

	if (req->flags & ISCSI_FLAG_CMD_FINAL)
		rsp->flags = ISCSI_FLAG_CMD_FINAL;

	rsp->statsn = cpu_to_be32(iscsi_conn->stat_sn++);
	rsp->exp_cmdsn = cpu_to_be32(iscsi_conn->exp_cmd_sn);
	rsp->max_cmdsn = cpu_to_be32(iscsi_conn->max_cmd_sn);

	return 0;
}
