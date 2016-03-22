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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include "iscsid.h"
#include "tgtadm.h"
#include "tgtd.h"
#include "target.h"
#include "util.h"

LIST_HEAD(iscsi_targets_list);

static int netmask_match_v6(struct sockaddr *sa1, struct sockaddr *sa2, uint32_t mbit)
{
	uint16_t mask, a1[8], a2[8];
	int i;

	for (i = 0; i < 8; i++) {
		a1[i] = ntohs(((struct sockaddr_in6 *) sa1)->sin6_addr.s6_addr16[i]);
		a2[i] = ntohs(((struct sockaddr_in6 *) sa2)->sin6_addr.s6_addr16[i]);
	}

	for (i = 0; i < mbit / 16; i++)
		if (a1[i] ^ a2[i])
			return 0;

	if (mbit % 16) {
		mask = ~((1 << (16 - (mbit % 16))) - 1);
		if ((mask & a1[mbit / 16]) ^ (mask & a2[mbit / 16]))
			return 0;
	}

	return 1;
}

static int netmask_match_v4(struct sockaddr *sa1, struct sockaddr *sa2, uint32_t mbit)
{
	uint32_t s1, s2, mask = ~((1 << (32 - mbit)) - 1);

	s1 = htonl(((struct sockaddr_in *) sa1)->sin_addr.s_addr);
	s2 = htonl(((struct sockaddr_in *) sa2)->sin_addr.s_addr);

	if (~mask & s1)
		return 0;

	if (!((mask & s2) ^ (mask & s1)))
		return 1;

	return 0;
}

static int netmask_match(struct sockaddr *sa1, struct sockaddr *sa2, char *buf)
{
	uint32_t mbit;
	uint8_t family = sa1->sa_family;

	mbit = strtoul(buf, NULL, 0);
	if ((family == AF_INET && mbit > 31) ||
	    (family == AF_INET6 && mbit > 127))
		return 0;

	if (family == AF_INET)
		return netmask_match_v4(sa1, sa2, mbit);

	return netmask_match_v6(sa1, sa2, mbit);
}

static int address_match(struct sockaddr *sa1, struct sockaddr *sa2)
{
	if (sa1->sa_family == AF_INET)
		return ((struct sockaddr_in *) sa1)->sin_addr.s_addr ==
			((struct sockaddr_in *) sa2)->sin_addr.s_addr;
	else {
		struct in6_addr *a1, *a2;

		a1 = &((struct sockaddr_in6 *) sa1)->sin6_addr;
		a2 = &((struct sockaddr_in6 *) sa2)->sin6_addr;

		return (a1->s6_addr32[0] == a2->s6_addr32[0] &&
			a1->s6_addr32[1] == a2->s6_addr32[1] &&
			a1->s6_addr32[2] == a2->s6_addr32[2] &&
			a1->s6_addr32[3] == a2->s6_addr32[3]);
	}

	return 0;
}

static int ip_match(struct iscsi_connection *conn, char *address)
{
	struct sockaddr_storage from;
	struct addrinfo hints, *res;
	socklen_t len;
	char *str, *p, *q;
	int err;

	len = sizeof(from);
	err = conn->tp->ep_getpeername(conn, (struct sockaddr *) &from, &len);
	if (err < 0)
		return -EPERM;

	str = p = strdup(address);
	if (!p)
		return -EPERM;

	if (!strcmp(p, "ALL")) {
		err = 0;
		goto out;
	}

	if (*p == '[') {
		p++;
		if (!(q = strchr(p, ']'))) {
			err = -EPERM;
			goto out;
		}
		*(q++) = '\0';
	} else
		q = p;

	if ((q = strchr(q, '/')))
		*(q++) = '\0';

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICHOST;

	err = getaddrinfo(p, NULL, &hints, &res);
	if (err < 0) {
		err = -EPERM;
		goto out;
	}

	if (q)
		err = netmask_match(res->ai_addr, (struct sockaddr *) &from, q);
	else
		err = address_match(res->ai_addr, (struct sockaddr *) &from);

	err = !err;

	freeaddrinfo(res);
out:
	free(str);
	return err;
}

int ip_acl(int tid, struct iscsi_connection *conn)
{
	int idx, err;
	char *addr;

	for (idx = 0;; idx++) {
		addr = acl_get(tid, idx);
		if (!addr)
			break;

		err = ip_match(conn, addr);
		if (!err)
			return 0;
	}
	return -EPERM;
}

static int iqn_match(struct iscsi_connection *conn, char *name)
{
	return strcmp(conn->initiator, name);
}

int iqn_acl(int tid, struct iscsi_connection *conn)
{
	int idx, enable, err;
	char *name;

	enable = 0;
	for (idx = 0;; idx++) {
		name = iqn_acl_get(tid, idx);
		if (!name)
			break;

		enable = 1;
		err = iqn_match(conn, name);
		if (!err)
			return 0;
	}

	if (!enable)
		return 0;
	else
		return -EPERM;
}

static int
get_redirect_address(char *callback, char *buffer, int buflen,
			char **address, char **ip_port, int *rsn)
{
	char *p, *addr, *port;

	bzero(buffer, buflen);
	if (call_program(callback, NULL, NULL, buffer, buflen, 0))
		return -1;

	/* syntax is string_addr:string_port:string_reason */
	addr = p = buffer;
	if (*p == '[') {
		while (*p != ']' && *p != '\0')
			p++;
		if (*p == ']') {
			p++;
			if (*p != ':')
				return -1;
		}
	} else {
		while (*p != ':' && *p != '\0')
			p++;
	}
	if (!*p)
		return -1;
	*p = '\0';
	port = ++p;
	while (*p != ':' && *p != '\0')
		p++;
	if (!*p)
		return -1;
	*p = '\0';
	p++;
	if (!strncmp(p, "Temporary", 9))
		*rsn = ISCSI_LOGIN_STATUS_TGT_MOVED_TEMP;
	else if (!strncmp(p, "Permanent", 9))
		*rsn = ISCSI_LOGIN_STATUS_TGT_MOVED_PERM;
	else
		return -1;
	*address = addr;
	*ip_port = port;
	return 0;
}

int target_redirected(struct iscsi_target *target,
	struct iscsi_connection *conn, char *buf, int *reason)
{
	struct sockaddr_storage from;
	struct addrinfo hints, *res;
	socklen_t len;
	int ret, rsn = 0;
	char *p, *q, *str, *port = NULL, *addr;
	char buffer[NI_MAXHOST + NI_MAXSERV + 4];
	char dst[INET6_ADDRSTRLEN], in_buf[1024];

	len = sizeof(from);
	ret = conn->tp->ep_getpeername(conn, (struct sockaddr *)&from, &len);
	if (ret < 0)
		return 0;

	ret = 1;
	if (target->redirect_info.callback) {
		p = in_buf;
		p += sprintf(p, "%s ", target->redirect_info.callback);
		p += sprintf(p, "%s ", tgt_targetname(target->tid));
		ret = getnameinfo((struct sockaddr *)&from, sizeof(from), dst,
				sizeof(dst), NULL, 0, NI_NUMERICHOST);
		if (ret)
			goto predefined;
		sprintf(p, "%s", dst);
		ret = get_redirect_address(in_buf, buffer,
					sizeof(buffer), &addr, &port, &rsn);
		if (ret)
			return -1;
	}

predefined:
	if (ret) {
		if (!strlen(target->redirect_info.addr))
			return 0;

		addr = target->redirect_info.addr;
		port = target->redirect_info.port;
		rsn = target->redirect_info.reason;
	}

	if (rsn != ISCSI_LOGIN_STATUS_TGT_MOVED_TEMP &&
	    rsn != ISCSI_LOGIN_STATUS_TGT_MOVED_PERM)
		return 0;

	p = strdup(addr);
	if (!p)
		return 0;
	str = p;

	if (*p == '[') {
		p++;
		if (!(q = strchr(p, ']'))) {
			free(str);
			return 0;
		}
		*(q++) = '\0';
	}

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICHOST;

	ret = getaddrinfo(p, NULL, &hints, &res);
	if (ret < 0) {
		free(str);
		return 0;
	}

	ret = address_match(res->ai_addr, (struct sockaddr *)&from);
	freeaddrinfo(res);
	free(str);

	if (!ret) {
		sprintf(buf, "%s:%s", addr, port);
		*reason = rsn;
	}
	return !ret;
}

void target_list_build(struct iscsi_connection *conn, char *addr, char *name)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (name && strcmp(tgt_targetname(target->tid), name))
			continue;

		if (ip_acl(target->tid, conn))
			continue;

		if (iqn_acl(target->tid, conn))
			continue;

		if (isns_scn_access(target->tid, conn->initiator))
			continue;

		text_key_add(conn, "TargetName", tgt_targetname(target->tid));
		text_key_add(conn, "TargetAddress", addr);
	}
}

struct iscsi_target *target_find_by_name(const char *name)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (!strcmp(tgt_targetname(target->tid), name))
			return target;
	}

	return NULL;
}

struct iscsi_target* target_find_by_id(int tid)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (target->tid == tid)
			return target;
	}

	return NULL;
}

void iscsi_target_destroy(int tid, int force)
{
	struct iscsi_target* target;
	struct iscsi_session *session, *stmp;
	struct iscsi_connection *conn, *ctmp;

	target = target_find_by_id(tid);
	if (!target) {
		eprintf("can't find the target %d\n", tid);
		return;
	}

	if (!force && target->nr_sessions) {
		eprintf("the target %d still has sessions\n", tid);
		return;
	}

	list_for_each_entry_safe(session, stmp, &target->sessions_list, slist) {
		list_for_each_entry_safe(conn, ctmp, &session->conn_list, clist) {
		    eprintf("close conn %p", conn);
			conn_close(conn);
		}
	}

	if (!list_empty(&target->sessions_list)) {
		eprintf("bug still have sessions %d\n", tid);
		exit(-1);
	}

	list_del(&target->tlist);
	if (target->redirect_info.callback)
		free(target->redirect_info.callback);
	free(target);
	isns_target_deregister(tgt_targetname(tid));

	return;
}

int iscsi_target_create(struct target *t)
{
	int tid = t->tid;
	struct iscsi_target *target;
	struct param default_tgt_session_param[] = {
		[ISCSI_PARAM_MAX_RECV_DLENGTH] = {0, 8192},
		[ISCSI_PARAM_HDRDGST_EN] = {0, DIGEST_NONE},
		[ISCSI_PARAM_DATADGST_EN] = {0, DIGEST_NONE},
		[ISCSI_PARAM_INITIAL_R2T_EN] = {0, 1},
		[ISCSI_PARAM_MAX_R2T] = {0, 1},
		[ISCSI_PARAM_IMM_DATA_EN] = {0, 1},
		[ISCSI_PARAM_FIRST_BURST] = {0, 65536},
		[ISCSI_PARAM_MAX_BURST] = {0, 262144},
		[ISCSI_PARAM_PDU_INORDER_EN] = {0, 1},
		[ISCSI_PARAM_DATASEQ_INORDER_EN] = {0, 1},
		[ISCSI_PARAM_ERL] = {0, 0},
		[ISCSI_PARAM_IFMARKER_EN] = {0, 0},
		[ISCSI_PARAM_OFMARKER_EN] = {0, 0},
		[ISCSI_PARAM_DEFAULTTIME2WAIT] = {0, 2},
		[ISCSI_PARAM_DEFAULTTIME2RETAIN] = {0, 20},
		[ISCSI_PARAM_OFMARKINT] = {0, 2048},
		[ISCSI_PARAM_IFMARKINT] = {0, 2048},
		[ISCSI_PARAM_MAXCONNECTIONS] = {0, 1},
		[ISCSI_PARAM_RDMA_EXTENSIONS] = {0, 1},
		[ISCSI_PARAM_TARGET_RDSL] = {0, 262144},
		[ISCSI_PARAM_INITIATOR_RDSL] = {0, 262144},
		[ISCSI_PARAM_MAX_OUTST_PDU] =  {0, 0},  /* not in open-iscsi */
		/* "local" parmas, never sent to the initiator */
		[ISCSI_PARAM_MAX_XMIT_DLENGTH] = {0, 8192},  /* do not edit */
		[ISCSI_PARAM_MAX_QUEUE_CMD] = {0, MAX_QUEUE_CMD_DEF},
	};

	target = malloc(sizeof(*target));
	if (!target)
		return -ENOMEM;

	memset(target, 0, sizeof(*target));

	memcpy(target->session_param, default_tgt_session_param,
	       sizeof(target->session_param));

	INIT_LIST_HEAD(&target->tlist);
	INIT_LIST_HEAD(&target->sessions_list);
	INIT_LIST_HEAD(&target->isns_list);
	target->tid = tid;
	target->nop_interval = default_nop_interval;
	target->nop_count = default_nop_count;
	list_add_tail(&target->tlist, &iscsi_targets_list);

	isns_target_register(tgt_targetname(tid));
	return 0;
}

static int iscsi_session_param_update(struct iscsi_target* target, int idx, char *str)
{
	int err;
	unsigned int val;

	err = param_str_to_val(session_keys, idx, str, &val);
	if (err)
		return err;

	err = param_check_val(session_keys, idx, &val);
	if (err < 0)
		return err;

	target->session_param[idx].val = val;

	dprintf("%s %s %u\n", session_keys[idx].name, str, val);

	return 0;
}

tgtadm_err iscsi_target_update(int mode, int op, int tid, uint64_t sid, uint64_t lun,
			       uint32_t cid, char *name)
{
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;
	int idx, err;
	char *str;
	struct iscsi_target* target;

	switch (mode) {
	case MODE_SYSTEM:
		adm_err = isns_update(name);
		break;
	case MODE_TARGET:
		target = target_find_by_id(tid);
		if (!target)
			return TGTADM_NO_TARGET;

		str = name + strlen(name) + 1;

		dprintf("%s:%s\n", name, str);

		if (!strncmp(name, "RedirectAddress", 15)) {
			snprintf(target->redirect_info.addr,
				 sizeof(target->redirect_info.addr), "%s", str);
			adm_err = TGTADM_SUCCESS;
			break;
		} else if (!strncmp(name, "RedirectPort", 12)) {
			snprintf(target->redirect_info.port,
				 sizeof(target->redirect_info.port), "%s", str);
			adm_err = TGTADM_SUCCESS;
			break;
		} else if (!strncmp(name, "RedirectReason", 14)) {
			if (!strncmp(str, "Temporary", 9)) {
				target->redirect_info.reason =
					ISCSI_LOGIN_STATUS_TGT_MOVED_TEMP;
				adm_err = TGTADM_SUCCESS;
			} else if (!strncmp(str, "Permanent", 9)) {
				target->redirect_info.reason =
					ISCSI_LOGIN_STATUS_TGT_MOVED_PERM;
				adm_err = TGTADM_SUCCESS;
			} else
				break;
		} else if (!strncmp(name, "RedirectCallback", 16)) {
			target->redirect_info.callback = strdup(str);
			if (!target->redirect_info.callback) {
				adm_err = TGTADM_NOMEM;
				break;
			}
			adm_err = TGTADM_SUCCESS;
		} else if (!strncmp(name, "nop_count", 9)) {
			err = iscsi_update_target_nop_count(tid,
				atoi(&name[10]));
			adm_err = !err ? TGTADM_SUCCESS :
				TGTADM_INVALID_REQUEST;
			break;
		} else if (!strncmp(name, "nop_interval", 12)) {
			err = iscsi_update_target_nop_interval(tid,
				atoi(&name[13]));
			adm_err = !err ? TGTADM_SUCCESS :
				TGTADM_INVALID_REQUEST;
			break;
		}

		idx = param_index_by_name(name, session_keys);
		if (idx >= 0) {
			err = iscsi_session_param_update(target, idx, str);
			adm_err = !err ? TGTADM_SUCCESS : TGTADM_INVALID_REQUEST;
		}
		break;
	case MODE_CONNECTION:
		if (op == OP_DELETE)
			adm_err = conn_close_admin(tid, sid, cid);
		break;
	default:
		break;
	}
	return adm_err;
}

static tgtadm_err show_iscsi_param(struct param *param, struct concat_buf *b)
{
	struct iscsi_key *keys = session_keys;
	int i;
	char value[64];

	for (i = 0; session_keys[i].name; i++) {
		param_val_to_str(keys, i, param[i].val, value);
		concat_printf(b, "%s=%s\n", keys[i].name, value);
	}

	return TGTADM_SUCCESS;
}

static struct iscsi_session *iscsi_target_find_session(
	struct iscsi_target *target,
	uint64_t sid)
{
	struct iscsi_session *session;

	list_for_each_entry(session, &target->sessions_list, slist) {
		if (session->tsih == sid)
			return session;
	}

	return NULL;

}

static tgtadm_err iscsi_target_show_session(struct iscsi_target *target, uint64_t sid,
					    struct concat_buf *b)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;
	struct iscsi_session *session;

	session = iscsi_target_find_session(target, sid);
	if (session)
		adm_err = show_iscsi_param(session->session_param, b);

	return adm_err;
}

static tgtadm_err iscsi_target_show_connections(struct iscsi_target *target,
						uint64_t sid,
						struct concat_buf *b)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;
	struct iscsi_session *session;
	struct iscsi_connection *conn;
	char addr[128];

	list_for_each_entry(session, &target->sessions_list, slist) {
		list_for_each_entry(conn, &session->conn_list, clist) {
			memset(addr, 0, sizeof(addr));
			conn->tp->ep_show(conn, addr, sizeof(addr));

			concat_printf(b, "Session: %u\n"
				_TAB1 "Connection: %u\n"
				_TAB2 "Initiator: %s\n"
				_TAB2 "%s\n",
				session->tsih,
				conn->cid,
				session->initiator,
				addr);
		}
	}
	return adm_err;
}

static tgtadm_err iscsi_target_show_portals(struct iscsi_target *target, uint64_t sid,
					    struct concat_buf *b)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;
	struct iscsi_portal *portal;

	list_for_each_entry(portal, &iscsi_portals_list,
			iscsi_portal_siblings) {
		int is_ipv6;

		is_ipv6 = strchr(portal->addr, ':') != NULL;
		concat_printf(b, "Portal: %s%s%s:%d,%d\n",
			       is_ipv6 ? "[" : "",
			       portal->addr,
			       is_ipv6 ? "]" : "",
			       portal->port ? portal->port : ISCSI_LISTEN_PORT,
			       portal->tpgt);
	}

	return adm_err;
}

static tgtadm_err show_redirect_info(struct iscsi_target *target, struct concat_buf *b)
{
	tgtadm_err adm_err = TGTADM_SUCCESS;

	concat_printf(b, "RedirectAddress=%s\n", target->redirect_info.addr);
	concat_printf(b, "RedirectPort=%s\n", target->redirect_info.port);
	if (target->redirect_info.reason == ISCSI_LOGIN_STATUS_TGT_MOVED_TEMP)
		concat_printf(b, "RedirectReason=Temporary\n");
	else if (target->redirect_info.reason == ISCSI_LOGIN_STATUS_TGT_MOVED_PERM)
		concat_printf(b, "RedirectReason=Permanent\n");
	else
		concat_printf(b, "RedirectReason=Unknown\n");

	return adm_err;
}

static tgtadm_err show_redirect_callback(struct iscsi_target *target, struct concat_buf *b)
{
	concat_printf(b, "RedirectCallback=%s\n", target->redirect_info.callback);

	return TGTADM_SUCCESS;
}

tgtadm_err iscsi_target_show(int mode, int tid, uint64_t sid, uint32_t cid, uint64_t lun,
			     struct concat_buf *b)
{
	struct iscsi_target* target = NULL;
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	if (mode != MODE_SYSTEM && mode != MODE_PORTAL) {
	    target = target_find_by_id(tid);
	    if (!target)
		    return TGTADM_NO_TARGET;
	}

	switch (mode) {
	case MODE_SYSTEM:
		adm_err = isns_show(b);
		break;
	case MODE_TARGET:
		if (target->redirect_info.callback)
			adm_err = show_redirect_callback(target, b);
		else if (strlen(target->redirect_info.addr))
			adm_err = show_redirect_info(target, b);
		else
			adm_err = show_iscsi_param(target->session_param, b);
		break;
	case MODE_SESSION:
		adm_err = iscsi_target_show_session(target, sid, b);
		break;
	case MODE_PORTAL:
		adm_err = iscsi_target_show_portals(target, sid, b);
		break;
	case MODE_CONNECTION:
		adm_err = iscsi_target_show_connections(target, sid, b);
		break;
	default:
		break;
	}

	return adm_err;
}

static void _stat_iscsi_conn_hdr(struct concat_buf *b)
{
	concat_printf(b,
		"sid cid rxdata_octets txdata_octets dataout_pdus datain_pdus cmd_pdus rsp_pdus\n");
}

static void _stat_iscsi_conn(struct iscsi_connection *conn, struct concat_buf *b)
{
	concat_printf(b, "%3d %3d"
		      " %13" PRIu64
		      " %13" PRIu64
		      " %12" PRIu32
		      " %11" PRIu32
		      " %8" PRIu32
		      " %8" PRIu32 "\n",
		      (unsigned int)conn->session->tsih,
		      (unsigned int)conn->cid,
		      conn->stats.rxdata_octets,
		      conn->stats.txdata_octets,
		      conn->stats.dataout_pdus,
		      conn->stats.datain_pdus,
		      conn->stats.scsicmd_pdus,
		      conn->stats.scsirsp_pdus);
}

static tgtadm_err _stat_iscsi_session(struct iscsi_session *session,
				      uint64_t lun, int filter_lun,
				      struct concat_buf *b)
{
	struct iscsi_target *target = session->target;
	uint64_t itn_id = session->tsih;
	int tid = target->tid;	/* global target id */
	struct it_nexus *itn;
	struct it_nexus_lu_info *itn_lu;
	struct scsi_lu *lu;
	struct iscsi_connection *conn;

	dprintf("tsih:%d lun:%" PRIu64 " filter_lun:%d\n",
		(unsigned int)session->tsih, lun, filter_lun);

	itn = it_nexus_lookup(tid, itn_id);
	if (!itn) {
		eprintf("invalid nexus %d %" PRIx64 "\n", tid, itn_id);
		return TGTADM_NO_SESSION;
	}

	tgt_stat_header(b);
	list_for_each_entry(itn_lu, &itn->itn_itl_info_list, itn_itl_info_siblings) {
		lu = itn_lu->lu;
		tgt_stat_line(tid, lu->lun, session->tsih, &itn_lu->stat, b);
	}

	if (!list_empty(&session->conn_list)) {
		concat_printf(b, "\n");
		_stat_iscsi_conn_hdr(b);
	}
	list_for_each_entry(conn, &session->conn_list, clist) {
		_stat_iscsi_conn(conn, b);
	}

	return TGTADM_SUCCESS;
}

static tgtadm_err iscsi_stat_connection(uint64_t sid, uint32_t cid, struct concat_buf *b)
{
	struct iscsi_session *session;
	struct iscsi_connection *conn;

	dprintf("sid:%" PRIu64 "cid:%" PRIu32 "\n", sid, cid);

	session = session_lookup_by_tsih((uint16_t)sid);
	if (!session)
		return TGTADM_NO_SESSION;

	conn = conn_find(session, cid);
	if (!conn)
		return TGTADM_NO_CONNECTION;

	_stat_iscsi_conn_hdr(b);
	_stat_iscsi_conn(conn, b);

	return TGTADM_SUCCESS;
}

static tgtadm_err iscsi_stat_session_by_sid(uint64_t sid, struct concat_buf *b)
{
	struct iscsi_session *session;

	dprintf("sid:%" PRIu64 "\n", sid);

	session = session_lookup_by_tsih((uint16_t)sid);
	if (session)
		return _stat_iscsi_session(session, 0, 0, b);
	else
		return TGTADM_NO_SESSION;
}

static tgtadm_err iscsi_stat_device_by_id(uint64_t lun, uint64_t sid, struct concat_buf *b)
{
	struct iscsi_session *session;

	dprintf("lun:%" PRIu64 " sid:%" PRIu64 "\n", lun, sid);

	session = session_lookup_by_tsih((uint16_t)sid);
	if (session)
		return _stat_iscsi_session(session, lun, 1, b);
	else
		return TGTADM_NO_SESSION;
}

tgtadm_err iscsi_stat(int mode, int tid, uint64_t sid, uint32_t cid, uint64_t lun,
		      struct concat_buf *b)
{
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;

	dprintf("mode:%d tid:%d sid:%" PRIu64 " cid:%" PRIu32 " lun:%" PRIx64 "\n",
		mode, tid, sid, cid, lun);

	switch (mode) {
	case MODE_DEVICE:
		adm_err = iscsi_stat_device_by_id(lun, sid, b);
		break;
	case MODE_SESSION:
		adm_err = iscsi_stat_session_by_sid(sid, b);
		break;
	case MODE_CONNECTION:
		adm_err = iscsi_stat_connection(sid, cid, b);
		break;
	default:
		break;
	}

	return adm_err;
}

