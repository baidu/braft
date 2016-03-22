/*
 * iSNS functions
 *
 * Copyright (C) 2006 FUJITA Tomonori <tomof@acm.org>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/types.h>

#include "iscsid.h"
#include "parser.h"
#include "tgtd.h"
#include "util.h"
#include "work.h"
#include "list.h"
#include "isns_proto.h"
#include "tgtadm.h"

#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))
#define BUFSIZE (1 << 18)

#define EID_NAME_KEY "eid"

struct isns_io {
	char *buf;
	int offset;
};

struct isns_qry_mgmt {
	char name[ISCSI_NAME_LEN];
	uint16_t transaction;
	struct list_head qlist;
};

struct isns_initiator {
	char name[ISCSI_NAME_LEN];
	struct list_head ilist;
};

struct tgt_work timeout_work;

static LIST_HEAD(qry_list);
static uint16_t scn_listen_port;
static int use_isns, use_isns_ac, isns_fd, scn_listen_fd, scn_fd;
static struct isns_io isns_rx, scn_rx;
static char *rxbuf;
static uint16_t transaction;
static char eid[ISCSI_NAME_LEN];
static struct sockaddr_storage ss;

/*
 * Section 6.2.6
 * The registration SHALL be removed from the iSNS database if an iSNS
 * Protocol message is not received from the iSNS client before the
 * registration period has expired. Receipt of any iSNS Protocol
 * message from the iSNS client automatically refreshes the Entity
 * Registration Period and Entity Registration Timestamp. To prevent a
 * registration from expiring, the iSNS client should send an iSNS
 * Protocol message to the iSNS server at intervals shorter than the
 * registration period.
 *
 * Implementor's Note:
 * We send a DevAttrQry message to the iSNS server every isns_timeout
 * seconds to keep our entries alive at the iSNS server.
 * To start with, we assume that the registration period is greater
 * than 30 seconds. After we register an entity we query for the
 * registration period and change the isns_timeout to be 10 seconds less
 * than the regiistration period. If the iSNS server doesn't respond
 * with a registration period we stick with 30 seconds.
 */
#define DEFAULT_ISNS_TIMEOUT 30 /* seconds */
static uint32_t isns_timeout = DEFAULT_ISNS_TIMEOUT;

static char isns_addr[NI_MAXHOST];
static int isns_port = ISNS_PORT;
static int num_targets = 0;

int isns_scn_access(int tid, char *name)
{
	struct isns_initiator *ini;
	struct iscsi_target *target = target_find_by_id(tid);

	if (!use_isns || !use_isns_ac)
		return 0;

	if (!target)
		return -EPERM;

	list_for_each_entry(ini, &target->isns_list, ilist) {
		if (!strcmp(ini->name, name))
			return 0;
	}
	return -EPERM;
}

static int isns_get_ip(int fd)
{
	int err;
	struct sockaddr_storage lss;
	socklen_t slen = sizeof(lss);

	err = getsockname(fd, (struct sockaddr *) &lss, &slen);
	if (err) {
		eprintf("getsockname error %s!\n", gai_strerror(err));
		return err;
	}

	err = getnameinfo((struct sockaddr *) &lss, sizeof(lss),
			  eid, sizeof(eid), NULL, 0, NI_NUMERICHOST);
	if (err) {
		eprintf("getnameinfo error %s!\n", gai_strerror(err));
		return err;
	}

	return 0;
}

static void isns_handle(int fd, int events, void *data);

static int isns_connect(void)
{
	int fd, err;

	fd = socket(ss.ss_family, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0) {
		eprintf("unable to create (%s) %d!\n", strerror(errno),
			ss.ss_family);
		return -1;
	}

	err = connect(fd, (struct sockaddr *) &ss, sizeof(ss));
	if (err < 0) {
		eprintf("unable to connect (%s) %d!\n", strerror(errno),
			ss.ss_family);
		close(fd);
		return -1;
	}

	if (!strlen(eid)) {
		err = isns_get_ip(fd);
		if (err) {
			close(fd);
			return -1;
		}
	}

	isns_fd = fd;
	tgt_event_add(fd, EPOLLIN, isns_handle, NULL);

	return fd;
}

static void isns_hdr_init(struct isns_hdr *hdr, uint16_t function,
			  uint16_t length, uint16_t flags,
			  uint16_t trans, uint16_t sequence)
{
	hdr->version = htons(0x0001);
	hdr->function = htons(function);
	hdr->length = htons(length);
	hdr->flags = htons(flags);
	hdr->transaction = htons(trans);
	hdr->sequence = htons(sequence);
}

static int isns_tlv_set(struct isns_tlv **tlv, uint32_t tag, uint32_t length,
			void *value)
{
	if (length)
		memcpy((*tlv)->value, value, length);
	if (length % ISNS_ALIGN)
		length += (ISNS_ALIGN - (length % ISNS_ALIGN));

	(*tlv)->tag = htonl(tag);
	(*tlv)->length = htonl(length);

	length += sizeof(struct isns_tlv);
	*tlv = (struct isns_tlv *) ((char *) *tlv + length);

	return length;
}

static int isns_tlv_set_string(struct isns_tlv **tlv, uint32_t tag, char *str)
{
	return isns_tlv_set(tlv, tag, strlen(str) + 1, str);
}

static int isns_scn_deregister(char *name)
{
	int err;
	uint16_t flags, length = 0;
	char buf[2048];
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;

	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);

	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_SCN_DEREG, length, flags,
		      ++transaction, 0);

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	return 0;
}

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define set_scn_flag(x)						\
{								\
	x = (x & 0x55555555) << 1 | (x & 0xaaaaaaaa) >> 1;	\
	x = (x & 0x33333333) << 2 | (x & 0xcccccccc) >> 2;	\
	x = (x & 0x0f0f0f0f) << 4 | (x & 0xf0f0f0f0) >> 4;	\
	x = (x & 0x00ff00ff) << 8 | (x & 0xff00ff00) >> 8;	\
	x = (x & 0x0000ffff) << 16 | (x & 0xffff0000) >> 16;	\
}
#else
#define set_scn_flag(x)
#endif

static int isns_scn_register(char *name)
{
	int err;
	uint16_t flags, length = 0;
	uint32_t scn_flags;
	char buf[4096];
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;

	if (list_empty(&iscsi_targets_list))
		return 0;

	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set(&tlv, 0, 0, 0);

	scn_flags = ISNS_SCN_FLAG_INITIATOR | ISNS_SCN_FLAG_OBJECT_REMOVE |
		ISNS_SCN_FLAG_OBJECT_ADDED | ISNS_SCN_FLAG_OBJECT_UPDATED;
	set_scn_flag(scn_flags);
	scn_flags = htonl(scn_flags);

	length += isns_tlv_set(&tlv, ISNS_ATTR_ISCSI_SCN_BITMAP,
			       sizeof(scn_flags), &scn_flags);

	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_SCN_REG, length, flags, ++transaction, 0);

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	return 0;
}

static int isns_eid_attr_query(void)
{
	int err;
	uint16_t flags, length = 0;
	char buf[4096];
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;
	struct iscsi_target *target;
	struct isns_qry_mgmt *mgmt;
	char *name;

	eprintf("\n");
	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	mgmt = malloc(sizeof(*mgmt));
	if (!mgmt)
		return 0;
	list_add(&mgmt->qlist, &qry_list);

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	strcpy(mgmt->name, EID_NAME_KEY);
	target = list_first_entry(&iscsi_targets_list,
				  struct iscsi_target, tlist);
	name = tgt_targetname(target->tid);

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ENTITY_IDENTIFIER, eid);
	length += isns_tlv_set(&tlv, 0, 0, 0);
	length += isns_tlv_set(&tlv, ISNS_ATTR_REGISTRATION_PERIOD, 0, 0);

	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_DEV_ATTR_QRY, length, flags,
		      ++transaction, 0);
	mgmt->transaction = transaction;

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	return 0;
}

static int isns_attr_query(char *name)
{
	int err;
	uint16_t flags, length = 0;
	char buf[4096];
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;
	struct iscsi_target *target;
	uint32_t node = htonl(ISNS_NODE_INITIATOR);
	struct isns_qry_mgmt *mgmt;

	if (list_empty(&iscsi_targets_list))
		return 0;

	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	mgmt = malloc(sizeof(*mgmt));
	if (!mgmt)
		return 0;
	list_add(&mgmt->qlist, &qry_list);

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	if (name)
		snprintf(mgmt->name, sizeof(mgmt->name), "%s", name);
	else {
		mgmt->name[0] = '\0';
		target = list_first_entry(&iscsi_targets_list,
					  struct iscsi_target, tlist);
		name = tgt_targetname(target->tid);
	}

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set(&tlv, ISNS_ATTR_ISCSI_NODE_TYPE,
			       sizeof(node), &node);
	length += isns_tlv_set(&tlv, 0, 0, 0);
	length += isns_tlv_set(&tlv, ISNS_ATTR_ISCSI_NAME, 0, 0);
	length += isns_tlv_set(&tlv, ISNS_ATTR_ISCSI_NODE_TYPE, 0, 0);
	length += isns_tlv_set(&tlv, ISNS_ATTR_PORTAL_IP_ADDRESS, 0, 0);

	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_DEV_ATTR_QRY, length, flags,
		      ++transaction, 0);
	mgmt->transaction = transaction;

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	return 0;
}

int isns_target_register(char *name)
{
	unsigned char buf[4096];
	uint16_t flags = 0, length = 0;
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;
	struct iscsi_target *target;
	uint32_t node = htonl(ISNS_NODE_TARGET);
	uint32_t type = htonl(2);
	int err;
	struct iscsi_portal *portal;

	if (!use_isns)
		return 0;

	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	target = list_first_entry(&iscsi_targets_list,
				struct iscsi_target, tlist);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME,
					tgt_targetname(target->tid));
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ENTITY_IDENTIFIER, eid);

	length += isns_tlv_set(&tlv, 0, 0, 0);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ENTITY_IDENTIFIER, eid);

	if (!num_targets) {
		list_for_each_entry(portal, &iscsi_portals_list,
				    iscsi_portal_siblings) {
			uint8_t t_ip[16];
			uint32_t t_port = htonl(portal->port);
			memset(t_ip, 0, 16);

			/*
			 * If listening on all ports, iSNS listings will
			 * reflect local IP we connected to iSNS server with.
			 */
			if (portal->af == AF_INET) {
				uint32_t addr;

				if (!strcmp("0.0.0.0", portal->addr)) {
					if (ss.ss_family != AF_INET)
						continue;

					addr = ((struct sockaddr_in *) &ss)->sin_addr.s_addr;
				} else {
					inet_pton(AF_INET, portal->addr, &addr);
				}

				/* RFC 4171 6.3.1: convert v4 to mapped v6 */
				t_ip[10] = t_ip[11] = 0xff;
				t_ip[15] = 0xff & (addr >> 24);
				t_ip[14] = 0xff & (addr >> 16);
				t_ip[13] = 0xff & (addr >> 8);
				t_ip[12] = 0xff & addr;
			} else {
				if (!strcmp("::", portal->addr)) {
					int i;

					if (ss.ss_family != AF_INET6)
						continue;

					for (i = 0; i < ARRAY_SIZE(t_ip); i++)
						t_ip[i] = ((struct sockaddr_in6 *) &ss)->sin6_addr.s6_addr[i];
				} else {
					inet_pton(AF_INET6, portal->addr, t_ip);
				}
			}

			length += isns_tlv_set(&tlv, ISNS_ATTR_ENTITY_PROTOCOL,
					       sizeof(type), &type);
			length += isns_tlv_set(&tlv, ISNS_ATTR_PORTAL_IP_ADDRESS,
					       sizeof(t_ip), &t_ip);
			length += isns_tlv_set(&tlv, ISNS_ATTR_PORTAL_PORT,
					       sizeof(t_port), &t_port);
		}
		flags = ISNS_FLAG_REPLACE;

		if (scn_listen_port) {
			uint32_t sport = htonl(scn_listen_port);
			length += isns_tlv_set(&tlv, ISNS_ATTR_SCN_PORT,
					       sizeof(sport), &sport);
		}
		add_work(&timeout_work, DEFAULT_ISNS_TIMEOUT);
	}

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set(&tlv, ISNS_ATTR_ISCSI_NODE_TYPE,
			       sizeof(node), &node);

	flags |= ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_DEV_ATTR_REG, length, flags,
		      ++transaction, 0);

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	if (scn_listen_port)
		isns_scn_register(name);

	if (!num_targets)
		isns_eid_attr_query();
	isns_attr_query(name);
	num_targets++;

	return 0;
}

static void free_all_acl(struct iscsi_target *target)
{
	struct isns_initiator *ini;

	while (!list_empty(&target->isns_list)) {
		ini = list_first_entry(&target->isns_list, typeof(*ini), ilist);
		list_del(&ini->ilist);
		free(ini);
	}
}

int isns_target_deregister(char *name)
{
	char buf[4096];
	uint16_t flags, length = 0;
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;
	int err;
	struct iscsi_target *target;

	target = target_find_by_name(name);
	if (target)
		free_all_acl(target);

	if (!use_isns)
		return 0;

	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	num_targets--;
	isns_scn_deregister(name);

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);
	length += isns_tlv_set(&tlv, 0, 0, 0);
	if (!num_targets) {
		del_work(&timeout_work);
		length += isns_tlv_set_string(&tlv, ISNS_ATTR_ENTITY_IDENTIFIER,
					      eid);
	} else
		length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);

	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_DEV_DEREG, length, flags,
		      ++transaction, 0);

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	return 0;
}

static int recv_hdr(int fd, struct isns_io *rx, struct isns_hdr *hdr)
{
	int err;

	if (rx->offset < sizeof(*hdr)) {
		err = read(fd, rx->buf + rx->offset,
			   sizeof(*hdr) - rx->offset);
		if (err < 0) {
			if (errno == EAGAIN || errno == EINTR)
				return -EAGAIN;
			eprintf("header read error %d %d %d %d\n",
				fd, err, errno, rx->offset);
			return -1;
		} else if (err == 0)
			return -1;

		dprintf("header %d %d bytes!\n", fd, err);
		rx->offset += err;

		if (rx->offset < sizeof(*hdr)) {
			dprintf("header wait %d %d\n", rx->offset, err);
			return -EAGAIN;
		}
	}

	return 0;
}

#define get_hdr_param(hdr, function, length, flags, transaction, sequence)	\
{										\
	function = ntohs(hdr->function);					\
	length = ntohs(hdr->length);						\
	flags = ntohs(hdr->flags);						\
	transaction = ntohs(hdr->transaction);					\
	sequence = ntohs(hdr->sequence);					\
	dprintf("got a header %x %u %x %u %u\n", function, length, flags,	\
		transaction, sequence);						\
}

static int recv_pdu(int fd, struct isns_io *rx, struct isns_hdr *hdr)
{
	uint16_t function, length, flags, transaction, sequence;
	int err;

	err = recv_hdr(fd, rx, hdr);
	if (err)
		return err;

	/* Now we got a complete header */
	get_hdr_param(hdr, function, length, flags, transaction, sequence);

	if (length + sizeof(*hdr) > BUFSIZE) {
		eprintf("FIXME we cannot handle this yet %u!\n", length);
		return -1;
	}

	if (rx->offset < length + sizeof(*hdr)) {
		err = read(fd, rx->buf + rx->offset,
			   length + sizeof(*hdr) - rx->offset);
		if (err < 0) {
			if (errno == EAGAIN || errno == EINTR)
				return -EAGAIN;
			eprintf("pdu read error %d %d %d %d\n",
				fd, err, errno, rx->offset);
			return -1;
		} else if (err == 0)
			return -1;

		dprintf("pdu %u %u\n", fd, err);
		rx->offset += err;

		if (rx->offset < length + sizeof(*hdr)) {
			eprintf("pdu wait %d %d\n", rx->offset, err);
			return -EAGAIN;
		}
	}

	/* Now we got everything. */
	rx->offset = 0;

	return 0;
}

#define print_unknown_pdu(hdr)						\
{									\
	uint16_t function, length, flags, transaction, sequence;	\
	get_hdr_param(hdr, function, length, flags, transaction,	\
		      sequence)						\
	eprintf("unknown function %x %u %x %u %u\n",			\
		function, length, flags, transaction, sequence);	\
}

static char *print_scn_pdu(struct isns_hdr *hdr)
{
	struct isns_tlv *tlv = (struct isns_tlv *) hdr->pdu;
	uint16_t function, length, flags, transaction, sequence;
	char *name = NULL;
	static char iscsi_name[224];

	get_hdr_param(hdr, function, length, flags, transaction, sequence);

	while (length) {
		uint32_t vlen = ntohl(tlv->length);

		if (vlen + sizeof(*tlv) > length)
			vlen = length - sizeof(*tlv);

		switch (ntohl(tlv->tag)) {
		case ISNS_ATTR_ISCSI_NAME:
			eprintf("scn name: %u, %s\n", vlen, (char *) tlv->value);
			if (!name) {
				snprintf(iscsi_name, sizeof(iscsi_name), "%s", (char *)tlv->value);
				name = iscsi_name;
			}
			break;
		case ISNS_ATTR_TIMESTAMP:
/* 			log_error("%u : %u : %" PRIx64, ntohl(tlv->tag), vlen, */
/* 				  *((uint64_t *) tlv->value)); */
			break;
		case ISNS_ATTR_ISCSI_SCN_BITMAP:
			eprintf("scn bitmap : %x\n", *((uint32_t *) tlv->value));
			break;
		}

		length -= (sizeof(*tlv) + vlen);
		tlv = (struct isns_tlv *) ((char *) tlv->value + vlen);
	}

	return name;
}

static void qry_rsp_handle(struct isns_hdr *hdr)
{
	struct isns_tlv *tlv;
	uint16_t function, length, flags, transaction, sequence;
	uint32_t status = (uint32_t) (*hdr->pdu);
	struct isns_qry_mgmt *mgmt, *n;
	struct iscsi_target *target = NULL;
	struct isns_initiator *ini;
	char *name = NULL;
	int reg_period = 0;

	get_hdr_param(hdr, function, length, flags, transaction, sequence);

	list_for_each_entry_safe(mgmt, n, &qry_list, qlist) {
		if (mgmt->transaction == transaction) {
			list_del(&mgmt->qlist);
			goto found;
		}
	}

	eprintf("transaction not found %u\n", transaction);
	return;
found:

	if (status) {
		eprintf("error response %u\n", status);
		goto free_qry_mgmt;
	}

	if (!strlen(mgmt->name)) {
		dprintf("skip %u\n", transaction);
		goto free_qry_mgmt;
	}

	if (strcmp(mgmt->name, EID_NAME_KEY)) {
		target = target_find_by_name(mgmt->name);
		if (!target) {
			eprintf("invalid tid %s\n", mgmt->name);
			goto free_qry_mgmt;
		}

		free_all_acl(target);
	}

	/* skip status */
	tlv = (struct isns_tlv *) ((char *) hdr->pdu + 4);

	if (length < 4)
		goto free_qry_mgmt;
	length -= 4;

	while (length) {
		uint32_t vlen = ntohl(tlv->length);

		if (vlen + sizeof(*tlv) > length)
			vlen = length - sizeof(*tlv);

		switch (ntohl(tlv->tag)) {
		case ISNS_ATTR_ISCSI_NAME:
			name = (char *) tlv->value;
			break;
		case ISNS_ATTR_ISCSI_NODE_TYPE:
			if (ntohl(*(tlv->value)) == ISNS_NODE_INITIATOR && name) {
				eprintf("%s\n", (char *) name);
				ini = malloc(sizeof(*ini));
				if (!ini)
					goto free_qry_mgmt;
				snprintf(ini->name, sizeof(ini->name), "%s", name);
				list_add(&ini->ilist, &target->isns_list);
			} else
				name = NULL;
			break;
		case ISNS_ATTR_REGISTRATION_PERIOD:
			reg_period = ntohl(*(tlv->value));
		default:
			name = NULL;
			break;
		}

		length -= (sizeof(*tlv) + vlen);
		tlv = (struct isns_tlv *) ((char *) tlv->value + vlen);
	}

	/*
	 * If the iSNS server provided a registration period, change
	 * isns_timeout to be slighly less than the period, to make
	 * sure our registrations are not kicked out
	 */
	if (reg_period)
		isns_timeout = reg_period - 10;

free_qry_mgmt:
	free(mgmt);
}

static void isns_handle(int fd, int events, void *data)
{
	int err;
	struct isns_io *rx = &isns_rx;
	struct isns_hdr *hdr = (struct isns_hdr *) rx->buf;
	uint16_t function, length, flags, transaction, sequence;
	char *name = NULL;

	err = recv_pdu(isns_fd, rx, hdr);
	if (err) {
		if (err == -EAGAIN)
			return;
		dprintf("close connection %d\n", isns_fd);
		tgt_event_del(isns_fd);
		close(isns_fd);
		isns_fd = 0;
		return;
	}

	get_hdr_param(hdr, function, length, flags, transaction, sequence);

	switch (function) {
	case ISNS_FUNC_DEV_ATTR_REG_RSP:
		break;
	case ISNS_FUNC_DEV_ATTR_QRY_RSP:
		qry_rsp_handle(hdr);
		break;
	case ISNS_FUNC_DEV_DEREG_RSP:
	case ISNS_FUNC_SCN_REG_RSP:
	case ISNS_FUNC_SCN_DEREG_RSP:
		break;
	case ISNS_FUNC_SCN:
		name = print_scn_pdu(hdr);
		if (name) {
			eprintf("%s\n", name);
			isns_attr_query(name);
		}
		break;
	default:
		print_unknown_pdu(hdr);
	}

	return;
}

static void send_scn_rsp(char *name, uint16_t transaction)
{
	char buf[1024];
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;
	uint16_t flags, length = 0;
	int err;

	memset(buf, 0, sizeof(buf));
	*((uint32_t *) hdr->pdu) = 0;
	tlv = (struct isns_tlv *) ((char *) hdr->pdu + 4);
	length +=4;

	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME, name);

	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_SCN_RSP, length, flags, transaction, 0);

	err = write(scn_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);
}

static void isns_scn_handle(int fd, int events, void *data)
{
	int err;
	struct isns_io *rx = &scn_rx;
	struct isns_hdr *hdr = (struct isns_hdr *) rx->buf;
	uint16_t function, length, flags, transaction, sequence;
	char *name = NULL;

	err = recv_pdu(scn_fd, rx, hdr);
	if (err) {
		if (err == -EAGAIN)
			return;
		dprintf("close connection %d\n", scn_fd);
		tgt_event_del(scn_fd);
		close(scn_fd);
		scn_fd = 0;
		return;
	}

	get_hdr_param(hdr, function, length, flags, transaction, sequence);

	switch (function) {
	case ISNS_FUNC_SCN:
		name = print_scn_pdu(hdr);
		break;
	default:
		print_unknown_pdu(hdr);
	}

	if (name) {
		send_scn_rsp(name, transaction);
		isns_attr_query(name);
	}

	return;
}

static void scn_accept_connection(int dummy, int events, void *data)
{
	struct sockaddr_storage from;
	socklen_t slen;
	int fd, err, opt = 1;

	slen = sizeof(from);
	fd = accept(scn_listen_fd, (struct sockaddr *) &from, &slen);
	if (fd < 0) {
		eprintf("accept error %m\n");
		return;
	}
	eprintf("Accept scn connection %d\n", fd);

	err = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
	if (err)
		eprintf("%m\n");
	/* not critical, so ignore. */

	scn_fd = fd;
	tgt_event_add(scn_fd, EPOLLIN, isns_scn_handle, NULL);

	return;
}

static int scn_init(char *addr)
{
	int fd, opt, err;
	struct sockaddr_storage lss;
	socklen_t slen;

	fd = socket(ss.ss_family, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0) {
		eprintf("%m\n");
		return -errno;
	}

	opt = 1;
	if (ss.ss_family == AF_INET6) {
		err = setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &opt, sizeof(opt));
		if (err)
			eprintf("%m\n");
		goto out;
	}

	err = listen(fd, 5);
	if (err) {
		eprintf("%m\n");
		goto out;
	}

	slen = sizeof(lss);
	err = getsockname(fd, (struct sockaddr *) &lss, &slen);
	if (err) {
		eprintf("%m\n");
		goto out;
	}

	/* protocol independent way ? */
	if (lss.ss_family == AF_INET6)
		scn_listen_port = ntohs(((struct sockaddr_in6 *) &lss)->sin6_port);
	else
		scn_listen_port = ntohs(((struct sockaddr_in *) &lss)->sin_port);

	eprintf("scn listen port %u %d %d\n", scn_listen_port, fd, err);
out:
	if (err)
		close(fd);
	else {
		scn_listen_fd = fd;
		tgt_event_add(fd, EPOLLIN, scn_accept_connection, NULL);
	}

	return err;
}

static void isns_timeout_fn(void *data)
{
	struct tgt_work *w = data;

	isns_attr_query(NULL);
	add_work(w, isns_timeout);
}

int isns_init(void)
{
	int err;
	char p[8];
	struct addrinfo hints, *res;
	struct iscsi_target *target;

	snprintf(p, sizeof(p), "%d", isns_port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	err = getaddrinfo(isns_addr, (char *) &p, &hints, &res);
	if (err) {
		eprintf("getaddrinfo error %s\n", isns_addr);
		return -1;
	}
	memcpy(&ss, res->ai_addr, sizeof(*res->ai_addr));
	freeaddrinfo(res);

	rxbuf = calloc(2, BUFSIZE);
	if (!rxbuf) {
		eprintf("oom\n");
		return -1;
	}

	scn_init(isns_addr);

	isns_rx.buf = rxbuf;
	isns_rx.offset = 0;
	scn_rx.buf = rxbuf + BUFSIZE;
	scn_rx.offset = 0;

	use_isns = 1;

	if (!num_targets)
		list_for_each_entry(target, &iscsi_targets_list, tlist)
			isns_target_register(tgt_targetname(target->tid));

	timeout_work.func = isns_timeout_fn;
	timeout_work.data = &timeout_work;

	return 0;
}

int isns_eid_deregister(void)
{
	char buf[4096];
	uint16_t flags, length = 0;
	struct isns_hdr *hdr = (struct isns_hdr *) buf;
	struct isns_tlv *tlv;
	struct iscsi_target *target;
	int err;

	if (!isns_fd)
		if (isns_connect() < 0)
			return 0;

	memset(buf, 0, sizeof(buf));
	tlv = (struct isns_tlv *) hdr->pdu;

	target = list_first_entry(&iscsi_targets_list,
				struct iscsi_target, tlist);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ISCSI_NAME,
					tgt_targetname(target->tid));
	length += isns_tlv_set(&tlv, 0, 0, 0);
	length += isns_tlv_set_string(&tlv, ISNS_ATTR_ENTITY_IDENTIFIER,
					      eid);
	flags = ISNS_FLAG_CLIENT | ISNS_FLAG_LAST_PDU | ISNS_FLAG_FIRST_PDU;
	isns_hdr_init(hdr, ISNS_FUNC_DEV_DEREG, length, flags,
		      ++transaction, 0);

	err = write(isns_fd, buf, length + sizeof(struct isns_hdr));
	if (err < 0)
		eprintf("%d %m\n", length);

	return 0;
}
void isns_exit(void)
{
	struct iscsi_target *target;

	if (!use_isns)
		return;

	if (num_targets) {
		del_work(&timeout_work);
		list_for_each_entry(target, &iscsi_targets_list, tlist)
			free_all_acl(target);

		isns_eid_deregister();
	}

	if (isns_fd) {
		tgt_event_del(isns_fd);
		close(isns_fd);
	}
	if (scn_listen_fd) {
		tgt_event_del(scn_listen_fd);
		close(scn_listen_fd);
	}
	if (scn_fd) {
		tgt_event_del(scn_fd);
		close(scn_fd);
	}

	num_targets = use_isns = isns_fd = scn_listen_fd = scn_fd = 0;
	free(rxbuf);
}

tgtadm_err isns_show(struct concat_buf *b)
{
	concat_printf(b, "iSNS:\n");
	concat_printf(b, _TAB1 "iSNS=%s\n", use_isns ? "On" : "Off");
	concat_printf(b, _TAB1 "iSNSServerIP=%s\n", isns_addr);
	concat_printf(b, _TAB1 "iSNSServerPort=%d\n", isns_port);
	concat_printf(b, _TAB1 "iSNSAccessControl=%s\n",
		      use_isns_ac ? "On" : "Off");

	return TGTADM_SUCCESS;
}

enum {
	Opt_isns, Opt_ip, Opt_port, Opt_ac, Opt_state, Opt_err,
};

static match_table_t tokens = {
	{Opt_isns, "iSNS=%s"},
	{Opt_ip, "iSNSServerIP=%s"},
	{Opt_port, "iSNSServerPort=%d"},
	{Opt_ac, "iSNSAccessControl=%s"},
	{Opt_state, "State=%s"},
	{Opt_err, NULL},
};

tgtadm_err isns_update(char *params)
{
    tgtadm_err adm_err = TGTADM_SUCCESS;
	char *p;

	while ((p = strsep(&params, ",")) != NULL) {
		substring_t args[MAX_OPT_ARGS];
		int token, isns;
		char tmp[16];

		if (!*p)
			continue;
		token = match_token(p, tokens, args);

		switch (token) {
		case Opt_isns:
			match_strncpy(tmp, &args[0], sizeof(tmp));
			if (!strcmp(tmp, "On"))
				isns = 1;
			else if (!strcmp(tmp, "Off"))
				isns = 0;
			else {
				adm_err = TGTADM_INVALID_REQUEST;
				break;
			}
			if (use_isns == isns)
				break;
			if (isns)
				isns_init();
			else
				isns_exit();
			break;
		case Opt_ip:
			match_strncpy(isns_addr, &args[0], sizeof(isns_addr));
			break;
		case Opt_port:
			if (match_int(&args[0], &isns_port))
				adm_err = TGTADM_INVALID_REQUEST;
			break;
		case Opt_ac:
			match_strncpy(tmp, &args[0], sizeof(tmp));
			use_isns_ac = !strcmp(tmp, "On");
			break;
		case Opt_state:
			match_strncpy(tmp, &args[0], sizeof(tmp));
			adm_err = system_set_state(tmp);
			break;
		default:
			adm_err = TGTADM_INVALID_REQUEST;
		}
	}

	return adm_err;
}
