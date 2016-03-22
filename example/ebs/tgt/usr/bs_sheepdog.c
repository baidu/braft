/*
 * Copyright (C) 2010 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2013 Nippon Telegraph and Telephone Corporation.
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
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <pthread.h>
#include <limits.h>
#include <ctype.h>
#include <sys/un.h>

#include "list.h"
#include "tgtd.h"
#include "util.h"
#include "log.h"
#include "scsi.h"
#include "bs_thread.h"

#define SD_PROTO_VER 0x01

#define SD_DEFAULT_ADDR "localhost"
#define SD_DEFAULT_PORT 7000

#define SD_OP_CREATE_AND_WRITE_OBJ  0x01
#define SD_OP_READ_OBJ       0x02
#define SD_OP_WRITE_OBJ      0x03
/* 0x04 is used internally by Sheepdog */
#define SD_OP_DISCARD_OBJ    0x05

#define SD_OP_NEW_VDI        0x11
#define SD_OP_LOCK_VDI       0x12
#define SD_OP_RELEASE_VDI    0x13
#define SD_OP_GET_VDI_INFO   0x14
#define SD_OP_READ_VDIS      0x15
#define SD_OP_FLUSH_VDI      0x16
#define SD_OP_DEL_VDI        0x17

#define SD_FLAG_CMD_WRITE    0x01
#define SD_FLAG_CMD_COW      0x02
#define SD_FLAG_CMD_CACHE    0x04 /* Writeback mode for cache */
#define SD_FLAG_CMD_DIRECT   0x08 /* Don't use cache */

#define SD_RES_SUCCESS       0x00 /* Success */
#define SD_RES_UNKNOWN       0x01 /* Unknown error */
#define SD_RES_NO_OBJ        0x02 /* No object found */
#define SD_RES_EIO           0x03 /* I/O error */
#define SD_RES_VDI_EXIST     0x04 /* Vdi exists already */
#define SD_RES_INVALID_PARMS 0x05 /* Invalid parameters */
#define SD_RES_SYSTEM_ERROR  0x06 /* System error */
#define SD_RES_VDI_LOCKED    0x07 /* Vdi is locked */
#define SD_RES_NO_VDI        0x08 /* No vdi found */
#define SD_RES_NO_BASE_VDI   0x09 /* No base vdi found */
#define SD_RES_VDI_READ      0x0A /* Cannot read requested vdi */
#define SD_RES_VDI_WRITE     0x0B /* Cannot write requested vdi */
#define SD_RES_BASE_VDI_READ 0x0C /* Cannot read base vdi */
#define SD_RES_BASE_VDI_WRITE   0x0D /* Cannot write base vdi */
#define SD_RES_NO_TAG        0x0E /* Requested tag is not found */
#define SD_RES_STARTUP       0x0F /* Sheepdog is on starting up */
#define SD_RES_VDI_NOT_LOCKED   0x10 /* Vdi is not locked */
#define SD_RES_SHUTDOWN      0x11 /* Sheepdog is shutting down */
#define SD_RES_NO_MEM        0x12 /* Cannot allocate memory */
#define SD_RES_FULL_VDI      0x13 /* we already have the maximum vdis */
#define SD_RES_VER_MISMATCH  0x14 /* Protocol version mismatch */
#define SD_RES_NO_SPACE      0x15 /* Server has no room for new objects */
#define SD_RES_WAIT_FOR_FORMAT  0x16 /* Waiting for a format operation */
#define SD_RES_WAIT_FOR_JOIN    0x17 /* Waiting for other nodes joining */
#define SD_RES_JOIN_FAILED   0x18 /* Target node had failed to join sheepdog */
#define SD_RES_HALT          0x19 /* Sheepdog is stopped serving IO request */
#define SD_RES_READONLY      0x1A /* Object is read-only */

/*
 * Object ID rules
 *
 *  0 - 19 (20 bits): data object space
 * 20 - 31 (12 bits): reserved data object space
 * 32 - 55 (24 bits): vdi object space
 * 56 - 59 ( 4 bits): reserved vdi object space
 * 60 - 63 ( 4 bits): object type identifier space
 */

#define VDI_SPACE_SHIFT   32
#define VDI_BIT (UINT64_C(1) << 63)
#define VMSTATE_BIT (UINT64_C(1) << 62)
#define MAX_DATA_OBJS (UINT64_C(1) << 20)
#define MAX_CHILDREN 1024
#define SD_MAX_VDI_LEN 256
#define SD_MAX_VDI_TAG_LEN 256
#define SD_NR_VDIS   (1U << 24)
#define SD_DATA_OBJ_SIZE (UINT64_C(1) << 22)
#define SD_MAX_VDI_SIZE (SD_DATA_OBJ_SIZE * MAX_DATA_OBJS)
#define SECTOR_SIZE 512

#define CURRENT_VDI_ID 0

struct sheepdog_req {
	uint8_t proto_ver;
	uint8_t opcode;
	uint16_t flags;
	uint32_t epoch;
	uint32_t id;
	uint32_t data_length;
	uint32_t opcode_specific[8];
};

struct sheepdog_rsp {
	uint8_t proto_ver;
	uint8_t opcode;
	uint16_t flags;
	uint32_t epoch;
	uint32_t id;
	uint32_t data_length;
	uint32_t result;
	uint32_t opcode_specific[7];
};

struct sheepdog_obj_req {
	uint8_t proto_ver;
	uint8_t opcode;
	uint16_t flags;
	uint32_t epoch;
	uint32_t id;
	uint32_t data_length;
	uint64_t oid;
	uint64_t cow_oid;
	uint32_t copies;
	uint32_t rsvd;
	uint64_t offset;
};

struct sheepdog_obj_rsp {
	uint8_t proto_ver;
	uint8_t opcode;
	uint16_t flags;
	uint32_t epoch;
	uint32_t id;
	uint32_t data_length;
	uint32_t result;
	uint32_t copies;
	uint32_t pad[6];
};

struct sheepdog_vdi_req {
	uint8_t proto_ver;
	uint8_t opcode;
	uint16_t flags;
	uint32_t epoch;
	uint32_t id;
	uint32_t data_length;
	uint64_t vdi_size;
	uint32_t vdi_id;
	uint32_t copies;
	uint32_t snapid;
	uint32_t pad[3];
};

struct sheepdog_vdi_rsp {
	uint8_t proto_ver;
	uint8_t opcode;
	uint16_t flags;
	uint32_t epoch;
	uint32_t id;
	uint32_t data_length;
	uint32_t result;
	uint32_t rsvd;
	uint32_t vdi_id;
	uint32_t pad[5];
};

struct sheepdog_inode {
	char name[SD_MAX_VDI_LEN];
	char tag[SD_MAX_VDI_TAG_LEN];
	uint64_t create_time;
	uint64_t snap_ctime;
	uint64_t vm_clock_nsec;
	uint64_t vdi_size;
	uint64_t vm_state_size;
	uint16_t copy_policy;
	uint8_t nr_copies;
	uint8_t block_size_shift;
	uint32_t snap_id;
	uint32_t vdi_id;
	uint32_t parent_vdi_id;
	uint32_t child_vdi_id[MAX_CHILDREN];
	uint32_t data_vdi_id[MAX_DATA_OBJS];
};

#define SD_INODE_SIZE (sizeof(struct sheepdog_inode))

struct sheepdog_fd_list {
	int fd;
	pthread_t id;

	struct list_head list;
};

#define UNIX_PATH_MAX 108

struct sheepdog_access_info {
	int is_unix;

	/* tcp */
	char hostname[HOST_NAME_MAX + 1];
	int port;

	/* unix domain socket */
	char uds_path[UNIX_PATH_MAX];

	/* if the opened VDI is a snapshot, write commands cannot be issued */
	int is_snapshot;

	/*
	 * maximum length of fd_list_head: nr_iothreads + 1
	 * (+ 1 is for main thread)
	 *
	 * TODO: more effective data structure for handling massive parallel
	 * access
	 */
	struct list_head fd_list_head;
	pthread_rwlock_t fd_list_lock;

	uint32_t min_dirty_data_idx;
	uint32_t max_dirty_data_idx;

	struct sheepdog_inode inode;
	pthread_rwlock_t inode_lock;
};

static inline int is_data_obj_writeable(struct sheepdog_inode *inode,
					unsigned int idx)
{
	return inode->vdi_id == inode->data_vdi_id[idx];
}

static inline int is_data_obj(uint64_t oid)
{
	return !(VDI_BIT & oid);
}

static inline uint64_t data_oid_to_idx(uint64_t oid)
{
	return oid & (MAX_DATA_OBJS - 1);
}

static inline uint64_t vid_to_vdi_oid(uint32_t vid)
{
	return VDI_BIT | ((uint64_t)vid << VDI_SPACE_SHIFT);
}

static inline uint64_t vid_to_vmstate_oid(uint32_t vid, uint32_t idx)
{
	return VMSTATE_BIT | ((uint64_t)vid << VDI_SPACE_SHIFT) | idx;
}

static inline uint64_t vid_to_data_oid(uint32_t vid, uint32_t idx)
{
	return ((uint64_t)vid << VDI_SPACE_SHIFT) | idx;
}

static const char *sd_strerror(int err)
{
	int i;

	static const struct {
		int err;
		const char *desc;
	} errors[] = {
		{SD_RES_SUCCESS,
		 "Success"},
		{SD_RES_UNKNOWN,
		 "Unknown error"},
		{SD_RES_NO_OBJ, "No object found"},
		{SD_RES_EIO, "I/O error"},
		{SD_RES_VDI_EXIST, "VDI exists already"},
		{SD_RES_INVALID_PARMS, "Invalid parameters"},
		{SD_RES_SYSTEM_ERROR, "System error"},
		{SD_RES_VDI_LOCKED, "VDI is already locked"},
		{SD_RES_NO_VDI, "No vdi found"},
		{SD_RES_NO_BASE_VDI, "No base VDI found"},
		{SD_RES_VDI_READ, "Failed read the requested VDI"},
		{SD_RES_VDI_WRITE, "Failed to write the requested VDI"},
		{SD_RES_BASE_VDI_READ, "Failed to read the base VDI"},
		{SD_RES_BASE_VDI_WRITE, "Failed to write the base VDI"},
		{SD_RES_NO_TAG, "Failed to find the requested tag"},
		{SD_RES_STARTUP, "The system is still booting"},
		{SD_RES_VDI_NOT_LOCKED, "VDI isn't locked"},
		{SD_RES_SHUTDOWN, "The system is shutting down"},
		{SD_RES_NO_MEM, "Out of memory on the server"},
		{SD_RES_FULL_VDI, "We already have the maximum vdis"},
		{SD_RES_VER_MISMATCH, "Protocol version mismatch"},
		{SD_RES_NO_SPACE, "Server has no space for new objects"},
		{SD_RES_WAIT_FOR_FORMAT, "Sheepdog is waiting for a format operation"},
		{SD_RES_WAIT_FOR_JOIN, "Sheepdog is waiting for other nodes joining"},
		{SD_RES_JOIN_FAILED, "Target node had failed to join sheepdog"},
		{SD_RES_HALT, "Sheepdog is stopped serving IO request"},
		{SD_RES_READONLY, "Object is read-only"},
	};

	for (i = 0; i < ARRAY_SIZE(errors); ++i) {
		if (errors[i].err == err)
			return errors[i].desc;
	}

	return "Invalid error code";
}

static int connect_to_sdog_tcp(const char *addr, int port)
{
	char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
	int fd, ret;
	struct addrinfo hints, *res, *res0;
	char port_s[6];

	if (!addr) {
		addr = SD_DEFAULT_ADDR;
		port = SD_DEFAULT_PORT;
	}

	memset(port_s, 0, 6);
	snprintf(port_s, 5, "%d", port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;

	ret = getaddrinfo(addr, port_s, &hints, &res0);
	if (ret) {
		eprintf("unable to get address info %s, %s\n",
			addr, strerror(errno));
		return -1;
	}

	for (res = res0; res; res = res->ai_next) {
		ret = getnameinfo(res->ai_addr, res->ai_addrlen, hbuf,
				  sizeof(hbuf), sbuf, sizeof(sbuf),
				  NI_NUMERICHOST | NI_NUMERICSERV);
		if (ret)
			continue;

		fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (fd < 0)
			continue;

reconnect:
		ret = connect(fd, res->ai_addr, res->ai_addrlen);
		if (ret < 0) {
			if (errno == EINTR)
				goto reconnect;

			close(fd);
			break;
		}

		dprintf("connected to %s:%d\n", addr, port);
		goto success;
	}
	fd = -1;
	eprintf("failed connect to %s:%d\n", addr, port);
success:
	freeaddrinfo(res0);
	return fd;
}

static int connect_to_sdog_unix(const char *path)
{
	int fd, ret;
	struct sockaddr_un un;

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		eprintf("socket() failed: %m\n");
		return -1;
	}

	memset(&un, 0, sizeof(un));
	un.sun_family = AF_UNIX;
	strncpy(un.sun_path, path, sizeof(un.sun_path) - 1);

	ret = connect(fd, (const struct sockaddr *)&un, (socklen_t)sizeof(un));
	if (ret < 0) {
		eprintf("connect() failed: %m\n");
		close(fd);
		return -1;
	}

	return fd;
}

static int get_my_fd(struct sheepdog_access_info *ai)
{
	pthread_t self_id = pthread_self();
	struct sheepdog_fd_list *p;
	int fd;

	pthread_rwlock_rdlock(&ai->fd_list_lock);
	list_for_each_entry(p, &ai->fd_list_head, list) {
		if (p->id == self_id) {
			pthread_rwlock_unlock(&ai->fd_list_lock);
			return p->fd;
		}
	}
	pthread_rwlock_unlock(&ai->fd_list_lock);

	if (ai->is_unix)
		fd = connect_to_sdog_unix(ai->uds_path);
	else
		fd = connect_to_sdog_tcp(ai->hostname, ai->port);
	if (fd < 0)
		return -1;

	p = zalloc(sizeof(*p));
	if (!p) {
		close(fd);
		return -1;
	}

	p->id = self_id;
	p->fd = fd;
	INIT_LIST_HEAD(&p->list);

	pthread_rwlock_wrlock(&ai->fd_list_lock);
	list_add_tail(&p->list, &ai->fd_list_head);
	pthread_rwlock_unlock(&ai->fd_list_lock);

	return p->fd;
}

static void close_my_fd(struct sheepdog_access_info *ai, int fd)
{
	struct sheepdog_fd_list *p;
	int closed = 0;

	pthread_rwlock_wrlock(&ai->fd_list_lock);
	list_for_each_entry(p, &ai->fd_list_head, list) {
		if (p->fd == fd) {
			close(fd);
			list_del(&p->list);
			free(p);
			closed = 1;

			break;
		}
	}
	pthread_rwlock_unlock(&ai->fd_list_lock);

	if (!closed)
		eprintf("unknown fd to close: %d\n", fd);
}

static int do_read(int sockfd, void *buf, int len)
{
	int ret;
reread:
	ret = read(sockfd, buf, len);

	if (!ret) {
		eprintf("connection is closed (%d bytes left)\n", len);
		return 1;
	}

	if (ret < 0) {
		if (errno == EINTR || errno == EAGAIN)
			goto reread;

		eprintf("failed to read from socket: %d, %s\n",
			ret, strerror(errno));

		return 1;
	}

	len -= ret;
	buf = (char *)buf + ret;
	if (len)
		goto reread;

	return 0;
}

static void forward_iov(struct msghdr *msg, int len)
{
	while (msg->msg_iov->iov_len <= len) {
		len -= msg->msg_iov->iov_len;
		msg->msg_iov++;
		msg->msg_iovlen--;
	}

	msg->msg_iov->iov_base = (char *) msg->msg_iov->iov_base + len;
	msg->msg_iov->iov_len -= len;
}


static int do_write(int sockfd, struct msghdr *msg, int len)
{
	int ret;
rewrite:
	ret = sendmsg(sockfd, msg, 0);
	if (ret < 0) {
		if (errno == EINTR || errno == EAGAIN)
			goto rewrite;

		eprintf("failed to write to socket: %d, %s\n",
			 ret, strerror(errno));
		return 1;
	}

	len -= ret;
	if (len) {
		forward_iov(msg, ret);
		goto rewrite;
	}

	return 0;
}

static int send_req(int sockfd, struct sheepdog_req *hdr, void *data,
		    unsigned int *wlen)
{
	int ret;
	struct iovec iov[2];
	struct msghdr msg;

	memset(&msg, 0, sizeof(msg));
	msg.msg_iov = iov;

	msg.msg_iovlen = 1;
	iov[0].iov_base = hdr;
	iov[0].iov_len = sizeof(*hdr);

	if (*wlen) {
		msg.msg_iovlen++;
		iov[1].iov_base = data;
		iov[1].iov_len = *wlen;
	}

	ret = do_write(sockfd, &msg, sizeof(*hdr) + *wlen);
	if (ret) {
		eprintf("failed to send a req, %s\n", strerror(errno));
		ret = -1;
	}

	return ret;
}

static int do_req(struct sheepdog_access_info *ai, struct sheepdog_req *hdr,
		  void *data, unsigned int *wlen, unsigned int *rlen)
{
	int ret, sockfd, count = 0;

retry:
	if (count++) {
		eprintf("retrying to reconnect (%d)\n", count);
		if (0 <= sockfd)
			close_my_fd(ai, sockfd);

		sleep(1);
	}

	sockfd = get_my_fd(ai);
	if (sockfd < 0)
		goto retry;

	ret = send_req(sockfd, hdr, data, wlen);
	if (ret)
		goto retry;

	/* FIXME: retrying COW request should be handled in graceful way */
	ret = do_read(sockfd, hdr, sizeof(*hdr));
	if (ret)
		goto retry;

	if (hdr->data_length < *rlen)
		*rlen = hdr->data_length;

	if (*rlen) {
		ret = do_read(sockfd, data, *rlen);
		if (ret)
			goto retry;
	}

	return 0;
}

static int find_vdi_name(struct sheepdog_access_info *ai, char *filename,
			 uint32_t snapid, char *tag, uint32_t *vid,
			 int for_snapshot);
static int read_object(struct sheepdog_access_info *ai, char *buf, uint64_t oid,
		       int copies, unsigned int datalen, uint64_t offset);

static int reload_inode(struct sheepdog_access_info *ai)
{
	int ret;
	char tag[SD_MAX_VDI_TAG_LEN];
	uint32_t vid;

	memset(tag, 0, sizeof(tag));

	ret = find_vdi_name(ai, ai->inode.name, CURRENT_VDI_ID, tag, &vid, 0);
	if (ret)
		return -1;

	ret = read_object(ai, (char *)&ai->inode, vid_to_vdi_oid(vid),
			  ai->inode.nr_copies, SD_INODE_SIZE, 0);
	if (ret)
		return -1;

	return 0;
}

static int read_write_object(struct sheepdog_access_info *ai, char *buf,
			     uint64_t oid, int copies,
			     unsigned int datalen, uint64_t offset,
			     int write, int create, uint64_t old_oid,
			     uint16_t flags, int *need_reload)
{
	struct sheepdog_obj_req hdr;
	struct sheepdog_obj_rsp *rsp = (struct sheepdog_obj_rsp *)&hdr;
	unsigned int wlen, rlen;
	int ret;

	memset(&hdr, 0, sizeof(hdr));

	hdr.proto_ver = SD_PROTO_VER;
	hdr.flags = flags;
	if (write) {
		wlen = datalen;
		rlen = 0;
		hdr.flags |= SD_FLAG_CMD_WRITE;
		if (create) {
			hdr.opcode = SD_OP_CREATE_AND_WRITE_OBJ;
			hdr.cow_oid = old_oid;
		} else {
			hdr.opcode = SD_OP_WRITE_OBJ;
		}
	} else {
		wlen = 0;
		rlen = datalen;
		hdr.opcode = SD_OP_READ_OBJ;
	}
	hdr.oid = oid;
	hdr.data_length = datalen;
	hdr.offset = offset;
	hdr.copies = copies;

	ret = do_req(ai, (struct sheepdog_req *)&hdr, buf, &wlen, &rlen);
	if (ret) {
		eprintf("failed to send a request to the sheep\n");
		return -1;
	}

	switch (rsp->result) {
	case SD_RES_SUCCESS:
		return 0;
	case SD_RES_READONLY:
		if (need_reload)
			*need_reload = 1;
		return 0;
	default:
		eprintf("%s (oid: %" PRIx64 ", old_oid: %" PRIx64 ")\n",
			sd_strerror(rsp->result), oid, old_oid);
		return -1;
	}
}

static int read_object(struct sheepdog_access_info *ai, char *buf,
		       uint64_t oid, int copies,
		       unsigned int datalen, uint64_t offset)
{
	return read_write_object(ai, buf, oid, copies, datalen, offset,
				 0, 0, 0, 0, NULL);
}

static int write_object(struct sheepdog_access_info *ai, char *buf,
			uint64_t oid, int copies,
			unsigned int datalen, uint64_t offset, int create,
			uint64_t old_oid, uint16_t flags, int *need_reload)
{
	return read_write_object(ai, buf, oid, copies, datalen, offset, 1,
				 create, old_oid, flags, need_reload);
}

static int sd_sync(struct sheepdog_access_info *ai)
{
	int ret;
	struct sheepdog_obj_req hdr;
	struct sheepdog_obj_rsp *rsp = (struct sheepdog_obj_rsp *)&hdr;
	unsigned int wlen = 0, rlen = 0;

	memset(&hdr, 0, sizeof(hdr));

	hdr.proto_ver = SD_PROTO_VER;
	hdr.opcode = SD_OP_FLUSH_VDI;
	hdr.oid = vid_to_vdi_oid(ai->inode.vdi_id);

	ret = do_req(ai, (struct sheepdog_req *)&hdr, NULL, &wlen, &rlen);
	if (ret) {
		eprintf("failed to send a request to the sheep\n");
		return -1;
	}

	switch (rsp->result) {
	case SD_RES_SUCCESS:
	case SD_RES_INVALID_PARMS:
		/*
		 * SD_RES_INVALID_PARMS means the sheep daemon doesn't use
		 * object caches
		 */
		return 0;
	default:
		eprintf("%s\n", sd_strerror(rsp->result));
		return -1;
	}
}

static int update_inode(struct sheepdog_access_info *ai)
{
	int ret = 0;
	uint64_t oid = vid_to_vdi_oid(ai->inode.vdi_id);
	uint32_t min, max, offset, data_len;

	min = ai->min_dirty_data_idx;
	max = ai->max_dirty_data_idx;

	if (max < min)
		goto end;

	offset = sizeof(ai->inode) - sizeof(ai->inode.data_vdi_id) +
		min * sizeof(ai->inode.data_vdi_id[0]);
	data_len = (max - min + 1) * sizeof(ai->inode.data_vdi_id[0]);

	ret = write_object(ai, (char *)&ai->inode + offset, oid,
			   ai->inode.nr_copies, data_len, offset,
			   0, 0, 0, NULL);
	if (ret < 0)
		eprintf("sync inode failed\n");

end:
	ai->min_dirty_data_idx = UINT32_MAX;
	ai->max_dirty_data_idx = 0;

	return ret;
}

static int sd_io(struct sheepdog_access_info *ai, int write, char *buf, int len,
		 uint64_t offset)
{
	uint32_t vid;
	unsigned long idx = offset / SD_DATA_OBJ_SIZE;
	unsigned long max =
		(offset + len + (SD_DATA_OBJ_SIZE - 1)) / SD_DATA_OBJ_SIZE;
	unsigned obj_offset = offset % SD_DATA_OBJ_SIZE;
	size_t size, rest = len;
	int ret = 0, create = 0;
	uint64_t oid, old_oid;
	uint16_t flags = 0;
	int need_update_inode = 0, need_reload_inode;
	int nr_copies = ai->inode.nr_copies;

	if (write)
		pthread_rwlock_wrlock(&ai->inode_lock);
	else
		pthread_rwlock_rdlock(&ai->inode_lock);

	for (; idx < max; idx++) {
		size = SD_DATA_OBJ_SIZE - obj_offset;
		size = min_t(size_t, size, rest);

retry:
		vid = ai->inode.vdi_id;
		oid = vid_to_data_oid(ai->inode.data_vdi_id[idx], idx);
		old_oid = 0;

		if (write) {
			if (ai->inode.data_vdi_id[idx] != vid) {
				create = 1;

				if (ai->inode.data_vdi_id[idx]) {
					/* COW */
					old_oid = oid;
					flags = SD_FLAG_CMD_COW;
				}

				oid = vid_to_data_oid(ai->inode.vdi_id, idx);

				ai->min_dirty_data_idx =
					min_t(uint32_t,
					      idx, ai->min_dirty_data_idx);
				ai->max_dirty_data_idx =
					max_t(uint32_t,
					      idx, ai->max_dirty_data_idx);

				ai->inode.data_vdi_id[idx] = vid;
			}

			need_reload_inode = 0;
			ret = write_object(ai, buf + (len - rest),
					   oid, nr_copies, size,
					   obj_offset, create,
					   old_oid, flags, &need_reload_inode);
			if (!ret) {
				if (need_reload_inode) {
					ret = reload_inode(ai);
					if (!ret)
						goto retry;
				}

				if (create) {
					need_update_inode = 1;
					create = 0;
				}
			}
		} else {
			if (!ai->inode.data_vdi_id[idx]) {
				memset(buf, 0, size);
				goto done;
			}

			ret = read_object(ai, buf + (len - rest),
					  oid, nr_copies, size,
					  obj_offset);
		}

		if (ret) {
			eprintf("%lu %d\n", idx, ret);
			goto out;
		}

done:
		rest -= size;
		obj_offset = 0;
	}

	if (need_update_inode)
		ret = update_inode(ai);

out:
	pthread_rwlock_unlock(&ai->inode_lock);

	return ret;
}

static int find_vdi_name(struct sheepdog_access_info *ai, char *filename,
			 uint32_t snapid, char *tag, uint32_t *vid,
			 int for_snapshot)
{
	int ret;
	struct sheepdog_vdi_req hdr;
	struct sheepdog_vdi_rsp *rsp = (struct sheepdog_vdi_rsp *)&hdr;
	unsigned int wlen, rlen = 0;
	char buf[SD_MAX_VDI_LEN + SD_MAX_VDI_TAG_LEN];

	memset(buf, 0, sizeof(buf));
	strncpy(buf, filename, SD_MAX_VDI_LEN - 1);
	strncpy(buf + SD_MAX_VDI_LEN, tag, SD_MAX_VDI_TAG_LEN - 1);

	memset(&hdr, 0, sizeof(hdr));
	if (for_snapshot)
		hdr.opcode = SD_OP_GET_VDI_INFO;
	else
		hdr.opcode = SD_OP_LOCK_VDI;

	wlen = SD_MAX_VDI_LEN + SD_MAX_VDI_TAG_LEN;
	hdr.proto_ver = SD_PROTO_VER;
	hdr.data_length = wlen;
	hdr.snapid = snapid;
	hdr.flags = SD_FLAG_CMD_WRITE;

	ret = do_req(ai, (struct sheepdog_req *)&hdr, buf, &wlen, &rlen);
	if (ret) {
		ret = -1;
		goto out;
	}

	if (rsp->result != SD_RES_SUCCESS) {
		eprintf("cannot get vdi info, %s, %s %d %s\n",
			sd_strerror(rsp->result), filename, snapid, tag);
		ret = -1;
		goto out;
	}
	*vid = rsp->vdi_id;

	ret = 0;

out:
	return ret;
}

static int sd_open(struct sheepdog_access_info *ai, char *filename, int flags)
{
	int ret = 0, i, len, fd;
	uint32_t vid = 0;
	char *orig_filename;

	uint32_t snapid = -1;
	char tag[SD_MAX_VDI_TAG_LEN + 1];
	char vdi_name[SD_MAX_VDI_LEN + 1];
	char *saveptr = NULL, *result;
	enum {
		EXPECT_PROTO,
		EXPECT_PATH,
		EXPECT_HOST,
		EXPECT_PORT,
		EXPECT_VDI,
		EXPECT_TAG_OR_SNAP,
		EXPECT_NOTHING,
	} parse_state = EXPECT_PROTO;

	memset(tag, 0, sizeof(tag));
	memset(vdi_name, 0, sizeof(vdi_name));

	orig_filename = strdup(filename);
	if (!orig_filename) {
		eprintf("saving original filename failed\n");
		return -1;
	}

	/*
	 * expected form of filename:
	 *
	 * unix:<path_of_unix_domain_socket>:<vdi>
	 * unix:<path_of_unix_domain_socket>:<vdi>:<tag>
	 * unix:<path_of_unix_domain_socket>:<vdi>:<snapid>
	 * tcp:<host>:<port>:<vdi>
	 * tcp:<host>:<port>:<vdi>:<tag>
	 * tcp:<host>:<port>:<vdi>:<snapid>
	 */

	result = strtok_r(filename, ":", &saveptr);

	do {
		switch (parse_state) {
		case EXPECT_PROTO:
			if (!strcmp("unix", result)) {
				ai->is_unix = 1;
				parse_state = EXPECT_PATH;
			} else if (!strcmp("tcp", result)) {
				ai->is_unix = 0;
				parse_state = EXPECT_HOST;
			} else {
				eprintf("unknown protocol of sheepdog vdi:"\
					" %s\n", result);
				ret = -1;
				goto out;
			}
			break;
		case EXPECT_PATH:
			strncpy(ai->uds_path, result, UNIX_PATH_MAX - 1);
			parse_state = EXPECT_VDI;
			break;
		case EXPECT_HOST:
			strncpy(ai->hostname, result, HOST_NAME_MAX);
			parse_state = EXPECT_PORT;
			break;
		case EXPECT_PORT:
			len = strlen(result);
			for (i = 0; i < len; i++) {
				if (!isdigit(result[i])) {
					eprintf("invalid tcp port number:"\
						" %s\n", result);
					ret = -1;
					goto out;
				}
			}

			ai->port = atoi(result);
			parse_state = EXPECT_VDI;
			break;
		case EXPECT_VDI:
			strncpy(vdi_name, result, SD_MAX_VDI_LEN);
			parse_state = EXPECT_TAG_OR_SNAP;
			break;
		case EXPECT_TAG_OR_SNAP:
			len = strlen(result);
			for (i = 0; i < len; i++) {
				if (!isdigit(result[i])) {
					/* result is a tag */
					strncpy(tag, result,
						SD_MAX_VDI_TAG_LEN);
					goto trans_to_expect_nothing;
				}
			}

			snapid = atoi(result);
trans_to_expect_nothing:
			parse_state = EXPECT_NOTHING;
			break;
		case EXPECT_NOTHING:
			eprintf("invalid VDI path of sheepdog, unexpected"\
				" token: %s (entire: %s)\n",
				result, orig_filename);
			ret = -1;
			goto out;
		default:
			eprintf("BUG: invalid state of parser: %d\n",
				parse_state);
			exit(1);
		}
	} while ((result = strtok_r(NULL, ":", &saveptr)) != NULL);

	if (parse_state != EXPECT_NOTHING &&
	    parse_state != EXPECT_TAG_OR_SNAP) {
		eprintf("invalid VDI path of sheepdog: %s (state: %d)\n",
			orig_filename, parse_state);
		ret = -1;
		goto out;
	}

	dprintf("protocol: %s\n", ai->is_unix ? "unix" : "tcp");
	if (ai->is_unix)
		dprintf("path of unix domain socket: %s\n", ai->uds_path);
	else
		dprintf("hostname: %s, port: %d\n", ai->hostname, ai->port);

	/*
	 * test connection for validating command line option
	 *
	 * if this step is skipped, the main thread of tgtd will try to
	 * reconnect to sheep process forever
	 */
	fd = ai->is_unix ?
		connect_to_sdog_unix(ai->uds_path) :
		connect_to_sdog_tcp(ai->hostname, ai->port);

	if (fd < 0) {
		eprintf("connecting to sheep process failed, "\
			"please verify the --backing-store option: %s",
			orig_filename);
		ret = -1;
		goto out;
	}

	close(fd);		/* we don't need this connection */

	if (snapid == -1)
		dprintf("tag: %s\n", tag);
	else
		dprintf("snapid: %d\n", snapid);

	dprintf("VDI name: %s\n", vdi_name);
	ai->is_snapshot = !(snapid == -1) || strlen(tag);
	ret = find_vdi_name(ai, vdi_name, snapid == -1 ? 0 : snapid, tag, &vid,
			    ai->is_snapshot);
	if (ret)
		goto out;

	ai->min_dirty_data_idx = UINT32_MAX;
	ai->max_dirty_data_idx = 0;

	ret = read_object(ai, (char *)&ai->inode, vid_to_vdi_oid(vid),
			  0, SD_INODE_SIZE, 0);
	if (ret)
		goto out;

	ret = 0;
out:
	strcpy(filename, orig_filename);
	free(orig_filename);

	return ret;
}

static void sd_close(struct sheepdog_access_info *ai)
{
	struct sheepdog_vdi_req hdr;
	struct sheepdog_vdi_rsp *rsp = (struct sheepdog_vdi_rsp *)&hdr;
	unsigned int wlen = 0, rlen = 0;
	int ret;

	memset(&hdr, 0, sizeof(hdr));

	hdr.opcode = SD_OP_RELEASE_VDI;
	hdr.vdi_id = ai->inode.vdi_id;

	ret = do_req(ai, (struct sheepdog_req *)&hdr, NULL, &wlen, &rlen);

	if (!ret && rsp->result != SD_RES_SUCCESS &&
	    rsp->result != SD_RES_VDI_NOT_LOCKED)
		eprintf("%s, %s", sd_strerror(rsp->result), ai->inode.name);
}

static void set_medium_error(int *result, uint8_t *key, uint16_t *asc)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = MEDIUM_ERROR;
	*asc = ASC_READ_ERROR;
}

static int create_branch(struct sheepdog_access_info *ai)
{
	struct sheepdog_vdi_req hdr;
	struct sheepdog_vdi_rsp *rsp = (struct sheepdog_vdi_rsp *)&hdr;
	unsigned int wlen = 0, rlen;
	int ret;

	ret = pthread_rwlock_wrlock(&ai->inode_lock);
	if (ret) {
		eprintf("failed to get inode lock %s\n", strerror(ret));
		return -1;
	}

	if (!ai->is_snapshot)
		/* check again the snapshot flag to avoid race condition */
		goto out;

	memset(&hdr, 0, sizeof(hdr));
	hdr.opcode = SD_OP_DEL_VDI;
	hdr.vdi_id = ai->inode.vdi_id;
	hdr.flags = SD_FLAG_CMD_WRITE;
	wlen = SD_MAX_VDI_LEN;
	rlen = 0;
	hdr.data_length = wlen;

	ret = do_req(ai, (struct sheepdog_req *)&hdr, ai->inode.name,
		     &wlen, &rlen);
	if (ret) {
		eprintf("deleting snapshot VDI for creating branch failed\n");
		goto out;
	}

	memset(&hdr, 0, sizeof(hdr));
	hdr.opcode = SD_OP_NEW_VDI;
	hdr.vdi_id = ai->inode.vdi_id;

	hdr.flags = SD_FLAG_CMD_WRITE;
	wlen = SD_MAX_VDI_LEN;
	rlen = 0;
	hdr.data_length = wlen;
	hdr.vdi_size = ai->inode.vdi_size;
	ret = do_req(ai, (struct sheepdog_req *)&hdr, ai->inode.name,
		     &wlen, &rlen);
	if (ret) {
		eprintf("creating new VDI for creating branch failed\n");
		goto out;
	}

	ret = read_object(ai, (char *)&ai->inode, vid_to_vdi_oid(rsp->vdi_id),
			  ai->inode.nr_copies, SD_INODE_SIZE, 0);
	if (ret) {
		eprintf("reloading new inode object failed");
		goto out;
	}

	ai->is_snapshot = 0;
	dprintf("creating branch from snapshot, new VDI ID: %x\n", rsp->vdi_id);
out:
	pthread_rwlock_unlock(&ai->inode_lock);

	return ret;
}

static void bs_sheepdog_request(struct scsi_cmd *cmd)
{
	int ret = 0;
	uint32_t length = 0;
	int result = SAM_STAT_GOOD;
	uint8_t key = 0;
	uint16_t asc = 0;
	struct bs_thread_info *info = BS_THREAD_I(cmd->dev);
	struct sheepdog_access_info *ai =
		(struct sheepdog_access_info *)(info + 1);

	switch (cmd->scb[0]) {
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
		ret = sd_sync(ai);
		if (ret)
			set_medium_error(&result, &key, &asc);
		break;
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		if (ai->is_snapshot) {
			ret = create_branch(ai);
			if (ret) {
				eprintf("creating writable VDI from"\
					" snapshot failed\n");
				set_medium_error(&result, &key, &asc);

				break;
			}
		}

		length = scsi_get_out_length(cmd);
		ret = sd_io(ai, 1, scsi_get_out_buffer(cmd),
			    length, cmd->offset);

		if (ret)
			set_medium_error(&result, &key, &asc);
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		length = scsi_get_in_length(cmd);
		ret = sd_io(ai, 0, scsi_get_in_buffer(cmd),
			    length, cmd->offset);
		if (ret)
			set_medium_error(&result, &key, &asc);
		break;
	default:
		eprintf("cmd->scb[0]: %x\n", cmd->scb[0]);
		break;
	}

	dprintf("io done %p %x %d %u\n", cmd, cmd->scb[0], ret, length);

	scsi_set_result(cmd, result);

	if (result != SAM_STAT_GOOD) {
		eprintf("io error %p %x %d %d %" PRIu64 ", %m\n",
			cmd, cmd->scb[0], ret, length, cmd->offset);
		sense_data_build(cmd, key, asc);
	}
}

static int bs_sheepdog_open(struct scsi_lu *lu, char *path,
			    int *fd, uint64_t *size)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
	struct sheepdog_access_info *ai =
		(struct sheepdog_access_info *)(info + 1);
	int ret;

	ret = sd_open(ai, path, 0);
	if (ret)
		return ret;

	*size = ai->inode.vdi_size;

	return 0;
}

static void bs_sheepdog_close(struct scsi_lu *lu)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
	struct sheepdog_access_info *ai =
		(struct sheepdog_access_info *)(info + 1);

	sd_close(ai);
}

static tgtadm_err bs_sheepdog_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
	struct sheepdog_access_info *ai =
		(struct sheepdog_access_info *)(info + 1);

	INIT_LIST_HEAD(&ai->fd_list_head);
	pthread_rwlock_init(&ai->fd_list_lock, NULL);
	pthread_rwlock_init(&ai->inode_lock, NULL);

	return bs_thread_open(info, bs_sheepdog_request, nr_iothreads);
}

static void bs_sheepdog_exit(struct scsi_lu *lu)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
	struct sheepdog_access_info *ai =
		(struct sheepdog_access_info *)(info + 1);

	struct sheepdog_fd_list *p, *next;

	bs_thread_close(info);

	list_for_each_entry_safe(p, next, &ai->fd_list_head, list) {
		close(p->fd);
		list_del(&p->list);
		free(p);
	}

	pthread_rwlock_destroy(&ai->fd_list_lock);
	pthread_rwlock_destroy(&ai->inode_lock);

	dprintf("cleaned logical unit %p safely\n", lu);
}

static struct backingstore_template sheepdog_bst = {
	.bs_name		= "sheepdog",
	.bs_datasize		=
	sizeof(struct bs_thread_info) + sizeof(struct sheepdog_access_info),
	.bs_open		= bs_sheepdog_open,
	.bs_close		= bs_sheepdog_close,
	.bs_init		= bs_sheepdog_init,
	.bs_exit		= bs_sheepdog_exit,
	.bs_cmd_submit		= bs_thread_cmd_submit,
};

__attribute__((constructor)) static void __constructor(void)
{
	unsigned char opcodes[] = {
		ALLOW_MEDIUM_REMOVAL,
		FORMAT_UNIT,
		INQUIRY,
		MAINT_PROTOCOL_IN,
		MODE_SELECT,
		MODE_SELECT_10,
		MODE_SENSE,
		MODE_SENSE_10,
		PERSISTENT_RESERVE_IN,
		PERSISTENT_RESERVE_OUT,
		READ_10,
		READ_12,
		READ_16,
		READ_6,
		READ_CAPACITY,
		RELEASE,
		REPORT_LUNS,
		REQUEST_SENSE,
		RESERVE,
		SEND_DIAGNOSTIC,
		SERVICE_ACTION_IN,
		START_STOP,
		SYNCHRONIZE_CACHE,
		SYNCHRONIZE_CACHE_16,
		TEST_UNIT_READY,
		WRITE_10,
		WRITE_12,
		WRITE_16,
		WRITE_6
	};

	bs_create_opcode_map(&sheepdog_bst, opcodes, ARRAY_SIZE(opcodes));

	register_backingstore_template(&sheepdog_bst);
}
