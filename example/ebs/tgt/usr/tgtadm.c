/*
 * SCSI target daemon management interface
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
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#include "scsi.h"
#include "util.h"
#include "list.h"
#include "tgtadm.h"

#define NO_LOGGING
#include "log.h"

#define BUFSIZE 4096

static const char program_name[] = "tgtadm";
static int debug;

static const char *tgtadm_strerror(int err)
{
	static const struct {
		enum tgtadm_errno err;
		char *desc;
	} errors[] = {
		{ TGTADM_SUCCESS, "success" },
		{ TGTADM_UNKNOWN_ERR, "unknown error" },
		{ TGTADM_NOMEM, "out of memory" },
		{ TGTADM_NO_DRIVER, "can't find the driver" },
		{ TGTADM_NO_TARGET, "can't find the target" },
		{ TGTADM_NO_LUN, "can't find the logical unit" },
		{ TGTADM_NO_SESSION, "can't find the session" },
		{ TGTADM_NO_CONNECTION, "can't find the connection" },
		{ TGTADM_NO_BINDING, "can't find the binding" },
		{ TGTADM_TARGET_EXIST, "this target already exists" },
		{ TGTADM_BINDING_EXIST, "this binding already exists" },
		{ TGTADM_LUN_EXIST,
		  "this logical unit number already exists" },
		{ TGTADM_ACL_EXIST,
		  "this access control rule already exists" },
		{ TGTADM_ACL_NOEXIST,
		  "this access control rule does not exist" },
		{ TGTADM_USER_EXIST, "this account already exists" },
		{ TGTADM_NO_USER, "can't find the account" },
		{ TGTADM_TOO_MANY_USER, "too many accounts" },
		{ TGTADM_INVALID_REQUEST, "invalid request" },
		{ TGTADM_OUTACCOUNT_EXIST,
		  "this target already has an outgoing account" },
		{ TGTADM_TARGET_ACTIVE, "this target is still active" },
		{ TGTADM_LUN_ACTIVE,
		  "this logical unit is still active" },
		{ TGTADM_DRIVER_ACTIVE, "this driver is busy" },
		{ TGTADM_UNSUPPORTED_OPERATION,
		  "this operation isn't supported" },
		{ TGTADM_UNKNOWN_PARAM, "unknown parameter" },
		{ TGTADM_PREVENT_REMOVAL,
		  "this device has Prevent Removal set" }
	};
	int i;

	for (i = 0; i < ARRAY_SIZE(errors); ++i)
		if (errors[i].err == err)
			return errors[i].desc;

	return "(unknown tgtadm_errno)";
}

struct option const long_options[] = {
	{"debug", no_argument, NULL, 'd'},
	{"help", no_argument, NULL, 'h'},
	{"version", no_argument, NULL, 'V'},
	{"lld", required_argument, NULL, 'L'},
	{"op", required_argument, NULL, 'o'},
	{"mode", required_argument, NULL, 'm'},
	{"tid", required_argument, NULL, 't'},
	{"sid", required_argument, NULL, 's'},
	{"cid", required_argument, NULL, 'c'},
	{"lun", required_argument, NULL, 'l'},
	{"name", required_argument, NULL, 'n'},
	{"value", required_argument, NULL, 'v'},
	{"backing-store", required_argument, NULL, 'b'},
	{"bstype", required_argument, NULL, 'E'},
	{"bsopts", required_argument, NULL, 'S'},
	{"bsoflags", required_argument, NULL, 'f'},
	{"blocksize", required_argument, NULL, 'y'},
	{"targetname", required_argument, NULL, 'T'},
	{"initiator-address", required_argument, NULL, 'I'},
	{"initiator-name", required_argument, NULL, 'Q'},
	{"user", required_argument, NULL, 'u'},
	{"password", required_argument, NULL, 'p'},
	{"host", required_argument, NULL, 'H'},
	{"force", no_argument, NULL, 'F'},
	{"params", required_argument, NULL, 'P'},
	{"bus", required_argument, NULL, 'B'},
	{"device-type", required_argument, NULL, 'Y'},
	{"outgoing", no_argument, NULL, 'O'},
	{"control-port", required_argument, NULL, 'C'},
	{NULL, 0, NULL, 0},
};

static char *short_options =
		"dhVL:o:m:t:s:c:l:n:v:b:E:f:y:T:I:Q:u:p:H:F:P:B:Y:O:C:S:";

static void usage(int status)
{
	if (status != 0) {
		fprintf(stderr, "Try `%s --help' for more information.\n",
			program_name);
		exit(EINVAL);
	}

	printf("Linux SCSI Target administration utility, version %s\n\n"
		"Usage: %s [OPTION]\n"
		"--lld <driver> --mode target --op new --tid <id> --targetname <name>\n"
		"\tadd a new target with <id> and <name>. <id> must not be zero.\n"
		"--lld <driver> --mode target --op delete [--force] --tid <id>\n"
		"\tdelete the specific target with <id>.\n"
		"\tWith force option, the specific target is deleted\n"
		"\teven if there is an activity.\n"
		"--lld <driver> --mode target --op show\n"
		"\tshow all the targets.\n"
		"--lld <driver> --mode target --op show --tid <id>\n"
		"\tshow the specific target's parameters.\n"
		"--lld <driver> --mode target --op update --tid <id> --name <param> --value <value>\n"
		"\tchange the target parameters of the target with <id>.\n"
		"--lld <driver> --mode target --op bind --tid <id> --initiator-address <address>\n"
		"--lld <driver> --mode target --op bind --tid <id> --initiator-name <name>\n"
		"\tenable the target to accept the specific initiators.\n"
		"--lld <driver> --mode target --op unbind --tid <id> --initiator-address <address>\n"
		"--lld <driver> --mode target --op unbind --tid <id> --initiator-name <name>\n"
		"\tdisable the specific permitted initiators.\n"
		"--lld <driver> --mode logicalunit --op new --tid <id> --lun <lun>\n"
		"  --backing-store <path> --bstype <type> --bsopts <bs options> --bsoflags <options>\n"
		"\tadd a new logical unit with <lun> to the specific\n"
		"\ttarget with <id>. The logical unit is offered\n"
		"\tto the initiators. <path> must be block device files\n"
		"\t(including LVM and RAID devices) or regular files.\n"
		"\tbstype option is optional.\n"
		"\tbsopts are specific to the bstype.\n"
		"\tbsoflags supported options are sync and direct\n"
		"\t(sync:direct for both).\n"
		"--lld <driver> --mode logicalunit --op delete --tid <id> --lun <lun>\n"
		"\tdelete the specific logical unit with <lun> that\n"
		"\tthe target with <id> has.\n"
		"--lld <driver> --mode account --op new --user <name> --password <pass>\n"
		"\tadd a new account with <name> and <pass>.\n"
		"--lld <driver> --mode account --op delete --user <name>\n"
		"\tdelete the specific account having <name>.\n"
		"--lld <driver> --mode account --op bind --tid <id> --user <name> [--outgoing]\n"
		"\tadd the specific account having <name> to\n"
		"\tthe specific target with <id>.\n"
		"\t<user> could be <IncomingUser> or <OutgoingUser>.\n"
		"\tIf you use --outgoing option, the account will\n"
		"\tbe added as an outgoing account.\n"
		"--lld <driver> --mode account --op unbind --tid <id> --user <name> [--outgoing]\n"
		"\tdelete the specific account having <name> from specific\n"
		"\ttarget. The --outgoing option must be added if you\n"
		"\tdelete an outgoing account.\n"
		"--lld <driver> --mode lld --op start\n"
		"\tStart the specified lld without restarting the tgtd process.\n"
		"--control-port <port> use control port <port>\n"
		"--help\n"
		"\tdisplay this help and exit\n\n"
		"Report bugs to <stgt@vger.kernel.org>.\n",
		TGT_VERSION, program_name);
	exit(0);
}

static void version(void)
{
	printf("%s\n", TGT_VERSION);
	exit(0);
}

/* default port to use for the mgmt channel */
static short int control_port;

static int ipc_mgmt_connect(int *fd)
{
	int err;
	struct sockaddr_un addr;
	char mgmt_path[256];

	*fd = socket(AF_LOCAL, SOCK_STREAM, 0);
	if (*fd < 0) {
		eprintf("can't create a socket, %m\n");
		return errno;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_LOCAL;
	sprintf(mgmt_path, "%s.%d", TGT_IPC_NAMESPACE, control_port);
	strncpy(addr.sun_path, mgmt_path, sizeof(addr.sun_path));

	err = connect(*fd, (struct sockaddr *) &addr, sizeof(addr));
	if (err < 0)
		return errno;

	return 0;
}

static int ipc_mgmt_rsp(int fd, struct tgtadm_req *req)
{
	struct tgtadm_rsp rsp;
	int err, len, done;
	char *buf;

retry:
	err = recv(fd, &rsp, sizeof(rsp), MSG_WAITALL);
	if (err < 0) {
		if (errno == EAGAIN)
			goto retry;
		else if (errno == EINTR)
			eprintf("interrupted by a signal\n");
		else
			eprintf("can't get the response, %m\n");

		return errno;
	} else if (err == 0) {
		eprintf("tgtd closed the socket\n");
		return 0;
	} else if (err != sizeof(rsp)) {
		eprintf("a partial response\n");
		return 0;
	}

	if (rsp.err != TGTADM_SUCCESS) {
		eprintf("%s\n",	tgtadm_strerror(rsp.err));
		return EINVAL;
	}

	if (req->mode == MODE_SYSTEM && req->op == OP_DELETE) {
		while (1) {
			int __fd, ret;
			struct timeval tv;

			ret = ipc_mgmt_connect(&__fd);
			if (ret == ECONNREFUSED)
				break;

			close(__fd);

			tv.tv_sec = 0;
			tv.tv_usec = 100 * 1000;

			select(0, NULL, NULL, NULL, &tv);
		}
	}

	len = rsp.len - sizeof(rsp);
	if (!len)
		return 0;

	buf = malloc(len);
	if (!buf) {
		fprintf(stderr, "failed to allocate %d bytes", len);
		return -ENOMEM;
	}
	done = 0;
	while (len > done) {
		int ret;
		ret = read(fd, buf + done, len - done);
		if (ret < 0) {
			if (errno == EAGAIN)
				continue;
			fprintf(stderr, "failed to read from tgtd, %d", errno);
			break;
		}
		done += ret;
	}

	if (done == len)
		fputs(buf, stdout);
	free(buf);

	return 0;
}

static int ipc_mgmt_req(struct tgtadm_req *req, struct concat_buf *b)
{
	int err, fd = 0, done = 0;

	req->len = sizeof(*req) + b->size;

	err = ipc_mgmt_connect(&fd);
	if (err < 0) {
		eprintf("can't connect to tgt daemon, %m\n");
		goto out;
	}

	err = write(fd, req, sizeof(*req));
	if (err < 0 || err != sizeof(*req)) {
		eprintf("failed to send request hdr to tgt daemon, %m\n");
		err = errno;
		goto out;
	}

	while (done < b->size) {
		err = concat_write(b, fd, done);
		if (err > 0)
			done += err;
		else if (errno != EAGAIN) {
			eprintf("failed to send request buf to "
				"tgt daemon, %m\n");
			err = errno;
			goto out;
		}
	}

	dprintf("sent to tgtd %d\n", req->len);

	err = ipc_mgmt_rsp(fd, req);
out:
	if (fd > 0)
		close(fd);
	concat_buf_release(b);
	return err;
}

static int filter(const struct dirent *dir)
{
	return strcmp(dir->d_name, ".") && strcmp(dir->d_name, "..");
}

static int bus_to_host(char *bus)
{
	int i, nr, host = -1;
	char path[PATH_MAX], *p;
	char key[] = "host";
	struct dirent **namelist;

	p = strchr(bus, ',');
	if (!p)
		return -EINVAL;
	*(p++) = '\0';

	snprintf(path, sizeof(path), "/sys/bus/%s/devices/%s", bus, p);
	nr = scandir(path, &namelist, filter, alphasort);
	if (!nr)
		return -ENOENT;

	for (i = 0; i < nr; i++) {
		if (strncmp(namelist[i]->d_name, key, strlen(key)))
			continue;
		p = namelist[i]->d_name + strlen(key);
		host = strtoull(p, NULL, 10);
	}

	for (i = 0; i < nr; i++)
		free(namelist[i]);
	free(namelist);

	if (host == -1) {
		eprintf("can't find bus: %s\n", bus);
		exit(EINVAL);
	}
	return host;
}

static int str_to_device_type(char *str)
{
	if (!strcmp(str, "disk"))
		return TYPE_DISK;
	else if (!strcmp(str, "tape"))
		return TYPE_TAPE;
	else if (!strcmp(str, "cd"))
		return TYPE_MMC;
	else if (!strcmp(str, "changer"))
		return TYPE_MEDIUM_CHANGER;
	else if (!strcmp(str, "osd"))
		return TYPE_OSD;
	else if (!strcmp(str, "ssc"))
		return TYPE_TAPE;
	else if (!strcmp(str, "pt"))
		return TYPE_PT;
	else {
		eprintf("unknown target type: %s\n", str);
		exit(EINVAL);
	}
}

static int str_to_mode(char *str)
{
	if (!strcmp("system", str) || !strcmp("sys", str))
		return MODE_SYSTEM;
	else if (!strcmp("target", str) || !strcmp("tgt", str))
		return MODE_TARGET;
	else if (!strcmp("logicalunit", str) || !strcmp("lu", str))
		return MODE_DEVICE;
	else if (!strcmp("portal", str) || !strcmp("pt", str))
		return MODE_PORTAL;
	else if (!strcmp("session", str) || !strcmp("sess", str))
		return MODE_SESSION;
	else if (!strcmp("connection", str) || !strcmp("conn", str))
		return MODE_CONNECTION;
	else if (!strcmp("account", str))
		return MODE_ACCOUNT;
	else if (!strcmp("lld", str))
		return MODE_LLD;
	else {
		eprintf("unknown mode: %s\n", str);
		exit(1);
	}
}

static int str_to_op(char *str)
{
	if (!strcmp("new", str))
		return OP_NEW;
	else if (!strcmp("delete", str))
		return OP_DELETE;
	else if (!strcmp("bind", str))
		return OP_BIND;
	else if (!strcmp("unbind", str))
		return OP_UNBIND;
	else if (!strcmp("show", str))
		return OP_SHOW;
	else if (!strcmp("update", str))
		return OP_UPDATE;
	else if (!strcmp("stat", str))
		return OP_STATS;
	else if (!strcmp("start", str))
		return OP_START;
	else if (!strcmp("stop", str))
		return OP_STOP;
	else {
		eprintf("unknown operation: %s\n", str);
		exit(1);
	}
}

static void bad_optarg(int ret, int ch, char *optarg)
{
	if (ret == ERANGE)
		fprintf(stderr, "-%c argument value '%s' out of range\n",
			ch, optarg);
	else
		fprintf(stderr, "-%c argument value '%s' invalid\n",
			ch, optarg);
	usage(ret);
}

static int verify_mode_params(int argc, char **argv, char *allowed)
{
	int ch, longindex;
	int ret = 0;

	optind = 0;

	while ((ch = getopt_long(argc, argv, short_options,
				 long_options, &longindex)) >= 0) {
		if (!strchr(allowed, ch) && !strchr("d", ch)) {
			ret = ch;
			break;
		}
	}

	return ret;
}

int main(int argc, char **argv)
{
	int ch, longindex, rc;
	int op, tid, mode, dev_type, ac_dir;
	uint32_t cid, hostno;
	uint64_t sid, lun, force;
	char *name, *value, *path, *targetname, *address, *iqnname, *targetOps;
	char *portalOps, *bstype, *bsopts;
	char *bsoflags;
	char *blocksize;
	char *user, *password;
	struct tgtadm_req adm_req = {0}, *req = &adm_req;
	struct concat_buf b;
	char *op_name;

	op = tid = mode = -1;
	cid = hostno = sid = 0;
	lun = UINT64_MAX;

	rc = 0;
	dev_type = TYPE_DISK;
	ac_dir = ACCOUNT_TYPE_INCOMING;
	name = value = path = targetname = address = iqnname = NULL;
	targetOps = portalOps = bstype = bsopts = NULL;
	bsoflags = blocksize = user = password = op_name = NULL;
	force = 0;

	optind = 1;
	while ((ch = getopt_long(argc, argv, short_options,
				 long_options, &longindex)) >= 0) {
		errno = 0;
		switch (ch) {
		case 'L':
			strncpy(req->lld, optarg, sizeof(req->lld));
			break;
		case 'o':
			op = str_to_op(optarg);
			op_name = optarg;
			break;
		case 'm':
			mode = str_to_mode(optarg);
			break;
		case 't':
			rc = str_to_int_ge(optarg, tid, 0);
			if (rc)
				bad_optarg(rc, ch, optarg);
			break;
		case 's':
			rc = str_to_int(optarg, sid);
			if (rc)
				bad_optarg(rc, ch, optarg);
			break;
		case 'c':
			rc = str_to_int(optarg, cid);
			if (rc)
				bad_optarg(rc, ch, optarg);
			break;
		case 'l':
			rc = str_to_int(optarg, lun);
			if (rc)
				bad_optarg(rc, ch, optarg);
			break;
		case 'P':
			if (mode == MODE_PORTAL)
				portalOps = optarg;
			else
				targetOps = optarg;
			break;
		case 'n':
			name = optarg;
			break;
		case 'v':
			value = optarg;
			break;
		case 'b':
			path = optarg;
			break;
		case 'T':
			targetname = optarg;
			break;
		case 'I':
			address = optarg;
			break;
		case 'Q':
			iqnname = optarg;
			break;
		case 'u':
			user = optarg;
			break;
		case 'p':
			password = optarg;
			break;
		case 'B':
			hostno = bus_to_host(optarg);
			break;
		case 'H':
			rc = str_to_int_ge(optarg, hostno, 0);
			if (rc)
				bad_optarg(rc, ch, optarg);
			break;
		case 'F':
			force = 1;
			break;
		case 'f':
			bsoflags = optarg;
			break;
		case 'y':
			blocksize = optarg;
			break;
		case 'E':
			bstype = optarg;
			break;
		case 'S':
			bsopts = optarg;
			break;
		case 'Y':
			dev_type = str_to_device_type(optarg);
			break;
		case 'O':
			ac_dir = ACCOUNT_TYPE_OUTGOING;
			break;
		case 'C':
			rc = str_to_int_ge(optarg, control_port, 0);
			if (rc)
				bad_optarg(rc, ch, optarg);
			break;
		case 'V':
			version();
			break;
		case 'd':
			debug = 1;
			break;
		case 'h':
			usage(0);
			break;
		default:
			usage(1);
		}
	}

	if (optind < argc) {
		eprintf("unrecognized option '%s'\n", argv[optind]);
		usage(1);
	}

	if (op < 0) {
		eprintf("specify the operation type\n");
		exit(EINVAL);
	}

	if (mode < 0) {
		eprintf("specify the mode\n");
		exit(EINVAL);
	}

	if (mode == MODE_SYSTEM) {
		switch (op) {
		case OP_UPDATE:
			rc = verify_mode_params(argc, argv, "LmonvC");
			if (rc) {
				eprintf("system mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			if ((!name || !value)) {
				eprintf("update operation requires 'name'"
					" and 'value' options\n");
				exit(EINVAL);
			}
			break;
		case OP_SHOW:
		case OP_DELETE:
		case OP_STATS:
			break;
		default:
			eprintf("operation %s not supported in system mode\n",
				op_name);
			exit(EINVAL);
			break;
		}
	}

	if (mode == MODE_TARGET) {
		if ((tid <= 0 && (op != OP_SHOW))) {
			if (tid == 0)
				eprintf("'tid' cannot be 0\n");
			else
				eprintf("'tid' option is necessary\n");

			exit(EINVAL);
		}
		switch (op) {
		case OP_NEW:
			rc = verify_mode_params(argc, argv, "LmotTC");
			if (rc) {
				eprintf("target mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!targetname) {
				eprintf("creating new target requires "
					"a name, use --targetname\n");
				exit(EINVAL);
			}
			break;
		case OP_DELETE:
			rc = verify_mode_params(argc, argv, "LmotCF");
			if (rc) {
				eprintf("target mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			break;
		case OP_SHOW:
		case OP_STATS:
			rc = verify_mode_params(argc, argv, "LmotC");
			if (rc) {
				eprintf("target mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			break;
		case OP_BIND:
		case OP_UNBIND:
			rc = verify_mode_params(argc, argv, "LmotIQBHC");
			if (rc) {
				eprintf("target mode: option '-%c' is not "
					  "allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!address && !iqnname && !hostno) {
				eprintf("%s operation requires"
					" initiator-address, initiator-name"
					"or bus\n", op_name);
				exit(EINVAL);
			}
			break;
		case OP_UPDATE:
			rc = verify_mode_params(argc, argv, "LmotnvC");
			if (rc) {
				eprintf("target mode: option '-%c' is not " \
					  "allowed/supported\n", rc);
				exit(EINVAL);
			}
			if ((!name || !value)) {
				eprintf("update operation requires 'name'" \
						" and 'value' options\n");
				exit(EINVAL);
			}
			break;
		default:
			eprintf("operation %s not supported in target mode\n",
				op_name);
			exit(EINVAL);
			break;
		}
	}

	if (mode == MODE_ACCOUNT) {
		switch (op) {
		case OP_NEW:
			rc = verify_mode_params(argc, argv, "LmoupfC");
			if (rc) {
				eprintf("account mode: option '-%c' is "
					"not allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!user || !password) {
				eprintf("'user' and 'password' options "
					"are required\n");
				exit(EINVAL);
			}
			break;
		case OP_SHOW:
			rc = verify_mode_params(argc, argv, "LmoC");
			if (rc) {
				eprintf("account mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			break;
		case OP_DELETE:
			rc = verify_mode_params(argc, argv, "LmouC");
			if (rc) {
				eprintf("account mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			break;
		case OP_BIND:
			rc = verify_mode_params(argc, argv, "LmotuOC");
			if (rc) {
				eprintf("account mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!user) {
				eprintf("'user' option is necessary\n");
				exit(EINVAL);
			}
			if (tid == -1)
				tid = GLOBAL_TID;
			break;
		case OP_UNBIND:
			rc = verify_mode_params(argc, argv, "LmotuOC");
			if (rc) {
				eprintf("account mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!user) {
				eprintf("'user' option is necessary\n");
				exit(EINVAL);
			}
			if (tid == -1)
				tid = GLOBAL_TID;
			break;
		default:
			eprintf("operation %s not supported in account mode\n",
				op_name);
			exit(EINVAL);
			break;
		}
	}

	if (mode == MODE_DEVICE) {
		if (tid <= 0) {
			if (tid == 0)
				eprintf("'tid' must not be 0\n");
			else
				eprintf("'tid' option is necessary\n");
			exit(EINVAL);
		}
		if (lun == UINT64_MAX) {
			eprintf("'lun' option is necessary\n");
			exit(EINVAL);
		}
		switch (op) {
		case OP_NEW:
			rc = verify_mode_params(argc, argv, "LmofytlbEYCS");
			if (rc) {
				eprintf("logicalunit mode: option '-%c' is not "
					  "allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!path && dev_type != TYPE_MMC
			    && dev_type != TYPE_TAPE
			    && dev_type != TYPE_DISK) {
				eprintf("'backing-store' option "
						"is necessary\n");
				exit(EINVAL);
			}
			break;
		case OP_DELETE:
		case OP_STATS:
			rc = verify_mode_params(argc, argv, "LmotlC");
			if (rc) {
				eprintf("logicalunit mode: option '-%c' is not "
					  "allowed/supported\n", rc);
				exit(EINVAL);
			}
			break;
		case OP_UPDATE:
			rc = verify_mode_params(argc, argv, "LmofytlPC");
			if (rc) {
				eprintf("option '-%c' not supported in "
					"logicalunit mode\n", rc);
				exit(EINVAL);
			}
			break;
		default:
			eprintf("operation %s not supported in "
				"logicalunit mode\n", op_name);
			exit(EINVAL);
			break;
		}
	}

	if (mode == MODE_PORTAL) {
		switch (op) {
		case OP_NEW:
			rc = verify_mode_params(argc, argv, "LmoCP");
			if (rc) {
				eprintf("portal mode: option '-%c' is not "
					  "allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!portalOps) {
				eprintf("you must specify --param "
					  "portal=<portal>\n");
				exit(EINVAL);
			}
			break;
		case OP_DELETE:
			rc = verify_mode_params(argc, argv, "LmoCP");
			if (rc) {
				eprintf("portal mode: option '-%c' is not "
					  "allowed/supported\n", rc);
				exit(EINVAL);
			}
			if (!portalOps) {
				eprintf("you must specify --param "
					  "portal=<portal>\n");
				exit(EINVAL);
			}
			break;
		case OP_SHOW:
			rc = verify_mode_params(argc, argv, "LmoC");
			if (rc) {
				eprintf("option '-%c' not supported in "
					"portal mode\n", rc);
				exit(EINVAL);
			}
			break;
		default:
			eprintf("operation %s not supported in portal mode\n",
				op_name);
			exit(EINVAL);
			break;
		}
	}

	if (mode == MODE_LLD) {
		switch (op) {
		case OP_START:
		case OP_STOP:
		case OP_SHOW:
			rc = verify_mode_params(argc, argv, "LmoC");
			if (rc) {
				eprintf("lld mode: option '-%c' is not "
					"allowed/supported\n", rc);
				exit(EINVAL);
			}
			break;
		default:
			eprintf("option %d not supported in lld mode\n", op);
			exit(EINVAL);
			break;
		}
	}

	req->op = op;
	req->tid = tid;
	req->sid = sid;
	req->cid = cid;
	req->lun = lun;
	req->mode = mode;
	req->host_no = hostno;
	req->device_type = dev_type;
	req->ac_dir = ac_dir;
	req->force = force;

	concat_buf_init(&b);

	if (name)
		concat_printf(&b, "%s=%s", name, value);
	if (path)
		concat_printf(&b, "%spath=%s", concat_delim(&b, ","), path);

	if (req->device_type == TYPE_TAPE)
		concat_printf(&b, "%sbstype=%s", concat_delim(&b, ","),
			      "ssc");
	else if (bstype)
		concat_printf(&b, "%sbstype=%s", concat_delim(&b, ","),
			      bstype);
	if (bsopts)
		concat_printf(&b, "%sbsopts=%s", concat_delim(&b, ","),
			      bsopts);
	if (bsoflags)
		concat_printf(&b, "%sbsoflags=%s", concat_delim(&b, ","),
			      bsoflags);
	if (blocksize)
		concat_printf(&b, "%sblocksize=%s", concat_delim(&b, ","),
			      blocksize);
	if (targetname)
		concat_printf(&b, "%stargetname=%s", concat_delim(&b, ","),
			      targetname);
	if (address)
		concat_printf(&b, "%sinitiator-address=%s",
			      concat_delim(&b, ","), address);
	if (iqnname)
		concat_printf(&b, "%sinitiator-name=%s", concat_delim(&b, ","),
			      iqnname);
	if (user)
		concat_printf(&b, "%suser=%s", concat_delim(&b, ","),
			      user);
	if (password)
		concat_printf(&b, "%spassword=%s", concat_delim(&b, ","),
			      password);
	/* Trailing ',' makes parsing params in modules easier.. */
	if (targetOps)
		concat_printf(&b, "%stargetOps %s,", concat_delim(&b, ","),
			      targetOps);
	if (portalOps)
		concat_printf(&b, "%sportalOps %s,", concat_delim(&b, ","),
			      portalOps);

	if (b.err) {
		eprintf("BUFSIZE (%d bytes) isn't long enough\n", BUFSIZE);
		return EINVAL;
	}

	rc = concat_buf_finish(&b);
	if (rc) {
		eprintf("failed to create request, errno:%d\n", rc);
		exit(rc);
	}

	return ipc_mgmt_req(req, &b);
}
