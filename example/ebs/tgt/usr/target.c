/*
 * SCSI target daemon core functions
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
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "driver.h"
#include "target.h"
#include "scsi.h"
#include "iscsi/iscsid.h"
#include "tgtadm.h"
#include "parser.h"
#include "spc.h"

static LIST_HEAD(device_type_list);

static struct target global_target;

int device_type_register(struct device_type_template *t)
{
	list_add_tail(&t->device_type_siblings, &device_type_list);
	return 0;
}

static struct device_type_template *device_type_lookup(int type)
{
	struct device_type_template *t;

	list_for_each_entry(t, &device_type_list, device_type_siblings) {
		if (t->type == type)
			return t;
	}
	return NULL;
}

static LIST_HEAD(target_list);

static struct target *target_lookup(int tid)
{
	struct target *target;
	list_for_each_entry(target, &target_list, target_siblings)
		if (target->tid == tid)
			return target;
	return NULL;
}

static int target_name_lookup(char *name)
{
	struct target *target;
	list_for_each_entry(target, &target_list, target_siblings)
		if (!strcmp(target->name, name))
			return 1;
	return 0;
}

struct it_nexus *it_nexus_lookup(int tid, uint64_t itn_id)
{
	struct target *target;
	struct it_nexus *itn;

	target = target_lookup(tid);
	if (!target)
		return NULL;

	list_for_each_entry(itn, &target->it_nexus_list, nexus_siblings) {
		if (itn->itn_id == itn_id)
			return itn;
	}
	return NULL;
}

static int ua_sense_add(struct it_nexus_lu_info *itn_lu, uint16_t asc)
{
	struct ua_sense *uas;

	uas = zalloc(sizeof(*uas));
	if (!uas)
		return -ENOMEM;

	if (itn_lu->lu->attrs.sense_format) {
		/* descriptor format */
		uas->ua_sense_buffer[0] = 0x72;  /* current, not deferred */
		uas->ua_sense_buffer[1] = UNIT_ATTENTION;
		uas->ua_sense_buffer[2] = (asc >> 8) & 0xff;
		uas->ua_sense_buffer[3] = asc & 0xff;
		uas->ua_sense_len = 8;
	} else {
		/* fixed format */
		int len = 0xa;
		uas->ua_sense_buffer[0] = 0x70;  /* current, not deferred */
		uas->ua_sense_buffer[2] = UNIT_ATTENTION;
		uas->ua_sense_buffer[7] = len;
		uas->ua_sense_buffer[12] = (asc >> 8) & 0xff;
		uas->ua_sense_buffer[13] = asc & 0xff;
		uas->ua_sense_len = len + 8;
	}

	list_add_tail(&uas->ua_sense_siblings, &itn_lu->pending_ua_sense_list);

	return 0;
}

int ua_sense_del(struct scsi_cmd *cmd, int del)
{
	struct it_nexus_lu_info *itn_lu = cmd->itn_lu_info;
	struct ua_sense *uas = NULL;
	int len = sizeof(cmd->sense_buffer);

	if (!list_empty(&itn_lu->pending_ua_sense_list)) {
		uas = list_first_entry(&itn_lu->pending_ua_sense_list,
				       struct ua_sense,
				       ua_sense_siblings);
		memcpy(cmd->sense_buffer, uas->ua_sense_buffer,
		       min(uas->ua_sense_len, len));
		cmd->sense_len = min(uas->ua_sense_len, len);

		/*
		 * FIXME: we should hook the uas to the command
		 * instead of freeing it here. if a transport fails to
		 * send the response, we should revert the
		 * uas. Hooking the uas enable us to avoid memory
		 * allocation. But a driver can't tell us to free the
		 * command now.
		 */
		if (del) {
			list_del(&uas->ua_sense_siblings);
			free(uas);
		}
	}

	return uas ? 0 : 1;
}

void ua_sense_clear(struct it_nexus_lu_info *itn_lu, uint16_t asc)
{
	struct ua_sense *uas, *next;
	unsigned char *src;

	list_for_each_entry_safe(uas, next, &itn_lu->pending_ua_sense_list,
				 ua_sense_siblings) {
		if (uas->ua_sense_buffer[0] == 0x72)
			src = uas->ua_sense_buffer + 2;
		else
			src = uas->ua_sense_buffer + 12;

		if ((src[0] == ((asc >> 8) & 0xff)) &&
		    (src[1] == (asc & 0xff))) {
			list_del(&uas->ua_sense_siblings);
			free(uas);
		}
	}
}

static void ua_sense_pending_del(struct it_nexus_lu_info *itn_lu)
{
	struct ua_sense *uas;

	while (!list_empty(&itn_lu->pending_ua_sense_list)) {
		uas = list_first_entry(&itn_lu->pending_ua_sense_list,
				       struct ua_sense,
				       ua_sense_siblings);
		list_del(&uas->ua_sense_siblings);
		free(uas);
	}
}

static void it_nexus_del_lu_info(struct it_nexus *itn)
{
	struct it_nexus_lu_info *itn_lu;

	while (!list_empty(&itn->itn_itl_info_list)) {
		itn_lu = list_first_entry(&itn->itn_itl_info_list,
					  struct it_nexus_lu_info,
					  itn_itl_info_siblings);

		ua_sense_pending_del(itn_lu);

		list_del(&itn_lu->itn_itl_info_siblings);
		list_del(&itn_lu->lu_itl_info_siblings);
		free(itn_lu);
	}
}

void ua_sense_add_other_it_nexus(uint64_t itn_id, struct scsi_lu *lu,
				 uint16_t asc)
{
	struct it_nexus *itn;
	struct it_nexus_lu_info *itn_lu;
	int ret;

	list_for_each_entry(itn, &lu->tgt->it_nexus_list, nexus_siblings) {

		if (itn->itn_id == itn_id)
			continue;

		list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
				    itn_itl_info_siblings) {

			if (itn_lu->lu != lu)
				continue;

			ret = ua_sense_add(itn_lu, asc);
			if (ret)
				eprintf("fail to add ua %" PRIu64
					" %" PRIu64 "\n", lu->lun, itn_id);
		}
	}
}

void ua_sense_add_it_nexus(uint64_t itn_id, struct scsi_lu *lu,
				 uint16_t asc)
{
	struct it_nexus *itn;
	struct it_nexus_lu_info *itn_lu;
	int ret;

	list_for_each_entry(itn, &lu->tgt->it_nexus_list, nexus_siblings) {

		if (itn->itn_id == itn_id) {
			list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
					    itn_itl_info_siblings) {

				if (itn_lu->lu == lu) {
					ret = ua_sense_add(itn_lu, asc);
					if (ret)
						eprintf("fail to add ua %"
							PRIu64 " %" PRIu64
							"\n", lu->lun, itn_id);
					break;
				}
			}
			break;
		}
	}
}

int lu_prevent_removal(struct scsi_lu *lu)
{
	struct it_nexus *itn;
	struct it_nexus_lu_info *itn_lu;

	list_for_each_entry(itn, &lu->tgt->it_nexus_list, nexus_siblings) {
		list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
				    itn_itl_info_siblings) {
			if (itn_lu->lu == lu) {
				if (itn_lu->prevent & PREVENT_REMOVAL)
					return 1;
			}
		}
	}
	return 0;
}

int it_nexus_create(int tid, uint64_t itn_id, int host_no, char *info)
{
	int ret;
	struct target *target;
	struct it_nexus *itn;
	struct scsi_lu *lu;
	struct it_nexus_lu_info *itn_lu;
	struct timeval tv;

	dprintf("%d %" PRIu64 " %d\n", tid, itn_id, host_no);
	/* for reserve/release code */
	if (!itn_id)
		return -EINVAL;

	itn = it_nexus_lookup(tid, itn_id);
	if (itn)
		return -EEXIST;

	target = target_lookup(tid);

	itn = zalloc(sizeof(*itn));
	if (!itn)
		return -ENOMEM;

	itn->itn_id = itn_id;
	itn->host_no = host_no;
	itn->nexus_target = target;
	itn->info = info;
	INIT_LIST_HEAD(&itn->itn_itl_info_list);
	gettimeofday(&tv, NULL);
	itn->ctime = tv.tv_sec;

	list_for_each_entry(lu, &target->device_list, device_siblings) {
		itn_lu = zalloc(sizeof(*itn_lu));
		if (!itn_lu)
			goto out;
		itn_lu->lu = lu;
		itn_lu->itn_id = itn_id;
		INIT_LIST_HEAD(&itn_lu->pending_ua_sense_list);

		ret = ua_sense_add(itn_lu, ASC_POWERON_RESET);
		if (ret) {
			free(itn_lu);
			goto out;
		}

		list_add_tail(&itn_lu->lu_itl_info_siblings,
			      &lu->lu_itl_info_list);

		list_add(&itn_lu->itn_itl_info_siblings,
			 &itn->itn_itl_info_list);
	}

	INIT_LIST_HEAD(&itn->cmd_list);

	list_add_tail(&itn->nexus_siblings, &target->it_nexus_list);

	return 0;
out:
	it_nexus_del_lu_info(itn);
	free(itn);
	return -ENOMEM;
}

int it_nexus_destroy(int tid, uint64_t itn_id)
{
	struct it_nexus *itn;
	struct scsi_lu *lu;

	dprintf("%d %" PRIu64 "\n", tid, itn_id);

	itn = it_nexus_lookup(tid, itn_id);
	if (!itn)
		return -ENOENT;

	if (!list_empty(&itn->cmd_list))
		return -EBUSY;

	list_for_each_entry(lu, &itn->nexus_target->device_list,
			    device_siblings) {
		device_release(tid, itn_id, lu->lun, 0);
	}

	it_nexus_del_lu_info(itn);

	list_del(&itn->nexus_siblings);
	free(itn);
	return 0;
}

static struct scsi_lu *device_lookup(struct target *target, uint64_t lun)
{
	struct scsi_lu *lu;

	list_for_each_entry(lu, &target->device_list, device_siblings)
		if (lu->lun == lun)
			return lu;
	return NULL;
}

static void cmd_hlist_insert(struct it_nexus *itn, struct scsi_cmd *cmd)
{
	list_add(&cmd->c_hlist, &itn->cmd_list);
}

static void cmd_hlist_remove(struct scsi_cmd *cmd)
{
	list_del(&cmd->c_hlist);
}

static void tgt_cmd_queue_init(struct tgt_cmd_queue *q)
{
	q->active_cmd = 0;
	q->state = 0;
	INIT_LIST_HEAD(&q->queue);
}

tgtadm_err tgt_device_path_update(struct target *target, struct scsi_lu *lu,
				  char *path)
{
	int dev_fd;
	uint64_t size;
	int err;

	if (lu->path) {
		int ret;

		if (lu->attrs.online)
			return TGTADM_INVALID_REQUEST;

		ret = lu->dev_type_template.lu_offline(lu);
		if (ret)
			return ret;

		lu->bst->bs_close(lu);
		free(lu->path);
		lu->fd = 0;
		lu->addr = 0;
		lu->size = 0;
		lu->path = NULL;
	}

	path = strdup(path);
	if (!path)
		return TGTADM_NOMEM;

	err = lu->bst->bs_open(lu, path, &dev_fd, &size);
	if (err) {
		free(path);
		return TGTADM_INVALID_REQUEST;
	}

	lu->fd = dev_fd;
	lu->addr = 0;
	lu->size = size;
	lu->path = path;
	return lu->dev_type_template.lu_online(lu);
}

static struct scsi_lu *
__device_lookup(int tid, uint64_t lun, struct target **t)
{
	struct target *target;
	struct scsi_lu *lu;

	target = target_lookup(tid);
	if (!target)
		return NULL;

	lu = device_lookup(target, lun);
	if (!lu)
		return NULL;

	if (t)
		*t = target;

	return lu;
}

enum {
	Opt_path, Opt_bstype, Opt_bsopts, Opt_bsoflags, Opt_blocksize, Opt_err,
};

static match_table_t device_tokens = {
	{Opt_path, "path=%s"},
	{Opt_bstype, "bstype=%s"},
	{Opt_bsopts, "bsopts=%s"},
	{Opt_bsoflags, "bsoflags=%s"},
	{Opt_blocksize, "blocksize=%s"},
	{Opt_err, NULL},
};

static void __cmd_done(struct target *, struct scsi_cmd *);

tgtadm_err tgt_device_create(int tid, int dev_type, uint64_t lun, char *params,
		      int backing)
{
	char *p, *path = NULL, *bstype = NULL, *bsopts = NULL;
	char *bsoflags = NULL, *blocksize = NULL;
	int lu_bsoflags = 0;
	tgtadm_err adm_err = TGTADM_SUCCESS;
	struct target *target;
	struct scsi_lu *lu, *pos;
	struct device_type_template *t;
	struct backingstore_template *bst;
	struct it_nexus_lu_info *itn_lu, *itn_lu_pos;
	struct it_nexus *itn;
	char strflags[128];

	dprintf("%d %" PRIu64 "\n", tid, lun);

	while ((p = strsep(&params, ",")) != NULL) {
		substring_t args[MAX_OPT_ARGS];
		int token;
		if (!*p)
			continue;
		token = match_token(p, device_tokens, args);
		switch (token) {
		case Opt_path:
			path = match_strdup(&args[0]);
			break;
		case Opt_bstype:
			bstype = match_strdup(&args[0]);
			break;
		case Opt_bsopts:
			bsopts = match_strdup(&args[0]);
			break;
		case Opt_bsoflags:
			bsoflags = match_strdup(&args[0]);
			break;
		case Opt_blocksize:
			blocksize = match_strdup(&args[0]);
			break;
		default:
			break;
		}
	}

	target = target_lookup(tid);
	if (!target) {
		adm_err = TGTADM_NO_TARGET;
		goto out;
	}

	lu = device_lookup(target, lun);
	if (lu) {
		eprintf("device %" PRIu64 " already exists\n", lun);
		adm_err = TGTADM_LUN_EXIST;
		goto out;
	}

	bst = target->bst;
	if (backing) {
		if (bstype) {
			bst = get_backingstore_template(bstype);
			if (!bst) {
				eprintf("failed to find bstype, %s\n", bstype);
				adm_err = TGTADM_INVALID_REQUEST;
				goto out;
			}
		}
	} else
		bst = get_backingstore_template("null");

	if ((!strncmp(bst->bs_name, "bsg", 3) ||
	     !strncmp(bst->bs_name, "sg", 2)) &&
	    dev_type != TYPE_PT) {
		eprintf("Must set device type to pt for bsg/sg bstype\n");
		adm_err = TGTADM_INVALID_REQUEST;
		goto out;
	}

	if (!(!strncmp(bst->bs_name, "bsg", 3) ||
	     !strncmp(bst->bs_name, "sg", 2)) &&
	    dev_type == TYPE_PT) {
		eprintf("Must set bstype to bsg or sg for devicetype pt, not %s\n",
			bst->bs_name);
		adm_err = TGTADM_INVALID_REQUEST;
		goto out;
	}

	if (bsoflags) {
		lu_bsoflags = str_to_open_flags(bsoflags);
		if (lu_bsoflags == -1) {
			adm_err = TGTADM_INVALID_REQUEST;
			goto out;
		}
	}

	if (lu_bsoflags &&
	    ((bst->bs_oflags_supported & lu_bsoflags) != lu_bsoflags)) {
		eprintf("bsoflags %s not supported by backing store %s\n",
			open_flags_to_str(strflags,
			(bst->bs_oflags_supported & lu_bsoflags) ^ lu_bsoflags),
			bst->bs_name);
		adm_err = TGTADM_INVALID_REQUEST;
		goto out;
	}

	t = device_type_lookup(dev_type);
	if (!t) {
		eprintf("Unknown device type %d\n", dev_type);
		adm_err = TGTADM_INVALID_REQUEST;
		goto out;
	}

	lu = zalloc(sizeof(*lu) + bst->bs_datasize);
	if (!lu) {
		adm_err = TGTADM_NOMEM;
		goto out;
	}

	lu->dev_type_template = *t;
	lu->bst = bst;
	lu->tgt = target;
	lu->lun = lun;
	lu->bsoflags = lu_bsoflags;

	tgt_cmd_queue_init(&lu->cmd_queue);
	INIT_LIST_HEAD(&lu->registration_list);
	INIT_LIST_HEAD(&lu->lu_itl_info_list);
	INIT_LIST_HEAD(&lu->mode_pages);
	lu->prgeneration = 0;
	lu->pr_holder = NULL;

	lu->cmd_perform = &target_cmd_perform;
	lu->cmd_done = &__cmd_done;

	lu->blk_shift = 0;
	if (blocksize) {
		unsigned int bsize;
		unsigned int bshift;

		dprintf("blocksize=%s\n", blocksize);
		bsize = strtoul(blocksize, NULL, 0);

		bshift = get_blk_shift(bsize);
		if (bshift > 0)
			lu->blk_shift = bshift;
		else {
			if (bsize > 0)
				eprintf("invalid block size: %u\n", bsize);
			else
				eprintf("invalid block size string: %s\n",
					blocksize);
		}
	}

	if (lu->dev_type_template.lu_init) {
		adm_err = lu->dev_type_template.lu_init(lu);
		if (adm_err)
			goto fail_lu_init;
	}

	if (lu->bst->bs_init) {
		if (bsopts)
			dprintf("bsopts=%s\n", bsopts);
		adm_err = lu->bst->bs_init(lu, bsopts);
		if (adm_err)
			goto fail_lu_init;
	}

	if (backing && !path) {
		lu->attrs.removable = 1;
		lu->attrs.online    = 0;
	}

	if (backing && path) {
		adm_err = tgt_device_path_update(target, lu, path);
		if (adm_err)
			goto fail_bs_init;
	}

	if (tgt_drivers[target->lid]->lu_create)
		tgt_drivers[target->lid]->lu_create(lu);

	list_for_each_entry(pos, &target->device_list, device_siblings) {
		if (lu->lun < pos->lun)
			break;
	}
	list_add_tail(&lu->device_siblings, &pos->device_siblings);

	list_for_each_entry(itn, &target->it_nexus_list, nexus_siblings) {
		itn_lu = zalloc(sizeof(*itn_lu));
		if (!itn_lu)
			break;
		itn_lu->lu = lu;
		itn_lu->itn_id = itn->itn_id;
		INIT_LIST_HEAD(&itn_lu->pending_ua_sense_list);

		/* signal LUNs info change thru all LUNs in the nexus */
		list_for_each_entry(itn_lu_pos, &itn->itn_itl_info_list,
				    itn_itl_info_siblings) {
			int ret;

			ret = ua_sense_add(itn_lu_pos,
					   ASC_REPORTED_LUNS_DATA_HAS_CHANGED);
			if (ret) {
				adm_err = TGTADM_NOMEM;
				goto fail_bs_init;
			}
		}

		list_add_tail(&itn_lu->lu_itl_info_siblings,
			      &lu->lu_itl_info_list);

		list_add(&itn_lu->itn_itl_info_siblings,
			 &itn->itn_itl_info_list);
	}

	if (backing && !path)
		lu->dev_type_template.lu_offline(lu);

	dprintf("Add a logical unit %" PRIu64 " to the target %d %s\n", lun, tid, path);
out:
	if (bstype)
		free(bstype);
	if (blocksize)
		free(blocksize);
	if (path)
		free(path);
	if (bsoflags)
		free(bsoflags);
	return adm_err;

fail_bs_init:
	if (lu->bst->bs_exit)
		lu->bst->bs_exit(lu);
fail_lu_init:
	free(lu);
	goto out;
}

tgtadm_err tgt_device_destroy(int tid, uint64_t lun, int force)
{
	struct target *target;
	struct scsi_lu *lu;
	struct it_nexus *itn;
	struct it_nexus_lu_info *itn_lu, *next;
	struct registration *reg, *reg_next;
	int ret;

	dprintf("%u %" PRIu64 "\n", tid, lun);

	/* lun0 is special */
	if (!lun && !force)
		return TGTADM_INVALID_REQUEST;

	lu = __device_lookup(tid, lun, &target);
	if (!lu) {
		eprintf("device %" PRIu64 " not found\n", lun);
		return TGTADM_NO_LUN;
	}

	if (!list_empty(&lu->cmd_queue.queue) || lu->cmd_queue.active_cmd)
		return TGTADM_LUN_ACTIVE;

	if (lu->dev_type_template.lu_exit)
		lu->dev_type_template.lu_exit(lu);

	if (lu->path) {
		free(lu->path);
		lu->bst->bs_close(lu);
	}

	if (lu->bst->bs_exit)
		lu->bst->bs_exit(lu);

	list_for_each_entry(itn, &target->it_nexus_list, nexus_siblings) {
		list_for_each_entry_safe(itn_lu, next, &itn->itn_itl_info_list,
					 itn_itl_info_siblings) {
			if (itn_lu->lu == lu) {
				ua_sense_pending_del(itn_lu);

				list_del(&itn_lu->itn_itl_info_siblings);
				list_del(&itn_lu->lu_itl_info_siblings);
				free(itn_lu);
				break;
			}
		}
	}

	list_del(&lu->device_siblings);

	list_for_each_entry_safe(reg, reg_next, &lu->registration_list,
				 registration_siblings) {
		free(reg);
	}

	free(lu);

	list_for_each_entry(itn, &target->it_nexus_list, nexus_siblings) {
		list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
				    itn_itl_info_siblings) {

			ret = ua_sense_add(itn_lu,
					   ASC_REPORTED_LUNS_DATA_HAS_CHANGED);
			if (ret)
				eprintf("fail to add ua %" PRIu64
					" %" PRIu64 "\n", lun, itn->itn_id);
		}
	}

	return TGTADM_SUCCESS;
}

struct lu_phy_attr *lu_attr_lookup(int tid, uint64_t lun)
{
	struct target *target;
	struct scsi_lu *lu;

	lu = __device_lookup(tid, lun, &target);
	if (!lu)
		return NULL;
	return &lu->attrs;
}

/**
 * dtd_check_removable
 * @tid:	Target ID
 * @lun:	LUN
 *
 * check if a DT can have its media removed or not
 */
tgtadm_err dtd_check_removable(int tid, uint64_t lun)
{
	struct scsi_lu *lu;

	lu = __device_lookup(tid, lun, NULL);
	if (!lu)
		return TGTADM_NO_LUN;

	if (lu_prevent_removal(lu))
		return TGTADM_PREVENT_REMOVAL;

	if (!lu->attrs.removable)
		return TGTADM_INVALID_REQUEST;

	return TGTADM_SUCCESS;
}

/**
 * dtd_load_unload  --  Load / unload media
 * @tid:	Target ID
 * @lun:	LUN
 * @load:	True if load, not true - unload
 * @file:	filename of 'media' top open
 *
 * load/unload media from the DATA TRANSFER DEVICE.
 */
tgtadm_err dtd_load_unload(int tid, uint64_t lun, int load, char *file)
{
	struct target *target;
	struct scsi_lu *lu;
	tgtadm_err adm_err = TGTADM_SUCCESS;

	lu = __device_lookup(tid, lun, &target);
	if (!lu)
		return TGTADM_NO_LUN;

	if (lu_prevent_removal(lu))
		return TGTADM_PREVENT_REMOVAL;

	if (!lu->attrs.removable)
		return TGTADM_INVALID_REQUEST;

	if (lu->path) {
		lu->bst->bs_close(lu);
		free(lu->path);
		lu->path = NULL;
	}

	lu->size = 0;
	lu->fd = 0;

	adm_err = lu->dev_type_template.lu_offline(lu);
	if (adm_err)
		return adm_err;

	if (load) {
		lu->path = strdup(file);
		if (!lu->path)
			return TGTADM_NOMEM;
		lu->bst->bs_open(lu, file, &lu->fd, &lu->size);
		if (lu->fd < 0) {
			free(lu->path);
			lu->path = NULL;
			return TGTADM_UNSUPPORTED_OPERATION;
		}
		adm_err = lu->dev_type_template.lu_online(lu);
	}
	return adm_err;
}

int device_reserve(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu;

	lu = device_lookup(cmd->c_target, cmd->dev->lun);
	if (!lu) {
		eprintf("invalid target and lun %d %" PRIu64 "\n",
			cmd->c_target->tid, cmd->dev->lun);
		return 0;
	}

	if (lu->reserve_id && lu->reserve_id != cmd->cmd_itn_id) {
		dprintf("already reserved %" PRIu64 " %" PRIu64 "\n",
			lu->reserve_id, cmd->cmd_itn_id);
		return -EBUSY;
	}

	lu->reserve_id = cmd->cmd_itn_id;
	return 0;
}

int device_release(int tid, uint64_t itn_id, uint64_t lun, int force)
{
	struct target *target;
	struct scsi_lu *lu;

	lu = __device_lookup(tid, lun, &target);
	if (!lu) {
		eprintf("invalid target and lun %d %" PRIu64 "\n", tid, lun);
		return 0;
	}

	if (force || lu->reserve_id == itn_id) {
		lu->reserve_id = 0;
		return 0;
	}

	if (lu->reserve_id != itn_id)
		return 0;

	return -EBUSY;
}

int device_reserved(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu;

	lu = device_lookup(cmd->c_target, cmd->dev->lun);
	if (!lu || !lu->reserve_id || lu->reserve_id == cmd->cmd_itn_id)
		return 0;
	return -EBUSY;
}

tgtadm_err tgt_device_update(int tid, uint64_t dev_id, char *params)
{
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;
	struct target *target;
	struct scsi_lu *lu;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	lu = device_lookup(target, dev_id);
	if (!lu) {
		eprintf("device %" PRIu64 " not found\n", dev_id);
		return TGTADM_NO_LUN;
	}

	if (lu->dev_type_template.lu_config)
		adm_err = lu->dev_type_template.lu_config(lu, params);

	return adm_err;
}

void tgt_stat_header(struct concat_buf *b)
{
	concat_printf(b,
		"tgt lun sid "
		"rd_subm(bytes,cmds) rd_done(bytes,cmds) "
		"wr_subm(bytes,cmds) wr_done(bytes,cmds) "
		"errs\n");
}

void tgt_stat_line(int tid, uint64_t lun, uint64_t sid, struct lu_stat *stat,
		   struct concat_buf *b)
{
	concat_printf(b,
		"%3d %3" PRIu64 " %3" PRIu64 " "
		"%12" PRIu64 " %6" PRIu32 " "
		"%12" PRIu64 " %6" PRIu32 " "
		"%12" PRIu64 " %6" PRIu32 " "
		"%12" PRIu64 " %6" PRIu32 " "
		"%4" PRIu32 "\n",
		tid, lun, sid,
		stat->rd_subm_bytes, stat->rd_subm_cmds,
		stat->rd_done_bytes, stat->rd_done_cmds,
		stat->wr_subm_bytes, stat->wr_subm_cmds,
		stat->wr_done_bytes, stat->wr_done_cmds,
		stat->err_num);
}

void tgt_stat_device(struct target *target, struct scsi_lu *lu,
		     struct concat_buf *b)
{
	struct it_nexus_lu_info *itn_lu;

	list_for_each_entry(itn_lu, &lu->lu_itl_info_list,
			    lu_itl_info_siblings) {
		tgt_stat_line(target->tid, lu->lun, itn_lu->itn_id,
			      &itn_lu->stat, b);
	}
}

tgtadm_err tgt_stat_device_by_id(int tid, uint64_t dev_id, struct concat_buf *b)
{
	struct target *target;
	struct scsi_lu *lu;
	tgtadm_err adm_err = TGTADM_SUCCESS;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	lu = device_lookup(target, dev_id);
	if (!lu) {
		eprintf("device %" PRIu64 " not found\n", dev_id);
		return TGTADM_NO_LUN;
	}

	tgt_stat_header(b);
	tgt_stat_device(target, lu, b);

	return adm_err;
}

tgtadm_err tgt_stat_target(struct target *target, struct concat_buf *b)
{
	struct scsi_lu *lu;
	tgtadm_err adm_err = TGTADM_SUCCESS;

	list_for_each_entry(lu, &target->device_list, device_siblings)
		tgt_stat_device(target, lu, b);

	return adm_err;
}

tgtadm_err tgt_stat_target_by_id(int tid, struct concat_buf *b)
{
	struct target *target;
	tgtadm_err adm_err = TGTADM_SUCCESS;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	tgt_stat_header(b);
	adm_err = tgt_stat_target(target, b);

	return adm_err;
}

tgtadm_err tgt_stat_system(struct concat_buf *b)
{
	struct target *target;
	tgtadm_err adm_err = TGTADM_SUCCESS;

	tgt_stat_header(b);

	list_for_each_entry(target, &target_list, target_siblings)
		adm_err = tgt_stat_target(target, b);

	return adm_err;
}

static int cmd_enabled(struct tgt_cmd_queue *q, struct scsi_cmd *cmd)
{
	int enabled = 0;

	if (cmd->attribute != MSG_SIMPLE_TAG)
		dprintf("non simple attribute %" PRIx64 " %x %" PRIu64 " %d\n",
			cmd->tag, cmd->attribute,
			cmd->dev ? cmd->dev->lun : UINT64_MAX,
			q->active_cmd);

	switch (cmd->attribute) {
	case MSG_SIMPLE_TAG:
		if (!queue_blocked(q))
			enabled = 1;
		break;
	case MSG_ORDERED_TAG:
		if (!queue_blocked(q) && !queue_active(q))
			enabled = 1;
		break;
	case MSG_HEAD_TAG:
		enabled = 1;
		break;
	default:
		eprintf("unknown command attribute %x\n", cmd->attribute);
		cmd->attribute = MSG_ORDERED_TAG;
		if (!queue_blocked(q) && !queue_active(q))
			enabled = 1;
	}

	return enabled;
}

static void cmd_post_perform(struct tgt_cmd_queue *q, struct scsi_cmd *cmd)
{
	q->active_cmd++;
	switch (cmd->attribute) {
	case MSG_ORDERED_TAG:
	case MSG_HEAD_TAG:
		set_queue_blocked(q);
		break;
	}
}

static struct it_nexus_lu_info *it_nexus_lu_info_lookup(struct it_nexus *itn,
							uint64_t lun)
{
	struct it_nexus_lu_info *itn_lu;

	list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
			    itn_itl_info_siblings) {
		if (itn_lu->lu->lun == lun)
			return itn_lu;
	}
	return NULL;
}

int target_cmd_queue(int tid, struct scsi_cmd *cmd)
{
	struct target *target;
	struct it_nexus *itn;
	uint64_t dev_id, itn_id = cmd->cmd_itn_id;

	itn = it_nexus_lookup(tid, itn_id);
	if (!itn) {
		eprintf("invalid nexus %d %" PRIx64 "\n", tid, itn_id);
		return -ENOENT;
	}

	cmd->c_target = target = itn->nexus_target;
	cmd->it_nexus = itn;

	dev_id = scsi_get_devid(target->lid, cmd->lun);
	cmd->dev_id = dev_id;
	dprintf("%p %x %" PRIx64 "\n", cmd, cmd->scb[0], dev_id);
	cmd->dev = device_lookup(target, dev_id);
	/* use LUN0 */
	if (!cmd->dev)
		cmd->dev = list_first_entry(&target->device_list,
					    struct scsi_lu,
					    device_siblings);

	cmd->itn_lu_info = it_nexus_lu_info_lookup(itn, cmd->dev->lun);

	/* service delivery or target failure */
	if (target->target_state != SCSI_TARGET_READY)
		return -EBUSY;

	/* by default assume zero residual counts */
	scsi_set_in_resid(cmd, 0);
	scsi_set_in_transfer_len(cmd, scsi_get_in_length(cmd));
	scsi_set_out_resid(cmd, 0);
	scsi_set_out_transfer_len(cmd, scsi_get_out_length(cmd));

	/*
	 * Call struct scsi_lu->cmd_perform() that will either be setup for
	 * internal or passthrough CDB processing using 2 functions below.
	 */
	return cmd->dev->cmd_perform(tid, cmd);
}

/*
 * Used by all non bs_sg backstores for internal STGT port emulation
 */
int target_cmd_perform(int tid, struct scsi_cmd *cmd)
{
	struct tgt_cmd_queue *q = &cmd->dev->cmd_queue;
	int result, enabled = 0;

	cmd_hlist_insert(cmd->it_nexus, cmd);

	enabled = cmd_enabled(q, cmd);
	dprintf("%p %x %" PRIx64 " %d\n", cmd, cmd->scb[0], cmd->dev_id,
		enabled);

	if (enabled) {
		result = scsi_cmd_perform(cmd->it_nexus->host_no, cmd);

		cmd_post_perform(q, cmd);

		dprintf("%" PRIx64 " %x %p %p %" PRIu64 " %u %u %d %d\n",
			cmd->tag, cmd->scb[0], scsi_get_out_buffer(cmd),
			scsi_get_in_buffer(cmd), cmd->offset,
			scsi_get_out_length(cmd), scsi_get_in_length(cmd),
			result, cmd_async(cmd));

		set_cmd_processed(cmd);
		if (!cmd_async(cmd))
			target_cmd_io_done(cmd, result);
	} else {
		set_cmd_queued(cmd);
		dprintf("blocked %" PRIx64 " %x %" PRIu64 " %d\n",
			cmd->tag, cmd->scb[0], cmd->dev->lun, q->active_cmd);

		list_add_tail(&cmd->qlist, &q->queue);
	}

	return 0;
}

/*
 * Used by bs_sg for CDB passthrough to STGT LUNs
 */
int target_cmd_perform_passthrough(int tid, struct scsi_cmd *cmd)
{
	int result;

	dprintf("%p %x %" PRIx64 " PT\n", cmd, cmd->scb[0], cmd->dev_id);

	result = cmd->dev->dev_type_template.cmd_passthrough(tid, cmd);

	dprintf("%" PRIx64 " %x %p %p %" PRIu64 " %u %u %d %d\n",
		cmd->tag, cmd->scb[0], scsi_get_out_buffer(cmd),
		scsi_get_in_buffer(cmd), cmd->offset,
		scsi_get_out_length(cmd), scsi_get_in_length(cmd),
		result, cmd_async(cmd));

	set_cmd_processed(cmd);
	if (!cmd_async(cmd))
		target_cmd_io_done(cmd, result);

	return 0;
}

void target_cmd_io_done(struct scsi_cmd *cmd, int result)
{
	enum data_direction cmd_dir = scsi_get_data_dir(cmd);
	struct lu_stat *stat = &cmd->itn_lu_info->stat;
	int lid = cmd->c_target->lid;

	scsi_set_result(cmd, result);
	if (cmd_dir == DATA_WRITE) {
		stat->wr_done_bytes += scsi_get_out_length(cmd);
		stat->wr_done_cmds++;
	} else if (cmd_dir == DATA_READ) {
		stat->rd_done_bytes += scsi_get_in_length(cmd);
		stat->rd_done_cmds++;
	} else if (cmd_dir == DATA_BIDIRECTIONAL) {
		stat->wr_done_bytes += scsi_get_out_length(cmd);
		stat->rd_done_bytes += scsi_get_in_length(cmd);
		stat->bidir_done_cmds++;
	}
	if (result != SAM_STAT_GOOD)
		stat->err_num++;

	tgt_drivers[lid]->cmd_end_notify(cmd->cmd_itn_id, result, cmd);
	return;
}

static void post_cmd_done(struct tgt_cmd_queue *q)
{
	struct scsi_cmd *cmd, *tmp;
	int enabled, result;

	list_for_each_entry_safe(cmd, tmp, &q->queue, qlist) {
		enabled = cmd_enabled(q, cmd);
		if (enabled) {
			int tid = cmd->c_target->tid;
			uint64_t itn_id = cmd->cmd_itn_id;
			struct it_nexus *nexus;

			nexus = it_nexus_lookup(tid, itn_id);
			if (!nexus)
				eprintf("BUG: %" PRIu64 "\n", itn_id);

			list_del(&cmd->qlist);
			dprintf("perform %" PRIx64 " %x\n", cmd->tag,
				cmd->attribute);
			result = scsi_cmd_perform(nexus->host_no, cmd);
			cmd_post_perform(q, cmd);
			set_cmd_processed(cmd);
			if (!cmd_async(cmd))
				target_cmd_io_done(cmd, result);
		} else
			break;
	}
}

/*
 * Used by struct scsi_lu->cmd_done() for normal internal completion
 * (non passthrough)
 */
static void __cmd_done(struct target *target, struct scsi_cmd *cmd)
{
	struct tgt_cmd_queue *q;

	cmd_hlist_remove(cmd);

	dprintf("%p %p %u %u\n", scsi_get_out_buffer(cmd),
		scsi_get_in_buffer(cmd), scsi_get_out_length(cmd),
		scsi_get_in_length(cmd));

	q = &cmd->dev->cmd_queue;
	q->active_cmd--;
	switch (cmd->attribute) {
	case MSG_ORDERED_TAG:
	case MSG_HEAD_TAG:
		clear_queue_blocked(q);
		break;
	}

	post_cmd_done(q);
}

/*
 * Used by struct scsi_lu->cmd_done() for bs_sg (passthrough) completion
 */
void __cmd_done_passthrough(struct target *target, struct scsi_cmd *cmd)
{
	dprintf("%p %p %u %u\n", scsi_get_out_buffer(cmd),
		scsi_get_in_buffer(cmd), scsi_get_out_length(cmd),
		scsi_get_in_length(cmd));
}

void target_cmd_done(struct scsi_cmd *cmd)
{
	struct mgmt_req *mreq;

	mreq = cmd->mreq;
	if (mreq && !--mreq->busy) {
		mreq->result = mreq->function == ABORT_TASK ? -EEXIST : 0;
		tgt_drivers[cmd->c_target->lid]->mgmt_end_notify(mreq);
		free(mreq);
	}

	cmd->dev->cmd_done(cmd->c_target, cmd);
}

static int abort_cmd(struct target *target, struct mgmt_req *mreq,
		     struct scsi_cmd *cmd)
{
	int err = 0;

	eprintf("found %" PRIx64 " %lx\n", cmd->tag, cmd->state);

	if (cmd_processed(cmd)) {
		/*
		 * We've already sent this command to kernel space.
		 * We'll send the tsk mgmt response when we get the
		 * completion of this command.
		 */
		cmd->mreq = mreq;
		err = -EBUSY;
	} else {
		cmd->dev->cmd_done(target, cmd);
		target_cmd_io_done(cmd, TASK_ABORTED);
	}
	return err;
}

static int abort_task_set(struct mgmt_req *mreq, struct target *target,
			  uint64_t itn_id, uint64_t tag, uint8_t *lun, int all)
{
	struct scsi_cmd *cmd, *tmp;
	struct it_nexus *itn;
	int err, count = 0;

	eprintf("found %" PRIx64 " %d\n", tag, all);

	list_for_each_entry(itn, &target->it_nexus_list, nexus_siblings) {
		list_for_each_entry_safe(cmd, tmp, &itn->cmd_list, c_hlist) {
			if ((all && itn->itn_id == itn_id) ||
			    (cmd->tag == tag && itn->itn_id == itn_id) ||
			    (lun && !memcmp(cmd->lun, lun, sizeof(cmd->lun)))) {
				err = abort_cmd(target, mreq, cmd);
				if (err)
					mreq->busy++;
				count++;
			}
		}
	}
	return count;
}

enum mgmt_req_result target_mgmt_request(int tid, uint64_t itn_id,
					 uint64_t req_id, int function,
					 uint8_t *lun_buf, uint64_t tag,
					 int host_no)
{
	struct target *target;
	struct mgmt_req *mreq;
	int err = 0, count, send = 1;
	struct it_nexus *itn;
	struct it_nexus_lu_info *itn_lu;
	uint64_t lun;
	uint16_t asc;

	target = target_lookup(tid);
	if (!target) {
		eprintf("invalid tid %d\n", tid);
		return MGMT_REQ_FAILED;
	}

	mreq = zalloc(sizeof(*mreq));
	if (!mreq) {
		eprintf("failed to allocate mgmt_req\n");
		return MGMT_REQ_FAILED;
	}

	mreq->mid = req_id;
	mreq->function = function;

	switch (function) {
	case ABORT_TASK:
		count = abort_task_set(mreq, target, itn_id, tag, NULL, 0);
		if (mreq->busy)
			send = 0;
		if (!count)
			err = -EEXIST;
		break;
	case ABORT_TASK_SET:
		count = abort_task_set(mreq, target, itn_id, 0, NULL, 1);
		if (mreq->busy)
			send = 0;
		break;
	case CLEAR_ACA:
		eprintf("We don't support ACA\n");
		err = -EINVAL;
		break;
	case CLEAR_TASK_SET:
		/* TAS bit is set to zero. */
		lun = scsi_get_devid(target->lid, lun_buf);
		count = abort_task_set(mreq, target, itn_id, 0, lun_buf, 0);
		if (mreq->busy)
			send = 0;

		list_for_each_entry(itn, &target->it_nexus_list,
				    nexus_siblings) {

			list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
					    itn_itl_info_siblings) {

				if (itn_lu->lu->lun == lun) {
					asc = (itn->itn_id == itn_id) ?
						ASC_POWERON_RESET :
						ASC_CMDS_CLEARED_BY_ANOTHER_INI;

					asc = ua_sense_add(itn_lu, asc);
					break;
				}
			}
		}
		break;
	case LOGICAL_UNIT_RESET:
		lun = scsi_get_devid(target->lid, lun_buf);
		device_release(target->tid, itn_id, lun, 1);
		count = abort_task_set(mreq, target, itn_id, 0, lun_buf, 0);
		if (mreq->busy)
			send = 0;

		list_for_each_entry(itn, &target->it_nexus_list,
				    nexus_siblings) {

			list_for_each_entry(itn_lu, &itn->itn_itl_info_list,
					    itn_itl_info_siblings) {

				if (itn_lu->lu->lun == lun) {
					itn_lu->prevent = 0;
					ua_sense_add(itn_lu, ASC_POWERON_RESET);
					break;
				}
			}
		}
		break;
	default:
		err = -EINVAL;
		eprintf("Unknown task management %x\n", function);
	}

	if (send) {
		mreq->result = err;
		tgt_drivers[target->lid]->mgmt_end_notify(mreq);
		free(mreq);
	}

	if (err)
		return MGMT_REQ_FAILED;
	else if (send)
		return MGMT_REQ_DONE;

	return MGMT_REQ_QUEUED;
}

struct account_entry {
	int aid;
	char *user;
	char *password;
	struct list_head account_siblings;
};

static LIST_HEAD(account_list);

static struct account_entry *__account_lookup_id(int aid)
{
	struct account_entry *ac;

	list_for_each_entry(ac, &account_list, account_siblings)
		if (ac->aid == aid)
			return ac;
	return NULL;
}

static struct account_entry *__account_lookup_user(char *user)
{
	struct account_entry *ac;

	list_for_each_entry(ac, &account_list, account_siblings)
		if (!strcmp(ac->user, user))
			return ac;
	return NULL;
}

int account_lookup(int tid, int type, char *user, int ulen,
		   char *password, int plen)
{
	int i;
	struct target *target;
	struct account_entry *ac;

	if (tid == GLOBAL_TID)
		target = &global_target;
	else
		target = target_lookup(tid);
	if (!target)
		return -ENOENT;

	if (type == ACCOUNT_TYPE_INCOMING) {
		for (i = 0; i < target->account.nr_inaccount; i++) {
			ac = __account_lookup_id(target->account.in_aids[i]);
			if (ac) {
				if (!strcmp(ac->user, user))
					goto found;
			}
		}
	} else {
		ac = __account_lookup_id(target->account.out_aid);
		if (ac) {
			strncpy(user, ac->user, ulen);
			goto found;
		}
	}

	return -ENOENT;
found:
	strncpy(password, ac->password, plen);
	return 0;
}

tgtadm_err account_add(char *user, char *password)
{
	int aid;
	struct account_entry *ac;

	ac = __account_lookup_user(user);
	if (ac)
		return TGTADM_USER_EXIST;

	for (aid = 1; __account_lookup_id(aid) && aid < INT_MAX; aid++)
		;
	if (aid == INT_MAX)
		return TGTADM_TOO_MANY_USER;

	ac = zalloc(sizeof(*ac));
	if (!ac)
		return TGTADM_NOMEM;

	ac->aid = aid;
	ac->user = strdup(user);
	if (!ac->user)
		goto free_account;

	ac->password = strdup(password);
	if (!ac->password)
		goto free_username;

	list_add(&ac->account_siblings, &account_list);
	return 0;
free_username:
	free(ac->user);
free_account:
	free(ac);
	return TGTADM_NOMEM;
}

static tgtadm_err __inaccount_bind(struct target *target, int aid)
{
	int i;

	/* first, check whether we already have this account. */
	for (i = 0; i < target->account.max_inaccount; i++)
		if (target->account.in_aids[i] == aid)
			return TGTADM_USER_EXIST;

	if (target->account.nr_inaccount < target->account.max_inaccount) {
		for (i = 0; i < target->account.max_inaccount; i++)
			if (!target->account.in_aids[i])
				break;
		if (i == target->account.max_inaccount) {
			eprintf("bug %d\n", target->account.max_inaccount);
			return TGTADM_UNKNOWN_ERR;
		}

		target->account.in_aids[i] = aid;
	} else {
		int new_max = target->account.max_inaccount << 1;
		int *buf;

		buf = zalloc(new_max * sizeof(int));
		if (!buf)
			return TGTADM_NOMEM;

		memcpy(buf, target->account.in_aids,
		       target->account.max_inaccount * sizeof(int));
		free(target->account.in_aids);
		target->account.in_aids = buf;
		target->account.in_aids[target->account.max_inaccount] = aid;
		target->account.max_inaccount = new_max;
	}
	target->account.nr_inaccount++;

	return TGTADM_SUCCESS;
}

tgtadm_err account_ctl(int tid, int type, char *user, int bind)
{
	tgtadm_err adm_err = 0;
	struct target *target;
	struct account_entry *ac;
	int i;

	if (tid == GLOBAL_TID)
		target = &global_target;
	else
		target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	ac = __account_lookup_user(user);
	if (!ac)
		return TGTADM_NO_USER;

	if (bind) {
		if (type == ACCOUNT_TYPE_INCOMING)
			adm_err = __inaccount_bind(target, ac->aid);
		else {
			if (target->account.out_aid)
				adm_err = TGTADM_OUTACCOUNT_EXIST;
			else
				target->account.out_aid = ac->aid;
		}
	} else
		if (type == ACCOUNT_TYPE_INCOMING) {
			for (i = 0; i < target->account.max_inaccount; i++)
				if (target->account.in_aids[i] == ac->aid) {
					target->account.in_aids[i] = 0;
					target->account.nr_inaccount--;
					break;
				}

			if (i == target->account.max_inaccount)
				adm_err = TGTADM_NO_USER;
		} else
			if (target->account.out_aid == ac->aid)
				target->account.out_aid = 0;
			else
				adm_err = TGTADM_NO_USER;

	return adm_err;
}

tgtadm_err account_del(char *user)
{
	struct account_entry *ac;
	struct target *target;

	ac = __account_lookup_user(user);
	if (!ac)
		return TGTADM_NO_USER;

	list_for_each_entry(target, &target_list, target_siblings) {
		account_ctl(target->tid, ACCOUNT_TYPE_INCOMING, ac->user, 0);
		account_ctl(target->tid, ACCOUNT_TYPE_OUTGOING, ac->user, 0);
	}

	account_ctl(GLOBAL_TID, ACCOUNT_TYPE_INCOMING, ac->user, 0);
	account_ctl(GLOBAL_TID, ACCOUNT_TYPE_OUTGOING, ac->user, 0);

	list_del(&ac->account_siblings);
	free(ac->user);
	free(ac->password);
	free(ac);
	return TGTADM_SUCCESS;
}

int account_available(int tid, int dir)
{
	struct target *target;

	if (tid == GLOBAL_TID)
		target = &global_target;
	else
		target = target_lookup(tid);
	if (!target)
		return 0;

	if (dir == ACCOUNT_TYPE_INCOMING)
		return target->account.nr_inaccount;
	else
		return target->account.out_aid;
}

tgtadm_err acl_add(int tid, char *address)
{
	char *str;
	struct target *target;
	struct acl_entry *acl, *tmp;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	list_for_each_entry_safe(acl, tmp, &target->acl_list, aclent_list)
		if (!strcmp(address, acl->address))
			return TGTADM_ACL_EXIST;

	acl = zalloc(sizeof(*acl));
	if (!acl)
		return TGTADM_NOMEM;

	str = strdup(address);
	if (!str) {
		free(acl);
		return TGTADM_NOMEM;
	}

	acl->address = str;
	list_add_tail(&acl->aclent_list, &target->acl_list);

	return TGTADM_SUCCESS;
}

tgtadm_err acl_del(int tid, char *address)
{
	struct target *target;
	struct acl_entry *acl, *tmp;
	tgtadm_err adm_err = TGTADM_ACL_NOEXIST;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	list_for_each_entry_safe(acl, tmp, &target->acl_list, aclent_list) {
		if (!strcmp(address, acl->address)) {
			list_del(&acl->aclent_list);
			free(acl->address);
			free(acl);
			adm_err = TGTADM_SUCCESS;
			break;
		}
	}
	return adm_err;
}

char *acl_get(int tid, int idx)
{
	int i = 0;
	struct target *target;
	struct acl_entry *acl;

	target = target_lookup(tid);
	if (!target)
		return NULL;

	list_for_each_entry(acl, &target->acl_list, aclent_list) {
		if (idx == i++)
			return acl->address;
	}

	return NULL;
}

tgtadm_err iqn_acl_add(int tid, char *name)
{
	char *str;
	struct target *target;
	struct iqn_acl_entry *iqn_acl, *tmp;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	list_for_each_entry_safe(iqn_acl, tmp, &target->iqn_acl_list,
				 iqn_aclent_list) {
		if (!strcmp(name, iqn_acl->name))
			return TGTADM_ACL_EXIST;
	}

	iqn_acl = zalloc(sizeof(*iqn_acl));
	if (!iqn_acl)
		return TGTADM_NOMEM;

	str = strdup(name);
	if (!str) {
		free(iqn_acl);
		return TGTADM_NOMEM;
	}

	iqn_acl->name = str;
	list_add_tail(&iqn_acl->iqn_aclent_list, &target->iqn_acl_list);

	return TGTADM_SUCCESS;
}

tgtadm_err iqn_acl_del(int tid, char *name)
{
	struct target *target;
	struct iqn_acl_entry *iqn_acl, *tmp;
	tgtadm_err adm_err = TGTADM_ACL_NOEXIST;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	list_for_each_entry_safe(iqn_acl, tmp, &target->iqn_acl_list,
				 iqn_aclent_list) {

		if (!strcmp(name, iqn_acl->name)) {
			list_del(&iqn_acl->iqn_aclent_list);
			free(iqn_acl->name);
			free(iqn_acl);
			adm_err = TGTADM_SUCCESS;
			break;
		}
	}
	return adm_err;
}

char *iqn_acl_get(int tid, int idx)
{
	int i = 0;
	struct target *target;
	struct iqn_acl_entry *iqn_acl;

	target = target_lookup(tid);
	if (!target)
		return NULL;

	list_for_each_entry(iqn_acl, &target->iqn_acl_list, iqn_aclent_list) {
		if (idx == i++)
			return iqn_acl->name;
	}

	return NULL;
}

/*
 * if we have lots of host, use something like radix tree for
 * efficiency.
 */
static LIST_HEAD(bound_host_list);

struct bound_host {
	int host_no;
	struct target *target;
	struct list_head bhost_siblings;
};

tgtadm_err tgt_bind_host_to_target(int tid, int host_no)
{
	struct target *target;
	struct bound_host *bhost;

	target = target_lookup(tid);
	if (!target) {
		eprintf("can't find a target %d\n", tid);
		return TGTADM_NO_TARGET;
	}

	list_for_each_entry(bhost, &bound_host_list, bhost_siblings) {
		if (bhost->host_no == host_no) {
			eprintf("already bound %d\n", host_no);
			return TGTADM_BINDING_EXIST;
		}
	}

	bhost = malloc(sizeof(*bhost));
	if (!bhost)
		return TGTADM_NOMEM;

	bhost->host_no = host_no;
	bhost->target = target;

	list_add(&bhost->bhost_siblings, &bound_host_list);

	dprintf("bound the scsi host %d to the target %d\n", host_no, tid);

	return TGTADM_SUCCESS;
}

tgtadm_err tgt_unbind_host_to_target(int tid, int host_no)
{
	struct bound_host *bhost;

	list_for_each_entry(bhost, &bound_host_list, bhost_siblings) {
		if (bhost->host_no == host_no) {
			if (!list_empty(&bhost->target->it_nexus_list)) {
				eprintf("the target has IT_nexus\n");
				return TGTADM_TARGET_ACTIVE;
			}
			list_del(&bhost->bhost_siblings);
			free(bhost);
			return TGTADM_SUCCESS;
		}
	}
	return TGTADM_NO_BINDING;
}

enum scsi_target_state tgt_get_target_state(int tid)
{
	struct target *target;

	target = target_lookup(tid);
	if (!target)
		return -ENOENT;
	return target->target_state;
}

static struct {
	enum scsi_target_state value;
	char *name;
} target_state[] = {
	{SCSI_TARGET_OFFLINE, "offline"},
	{SCSI_TARGET_READY, "ready"},
};

static char *target_state_name(enum scsi_target_state state)
{
	int i;
	char *name = NULL;

	for (i = 0; i < ARRAY_SIZE(target_state); i++) {
		if (target_state[i].value == state) {
			name = target_state[i].name;
			break;
		}
	}
	return name;
}

tgtadm_err tgt_set_target_state(int tid, char *str)
{
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;
	struct target *target;
	int i;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	for (i = 0; i < ARRAY_SIZE(target_state); i++) {
		if (!strcmp(target_state[i].name, str)) {
			target->target_state = target_state[i].value;
			adm_err = TGTADM_SUCCESS;
			break;
		}
	}

	return adm_err;
}

static char *print_disksize(uint64_t size)
{
	static char buf[64];
	uint64_t mb;

	mb = ((size / 512) - (((size / 512) / 625) - 974)) / 1950;

	sprintf(buf, "%" PRIu64 " MB", mb);
	return buf;
}

static struct {
	int value;
	char *name;
} disk_type_names[] = {
	{TYPE_DISK, "disk"},
	{TYPE_TAPE, "tape"},
	{TYPE_PRINTER, "printer"},
	{TYPE_PROCESSOR, "processor"},
	{TYPE_WORM, "worm"},
	{TYPE_MMC, "cd/dvd"},
	{TYPE_SCANNER, "scanner"},
	{TYPE_MOD, "optical"},
	{TYPE_MEDIUM_CHANGER, "changer"},
	{TYPE_COMM, "communication"},
	{TYPE_RAID, "controller"},
	{TYPE_ENCLOSURE, "enclosure"},
	{TYPE_RBC, "rbc"},
	{TYPE_OSD, "osd"},
	{TYPE_NO_LUN, "No LUN"},
	{TYPE_PT, "passthrough"}
};

static char *print_type(int type)
{
	int i;
	char *name = NULL;

	for (i = 0; i < ARRAY_SIZE(disk_type_names); i++) {
		if (disk_type_names[i].value == type) {
			name = disk_type_names[i].name;
			break;
		}
	}
	return name;
}

tgtadm_err tgt_target_show_all(struct concat_buf *b)
{
	char strflags[128];
	struct target *target;
	struct scsi_lu *lu;
	struct acl_entry *acl;
	struct iqn_acl_entry *iqn_acl;
	struct it_nexus *nexus;

	list_for_each_entry(target, &target_list, target_siblings) {
		concat_printf(b, "Target %d: %s\n"
			 _TAB1 "System information:\n"
			 _TAB2 "Driver: %s\n"
			 _TAB2 "State: %s\n",
			 target->tid,
			 target->name,
			 tgt_drivers[target->lid]->name,
			 target_state_name(target->target_state));

		if (!strcmp(tgt_drivers[target->lid]->name, "iscsi"))
			iscsi_print_nop_settings(b, target->tid);

		concat_printf(b, _TAB1 "I_T nexus information:\n");

		list_for_each_entry(nexus, &target->it_nexus_list,
				    nexus_siblings) {

			concat_printf(b, _TAB2 "I_T nexus: %" PRIu64 "\n",
				      nexus->itn_id);
			if (nexus->info)
				concat_printf(b, "%s", nexus->info);
		}

		concat_printf(b, _TAB1 "LUN information:\n");
		list_for_each_entry(lu, &target->device_list, device_siblings)
			concat_printf(b,
				_TAB2 "LUN: %" PRIu64 "\n"
				_TAB3 "Type: %s\n"
				_TAB3 "SCSI ID: %s\n"
				_TAB3 "SCSI SN: %s\n"
				_TAB3 "Size: %s, Block size: %d\n"
				_TAB3 "Online: %s\n"
				_TAB3 "Removable media: %s\n"
				_TAB3 "Prevent removal: %s\n"
				_TAB3 "Readonly: %s\n"
				_TAB3 "SWP: %s\n"
				_TAB3 "Thin-provisioning: %s\n"
				_TAB3 "Backing store type: %s\n"
				_TAB3 "Backing store path: %s\n"
				_TAB3 "Backing store flags: %s\n",
				lu->lun,
				print_type(lu->attrs.device_type),
				lu->attrs.scsi_id,
				lu->attrs.scsi_sn,
				print_disksize(lu->size),
				1U << lu->blk_shift,
				lu->attrs.online ? "Yes" : "No",
				lu->attrs.removable ? "Yes" : "No",
				lu_prevent_removal(lu) ? "Yes" : "No",
				lu->attrs.readonly ? "Yes" : "No",
				lu->attrs.swp ? "Yes" : "No",
				lu->attrs.thinprovisioning ? "Yes" : "No",
				lu->bst ?
					(lu->bst->bs_name ? : "Unknown") :
					"None",
				lu->path ? : "None",
					open_flags_to_str(strflags,
							  lu->bsoflags));

		if (!strcmp(tgt_drivers[target->lid]->name, "iscsi") ||
		    !strcmp(tgt_drivers[target->lid]->name, "iser")) {
			int i, aid;

			concat_printf(b, _TAB1 "Account information:\n");
			for (i = 0; i < target->account.nr_inaccount; i++) {
				aid = target->account.in_aids[i];
				concat_printf(b, _TAB2 "%s\n",
					      __account_lookup_id(aid)->user);
			}
			if (target->account.out_aid) {
				aid = target->account.out_aid;
				concat_printf(b, _TAB2 "%s (outgoing)\n",
					 __account_lookup_id(aid)->user);
			}
		}

		concat_printf(b, _TAB1 "ACL information:\n");
		list_for_each_entry(acl, &target->acl_list, aclent_list)
			concat_printf(b, _TAB2 "%s\n", acl->address);

		list_for_each_entry(iqn_acl, &target->iqn_acl_list,
				    iqn_aclent_list) {
			concat_printf(b, _TAB2 "%s\n", iqn_acl->name);
		}

	}
	return TGTADM_SUCCESS;
}

char *tgt_targetname(int tid)
{
	struct target *target;

	target = target_lookup(tid);
	if (!target)
		return NULL;

	return target->name;
}

#define DEFAULT_NR_ACCOUNT 16

tgtadm_err tgt_target_create(int lld, int tid, char *args)
{
	struct target *target, *pos;
	char *p, *q, *targetname = NULL;
	struct backingstore_template *bst;

	p = args;
	while ((q = strsep(&p, ","))) {
		char *str;

		str = strchr(q, '=');
		if (str) {
			*str++ = '\0';

			if (!strcmp("targetname", q))
				targetname = str;
			else
				eprintf("Unknow option %s\n", q);
		}
	};

	if (!targetname)
		return TGTADM_INVALID_REQUEST;

	target = target_lookup(tid);
	if (target) {
		eprintf("Target id %d already exists\n", tid);
		return TGTADM_TARGET_EXIST;
	}

	if (target_name_lookup(targetname)) {
		eprintf("Target name %s already exists\n", targetname);
		return TGTADM_TARGET_EXIST;
	}

	bst = get_backingstore_template(tgt_drivers[lld]->default_bst);
	if (!bst)
		return TGTADM_INVALID_REQUEST;

	target = zalloc(sizeof(*target));
	if (!target)
		return TGTADM_NOMEM;

	target->name = strdup(targetname);
	if (!target->name) {
		free(target);
		return TGTADM_NOMEM;
	}

	target->account.in_aids = zalloc(DEFAULT_NR_ACCOUNT * sizeof(int));
	if (!target->account.in_aids) {
		free(target->name);
		free(target);
		return TGTADM_NOMEM;
	}
	target->account.max_inaccount = DEFAULT_NR_ACCOUNT;

	target->tid = tid;

	INIT_LIST_HEAD(&target->device_list);

	target->bst = bst;

	target->target_state = SCSI_TARGET_READY;
	target->lid = lld;

	list_for_each_entry(pos, &target_list, target_siblings)
		if (target->tid < pos->tid)
			break;

	list_add_tail(&target->target_siblings, &pos->target_siblings);

	INIT_LIST_HEAD(&target->acl_list);
	INIT_LIST_HEAD(&target->iqn_acl_list);
	INIT_LIST_HEAD(&target->it_nexus_list);

	tgt_device_create(tid, TYPE_RAID, 0, NULL, 0);

	if (tgt_drivers[lld]->target_create)
		tgt_drivers[lld]->target_create(target);

	list_add_tail(&target->lld_siblings, &tgt_drivers[lld]->target_list);

	dprintf("Succeed to create a new target %d\n", tid);

	return TGTADM_SUCCESS;
}

tgtadm_err tgt_target_destroy(int lld_no, int tid, int force)
{
	struct target *target;
	struct acl_entry *acl, *tmp;
	struct iqn_acl_entry *iqn_acl, *tmp1;
	struct scsi_lu *lu;
	tgtadm_err adm_err;

	target = target_lookup(tid);
	if (!target)
		return TGTADM_NO_TARGET;

	if (!force && !list_empty(&target->it_nexus_list)) {
		eprintf("target %d still has it nexus\n", tid);
		return TGTADM_TARGET_ACTIVE;
	}

	while (!list_empty(&target->device_list)) {
		/* we remove lun0 last */
		lu = list_entry(target->device_list.prev, struct scsi_lu,
				device_siblings);
		adm_err = tgt_device_destroy(tid, lu->lun, 1);
		if (adm_err != TGTADM_SUCCESS)
			return adm_err;
	}

    dprintf("destroy target:%d, lld:%d", tid, lld_no);

	if (tgt_drivers[lld_no]->target_destroy)
		tgt_drivers[lld_no]->target_destroy(tid, force);

	list_del(&target->target_siblings);

	list_for_each_entry_safe(acl, tmp, &target->acl_list, aclent_list) {
		list_del(&acl->aclent_list);
		free(acl->address);
		free(acl);
	}

	list_for_each_entry_safe(iqn_acl, tmp1, &target->iqn_acl_list,
				 iqn_aclent_list) {
		list_del(&iqn_acl->iqn_aclent_list);
		free(iqn_acl->name);
		free(iqn_acl);
	}

	list_del(&target->lld_siblings);

	free(target->account.in_aids);
	free(target->name);
	free(target);

	return TGTADM_SUCCESS;
}

tgtadm_err tgt_portal_create(int lld, char *args)
{
	char *portals = NULL;

	portals = strstr(args, "portal=");
	if (!portals) {
		eprintf("invalid option when creating portals: %s\n", args);
		return TGTADM_INVALID_REQUEST;
	}

	if (tgt_drivers[lld]->portal_create) {
		if (tgt_drivers[lld]->portal_create(portals)) {
			eprintf("failed to create portal %s\n", portals);
			return TGTADM_INVALID_REQUEST;
		}
	} else {
		eprintf("can not create portals for for this lld type\n");
		return TGTADM_INVALID_REQUEST;
	}

	dprintf("succeed to create new portals %s\n", portals);

	return TGTADM_SUCCESS;
}

tgtadm_err tgt_portal_destroy(int lld, char *args)
{
	char *portals = NULL;

	portals = strstr(args, "portal=");
	if (!portals) {
		eprintf("invalid option when destroying portals: %s\n", args);
		return TGTADM_INVALID_REQUEST;
	}

	if (tgt_drivers[lld]->portal_destroy) {
		if (tgt_drivers[lld]->portal_destroy(portals)) {
			eprintf("failed to destroy portal %s\n", portals);
			return TGTADM_INVALID_REQUEST;
		}
	} else {
		eprintf("can not destroy portals for for this lld type\n");
		return TGTADM_INVALID_REQUEST;
	}

	dprintf("succeed to destroy portals %s\n", portals);

	return TGTADM_SUCCESS;
}

tgtadm_err account_show(struct concat_buf *b)
{
	struct account_entry *ac;

	if (!list_empty(&account_list))
		concat_printf(b, "Account list:\n");

	list_for_each_entry(ac, &account_list, account_siblings)
		concat_printf(b, _TAB1 "%s\n", ac->user);

	return TGTADM_SUCCESS;
}

static struct {
	enum tgt_system_state value;
	char *name;
} system_state[] = {
	{TGT_SYSTEM_OFFLINE, "offline"},
	{TGT_SYSTEM_READY, "ready"},
};

static char *system_state_name(enum tgt_system_state state)
{
	int i;
	char *name = NULL;

	for (i = 0; i < ARRAY_SIZE(system_state); i++) {
		if (system_state[i].value == state) {
			name = system_state[i].name;
			break;
		}
	}
	return name;
}

static enum tgt_system_state sys_state = TGT_SYSTEM_READY;

tgtadm_err system_set_state(char *str)
{
	tgtadm_err adm_err = TGTADM_INVALID_REQUEST;
	int i;

	for (i = 0; i < ARRAY_SIZE(target_state); i++) {
		if (!strcmp(system_state[i].name, str)) {
			sys_state = system_state[i].value;
			adm_err = TGTADM_SUCCESS;
			break;
		}
	}
	return adm_err;
}

tgtadm_err system_show(int mode, struct concat_buf *b)
{
	struct backingstore_template *bst;
	struct device_type_template *devt;
	int i;
	char strflags[128];

	/* FIXME: too hacky */
	if (mode != MODE_SYSTEM)
		return TGTADM_SUCCESS;

	concat_printf(b, "System:\n");
	concat_printf(b, _TAB1 "State: %s\n", system_state_name(sys_state));
	concat_printf(b, _TAB1 "debug: %s\n", is_debug ? "on" : "off");

	concat_printf(b, "LLDs:\n");
	for (i = 0; tgt_drivers[i]; i++) {
		concat_printf(b, _TAB1 "%s: %s\n", tgt_drivers[i]->name,
			      driver_state_name(tgt_drivers[i]));
	}

	concat_printf(b, "Backing stores:\n");
	list_for_each_entry(bst, &bst_list, backingstore_siblings) {
		if (!bst->bs_oflags_supported)
			concat_printf(b, _TAB1 "%s\n", bst->bs_name);
		else
			concat_printf(b, _TAB1 "%s (bsoflags %s)\n",
				      bst->bs_name,
				      open_flags_to_str(strflags,
						bst->bs_oflags_supported));
	}

	concat_printf(b, "Device types:\n");
	list_for_each_entry(devt, &device_type_list, device_type_siblings)
		concat_printf(b, _TAB1 "%s\n", print_type(devt->type));

	if (global_target.account.nr_inaccount) {
		int i, aid;
		concat_printf(b, _TAB1 "%s\n", "Account information:\n");
		for (i = 0; i < global_target.account.nr_inaccount; i++) {
			aid = global_target.account.in_aids[i];
			concat_printf(b, _TAB1 "%s\n",
				      __account_lookup_id(aid)->user);
		}
		if (global_target.account.out_aid) {
			aid = global_target.account.out_aid;
			concat_printf(b, _TAB1 "%s (outgoing)\n",
				      __account_lookup_id(aid)->user);
		}
	}

	return TGTADM_SUCCESS;
}

tgtadm_err lld_show(struct concat_buf *b)
{
	struct target *target;
	int i;

	concat_printf(b, "LLDs:\n");
	for (i = 0; tgt_drivers[i]; i++) {
		concat_printf(b, _TAB1 "%s: %s\n", tgt_drivers[i]->name,
				  driver_state_name(tgt_drivers[i]));

		list_for_each_entry(target, &tgt_drivers[i]->target_list,
				    lld_siblings) {
			concat_printf(b, _TAB2 "Target %d: %s\n", target->tid,
				      target->name);
		}
	}

	return TGTADM_SUCCESS;
}

void update_lbppbe(struct scsi_lu *lu, int blksize)
{
	lu->attrs.lbppbe = 0;
	while (blksize > (1U << lu->blk_shift)) {
		lu->attrs.lbppbe++;
		blksize >>= 1;
	}
}

int is_system_available(void)
{
	return (sys_state == TGT_SYSTEM_READY);
}

int is_system_inactive(void)
{
	return list_empty(&target_list);
}

static void __attribute__((constructor)) target_constructor(void)
{
	static int global_target_aids[DEFAULT_NR_ACCOUNT];

	memset(global_target_aids, 0, sizeof(global_target_aids));
	global_target.account.in_aids = global_target_aids;
	global_target.account.max_inaccount = DEFAULT_NR_ACCOUNT;

	global_target.tid = GLOBAL_TID;

	INIT_LIST_HEAD(&global_target.acl_list);
	INIT_LIST_HEAD(&global_target.iqn_acl_list);
}
