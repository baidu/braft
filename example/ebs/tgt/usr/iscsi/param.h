/*
 * Copyright (C) 2005-2007 FUJITA Tomonori <tomof@acm.org>
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
#ifndef PARAMS_H
#define PARAMS_H

struct iscsi_key;

struct param {
	int state;
	unsigned int val;
};

#define KEY_STATE_START		0
#define KEY_STATE_REQUEST	1
#define KEY_STATE_DONE		2

struct iscsi_key_ops {
	int (*val_to_str)(unsigned int, char *);
	int (*str_to_val)(char *, unsigned int *);
	int (*check_val)(struct iscsi_key *, unsigned int *);
	void (*set_val)(struct param *, int, unsigned int *);
};

struct iscsi_key {
	char *name;
	unsigned int def;
	unsigned int min;
	unsigned int max;
	struct iscsi_key_ops *ops;
};

extern struct iscsi_key session_keys[];

extern void param_set_defaults(struct param *, struct iscsi_key *);
extern int param_index_by_name(char *, struct iscsi_key *);
extern int param_val_to_str(struct iscsi_key *, int, unsigned int, char *);
extern int param_str_to_val(struct iscsi_key *, int, char *, unsigned int *);
extern int param_check_val(struct iscsi_key *, int, unsigned int *);
extern void param_set_val(struct iscsi_key *, struct param *, int, unsigned int *);

#endif
