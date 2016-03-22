/*
 * driver routine
 *
 * Copyright (C) 2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2007 Mike Christie <michaelc@cs.wisc.edu>
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
#include <string.h>
#include <inttypes.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "driver.h"

#define MAX_NR_DRIVERS	32

struct tgt_driver *tgt_drivers[MAX_NR_DRIVERS] = {
};

int get_driver_index(char *name)
{
	int i;

	for (i = 0; tgt_drivers[i]; i++) {
		if (!strcmp(name, tgt_drivers[i]->name))
			return i;
	}

	return -ENOENT;
}

int register_driver(struct tgt_driver *drv)
{
	int i;

	for (i = 0; i < ARRAY_SIZE(tgt_drivers); i++)
		if (!tgt_drivers[i]) {
			drv->drv_state = DRIVER_REGD;
			tgt_drivers[i] = drv;
			return 0;
		}

	return -1;
}

const char *driver_state_name(struct tgt_driver *drv)
{
	switch (drv->drv_state) {
	case DRIVER_REGD:
		return "uninitialized";
	case DRIVER_INIT:
		return "ready";
	case DRIVER_ERR:
		return "error";
	case DRIVER_EXIT:
		return "stopped";
	default:
		return "unsupported";
	}
}
