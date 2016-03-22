/*
 * iSCSI transport functions
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
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>

#include "iscsid.h"
#include "transport.h"

static LIST_HEAD(iscsi_transport_list);

int lld_index;

int iscsi_init(int index, char *args)
{
	int err, nr = 0;
	struct iscsi_transport *t;

	lld_index = index;

	list_for_each_entry(t, &iscsi_transport_list,
			    iscsi_transport_siblings) {
		err = t->ep_init();
		if (!err)
			nr++;
	}

	return !nr;
}

void iscsi_exit(void)
{
	struct iscsi_transport *t;

	list_for_each_entry(t, &iscsi_transport_list,
			    iscsi_transport_siblings)
		if (t->ep_exit)
			t->ep_exit();
}

int iscsi_transport_register(struct iscsi_transport *t)
{
	list_add_tail(&t->iscsi_transport_siblings, &iscsi_transport_list);
	return 0;
}
