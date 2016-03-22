/*
 * concat_buf functions
 * appending formatted output to dynamically grown strings
 *
 * Copyright (C) 2011 Alexander Nezhinsky <alexandernf@mellanox.com>
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

#include <inttypes.h>
#include <unistd.h>
#include <stdio.h>

#include "log.h"
#include "util.h"

void concat_buf_init(struct concat_buf *b)
{
	b->streamf = open_memstream(&b->buf, &b->size);
	b->err = b->streamf ? 0 : errno;
	b->used = 0;
}

int concat_printf(struct concat_buf *b, const char *format, ...)
{
	va_list args;
	int nprinted;

	if (!b->err) {
		va_start(args, format);
		nprinted = vfprintf(b->streamf, format, args);
		if (nprinted >= 0)
			b->used += nprinted;
		else {
			b->err = nprinted;
			fclose(b->streamf);
			b->streamf = NULL;
		}
		va_end(args);
	}
	return b->err;
}

const char *concat_delim(struct concat_buf *b, const char *delim)
{
	return !b->used ? "" : delim;
}

int concat_buf_finish(struct concat_buf *b)
{
	if (b->streamf) {
		fclose(b->streamf);
		b->streamf = NULL;
		if (b->size)
			b->size++; /* account for trailing NULL char */
	}
	return b->err;
}

int concat_write(struct concat_buf *b, int fd, int offset)
{
	concat_buf_finish(b);

	if (b->err) {
		errno = b->err;
		return -1;
	}

	if (b->size - offset > 0)
		return write(fd, b->buf + offset, b->size - offset);
	else {
		errno = EINVAL;
		return -1;
	}
}

void concat_buf_release(struct concat_buf *b)
{
	concat_buf_finish(b);
	if (b->buf) {
		free(b->buf);
		memset(b, 0, sizeof(*b));
	}
}

