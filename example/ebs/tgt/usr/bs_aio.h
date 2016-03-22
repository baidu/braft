#ifndef __BS_AIO_H
#define __BS_AIO_H

/* this file is a workaround */

/*
 *  eventfd-aio-test by Davide Libenzi (test app for eventfd hooked into KAIO)
 *  Copyright (C) 2007  Davide Libenzi
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *  Davide Libenzi <davidel@xmailserver.org>
 *
 */

enum {
	IOCB_CMD_PREAD = 0,
		IOCB_CMD_PWRITE = 1,
		IOCB_CMD_FSYNC = 2,
		IOCB_CMD_FDSYNC = 3,
		/* These two are experimental.
		 * IOCB_CMD_PREADX = 4,
		 * IOCB_CMD_POLL = 5,
		 */
		IOCB_CMD_NOOP = 6,
		IOCB_CMD_PREADV = 7,
		IOCB_CMD_PWRITEV = 8,
};

#define IOCB_FLAG_RESFD		(1 << 0)

#if defined(__LITTLE_ENDIAN)
#define PADDED(x, y)	x, y
#elif defined(__BIG_ENDIAN)
#define PADDED(x, y)	y, x
#else
#error edit for your odd byteorder.
#endif

typedef unsigned long io_context_t;

struct io_event {
	uint64_t	data;		/* the data field from the iocb */
	uint64_t	obj;		/* what iocb this event came from */
	int64_t		res;		/* result code for this event */
	int64_t		res2;		/* secondary result */
};

struct iocb {
	/* these are internal to the kernel/libc. */
	uint64_t aio_data;	/* data to be returned in event's data */
	int32_t	PADDED(aio_key, aio_reserved1);
				/* the kernel sets aio_key to the req # */

	/* common fields */
	uint16_t aio_lio_opcode;	/* see IOCB_CMD_ above */
	int16_t	aio_reqprio;
	int32_t	aio_fildes;

	uint64_t aio_buf;
	uint64_t aio_nbytes;
	int64_t	aio_offset;

	/* extra parameters */
	uint64_t aio_reserved2;	/* TODO: use this for a (struct sigevent *) */

	/* flags for the "struct iocb" */
	int32_t	aio_flags;

	/*
	 * if the IOCB_FLAG_RESFD flag of "aio_flags" is set, this is an
	 * eventfd to signal AIO readiness to
	 */
	int32_t	aio_resfd;
}; /* 64 bytes */

static inline int io_setup(unsigned nr_reqs, io_context_t *ctx)
{
	return syscall(__NR_io_setup, nr_reqs, ctx);
}

static inline long io_destroy(io_context_t ctx)
{
	return syscall(__NR_io_destroy, ctx);
}

static inline int io_submit(io_context_t ctx, long n, struct iocb **paiocb)
{
	return syscall(__NR_io_submit, ctx, n, paiocb);
}

static inline long io_cancel(io_context_t ctx, struct iocb *aiocb,
			     struct io_event *res)
{
	return syscall(__NR_io_cancel, ctx, aiocb, res);
}

static inline long io_getevents(io_context_t ctx, long min_nr, long nr,
				struct io_event *events, struct timespec *tmo)
{
	return syscall(__NR_io_getevents, ctx, min_nr, nr, events, tmo);
}

static inline int eventfd(int count)
{
	return syscall(__NR_eventfd, count);
}

#endif
