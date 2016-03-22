/*
 * iSCSI Safe Logging and Tracing Library
 *
 * Copyright (C) 2004 Dmitry Yusupov, Alex Aizman
 * maintained by open-iscsi@googlegroups.com
 *
 * circular buffer code based on log.c from dm-multipath project
 *
 * heavily based on code from log.c:
 *   Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>,
 *   licensed under the terms of the GNU GPL v2.0,
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * See the file COPYING included with this distribution for more details.
 */

#ifndef LOG_H
#define LOG_H

#include <sys/sem.h>

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

union semun {
	int val;
	struct semid_ds *buf;
	unsigned short int *array;
	struct seminfo *__buf;
};

#define LOG_SPACE_SIZE 16384
#define MAX_MSG_SIZE 256

extern int log_daemon;
extern int log_level;

struct logmsg {
	short int prio;
	void *next;
	char *str;
};

struct logarea {
	int empty;
	int active;
	void *head;
	void *tail;
	void *start;
	void *end;
	char *buff;
	int semid;
	union semun semarg;
};

extern int log_init (char * progname, int size, int daemon, int debug);
extern void log_close (void);
extern void dump_logmsg (void *);
extern void log_warning(const char *fmt, ...)
	__attribute__ ((format (printf, 1, 2)));
extern void log_error(const char *fmt, ...)
	__attribute__ ((format (printf, 1, 2)));
extern void log_debug(const char *fmt, ...)
	__attribute__ ((format (printf, 1, 2)));

#ifdef NO_LOGGING
#define eprintf(fmt, args...)						\
do {									\
	fprintf(stderr, "%s: " fmt, program_name, ##args);		\
} while (0)

#define dprintf(fmt, args...)						\
do {									\
	if (debug)							\
		fprintf(stderr, "%s %d: " fmt,				\
			__FUNCTION__, __LINE__, ##args);		\
} while (0)
#else
#define eprintf(fmt, args...)						\
do {									\
	log_error("%s(%d) " fmt, __FUNCTION__, __LINE__, ##args);	\
} while (0)

#define dprintf(fmt, args...)						\
do {									\
	if (unlikely(is_debug))						\
		log_debug("%s(%d) " fmt, __FUNCTION__, __LINE__, ##args); \
} while (0)
#endif

#endif	/* LOG_H */
