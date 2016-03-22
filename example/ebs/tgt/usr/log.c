/*
 * Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>
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
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <signal.h>
#include <errno.h>
#include <sys/shm.h>
#include <sys/ipc.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/prctl.h>

#include "log.h"

#define LOGDBG 0

#if LOGDBG
#define logdbg(file, fmt, args...) fprintf(file, fmt, ##args)
#else
#define logdbg(file, fmt, args...) do {} while (0)
#endif

static struct logarea *la;
static char *log_name;
int is_debug = 0;
static pid_t pid;

static int logarea_init (int size)
{
	int shmid;
	extern char mgmt_path[];
	key_t semkey;

	logdbg(stderr,"enter logarea_init\n");

	if ((shmid = shmget(IPC_PRIVATE, sizeof(struct logarea),
			    0644 | IPC_CREAT | IPC_EXCL)) == -1) {
		syslog(LOG_ERR, "shmget logarea failed %d", errno);
		return 1;
	}

	la = shmat(shmid, NULL, 0);
	if (!la) {
		syslog(LOG_ERR, "shmat logarea failed %d", errno);
		return 1;
	}

	shmctl(shmid, IPC_RMID, NULL);

	if (size < MAX_MSG_SIZE)
		size = LOG_SPACE_SIZE;

	if ((shmid = shmget(IPC_PRIVATE, size,
			    0644 | IPC_CREAT | IPC_EXCL)) == -1) {
		syslog(LOG_ERR, "shmget msg failed %d", errno);
		shmdt(la);
		return 1;
	}

	la->start = shmat(shmid, NULL, 0);
	if (!la->start) {
		syslog(LOG_ERR, "shmat msg failed %d", errno);
		shmdt(la);
		return 1;
	}
	memset(la->start, 0, size);

	shmctl(shmid, IPC_RMID, NULL);

	la->empty = 1;
	la->end = la->start + size;
	la->head = la->start;
	la->tail = la->start;

	if ((shmid = shmget(IPC_PRIVATE, MAX_MSG_SIZE + sizeof(struct logmsg),
			    0644 | IPC_CREAT | IPC_EXCL)) == -1) {
		syslog(LOG_ERR, "shmget logmsg failed %d", errno);
		shmdt(la->start);
		shmdt(la);
		return 1;
	}
	la->buff = shmat(shmid, NULL, 0);
	if (!la->buff) {
		syslog(LOG_ERR, "shmat logmsgfailed %d", errno);
		shmdt(la->start);
		shmdt(la);
		return 1;
	}

	shmctl(shmid, IPC_RMID, NULL);

	if ((semkey = ftok(mgmt_path, 'a')) < 0) {
		syslog(LOG_ERR, "semkey failed %d", errno);
		return 1;
	}

        if ((semget(semkey, 0, IPC_EXCL)) > 0) {
		/* Semkey may exists after SIGKILL tgtd */
		syslog(LOG_WARNING, "semkey 0x%x already exists", semkey);
	}
	if ((la->semid = semget(semkey, 1, 0666 | IPC_CREAT)) < 0) {
		syslog(LOG_ERR, "semget failed %d", errno);
		shmdt(la->buff);
		shmdt(la->start);
		shmdt(la);
		return 1;
	}
	syslog(LOG_INFO, "semkey 0x%x", semkey);

	la->semarg.val=1;
	if (semctl(la->semid, 0, SETVAL, la->semarg) < 0) {
		syslog(LOG_ERR, "semctl failed %d", errno);
		shmdt(la->buff);
		shmdt(la->start);
		shmdt(la);
		return 1;
	}

	return 0;
}

static void free_logarea (void)
{
	if (!la)
		return;
	semctl(la->semid, 0, IPC_RMID, la->semarg);
	shmdt(la->buff);
	shmdt(la->start);
	shmdt(la);
	la = NULL;
}

#if LOGDBG
static void dump_logarea (void)
{
	struct logmsg * msg;

	logdbg(stderr, "\n==== area: start addr = %p, end addr = %p ====\n",
		la->start, la->end);
	logdbg(stderr, "|addr     |next     |prio|msg\n");

	for (msg = (struct logmsg *)la->head; (void *)msg != la->tail;
	     msg = msg->next)
		logdbg(stderr, "|%p |%p |%i   |%s\n", (void *)msg, msg->next,
				msg->prio, (char *)&msg->str);

	logdbg(stderr, "|%p |%p |%i   |%s\n", (void *)msg, msg->next,
			msg->prio, (char *)&msg->str);

	logdbg(stderr, "\n\n");
}
#endif

static int log_enqueue(int prio, const char *fmt, va_list ap)
{
	int len, fwd;
	char buff[MAX_MSG_SIZE];
	struct logmsg * msg;
	struct logmsg * lastmsg;

	lastmsg = (struct logmsg *)la->tail;

	if (!la->empty) {
		fwd = sizeof(struct logmsg) +
		      strlen((char *)&lastmsg->str) * sizeof(char) + 1;
		la->tail += fwd;
	}
	vsnprintf(buff, MAX_MSG_SIZE, fmt, ap);
	len = strlen(buff) * sizeof(char) + 1;

	/* not enough space on tail : rewind */
	if (la->head <= la->tail &&
	    (len + sizeof(struct logmsg)) > (la->end - la->tail)) {
		logdbg(stderr, "enqueue: rewind tail to %p\n", la->tail);
			la->tail = la->start;
	}

	/* not enough space on head : drop msg */
	if (la->head > la->tail &&
	    (len + sizeof(struct logmsg)) > (la->head - la->tail)) {
		logdbg(stderr, "enqueue: log area overrun, drop msg\n");

		if (!la->empty)
			la->tail = lastmsg;

		return 1;
	}

	/* ok, we can stage the msg in the area */
	la->empty = 0;
	msg = (struct logmsg *)la->tail;
	msg->prio = prio;
	memcpy((void *)&msg->str, buff, len);
	lastmsg->next = la->tail;
	msg->next = la->head;

	logdbg(stderr, "enqueue: %p, %p, %i, %s\n", (void *)msg, msg->next,
		msg->prio, (char *)&msg->str);

#if LOGDBG
	dump_logarea();
#endif
	return 0;
}

static int log_dequeue(void *buff)
{
	struct logmsg * src = (struct logmsg *)la->head;
	struct logmsg * dst = (struct logmsg *)buff;
	struct logmsg * lst = (struct logmsg *)la->tail;

	if (la->empty)
		return 1;

	int len = strlen((char *)&src->str) * sizeof(char) +
		  sizeof(struct logmsg) + 1;

	dst->prio = src->prio;
	memcpy(dst, src,  len);

	if (la->tail == la->head)
		la->empty = 1; /* we purge the last logmsg */
	else {
		la->head = src->next;
		lst->next = la->head;
	}
	logdbg(stderr, "dequeue: %p, %p, %i, %s\n",
		(void *)src, src->next, src->prio, (char *)&src->str);

	memset((void *)src, 0,  len);

	return la->empty;
}

/*
 * this one can block under memory pressure
 */
static void log_syslog (void * buff)
{
	struct logmsg * msg = (struct logmsg *)buff;

	syslog(msg->prio, "%s", (char *)&msg->str);
}

static void dolog(int prio, const char *fmt, va_list ap)
{
	struct timespec ts;
	struct sembuf ops;

	if (la) {
		ts.tv_sec = 0;
		ts.tv_nsec = 10000000;

		ops.sem_num = 0;
		ops.sem_flg = 0;
		ops.sem_op = -1;
		if (semtimedop(la->semid, &ops, 1, &ts) < 0) {
			syslog(LOG_ERR, "semop up failed");
			return;
		}

		log_enqueue(prio, fmt, ap);

		ops.sem_op = 1;
		if (semop(la->semid, &ops, 1) < 0) {
			syslog(LOG_ERR, "semop down failed");
			return;
		}
	} else {
		fprintf(stderr, "%s: ", log_name);
		vfprintf(stderr, fmt, ap);
		fflush(stderr);
	}
}

void log_warning(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	dolog(LOG_WARNING, fmt, ap);
	va_end(ap);
}

void log_error(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	dolog(LOG_ERR, fmt, ap);
	va_end(ap);
}

void log_debug(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	dolog(LOG_DEBUG, fmt, ap);
	va_end(ap);
}

static void log_flush(void)
{
	struct sembuf ops;

	if (!la)
		return;

	while (!la->empty) {
		ops.sem_num = 0;
		ops.sem_flg = 0;
		ops.sem_op = -1;
		if (semop(la->semid, &ops, 1) < 0) {
			syslog(LOG_ERR, "semop up failed");
			exit(1);
		}

		log_dequeue(la->buff);

		ops.sem_op = 1;
		if (semop(la->semid, &ops, 1) < 0) {
			syslog(LOG_ERR, "semop down failed");
			exit(1);
		}
		log_syslog(la->buff);
	}
}

static void log_sigsegv(void)
{
	log_error("tgtd logger exits abnormally, pid:%d\n", getpid());
	log_flush();
	closelog();
	free_logarea();
	exit(1);
}

int log_init(char *program_name, int size, int daemon, int debug)
{
	is_debug = debug;

	logdbg(stderr,"enter log_init\n");
	log_name = program_name;

	if (daemon) {
		struct sigaction sa_old;
		struct sigaction sa_new;

		openlog(log_name, 0, LOG_DAEMON);
		setlogmask (LOG_UPTO (LOG_DEBUG));

		if (logarea_init(size)) {
			syslog(LOG_ERR, "failed to initialize the logger\n");
			return 1;
		}

		la->active = 1;
		pid = fork();
		if (pid < 0) {
			syslog(LOG_ERR, "fail to fork the logger\n");
			return 1;
		} else if (pid) {
			syslog(LOG_WARNING,
			       "tgtd daemon started, pid:%d\n", getpid());
			syslog(LOG_WARNING,
			       "tgtd logger started, pid:%d debug:%d\n", pid, is_debug);
			return 0;
		}

		/* flush on daemon's crash */
		sa_new.sa_handler = (void*)log_sigsegv;
		sigemptyset(&sa_new.sa_mask);
		sa_new.sa_flags = 0;
		sigaction(SIGSEGV, &sa_new, &sa_old );

		prctl(PR_SET_PDEATHSIG, SIGSEGV);

		while (la->active) {
			log_flush();
			sleep(1);
		}

		exit(0);
	}

	return 0;
}

void log_close(void)
{
	if (la) {
		la->active = 0;
		waitpid(pid, NULL, 0);

		log_warning("tgtd logger stopped, pid:%d\n", pid);
		log_flush();
		closelog();
		free_logarea();
	}
}
