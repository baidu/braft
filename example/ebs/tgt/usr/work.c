/*
 * work scheduler, loosely timer-based
 *
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
 * Copyright (C) 2011 Alexander Nezhinsky <alexandern@voltaire.com>
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
#include <stdlib.h>
#include <stdint.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/time.h>

#include "list.h"
#include "util.h"
#include "log.h"
#include "work.h"
#include "tgtd.h"

#define WORK_TIMER_INT_MSEC     500
#define WORK_TIMER_INT_USEC     (WORK_TIMER_INT_MSEC * 1000)

static struct itimerval work_timer = {
	{0, WORK_TIMER_INT_USEC},
	{0, WORK_TIMER_INT_USEC}
};

static unsigned int elapsed_msecs;
static int timer_pending;
static int timer_fd[2] = {-1, -1};

static LIST_HEAD(active_work_list);
static LIST_HEAD(inactive_work_list);

static void execute_work(void);

static inline unsigned int timeval_to_msecs(struct timeval t)
{
	return t.tv_sec * 1000 + t.tv_usec / 1000;
}

static void work_timer_schedule_evt(void)
{
	unsigned int n = 0;
	int err;

	if (timer_pending)
		return;

	timer_pending = 1;

	err = write(timer_fd[1], &n, sizeof(n));
	if (err < 0)
		eprintf("Failed to write to pipe, %m\n");
}

static void work_timer_sig_handler(int data)
{
	work_timer_schedule_evt();
}

static void work_timer_evt_handler(int fd, int events, void *data)
{
	int err;
	static int first = 1;

	if (timer_fd[1] == -1) {
		unsigned long long s;
		struct timeval cur_time;

		err = read(timer_fd[0], &s, sizeof(s));
		if (err < 0) {
			if (err != -EAGAIN)
				eprintf("failed to read from timerfd, %m\n");
			return;
		}

		if (first) {
			first = 0;
			err = gettimeofday(&cur_time, NULL);
			if (err) {
				eprintf("gettimeofday failed, %m\n");
				exit(1);
			}
			elapsed_msecs = timeval_to_msecs(cur_time);
			return;
		}

		elapsed_msecs += (unsigned int)s * WORK_TIMER_INT_MSEC;
	} else {
		unsigned int n;
		struct timeval cur_time;

		err = read(timer_fd[0], &n, sizeof(n));
		if (err < 0) {
			eprintf("Failed to read from pipe, %m\n");
			return;
		}

		timer_pending = 0;

		err = gettimeofday(&cur_time, NULL);
		if (err) {
			eprintf("gettimeofday failed, %m\n");
			exit(1);
		}

		elapsed_msecs = timeval_to_msecs(cur_time);
	}

	execute_work();
}

int work_timer_start(void)
{
	struct timeval t;
	int err;

	if (elapsed_msecs)
		return 0;

	err = gettimeofday(&t, NULL);
	if (err) {
		eprintf("gettimeofday failed, %m\n");
		exit(1);
	}
	elapsed_msecs = timeval_to_msecs(t);

	timer_fd[0] = __timerfd_create(WORK_TIMER_INT_USEC);
	if (timer_fd[0] >= 0)
		eprintf("use timer_fd based scheduler\n");
	else {
		struct sigaction s;

		eprintf("use signal based scheduler\n");

		sigemptyset(&s.sa_mask);
		sigaddset(&s.sa_mask, SIGALRM);
		s.sa_flags = 0;
		s.sa_handler = work_timer_sig_handler;
		err = sigaction(SIGALRM, &s, NULL);
		if (err) {
			eprintf("Failed to setup timer handler\n");
			goto timer_err;
		}

		err = setitimer(ITIMER_REAL, &work_timer, 0);
		if (err) {
			eprintf("Failed to set timer\n");
			goto timer_err;
		}

		err = pipe(timer_fd);
		if (err) {
			eprintf("Failed to open timer pipe\n");
			goto timer_err;
		}
	}

	err = tgt_event_add(timer_fd[0], EPOLLIN, work_timer_evt_handler, NULL);
	if (err) {
		eprintf("failed to add timer event, fd:%d\n", timer_fd[0]);
		goto timer_err;
	}

	dprintf("started, timeout: %d msec\n", WORK_TIMER_INT_MSEC);
	return 0;

timer_err:
	work_timer_stop();
	return err;
}

void work_timer_stop(void)
{
	if (!elapsed_msecs)
		return;

	elapsed_msecs = 0;

	tgt_event_del(timer_fd[0]);

	if (timer_fd[0] > 0)
		close(timer_fd[0]);

	if (timer_fd[1] > 0) {
		int ret;
		close(timer_fd[1]);

		ret = setitimer(ITIMER_REAL, 0, 0);
		if (ret)
			eprintf("Failed to stop timer\n");
		else
			dprintf("Timer stopped\n");
	}
}

void add_work(struct tgt_work *work, unsigned int second)
{
	struct tgt_work *ent;
	struct timeval t;
	int err;

	if (second) {
		err = gettimeofday(&t, NULL);
		if (err) {
			eprintf("gettimeofday failed, %m\n");
			exit(1);
		}
		work->when = timeval_to_msecs(t) + second * 1000;

		list_for_each_entry(ent, &inactive_work_list, entry) {
			if (before(work->when, ent->when))
				break;
		}

		list_add_tail(&work->entry, &ent->entry);
	} else {
		list_add_tail(&work->entry, &active_work_list);
		work_timer_schedule_evt();
	}
}

void del_work(struct tgt_work *work)
{
	list_del_init(&work->entry);
}

static void execute_work()
{
	struct tgt_work *work, *n;

	list_for_each_entry_safe(work, n, &inactive_work_list, entry) {
		if (before(elapsed_msecs, work->when))
			break;

		list_del(&work->entry);
		list_add_tail(&work->entry, &active_work_list);
	}

	while (!list_empty(&active_work_list)) {
		work = list_first_entry(&active_work_list,
					struct tgt_work, entry);
		list_del_init(&work->entry);
		work->func(work->data);
	}
}
