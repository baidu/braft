/*
 * backing store routine
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
#include <dirent.h>
#include <dlfcn.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <syscall.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <linux/types.h>
#include <unistd.h>

#include "list.h"
#include "tgtd.h"
#include "tgtadm_error.h"
#include "util.h"
#include "bs_thread.h"
#include "scsi.h"

LIST_HEAD(bst_list);

static LIST_HEAD(finished_list);
static pthread_mutex_t finished_lock;

/* used by both bs_rdwr.c and bs_rbd.c */
int nr_iothreads = 16;

static int sig_fd = -1;

static int command_fd[2];
static int done_fd[2];
static pthread_t ack_thread;
/* protected by pipe */
static LIST_HEAD(ack_list);
static pthread_cond_t finished_cond;


void bs_create_opcode_map(struct backingstore_template *bst,
			  unsigned char *opcodes, int num)
{
	int i;

	for (i = 0; i < num; i++)
		set_bit(opcodes[i], bst->bs_supported_ops);
}

int is_bs_support_opcode(struct backingstore_template *bst, int op)
{
	/*
	 * assumes that this bs doesn't support supported_ops yet so
	 * returns success for the compatibility.
	 */
	if (!test_bit(TEST_UNIT_READY, bst->bs_supported_ops))
		return 1;

	return test_bit(op, bst->bs_supported_ops);
}

int register_backingstore_template(struct backingstore_template *bst)
{
	list_add(&bst->backingstore_siblings, &bst_list);

	return 0;
}

struct backingstore_template *get_backingstore_template(const char *name)
{
	struct backingstore_template *bst;

	list_for_each_entry(bst, &bst_list, backingstore_siblings) {
		if (!strcmp(name, bst->bs_name))
			return bst;
	}
	return NULL;
}

/* threading helper functions */

static void *bs_thread_ack_fn(void *arg)
{
	int command, ret, nr;
	struct scsi_cmd *cmd;

retry:
	ret = read(command_fd[0], &command, sizeof(command));
	if (ret < 0) {
		eprintf("ack pthread will be dead, %m\n");
		if (errno == EAGAIN || errno == EINTR)
			goto retry;

		goto out;
	}

	pthread_mutex_lock(&finished_lock);

	while (list_empty(&finished_list))
		pthread_cond_wait(&finished_cond, &finished_lock);

	while (!list_empty(&finished_list)) {
		cmd = list_first_entry(&finished_list,
				 struct scsi_cmd, bs_list);

		dprintf("found %p\n", cmd);

		list_del(&cmd->bs_list);
		list_add_tail(&cmd->bs_list, &ack_list);
	}

	pthread_mutex_unlock(&finished_lock);

	nr = 1;
rewrite:
	ret = write(done_fd[1], &nr, sizeof(nr));
	if (ret < 0) {
		eprintf("can't ack tgtd, %m\n");
		if (errno == EAGAIN || errno == EINTR)
			goto rewrite;

		goto out;
	}

	goto retry;
out:
	pthread_exit(NULL);
}

static void bs_thread_request_done(int fd, int events, void *data)
{
	struct scsi_cmd *cmd;
	int nr_events, ret;

	ret = read(done_fd[0], &nr_events, sizeof(nr_events));
	if (ret < 0) {
		eprintf("wrong wakeup\n");
		return;
	}

	while (!list_empty(&ack_list)) {
		cmd = list_first_entry(&ack_list,
				       struct scsi_cmd, bs_list);

		dprintf("back to tgtd, %p\n", cmd);

		list_del(&cmd->bs_list);
		target_cmd_io_done(cmd, scsi_get_result(cmd));
	}

rewrite:
	ret = write(command_fd[1], &nr_events, sizeof(nr_events));
	if (ret < 0) {
		eprintf("can't write done, %m\n");
		if (errno == EAGAIN || errno == EINTR)
			goto rewrite;

		return;
	}
}

static void bs_sig_request_done(int fd, int events, void *data)
{
	int ret;
	struct scsi_cmd *cmd;
	struct signalfd_siginfo siginfo[16];
	LIST_HEAD(list);

	ret = read(fd, (char *)siginfo, sizeof(siginfo));
	if (ret <= 0) {
		return;
	}

	pthread_mutex_lock(&finished_lock);
	list_splice_init(&finished_list, &list);
	pthread_mutex_unlock(&finished_lock);

	while (!list_empty(&list)) {
		cmd = list_first_entry(&list, struct scsi_cmd, bs_list);

		list_del(&cmd->bs_list);

		target_cmd_io_done(cmd, scsi_get_result(cmd));
	}
}

/* Unlock mutex even if thread is cancelled */
static void mutex_cleanup(void *mutex)
{
	pthread_mutex_unlock(mutex);
}

static void *bs_thread_worker_fn(void *arg)
{
	struct bs_thread_info *info = arg;
	struct scsi_cmd *cmd;
	sigset_t set;

	sigfillset(&set);
	sigprocmask(SIG_BLOCK, &set, NULL);

	while (1) {
		pthread_mutex_lock(&info->pending_lock);
		pthread_cleanup_push(mutex_cleanup, &info->pending_lock);

		while (list_empty(&info->pending_list))
			pthread_cond_wait(&info->pending_cond,
					  &info->pending_lock);

		cmd = list_first_entry(&info->pending_list,
				       struct scsi_cmd, bs_list);

		list_del(&cmd->bs_list);
		pthread_cleanup_pop(1); /* Unlock pending_lock mutex */

		info->request_fn(cmd);

		pthread_mutex_lock(&finished_lock);
		list_add_tail(&cmd->bs_list, &finished_list);
		pthread_mutex_unlock(&finished_lock);

		if (sig_fd < 0)
			pthread_cond_signal(&finished_cond);
		else
			kill(getpid(), SIGUSR2);
	}

	pthread_exit(NULL);
}

static int bs_init_signalfd(void)
{
	sigset_t mask;
	int ret;
	DIR *dir;

	dir = opendir(BSDIR);
	if (dir == NULL) {
		eprintf("could not open backing-store module directory %s\n",
			BSDIR);
	} else {
		struct dirent *dirent;
		void *handle;
		while ((dirent = readdir(dir))) {
			char *soname;
			void (*register_bs_module)(void);

			if (dirent->d_name[0] == '.') {
				continue;
			}

			ret = asprintf(&soname, "%s/%s", BSDIR,
					dirent->d_name);
			if (ret == -1) {
				eprintf("out of memory\n");
				continue;
			}
			handle = dlopen(soname, RTLD_NOW|RTLD_LOCAL);
			if (handle == NULL) {
				eprintf("failed to dlopen backing-store "
					"module %s error %s \n",
					soname, dlerror());
				free(soname);
				continue;
			}
			register_bs_module = dlsym(handle, "register_bs_module");
			if (register_bs_module == NULL) {
				eprintf("could not find register_bs_module "
					"symbol in module %s\n",
					soname);
				free(soname);
				continue;
			}
			register_bs_module();
			free(soname);
		}
		closedir(dir);
	}

	pthread_mutex_init(&finished_lock, NULL);

	sigemptyset(&mask);
	sigaddset(&mask, SIGUSR2);
	sigprocmask(SIG_BLOCK, &mask, NULL);

	sig_fd = __signalfd(-1, &mask, 0);
	if (sig_fd < 0)
		return 1;

	ret = tgt_event_add(sig_fd, EPOLLIN, bs_sig_request_done, NULL);
	if (ret < 0) {
		close (sig_fd);
		sig_fd = -1;

		return 1;
	}

	return 0;
}

static int bs_init_notify_thread(void)
{
	int ret;

	pthread_cond_init(&finished_cond, NULL);
	pthread_mutex_init(&finished_lock, NULL);

	ret = pipe(command_fd);
	if (ret) {
		eprintf("failed to create command pipe, %m\n");
		goto destroy_cond_mutex;
	}

	ret = pipe(done_fd);
	if (ret) {
		eprintf("failed to done command pipe, %m\n");
		goto close_command_fd;
	}

	ret = tgt_event_add(done_fd[0], EPOLLIN, bs_thread_request_done, NULL);
	if (ret) {
		eprintf("failed to add epoll event\n");
		goto close_done_fd;
	}

	ret = pthread_create(&ack_thread, NULL, bs_thread_ack_fn, NULL);
	if (ret) {
		eprintf("failed to create an ack thread, %s\n", strerror(ret));
		goto event_del;
	}

	ret = write(command_fd[1], &ret, sizeof(ret));
	if (ret <= 0)
		goto event_del;

	return 0;
event_del:
	tgt_event_del(done_fd[0]);

close_done_fd:
	close(done_fd[0]);
	close(done_fd[1]);
close_command_fd:
	close(command_fd[0]);
	close(command_fd[1]);
destroy_cond_mutex:
	pthread_cond_destroy(&finished_cond);
	pthread_mutex_destroy(&finished_lock);

	return 1;
}

int bs_init(void)
{
	int ret;

	ret = bs_init_signalfd();
	if (!ret) {
		eprintf("use signalfd notification\n");
		return 0;
	}

	ret = bs_init_notify_thread();
	if (!ret) {
		eprintf("use pthread notification\n");
		return 0;
	}

	return 1;
}

tgtadm_err bs_thread_open(struct bs_thread_info *info, request_func_t *rfn,
			  int nr_threads)
{
	int i, ret;

	info->worker_thread = zalloc(sizeof(pthread_t) * nr_threads);
	if (!info->worker_thread)
		return TGTADM_NOMEM;

	eprintf("%d\n", nr_threads);
	info->request_fn = rfn;

	INIT_LIST_HEAD(&info->pending_list);

	pthread_cond_init(&info->pending_cond, NULL);
	pthread_mutex_init(&info->pending_lock, NULL);

	for (i = 0; i < nr_threads; i++) {
		ret = pthread_create(&info->worker_thread[i], NULL,
				     bs_thread_worker_fn, info);

		if (ret) {
			eprintf("failed to create a worker thread, %d %s\n",
				i, strerror(ret));
			if (ret)
				goto destroy_threads;
		}
	}
	info->nr_worker_threads = nr_threads;

	return TGTADM_SUCCESS;
destroy_threads:

	for (; i > 0; i--) {
		if (info->worker_thread[i - 1]) {
			pthread_cancel(info->worker_thread[i - 1]);
			pthread_join(info->worker_thread[i - 1], NULL);
			eprintf("stopped the worker thread %d\n", i - 1);
		}
	}

	pthread_cond_destroy(&info->pending_cond);
	pthread_mutex_destroy(&info->pending_lock);
	free(info->worker_thread);

	return TGTADM_NOMEM;
}

void bs_thread_close(struct bs_thread_info *info)
{
	int i;

	for (i = 0; i < info->nr_worker_threads && info->worker_thread[i]; i++) {
		pthread_cancel(info->worker_thread[i]);
		pthread_join(info->worker_thread[i], NULL);
	}

	pthread_cond_destroy(&info->pending_cond);
	pthread_mutex_destroy(&info->pending_lock);
	free(info->worker_thread);
}

int bs_thread_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu = cmd->dev;
	struct bs_thread_info *info = BS_THREAD_I(lu);

	pthread_mutex_lock(&info->pending_lock);

	list_add_tail(&cmd->bs_list, &info->pending_list);

	pthread_mutex_unlock(&info->pending_lock);

	pthread_cond_signal(&info->pending_cond);

	set_cmd_async(cmd);

	return 0;
}

