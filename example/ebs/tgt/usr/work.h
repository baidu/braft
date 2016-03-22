#ifndef __SCHED_H
#define __SCHED_H

#if defined(__NR_timerfd_create) && defined(USE_TIMERFD)

#include <sys/timerfd.h>

static inline int __timerfd_create(unsigned int usec)
{
	struct itimerspec new_t, old_t;
	int fd, err;

	fd = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK);
	if (fd < 0)
		return -1;

	new_t.it_value.tv_sec = 0;
	new_t.it_value.tv_nsec = 1;

	new_t.it_interval.tv_sec = 0;
	new_t.it_interval.tv_nsec = usec * 1000;

	err = timerfd_settime(fd, TFD_TIMER_ABSTIME, &new_t, &old_t);
	if (err < 0) {
		close(fd);
		return -1;
	}
	return fd;
}

#else
	#define __timerfd_create(usec)	(-1)
#endif

struct tgt_work {
	struct list_head entry;
	void (*func)(void *);
	void *data;
	unsigned int when;
};

extern int work_timer_start(void);
extern void work_timer_stop(void);

extern void add_work(struct tgt_work *work, unsigned int second);
extern void del_work(struct tgt_work *work);

#endif
