#ifndef __UTIL_H__
#define __UTIL_H__

#include <byteswap.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <signal.h>
#include <syscall.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <linux/types.h>

#include "be_byteshift.h"

#define roundup(x, y) ((((x) + ((y) - 1)) / (y)) * (y))
#define DIV_ROUND_UP(x, y) (((x) + (y) - 1) / (y))
#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))
#define ALIGN(x,a) (((x)+(a)-1)&~((a)-1))

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define __cpu_to_be16(x) bswap_16(x)
#define __cpu_to_be32(x) bswap_32(x)
#define __cpu_to_be64(x) bswap_64(x)
#define __be16_to_cpu(x) bswap_16(x)
#define __be32_to_cpu(x) bswap_32(x)
#define __be64_to_cpu(x) bswap_64(x)
#define __cpu_to_le32(x) (x)
#else
#define __cpu_to_be16(x) (x)
#define __cpu_to_be32(x) (x)
#define __cpu_to_be64(x) (x)
#define __be16_to_cpu(x) (x)
#define __be32_to_cpu(x) (x)
#define __be64_to_cpu(x) (x)
#define __cpu_to_le32(x) bswap_32(x)
#endif

#define	DEFDMODE	(S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH)
#define	DEFFMODE	(S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

#define min(x,y) ({ \
	typeof(x) _x = (x);	\
	typeof(y) _y = (y);	\
	(void) (&_x == &_y);		\
	_x < _y ? _x : _y; })

#define max(x,y) ({ \
	typeof(x) _x = (x);	\
	typeof(y) _y = (y);	\
	(void) (&_x == &_y);		\
	_x > _y ? _x : _y; })

#define min_t(type,x,y) \
	({ type __x = (x); type __y = (y); __x < __y ? __x: __y; })
#define max_t(type,x,y) \
	({ type __x = (x); type __y = (y); __x > __y ? __x: __y; })

extern int get_blk_shift(unsigned int size);
extern int chrdev_open(char *modname, char *devpath, uint8_t minor, int *fd);
extern int backed_file_open(char *path, int oflag, uint64_t *size,
				uint32_t *blksize);
extern int set_non_blocking(int fd);
extern int str_to_open_flags(char *buf);
extern char *open_flags_to_str(char *dest, int flags);
extern int spc_memcpy(uint8_t *dst, uint32_t *dst_remain_len,
		      uint8_t *src, uint32_t src_len);

#define zalloc(size)			\
({					\
	void *ptr = malloc(size);	\
	if (ptr)			\
		memset(ptr, 0, size);	\
	else				\
		eprintf("%m\n");	\
	ptr;				\
})

static inline int before(uint32_t seq1, uint32_t seq2)
{
        return (int32_t)(seq1 - seq2) < 0;
}

static inline int after(uint32_t seq1, uint32_t seq2)
{
	return (int32_t)(seq2 - seq1) < 0;
}

/* is s2<=s1<=s3 ? */
static inline int between(uint32_t seq1, uint32_t seq2, uint32_t seq3)
{
	return seq3 - seq2 >= seq1 - seq2;
}

extern unsigned long pagesize, pageshift;

#if defined(__NR_signalfd) && defined(USE_SIGNALFD)

/*
 * workaround for broken linux/signalfd.h including
 * usr/include/linux/fcntl.h
 */
#define _LINUX_FCNTL_H

#include <linux/signalfd.h>

static inline int __signalfd(int fd, const sigset_t *mask, int flags)
{
	int fd2, ret;

	fd2 = syscall(__NR_signalfd, fd, mask, _NSIG / 8);
	if (fd2 < 0)
		return fd2;

	ret = fcntl(fd2, F_GETFL);
	if (ret < 0) {
		close(fd2);
		return -1;
	}

	ret = fcntl(fd2, F_SETFL, ret | O_NONBLOCK);
	if (ret < 0) {
		close(fd2);
		return -1;
	}

	return fd2;
}
#else
#define __signalfd(fd, mask, flags) (-1)
struct signalfd_siginfo {
};
#endif

/* convert string to integer, check for validity of the string numeric format
 * and the natural boundaries of the integer value type (first get a 64-bit
 * value and check that it fits the range of the destination integer).
 */
#define str_to_int(str, val)    			\
({      						\
	int ret = 0;    				\
	char *ptr;      				\
	unsigned long long ull_val;     		\
	ull_val = strtoull(str, &ptr, 0);       	\
	val = (typeof(val)) ull_val;    		\
	if (errno || ptr == str)			\
		ret = EINVAL;   			\
	else if (val != ull_val)			\
		ret = ERANGE;   			\
	ret;						\
})

/* convert to int and check: strictly greater than */
#define str_to_int_gt(str, val, minv)   		\
({      						\
	int ret = str_to_int(str, val); 		\
	if (!ret && (val <= minv))       		\
		ret = ERANGE;   			\
	ret;						\
})

/* convert and check: greater than or equal  */
#define str_to_int_ge(str, val, minv)   		\
({      						\
	int ret = str_to_int(str, val); 		\
	if (!ret && (val < minv))       		\
		ret = ERANGE;   			\
	ret;    					\
})

/* convert and check: strictly less than  */
#define str_to_int_lt(str, val, maxv)   		\
({      						\
	int ret = str_to_int(str, val); 		\
	if (!ret && (val >= maxv))       		\
		ret = ERANGE;				\
	ret;						\
})

/* convert and check: range, ends inclusive  */
#define str_to_int_range(str, val, minv, maxv)  	\
({      						\
	int ret = str_to_int(str, val); 		\
	if (!ret && (val < minv || val > maxv)) 	\
		ret = ERANGE;   			\
	ret;						\
})

struct concat_buf {
	FILE *streamf;
	int err;
	int used;
	char *buf;
	size_t size;
};

void concat_buf_init(struct concat_buf *b);
int concat_printf(struct concat_buf *b, const char *format, ...);
const char *concat_delim(struct concat_buf *b, const char *delim);
int concat_buf_finish(struct concat_buf *b);
int concat_write(struct concat_buf *b, int fd, int offset);
void concat_buf_release(struct concat_buf *b);


/* If we have recent enough glibc to support PUNCH HOLE we try to unmap
 * the region.
 */
static inline int unmap_file_region(int fd, off_t offset, off_t length)
{
#ifdef FALLOC_FL_PUNCH_HOLE
	if (fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
			offset, length) == 0)
		return 0;
#endif
	return -1;
}

#define BITS_PER_LONG __WORDSIZE
#define BITS_PER_BYTE           8
#define BITS_TO_LONGS(nr)       DIV_ROUND_UP(nr, BITS_PER_BYTE * sizeof(long))

static inline void set_bit(int nr, unsigned long *addr)
{
	addr[nr / BITS_PER_LONG] |= 1UL << (nr % BITS_PER_LONG);
}

static inline void clear_bit(int nr, unsigned long *addr)
{
	addr[nr / BITS_PER_LONG] &= ~(1UL << (nr % BITS_PER_LONG));
}

static __always_inline int test_bit(unsigned int nr, const unsigned long *addr)
{
	return ((1UL << (nr % BITS_PER_LONG)) &
		(((unsigned long *)addr)[nr / BITS_PER_LONG])) != 0;
}

#endif
