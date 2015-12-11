/*
 * =====================================================================================
 *
 *       Filename:  block_util.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/12/11 16:14:28
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#ifndef EXAMPLE_BLOCK_UTIL_H
#define EXAMPLE_BLOCK_UTIL_H

#include <base/iobuf.h>
#include <base/logging.h>

void read_at_offset(base::IOPortal* portal, int fd, off_t offset, size_t size);
void write_at_offset(const base::IOBuf& data, int fd, off_t offset, size_t size);

#endif
