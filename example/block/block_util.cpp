/*
 * =====================================================================================
 *
 *       Filename:  block_util.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/12/11 16:29:24
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <base/errno.h>
#include "block_util.h"

void read_at_offset(base::IOPortal* portal, int fd, off_t offset, size_t size) {
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t read_len = portal->append_from_file_descriptor(
                fd, static_cast<size_t>(left), offset);
        if (read_len > 0) {
            left -= read_len;
            offset += read_len;
        } else if (read_len == 0) {
            char zero_page[4096] = {0};
            ssize_t page_num = left / 4096;
            left -= (page_num * 4096);
            for (ssize_t i = 0; i < page_num; i++) {
                portal->append(zero_page, 4096);
            }
            portal->append(zero_page, left);
        } else {
            CHECK(false) << "read failed, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            break;
        }
    }

}

void write_at_offset(const base::IOBuf& data, int fd, off_t offset, size_t size) {
    CHECK_EQ(data.size(), size);
    base::IOBuf piece_data(data);
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        //base::IOBuf* pieces[] = {&piece_data};
        //ssize_t writen = base::IOBuf::cut_multiple_into_file_descriptor(fd, pieces, 1, offset);
        ssize_t writen = piece_data.cut_into_file_descriptor_at_offset(fd, offset, left);
        if (writen >= 0) {
            offset += writen;
            left -= writen;
            piece_data.pop_front(writen);
        } else if (errno == EINTR) {
            continue;
        } else {
            CHECK(false) << "write falied, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
        }
    }
}

