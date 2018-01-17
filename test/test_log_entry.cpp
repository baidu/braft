/*
 * =====================================================================================
 *
 *       Filename:  test_log_entry.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年11月27日 11时11分35秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include <butil/logging.h>
#include <butil/iobuf.h>
#include "braft/log_entry.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, LogEntry) {
    braft::LogEntry* entry = new braft::LogEntry();
    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));
    entry->type = braft::ENTRY_TYPE_CONFIGURATION;
    entry->peers = new std::vector<braft::PeerId>(peers);

    entry->AddRef();
    entry->Release();
    entry->Release();

    entry = new braft::LogEntry();
    entry->type = braft::ENTRY_TYPE_DATA;
    butil::IOBuf buf;
    buf.append("hello, world");
    entry->data = buf;
    entry->data = buf;

    entry->Release();
}
