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
#include <base/logging.h>
#include <base/iobuf.h>
#include "raft/log_entry.h"

class TestUsageSuits : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestUsageSuits, LogEntry) {
    raft::LogEntry* entry = new raft::LogEntry();
    std::vector<raft::PeerId> peers;
    peers.push_back(raft::PeerId("1.2.3.4:1000"));
    peers.push_back(raft::PeerId("1.2.3.4:2000"));
    peers.push_back(raft::PeerId("1.2.3.4:3000"));
    entry->type = raft::ENTRY_TYPE_CONFIGURATION;
    entry->add_peer(peers);

    entry->AddRef();
    entry->Release();
    entry->Release();

    entry = new raft::LogEntry();
    entry->type = raft::ENTRY_TYPE_DATA;
    base::IOBuf buf;
    buf.append("hello, world");
    entry->set_data(buf);
    entry->set_data(buf);

    entry->Release();
}
