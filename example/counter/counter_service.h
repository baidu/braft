// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
// Date: 2015/10/23 11:36:55

#ifndef PUBLIC_RAFT_EXAMPLE_COUNTER_SERVICE_H
#define PUBLIC_RAFT_EXAMPLE_COUNTER_SERVICE_H

#include <baidu/rpc/server.h>
#include "counter.pb.h"
#include "raft/raft.h"

namespace counter {

class Counter;
class CounterServiceImpl : public CounterService {
public:
    CounterServiceImpl(Counter* counter);
    virtual ~CounterServiceImpl();

    // rpc method
    virtual void fetch_and_add(google::protobuf::RpcController* controller,
                     const FetchAndAddRequest* request,
                     FetchAndAddResponse* response,
                     google::protobuf::Closure* done);

    virtual void get(google::protobuf::RpcController* controller,
                     const GetRequest* request,
                     GetResponse* response,
                     google::protobuf::Closure* done);

private:
    Counter* _counter;
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_COUNTER_SERVICE_H
