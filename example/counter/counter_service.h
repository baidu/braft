/*
 * =====================================================================================
 *
 *       Filename:  counter_service.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月23日 11时36分55秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
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

    void set_counter(Counter* counter) {
        _counter = counter;
    }

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
