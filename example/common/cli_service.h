/*
 * =====================================================================================
 *
 *       Filename:  cli_service.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/30 11:36:55
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_COMMON_CLI_SERVICE_H
#define PUBLIC_RAFT_EXAMPLE_COMMON_CLI_SERVICE_H

#include <baidu/rpc/server.h>
#include "cli.pb.h"
#include "raft/raft.h"

namespace example {

class CommonStateMachine;
class CliServiceImpl : public CliService {
public:
    CliServiceImpl(CommonStateMachine* state_machine) : _state_machine(state_machine) {}
    virtual ~CliServiceImpl() {}

    void set_state_machine(CommonStateMachine* state_machine) {
        _state_machine = state_machine;
    }

    // rpc method
    virtual void set_peer(google::protobuf::RpcController* controller,
                     const SetPeerRequest* request,
                     SetPeerResponse* response,
                     google::protobuf::Closure* done);

    virtual void stats(google::protobuf::RpcController* controller,
                     const StatsRequest* request,
                     StatsResponse* response,
                     google::protobuf::Closure* done);

    virtual void shutdown(google::protobuf::RpcController* controller,
                     const ShutdownRequest* request,
                     ShutdownResponse* response,
                     google::protobuf::Closure* done);

    virtual void snapshot(google::protobuf::RpcController* controller,
                     const SnapshotRequest* request,
                     SnapshotResponse* response,
                     google::protobuf::Closure* done);

private:
    CommonStateMachine* _state_machine;
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_COUNTER_SERVICE_H
