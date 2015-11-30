/*
 * =====================================================================================
 *
 *       Filename:  block_service.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/26 16:33:51
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_BLOCK_SERVICE_H
#define PUBLIC_RAFT_EXAMPLE_BLOCK_SERVICE_H

#include <baidu/rpc/server.h>
#include "raft/raft.h"
#include "block.pb.h"

namespace block {

class Block;
class BlockServiceImpl : public BlockService {
public:
    BlockServiceImpl(Block* block);
    virtual ~BlockServiceImpl();

    void set_block(Block* block) {
        _block = block;
    }

    virtual void write(google::protobuf::RpcController* controller,
                     const WriteRequest* request,
                     WriteResponse* response,
                     google::protobuf::Closure* done);

    virtual void read(google::protobuf::RpcController* controller,
                     const ReadRequest* request,
                     ReadResponse* response,
                     google::protobuf::Closure* done);

private:
    Block* _block;
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_BLOCK_SERVICE_H
