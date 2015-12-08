/*
 * =====================================================================================
 *
 *       Filename:  block.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/26 16:40:30
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_BLOCK_H
#define PUBLIC_RAFT_EXAMPLE_BLOCK_H

#include <base/iobuf.h>
#include <baidu/rpc/server.h>
#include "block.pb.h"
#include "raft/raft.h"
#include "state_machine.h"

namespace block {

class Block;
class WriteDone : public raft::Closure {
public:
    WriteDone(Block* block, baidu::rpc::Controller* controller,
            const WriteRequest* request, WriteResponse* response,
            google::protobuf::Closure* done)
        : _block(block), _controller(controller),
        _request(request), _response(response), _done(done) {}
    virtual ~WriteDone() {}

    virtual void Run();
private:
    Block* _block; // for leader
    baidu::rpc::Controller* _controller;
    const WriteRequest* _request;
    WriteResponse* _response;
    google::protobuf::Closure* _done;
};

class Block : public example::CommonStateMachine {
public:
    Block(const raft::GroupId& group_id, const raft::PeerId& peer_id);

    // init path
    int init(const raft::NodeOptions& options);

    // user method
    void write(int64_t offset, int32_t size, const base::IOBuf& data, WriteDone* done);
    int read(int64_t offset, int32_t size, base::IOBuf* data, int64_t index);

    // FSM method
    virtual void on_apply(const base::IOBuf& buf,
                          const int64_t index, raft::Closure* done);
    virtual void on_shutdown();
    virtual int on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done);
    virtual int on_snapshot_load(raft::SnapshotReader* reader);
    virtual void on_leader_start();
    virtual void on_leader_stop();

private:
    struct LogHeader {
        int meta_len;
        int body_len;

        LogHeader() : meta_len(0), body_len(0) {}
        LogHeader(int meta_len_, int body_len_) : meta_len(meta_len_), body_len(body_len_) {}
    };

    virtual ~Block();
    int get_fd();
    void set_fd(int fd);

    pthread_mutex_t _mutex;
    std::string _path;
    bool _is_leader;
    int64_t _applied_index;
    int _fd;
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_BLOCK_H
