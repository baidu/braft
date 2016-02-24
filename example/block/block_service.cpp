/*
 * =====================================================================================
 *
 *       Filename:  block_service.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/26 16:37:19
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include "block_service.h"
#include "block.h"

namespace block {

void WriteDone::Run() {
    _timer.stop();
    if (status().ok()) {
        VLOG(9) << "block: " << _block << " write success,"
            << " offset: " << _request->offset()
            << " size: " << _request->size()
            << " time: " << _timer.u_elapsed();
        _response->set_success(true);
    } else {
        LOG(WARNING) << "block: " << _block << " write failed,"
            << " offset: " << _request->offset()
            << " size: " << _request->size()
            << " time: " << _timer.u_elapsed()
            << " err: " << status();

        _response->set_success(false);
        _response->set_leader(base::endpoint2str(_block->leader()).c_str());
    }
    _done->Run();
    delete this;
}

BlockServiceImpl::BlockServiceImpl(Block* block) : _block(block) {
}

BlockServiceImpl::~BlockServiceImpl() {
}

void BlockServiceImpl::write(google::protobuf::RpcController* controller,
                             const WriteRequest* request,
                             WriteResponse* response,
                             google::protobuf::Closure* done) {
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);
    VLOG(9) << "received write offset: " << request->offset() << " size: " << request->size()
        << " from " << cntl->remote_side();

    // check block
    if (!_block) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "block not set");
        done->Run();
        return;
    }

    WriteDone* write_done = new WriteDone(_block, cntl, request, response, done);
    const base::IOBuf& data = cntl->request_attachment();
    CHECK_EQ(data.size(), static_cast<size_t>(request->size()));
    _block->write(request->offset(), request->size(), data, write_done);
}

void BlockServiceImpl::read(google::protobuf::RpcController* controller,
                            const ReadRequest* request,
                            ReadResponse* response,
                            google::protobuf::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);
    baidu::rpc::Controller* cntl =
        static_cast<baidu::rpc::Controller*>(controller);

    VLOG(9) << "received read offset: " << request->offset() << " size: " << request->size()
        << " from " << cntl->remote_side(); 

    // check block
    if (!_block) {
        cntl->SetFailed(baidu::rpc::SYS_EINVAL, "block not set");
        return;
    }

    int64_t index = 0;
    if (request->has_index()) {
        index = request->index();
    }
    base::IOBuf& data = cntl->response_attachment();
    if (0 == _block->read(request->offset(), request->size(), &data, index)) {
        response->set_success(true);
    } else {
        response->set_success(false);
        response->set_leader(base::endpoint2str(_block->leader()).c_str());
    }
}

}
