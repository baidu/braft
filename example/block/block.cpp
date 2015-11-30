/*
 * =====================================================================================
 *
 *       Filename:  block.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/29 22:10:22
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#include "block.h"
#include "raft/util.h"

namespace block {

Block::Block(const raft::GroupId& group_id, const raft::ReplicaId& replica_id)
    : example::CommonStateMachine(group_id, replica_id), _is_leader(false), _applied_index(0),
    _fd(-1) {
}

Block::~Block() {
    if (_fd >= 0) {
        ::close(_fd);
        _fd = -1;
    }
}

int Block::get_fd() {
    if (_fd < 0) {
        _fd = ::open(_path.c_str(), O_CREAT | O_RDWR, 0644);
        CHECK(_fd >= 0) << "open block failed: " << berror();
    }
    return _fd;
}

int Block::init(const raft::NodeOptions& options) {
    _path.clear();
    _path = raft::fileuri2path(options.snapshot_uri);
    _path.append("/data");

    return _node.init(options);
}

void Block::write(int64_t offset, int32_t size, const base::IOBuf& data,
                  WriteDone* done) {
    WriteRequest request;
    request.set_offset(offset);
    request.set_size(size);

    base::IOBuf log_meta;
    base::IOBufAsZeroCopyOutputStream wrapper(&log_meta);
    bool ok = request.SerializeToZeroCopyStream(&wrapper);
    CHECK(ok);

    base::IOBuf log_data;
    LogHeader header(log_meta.size(), data.size());
    log_data.append(&header, sizeof(header));
    log_data.append(log_meta);
    log_data.append(data);

    _node.apply(log_data, done);
}

int Block::read(int64_t offset, int32_t size, base::IOBuf* data, int64_t index) {
    bool ok = (_is_leader || index <= _applied_index);
    if (!ok) {
        return EINVAL;
    }

    base::IOPortal portal;
    ssize_t ret = portal.append_from_file_descriptor(get_fd(), size, offset);
    CHECK_EQ(ret, size);
    data->append(portal);

    LOG(NOTICE) << "read success, offset: " << offset << " size: " << size;
    return 0;
}

void Block::on_apply(const base::IOBuf &data, const int64_t index, raft::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);

    base::IOBuf log_data(data);
    LogHeader header;
    log_data.cutn(&header, sizeof(header));

    base::IOBuf log_meta;
    log_data.cutn(&log_meta, header.meta_len);

    base::IOBuf log_body;
    log_data.cutn(&log_body, header.body_len);

    WriteRequest request;
    base::IOBufAsZeroCopyInputStream wrapper(log_meta);
    bool ok = request.ParseFromZeroCopyStream(&wrapper);
    CHECK(ok);

    int64_t offset = request.offset();
    int32_t size = request.size();
    CHECK(static_cast<size_t>(size) == log_body.size())
        << "size: " << size << " data_size: " << log_body.size();

    ::lseek(get_fd(), offset, SEEK_SET);
    base::IOBuf* pieces[] = {&log_body};
    ssize_t writen = base::IOBuf::cut_multiple_into_file_descriptor(get_fd(), pieces, 1);
    CHECK_EQ(writen, size) << "writen: " << writen << " size: " << size;
    LOG(NOTICE) << "write success, offset: " << offset << " size: " << size;

    _applied_index = index;
}

void Block::on_shutdown() {
    //TODO:
    LOG(ERROR) << "on_shutdown";
    exit(1);
    //delete this;
}

int Block::on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
    baidu::rpc::ClosureGuard done_guard(done);

    LOG(NOTICE) << "on_snapshot_save do nothing";
    // snapshot save do nothing
    return 0;
}

int Block::on_snapshot_load(raft::SnapshotReader* reader) {
    LOG(NOTICE) << "on_snapshot_load do nothing";
    // snapshot load do nothing
    return 0;
}

void Block::on_leader_start() {
    LOG(INFO) << "on_leader_start, can accept get";
    _is_leader = true;
}

void Block::on_leader_stop() {
    LOG(INFO) << "on_leader_stop, can't accept get";
    _is_leader = false;
}

}

