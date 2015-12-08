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
#include "raft/snapshot.h"

namespace block {

Block::Block(const raft::GroupId& group_id, const raft::PeerId& peer_id)
    : example::CommonStateMachine(group_id, peer_id), _is_leader(false), _applied_index(0),
    _fd(-1) {
        pthread_mutex_init(&_mutex, NULL);
}

Block::~Block() {
    set_fd(-1);
    pthread_mutex_destroy(&_mutex);
}

int Block::get_fd() {
    //TODO: optimize
    std::lock_guard<pthread_mutex_t> guard(_mutex);
    if (_fd < 0) {
        _fd = ::open(_path.c_str(), O_CREAT | O_RDWR, 0644);
        CHECK(_fd >= 0) << "open block failed: " << berror();
    }
    return _fd;
}

void Block::set_fd(int fd) {
    std::lock_guard<pthread_mutex_t> guard(_mutex);
    ::close(_fd);
    _fd = fd;
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
    base::Timer timer;
    timer.start();
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

    //::lseek(get_fd(), offset, SEEK_SET);
    base::IOBuf* pieces[] = {&log_body};
    ssize_t writen = base::IOBuf::cut_multiple_into_file_descriptor(get_fd(), pieces, 1, offset);
    CHECK_EQ(writen, size) << "writen: " << writen << " size: " << size;

    timer.stop();
    LOG(NOTICE) << "write success, index: " << index << " time: " << timer.u_elapsed()
        << " offset: " << offset << " size: " << size;

    _applied_index = index;
}

void Block::on_shutdown() {
    //TODO:
    LOG(ERROR) << "on_shutdown";
    exit(1);
    //delete this;
}

int Block::on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
    base::Timer timer;
    timer.start();

    baidu::rpc::ClosureGuard done_guard(done);

    //raft::LocalSnapshotWriter* local_writer = dynamic_cast<raft::LocalSnapshotWriter*>(writer);
    std::string snapshot_path(writer->get_uri(base::EndPoint()));
    snapshot_path.append("/../data");
    std::string data_path(writer->get_uri(base::EndPoint()));
    data_path.append("/data");

    CHECK_EQ(0, link(raft::fileuri2path(snapshot_path).c_str(),
                     raft::fileuri2path(data_path).c_str()))
        << "link failed, src " << snapshot_path << " dst: " << data_path << " error: " << berror();

    timer.stop();

    LOG(NOTICE) << "on_snapshot_save, time: " << timer.u_elapsed()
        << " link " << snapshot_path << " to " << data_path;
    // snapshot save do nothing
    return 0;
}

int Block::on_snapshot_load(raft::SnapshotReader* reader) {
    base::Timer timer;
    timer.start();

    std::string snapshot_path(reader->get_uri(base::EndPoint()));
    snapshot_path.append("/../data");
    std::string data_path(reader->get_uri(base::EndPoint()));
    data_path.append("/data");

    unlink(raft::fileuri2path(snapshot_path).c_str());
    CHECK_EQ(0, link(raft::fileuri2path(data_path).c_str(),
                     raft::fileuri2path(snapshot_path).c_str()))
        << "link failed, src " << data_path << " dst: " << snapshot_path << " error: " << berror();

    timer.stop();

    int fd = ::open(raft::fileuri2path(snapshot_path).c_str(), O_RDWR, 0644);
    CHECK(fd >= 0) << "open snapshot file failed, path: "
        << snapshot_path << " error: " << berror();
    set_fd(fd);
    LOG(NOTICE) << "on_snapshot_load, time: " << timer.u_elapsed()
        << " link " << data_path << " to " << snapshot_path;
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

