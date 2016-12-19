// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/05 12:17:23

#include "raft/remote_file_copier.h"

#include <gflags/gflags.h>
#include <base/strings/string_piece.h>
#include <base/strings/string_number_conversions.h>
#include <base/files/file_path.h>
#include <base/file_util.h>
#include <bthread.h>
#include <baidu/rpc/controller.h>
#include "raft/util.h"

namespace raft {

DEFINE_int32(raft_max_byte_count_per_rpc, 1024 * 128 /*128K*/,
             "Maximum of block size per RPC");
BAIDU_RPC_VALIDATE_GFLAG(raft_max_byte_count_per_rpc, baidu::rpc::PositiveInteger);

RemoteFileCopier::RemoteFileCopier()
    : _reader_id(0)
{}

int RemoteFileCopier::init(const std::string& uri) {
    // Parse uri format: remote://ip:port/reader_id
    static const size_t prefix_size = strlen("remote://");
    base::StringPiece uri_str(uri);
    if (!uri_str.starts_with("remote://")) {
        LOG(ERROR) << "Invalid uri=" << uri;
        return -1;
    }
    uri_str.remove_prefix(prefix_size);
    size_t slash_pos = uri_str.find('/');
    base::StringPiece ip_and_port = uri_str.substr(0, slash_pos);
    uri_str.remove_prefix(slash_pos + 1);
    if (!base::StringToInt64(uri_str, &_reader_id)) {
        LOG(ERROR) << "Invalid reader_id_format=" << uri_str
                   << " in " << uri;
        return -1;
    }
    if (_channel.Init(ip_and_port.as_string().c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init Channel to " << ip_and_port;
        return -1;
    }
    return 0;
}

int RemoteFileCopier::read_piece_of_file(
            base::IOBuf* buf,
            const std::string& source,
            off_t offset,
            size_t max_count,
            long timeout_ms,
            bool* is_eof) {
    baidu::rpc::Controller cntl;
    GetFileRequest request;
    request.set_reader_id(_reader_id);
    request.set_filename(source);
    request.set_count(max_count);
    request.set_offset(offset);
    GetFileResponse response;
    FileService_Stub stub(&_channel);
    cntl.set_timeout_ms(timeout_ms);
    stub.get_file(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to issue RPC, " << cntl.ErrorText();
        return cntl.ErrorCode();
    }
    *is_eof = response.eof();
    buf->swap(cntl.response_attachment());
    return 0;
}

int RemoteFileCopier::copy_to_file(
        const std::string& source, const std::string& dest_path,
        const CopyOptions* options) {
    scoped_refptr<Session> session = start_to_copy_to_file(
                                        source, dest_path, options);
    session->join();
    return session->status().error_code();
}

int RemoteFileCopier::copy_to_iobuf(const std::string& source,
                                    base::IOBuf* dest_buf, 
                                    const CopyOptions* options) {
    scoped_refptr<Session> session = start_to_copy_to_iobuf(
                                        source, dest_buf, options);
    session->join();
    return session->status().error_code();
}

scoped_refptr<RemoteFileCopier::Session> 
RemoteFileCopier::start_to_copy_to_file(
                      const std::string& source,
                      const std::string& dest_path,
                      const CopyOptions* options) {
    base::fd_guard fd(
            ::open(dest_path.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644));
    
    if (fd < 0) {
        PLOG(ERROR) << "Fail to open " << dest_path;
        return NULL;
    }
    base::make_close_on_exec(fd);

    Session *session = new Session();
    session->_fd = fd.release();
    session->_request.set_filename(source);
    session->_request.set_reader_id(_reader_id);
    session->_channel = &_channel;
    session->send_next_rpc();
    if (options) {
        session->_options = *options;
    }
    return session;
}

scoped_refptr<RemoteFileCopier::Session> 
RemoteFileCopier::start_to_copy_to_iobuf(
                      const std::string& source,
                      base::IOBuf* dest_buf,
                      const CopyOptions* options) {
    dest_buf->clear();
    Session *session = new Session();
    session->_fd = -1;
    session->_buf = dest_buf;
    session->_request.set_filename(source);
    session->_request.set_reader_id(_reader_id);
    session->_channel = &_channel;
    session->send_next_rpc();
    if (options) {
        session->_options = *options;
    }
    return session;
}

RemoteFileCopier::Session::Session() 
    : _channel(NULL)
    , _fd(-1)
    , _retry_times(0)
    , _finished(false)
    , _buf(NULL)
    , _timer() {
    _done.owner = this;
    _finish_event.init();
}

RemoteFileCopier::Session::~Session() {
    if (_fd >=0 ) {
        ::close(_fd);
        _fd = -1;
    }
}

void RemoteFileCopier::Session::send_next_rpc() {
    _cntl.Reset();
    _response.Clear();
    // Not clear request as we need some fields of the previouse RPC
    off_t offset = _request.offset() + _request.count();
    const size_t max_count = 
            (!_buf) ? FLAGS_raft_max_byte_count_per_rpc : UINT_MAX;
    _cntl.set_timeout_ms(_options.timeout_ms);
    _request.set_offset(offset);
    _request.set_count(max_count);
    BAIDU_SCOPED_LOCK(_mutex);
    if (_finished) {
        return;
    }
    _rpc_call = _cntl.call_id();
    FileService_Stub stub(_channel);
    AddRef();  // Release in on_rpc_returned
    return stub.get_file(&_cntl, &_request, &_response, &_done);
}

void RemoteFileCopier::Session::on_rpc_returned() {
    scoped_refptr<Session> ref_gurad;
    Session* this_ref = this;
    ref_gurad.swap(&this_ref);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_cntl.Failed()) {
        // Reset count to make next rpc retry the previous one
        _request.set_count(0);
        if (_cntl.ErrorCode() == ECANCELED) {
            if (_st.ok()) {
                _st.set_error(_cntl.ErrorCode(), _cntl.ErrorText());
                return on_finished();
            }
        }
        if (_retry_times++ >= _options.max_retry) {
            if (_st.ok()) {
                _st.set_error(_cntl.ErrorCode(), _cntl.ErrorText());
                return on_finished();
            }
        }
        AddRef();
        if (raft_timer_add(
                    &_timer, 
                    base::milliseconds_from_now(_options.retry_interval_ms),
                    on_timer, this) != 0) {
            lck.unlock();
            LOG(ERROR) << "Fail to add timer";
            return on_timer(this);
        }
        return;
    }
    _retry_times = 0;
    if (_fd >= 0) {
        FileSegData data(_cntl.response_attachment());
        uint64_t seg_offset = 0;
        base::IOBuf seg_data;
        while (0 != data.next(&seg_offset, &seg_data)) {
            ssize_t nwritten = file_pwrite(seg_data, _fd, seg_offset);
            if (static_cast<size_t>(nwritten) != seg_data.size()) {
                PLOG(WARNING) << "Fail to write into fd=" << _fd;
                _st.set_error(errno, "%s", berror(errno));
                return on_finished();
            }
            seg_data.clear();
        }
    } else {
        FileSegData data(_cntl.response_attachment());
        uint64_t seg_offset = 0;
        base::IOBuf seg_data;
        while (0 != data.next(&seg_offset, &seg_data)) {
            CHECK_GE((size_t)seg_offset, _buf->length());
            _buf->resize(seg_offset);
            _buf->append(seg_data);
        }
    }
    if (_response.eof()) {
        on_finished();
        return;
    }
    lck.unlock();
    return send_next_rpc();
}

void RemoteFileCopier::Session::on_timer(void* arg) {
    Session* m = (Session*)arg;
    m->send_next_rpc();
    m->Release();
}

void RemoteFileCopier::Session::on_finished() {
    if (!_finished) {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
        _finished = true;
        _finish_event.signal();
    }
}

void RemoteFileCopier::Session::cancel() {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_finished) {
        return; 
    }
    baidu::rpc::StartCancel(_rpc_call);
    if (raft_timer_del(_timer) == 0) {
        // Release reference of the timer task
        Release();
    }
    if (_st.ok()) {
        _st.set_error(ECANCELED, "%s", berror(ECANCELED));
    }
    on_finished();
}

void RemoteFileCopier::Session::join() {
    _finish_event.wait();
}

} // namespace raft
