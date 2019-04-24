// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include "braft/remote_file_copier.h"

#include <gflags/gflags.h>
#include <butil/strings/string_piece.h>
#include <butil/strings/string_number_conversions.h>
#include <butil/files/file_path.h>
#include <butil/file_util.h>
#include <bthread/bthread.h>
#include <brpc/controller.h>
#include "braft/util.h"
#include "braft/snapshot.h"

namespace braft {

DEFINE_int32(raft_max_byte_count_per_rpc, 1024 * 128 /*128K*/,
             "Maximum of block size per RPC");
BRPC_VALIDATE_GFLAG(raft_max_byte_count_per_rpc, brpc::PositiveInteger);
DEFINE_bool(raft_allow_read_partly_when_install_snapshot, true,
            "Whether allowing read snapshot data partly");
BRPC_VALIDATE_GFLAG(raft_allow_read_partly_when_install_snapshot,
                    ::brpc::PassValidate);
DEFINE_bool(raft_enable_throttle_when_install_snapshot, true,
            "enable throttle when install snapshot, for both leader and follower");
BRPC_VALIDATE_GFLAG(raft_enable_throttle_when_install_snapshot,
                    ::brpc::PassValidate);

RemoteFileCopier::RemoteFileCopier()
    : _reader_id(0)
    , _throttle(NULL)
{}

int RemoteFileCopier::init(const std::string& uri, FileSystemAdaptor* fs, 
        SnapshotThrottle* throttle) {
    // Parse uri format: remote://ip:port/reader_id
    static const size_t prefix_size = strlen("remote://");
    butil::StringPiece uri_str(uri);
    if (!uri_str.starts_with("remote://")) {
        LOG(ERROR) << "Invalid uri=" << uri;
        return -1;
    }
    uri_str.remove_prefix(prefix_size);
    size_t slash_pos = uri_str.find('/');
    butil::StringPiece ip_and_port = uri_str.substr(0, slash_pos);
    uri_str.remove_prefix(slash_pos + 1);
    if (!butil::StringToInt64(uri_str, &_reader_id)) {
        LOG(ERROR) << "Invalid reader_id_format=" << uri_str
                   << " in " << uri;
        return -1;
    }
    if (_channel.Init(ip_and_port.as_string().c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init Channel to " << ip_and_port;
        return -1;
    }
    _fs = fs;
    _throttle = throttle;
    return 0;
}

int RemoteFileCopier::read_piece_of_file(
            butil::IOBuf* buf,
            const std::string& source,
            off_t offset,
            size_t max_count,
            long timeout_ms,
            bool* is_eof) {
    brpc::Controller cntl;
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

int RemoteFileCopier::copy_to_file(const std::string& source,
                                   const std::string& dest_path,
                                   const CopyOptions* options) {
    scoped_refptr<Session> session = start_to_copy_to_file(
            source, dest_path, options);
    if (session == NULL) {
        return -1;
    }
    session->join();
    return session->status().error_code();
}

int RemoteFileCopier::copy_to_iobuf(const std::string& source,
                                    butil::IOBuf* dest_buf, 
                                    const CopyOptions* options) {
    scoped_refptr<Session> session = start_to_copy_to_iobuf(
                                        source, dest_buf, options);
    if (session == NULL) {
        return -1;
    }
    session->join();
    return session->status().error_code();
}

scoped_refptr<RemoteFileCopier::Session> 
RemoteFileCopier::start_to_copy_to_file(
                      const std::string& source,
                      const std::string& dest_path,
                      const CopyOptions* options) {
    butil::File::Error e;
    FileAdaptor* file = _fs->open(dest_path, O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, NULL, &e);
    
    if (!file) {
        LOG(ERROR) << "Fail to open " << dest_path 
                   << ", " << butil::File::ErrorToString(e);
        return NULL;
    }

    scoped_refptr<Session> session(new Session());
    session->_dest_path = dest_path;
    session->_file = file;
    session->_request.set_filename(source);
    session->_request.set_reader_id(_reader_id);
    session->_channel = &_channel;
    if (options) {
        session->_options = *options;
    }
    // pass throttle to Session
    if (_throttle) {
        session->_throttle = _throttle;
    }
    session->send_next_rpc();
    return session;
}

scoped_refptr<RemoteFileCopier::Session> 
RemoteFileCopier::start_to_copy_to_iobuf(
                      const std::string& source,
                      butil::IOBuf* dest_buf,
                      const CopyOptions* options) {
    dest_buf->clear();
    scoped_refptr<Session> session(new Session());
    session->_file = NULL;
    session->_buf = dest_buf;
    session->_request.set_filename(source);
    session->_request.set_reader_id(_reader_id);
    session->_channel = &_channel;
    if (options) {
        session->_options = *options;
    }
    session->send_next_rpc();
    return session;
}

RemoteFileCopier::Session::Session() 
    : _channel(NULL)
    , _file(NULL)
    , _retry_times(0)
    , _finished(false)
    , _buf(NULL)
    , _timer()
    , _throttle(NULL)
    , _throttle_token_acquire_time_us(1)
{
    _done.owner = this;
}

RemoteFileCopier::Session::~Session() {
    if (_file) {
        _file->close();
        delete _file;
        _file = NULL;
    }
}

void RemoteFileCopier::Session::send_next_rpc() {
    _cntl.Reset();
    _response.Clear();
    // Not clear request as we need some fields of the previous RPC
    off_t offset = _request.offset() + _request.count();
    const size_t max_count = 
            (!_buf) ? FLAGS_raft_max_byte_count_per_rpc : UINT_MAX;
    _cntl.set_timeout_ms(_options.timeout_ms);
    _request.set_offset(offset);
    // Read partly when throttled
    _request.set_read_partly(FLAGS_raft_allow_read_partly_when_install_snapshot);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_finished) {
        return;
    }
    // throttle
    size_t new_max_count = max_count;
    if (_throttle && FLAGS_raft_enable_throttle_when_install_snapshot) {
        _throttle_token_acquire_time_us = butil::cpuwide_time_us();
        new_max_count = _throttle->throttled_by_throughput(max_count);
        if (new_max_count == 0) {
            // Reset count to make next rpc retry the previous one
            BRAFT_VLOG << "Copy file throttled, path: " << _dest_path;
            _request.set_count(0);
            AddRef();
            int64_t retry_interval_ms_when_throttled = 
                                    _throttle->get_retry_interval_ms();
            if (bthread_timer_add(
                    &_timer, 
                    butil::milliseconds_from_now(retry_interval_ms_when_throttled),
                    on_timer, this) != 0) {
                lck.unlock();
                LOG(ERROR) << "Fail to add timer";
                return on_timer(this);
            }
            return;
        }
    }
    _request.set_count(new_max_count);
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
    if (_finished) {
        return;
    }
    if (_cntl.Failed()) {
        // Reset count to make next rpc retry the previous one
        int64_t request_count = _request.count();
        _request.set_count(0);
        if (_cntl.ErrorCode() == ECANCELED) {
            if (_st.ok()) {
                _st.set_error(_cntl.ErrorCode(), _cntl.ErrorText());
                return on_finished();
            }
        }
        // Throttled reading failure does not increase _retry_times
        if (_cntl.ErrorCode() != EAGAIN && _retry_times++ >= _options.max_retry) {
            if (_st.ok()) {
                _st.set_error(_cntl.ErrorCode(), _cntl.ErrorText());
                return on_finished();
            }
        }
        // set retry time interval
        int64_t retry_interval_ms = _options.retry_interval_ms; 
        if (_cntl.ErrorCode() == EAGAIN && _throttle) {
            retry_interval_ms = _throttle->get_retry_interval_ms();
            // No token consumed, just return back, other nodes maybe able to use them
            if (FLAGS_raft_enable_throttle_when_install_snapshot) {
                _throttle->return_unused_throughput(
                        request_count, 0,
                        butil::cpuwide_time_us() - _throttle_token_acquire_time_us);
            }
        }
        AddRef();
        if (bthread_timer_add(
                    &_timer, 
                    butil::milliseconds_from_now(retry_interval_ms),
                    on_timer, this) != 0) {
            lck.unlock();
            LOG(ERROR) << "Fail to add timer";
            return on_timer(this);
        }
        return;
    }
    if (_throttle && FLAGS_raft_enable_throttle_when_install_snapshot &&
        _request.count() > (int64_t)_cntl.response_attachment().size()) {
        _throttle->return_unused_throughput(
                _request.count(), _cntl.response_attachment().size(),
                butil::cpuwide_time_us() - _throttle_token_acquire_time_us);
    }
    _retry_times = 0;
    // Reset count to |real_read_size| to make next rpc get the right offset
    if (_response.has_read_size() && (_response.read_size() != 0)
            && FLAGS_raft_allow_read_partly_when_install_snapshot) {
        _request.set_count(_response.read_size());
    }
    if (_file) {
        FileSegData data(_cntl.response_attachment());
        uint64_t seg_offset = 0;
        butil::IOBuf seg_data;
        while (0 != data.next(&seg_offset, &seg_data)) {
            ssize_t nwritten = _file->write(seg_data, seg_offset);
            if (static_cast<size_t>(nwritten) != seg_data.size()) {
                LOG(WARNING) << "Fail to write into file: " << _dest_path;
                _st.set_error(EIO, "%s", berror(EIO));
                return on_finished();
            }
            seg_data.clear();
        }
    } else {
        FileSegData data(_cntl.response_attachment());
        uint64_t seg_offset = 0;
        butil::IOBuf seg_data;
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

void* RemoteFileCopier::Session::send_next_rpc_on_timedout(void* arg) {
    Session* m = (Session*)arg;
    m->send_next_rpc();
    m->Release();
    return NULL;
}

void RemoteFileCopier::Session::on_timer(void* arg) {
    bthread_t tid;
    if (bthread_start_background(
                &tid, NULL, send_next_rpc_on_timedout, arg) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        send_next_rpc_on_timedout(arg);
    }
}

void RemoteFileCopier::Session::on_finished() {
    if (!_finished) {
        if (_file) {
            if (!_file->close()) {
                _st.set_error(EIO, "%s", berror(EIO));
            }
            delete _file;
            _file = NULL;
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
    brpc::StartCancel(_rpc_call);
    if (bthread_timer_del(_timer) == 0) {
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

} //  namespace braft
