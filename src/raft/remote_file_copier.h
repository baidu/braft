// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/05 11:42:50

#ifndef  PUBLIC_RAFT_REMOTE_FILE_COPIER_H
#define  PUBLIC_RAFT_REMOTE_FILE_COPIER_H

#include <baidu/rpc/channel.h>
#include <bthread/countdown_event.h>
#include "raft/file_service.pb.h"
#include "raft/util.h"
#include "raft/snapshot_throttle.h"

namespace raft {

struct CopyOptions {
    CopyOptions();
    int max_retry;
    long retry_interval_ms;
    long timeout_ms;
};

inline CopyOptions::CopyOptions()
    : max_retry(3)
    , retry_interval_ms(1000)  // 1s
    , timeout_ms(10L * 1000)   // 10s
{}

class FileAdaptor;
class FileSystemAdaptor;
class LocalSnapshotWriter;

class RemoteFileCopier {
public:
    // Stands for a copying session
    class Session : public base::RefCountedThreadSafe<Session> {
    public:
        Session();
        ~Session();
        // Cancel the copy process
        void cancel();
        // Wait until this file was copied from the remote reader
        void join();

        const base::Status& status() const { return _st; }
    private:
    friend class RemoteFileCopier;
    friend class Closure;
        struct Closure : google::protobuf::Closure {
            void Run() {
                owner->on_rpc_returned();
            }
            Session* owner;
        };
        void on_rpc_returned();
        void send_next_rpc();
        void on_finished();
        static void on_timer(void* arg);
        static void* send_next_rpc_on_timedout(void* arg);

        raft_mutex_t _mutex;
        base::Status _st;
        baidu::rpc::Channel* _channel;
        std::string _dest_path;
        FileAdaptor* _file;
        int _retry_times;
        bool _finished;
        baidu::rpc::CallId _rpc_call;
        base::IOBuf* _buf;
        bthread_timer_t _timer;
        CopyOptions _options;
        Closure _done;
        baidu::rpc::Controller _cntl;
        GetFileRequest _request;
        GetFileResponse _response;
        bthread::CountdownEvent _finish_event;
        scoped_refptr<SnapshotThrottle> _throttle;   
    };

    RemoteFileCopier();
    int init(const std::string& uri, FileSystemAdaptor* fs, 
            SnapshotThrottle* throttle);

    // Copy `source' from remote to dest
    int copy_to_file(const std::string& source, 
                     const std::string& dest_path,
                     const CopyOptions* options);
    int copy_to_iobuf(const std::string& source,
                      base::IOBuf* dest_buf, 
                      const CopyOptions* options);
    scoped_refptr<Session> start_to_copy_to_file(
                      const std::string& source,
                      const std::string& dest_path,
                      const CopyOptions* options);
    scoped_refptr<Session> start_to_copy_to_iobuf(
                      const std::string& source,
                      base::IOBuf* dest_buf,
                      const CopyOptions* options);
private:
    int read_piece_of_file(base::IOBuf* buf, const std::string& source,
                           off_t offset, size_t max_count,
                           long timeout_ms, bool* is_eof);
    DISALLOW_COPY_AND_ASSIGN(RemoteFileCopier);
    baidu::rpc::Channel _channel;
    int64_t _reader_id;
    scoped_refptr<FileSystemAdaptor> _fs;
    scoped_refptr<SnapshotThrottle> _throttle;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_REMOTE_FILE_COPIER_H
