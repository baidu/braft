// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/01/14 13:45:46

#include <fstream>
#include <bthread_unstable.h>
#include <gflags/gflags.h>
#include <base/containers/flat_map.h>
#include <base/logging.h>
#include <base/comlog_sink.h>
#include <base/memory/ref_counted.h>
#include <base/raw_pack.h>
#include <bthread.h>
#include <bthread/execution_queue.h>
#include <bvar/bvar.h>
#include <baidu/rpc/controller.h>
#include <baidu/rpc/server.h>
#include <raft/util.h>
#include <raft/storage.h>
#include <raft/fsync.h>
#include "state_machine.h"
#include "cli_service.h"
#include "ebs.pb.h"

DEFINE_string(ip_and_port, "0.0.0.0:8000", "server listen address");
DEFINE_string(peers, "", "cluster peer set");
DEFINE_int32(snapshot_interval, 120, "Interval between each snapshot");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election after no message received from leader in such time");
DEFINE_int32(block_num, 32, "Number of block");
DEFINE_int64(block_size, 1L * 1024 * 1024 * 1024, "Size of each block");
DEFINE_bool(sync_data, false, "Sync block on each write");
DEFINE_bool(use_rocksdb, true, "Store WAL in RocksDB");

BAIDU_RPC_VALIDATE_GFLAG(sync_data, ::baidu::rpc::PassValidate);

namespace raft {
DECLARE_bool(raft_sync);
}

namespace example {

bvar::LatencyRecorder g_write_file_latency("write_file");
bvar::LatencyRecorder g_normalized_write_file_latency("write_file_normalized");

using namespace baidu::ebs;

class Block : public CommonStateMachine {
public:

    class SharedFD : public base::RefCountedThreadSafe<SharedFD> {
    public:
        explicit SharedFD(int fd) : _fd(fd) {
            AddRef();
        }
        int fd() const { return _fd; }
    private:
    friend class base::RefCountedThreadSafe<SharedFD>;
        ~SharedFD() {
            if (_fd >= 0) {
                while (true) {
                    const int rc = ::close(_fd);
                    if (rc == 0 || errno != EINTR) {
                        break;
                    }
                }
                _fd = -1;
            }
        }
        
        int _fd;
    };

    typedef scoped_refptr<SharedFD> scoped_fd;

    scoped_fd get_fd() const {
        BAIDU_SCOPED_LOCK(_fd_mutex);
        return make_scoped_refptr(_fd);
    }

    Block(const raft::GroupId& group_id, const raft::PeerId& peer_id) 
        : CommonStateMachine(group_id, peer_id)
        , _fd(NULL)
        , _is_leader(false) {
        CHECK_EQ(0, bthread_mutex_init(&_fd_mutex, NULL));
        bthread::ExecutionQueueOptions options;
        options.max_tasks_size=256;
        CHECK_EQ(0, bthread::execution_queue_start(&_sync_queue, &options, 
                                                   do_sync, NULL));
    }

    virtual ~Block() {
        CHECK_EQ(0, bthread_mutex_destroy(&_fd_mutex));
        bthread::execution_queue_stop(_sync_queue);
    }

    void read(::google::protobuf::RpcController* controller,
              const ::baidu::ebs::ReadRequest* request,
              ::baidu::ebs::ReadResponse* response,
              ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        if (!_is_leader.load(boost::memory_order_acquire)) {
            response->set_error_code(EINVAL);
            return;
        }
        scoped_fd fd = get_fd();
        ssize_t left = request->size();
        off_t offset = request->offset();
        response->mutable_data()->resize(left);
        char *buf = (char*)response->mutable_data()->data();
        response->set_error_code(0);
        while (left > 0) {
            ssize_t read_len = pread(fd->fd(), buf, left, offset);
            if (read_len > 0) {
                left -= read_len;
                offset += read_len;
                buf += read_len;
            } else if (read_len == 0) {
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                response->set_error_code(errno);
                LOG(WARNING) << "read failed, err: " << berror()
                    << " fd: " << fd->fd() << " offset: ";
                break;
            }
        }
        if (left > 0) {
            response->mutable_data()->resize(request->size() - left);
        }
    }

    static int do_sync(void* /*meta*/, bthread::TaskIterator<SharedFD*>& iter) {
        if (iter.is_queue_stopped()) {
            return 0;
        }
        SharedFD* last = NULL;
        for (; iter; ++iter) {
            if (last) {
                last->Release();
            }
            last = *iter;
        }
        if (last) {
            raft::raft_fsync(last->fd());
            last->Release();
        }
        return 0;
    }

    struct WriteClosure : public raft::Closure {
    public:
        WriteClosure(baidu::rpc::Controller* cntl,
                     ::baidu::ebs::AckResponse* response,
                     ::google::protobuf::Closure* done) 
            : _cntl(cntl)
            , _response(response)
            , _done(done)
            , _log_index(0)
        {}
        void Run() {
            if (status().ok()) {
                _response->set_error_code(0);
                if (_log_index != 0) {
                    _response->set_applied_lrsn(_log_index);
                }
            } else {
                _response->set_error_code(status().error_code());
                _response->set_error_message(status().error_cstr());
            }
            _done->Run();
            delete this;
        }
        void set_log_index(int64_t log_index)
        { _log_index = log_index; }
    private:
        ~WriteClosure() {}
        baidu::rpc::Controller* _cntl;
        ::baidu::ebs::AckResponse* _response;
        ::google::protobuf::Closure* _done;
        int64_t _log_index;
    };

    int init(const raft::NodeOptions& options) {
        if (_node.init(options) != 0) {
            return -1;
        }
        if (_fd != NULL) {
            return 0;
        }
        const size_t pos = options.snapshot_uri.find("://");
        CHECK_NE(std::string::npos, pos);
        std::string path = options.snapshot_uri.substr(pos + 3);
        path.append("/data");
        int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            LOG(ERROR) << "Fail to open " << path;
            return -1;
        }
        if (ftruncate(fd, FLAGS_block_size) != 0) {
            LOG(ERROR) << "Fail to ftruncate " << path;
            close(fd);
            return -1;
        }
        _fd = new SharedFD(fd);
        return 0;
    }

    void write(::google::protobuf::RpcController* controller,
               const ::baidu::ebs::WriteRequest* request,
               ::baidu::ebs::AckResponse* response,
               ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
        base::IOBuf data;
        WriteRequest meta;
        meta.set_block_id(request->block_id());
        meta.set_offset(request->offset());
        meta.set_write_version(request->write_version());
        char head[8];
        base::RawPacker(head).pack32(meta.ByteSize())
                             .pack32(request->data().size());
        data.append(head, sizeof(head));
        base::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!meta.SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(baidu::rpc::EREQUEST, "Fail to serialize meta");
            return;
        }
        data.append(request->data());
        WriteClosure* c = new WriteClosure(cntl, response, done_guard.release());
        raft::Task task;
        task.data = &data;
        task.done = c;
        return _node.apply(task);
    }

    // FSM method
    void on_apply(raft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            char tmp[8];
            base::IOBuf data(iter.data());
            const void* head = data.fetch(tmp, 8);
            uint32_t meta_size;
            uint32_t data_size;
            base::RawUnpacker(head).unpack32(meta_size).unpack32(data_size);
            data.pop_front(8);
            base::IOBuf meta_buf;
            data.cutn(&meta_buf, meta_size);
            WriteRequest meta;
            base::IOBufAsZeroCopyInputStream wrapper(meta_buf);
            CHECK_GT(meta_size, 0u);
            CHECK_EQ(8ul + meta_size + data_size, iter.data().length());
            if (!meta.ParseFromZeroCopyStream(&wrapper)) {
                LOG(FATAL) << "Fail to parse meta";
                if (iter.done()) {
                    iter.done()->status().set_error(EINVAL, "Fail to prase meta");
                    raft::run_closure_in_bthread(iter.done());
                    continue;
                }
            }
            base::Timer timer;
            timer.start();
            const ssize_t towrite = data.length();
            CHECK_EQ(towrite, raft::file_pwrite(data, _fd->fd(), meta.offset()));
            timer.stop();
            if (iter.done()) {
                ((WriteClosure*)iter.done())->set_log_index(iter.index());
                raft::run_closure_in_bthread(iter.done());
            }
            if (FLAGS_sync_data) {
                _fd->AddRef();
                CHECK_EQ(0, bthread::execution_queue_execute(_sync_queue, _fd));
            }
            g_write_file_latency << timer.u_elapsed();
            g_normalized_write_file_latency << timer.u_elapsed() * 1024 / data.length();
        }
    }

    void on_shutdown() {
        LOG(ERROR) << "Not implemented";
    }

    struct SnapshotSaveMeta {
        raft::Closure* done;
        raft::SnapshotWriter* writer;
        SharedFD* fd;
    };

    static void* save_snapshot(void* arg) {
        SnapshotSaveMeta* meta = (SnapshotSaveMeta*)arg;
        std::unique_ptr<SnapshotSaveMeta> meta_guard(meta);
        baidu::rpc::ClosureGuard done_guard(meta->done);
        base::EndPoint local;

        std::string snapshot_path(meta->writer->get_path());
        snapshot_path.append("/../data");
        std::string data_path(meta->writer->get_path());
        data_path.append("/data");

        if (FLAGS_sync_data) {
            raft::raft_fsync(meta->fd->fd());
        }
        meta->fd->Release();
        CHECK_EQ(0, link(snapshot_path.c_str(),
                    data_path.c_str()))
            << "link failed, src " << snapshot_path << " dst: " << data_path << " error: " << berror();
        return NULL;
    }

    void on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
        SnapshotSaveMeta* meta = new SnapshotSaveMeta;
        meta->done = done;
        meta->writer = writer;
        _fd->AddRef();
        meta->fd = _fd;
        bthread_t t;
        if (bthread_start_urgent(&t, NULL, save_snapshot, meta)) {
            PLOG(ERROR) << "Fail to start bthread";
            save_snapshot(meta);
        }
    }

    int on_snapshot_load(raft::SnapshotReader* reader) {
        base::Timer timer;
        timer.start();

        std::string snapshot_path(reader->get_path());
        snapshot_path.append("/../data");
        std::string data_path(reader->get_path());
        data_path.append("/data");

        unlink(snapshot_path.c_str());
        CHECK_EQ(0, link(data_path.c_str(),
                         snapshot_path.c_str()))
            << "link failed, src " << data_path << " dst: " << snapshot_path << " error: " << berror();

        timer.stop();

        int fd = ::open(snapshot_path.c_str(), O_RDWR, 0644);
        CHECK(fd >= 0) << "open snapshot file failed, path: "
            << snapshot_path << " error: " << berror();
        LOG(NOTICE) << "on_snapshot_load, time: " << timer.u_elapsed()
            << " link " << data_path << " to " << snapshot_path;
        BAIDU_SCOPED_LOCK(_fd_mutex);
        if (_fd != NULL) {
            _fd->Release();
        }
        _fd = new SharedFD(fd);
        return 0;
    }

    void on_leader_start(int64_t term) {
        LOG(INFO) << "leader start at term: " << term;
        _is_leader.store(true, boost::memory_order_release);
    }
    void on_leader_stop() {
        _is_leader.store(false, boost::memory_order_release);
    }

    void apply(base::IOBuf *iobuf, raft::Closure* done) {
        raft::Task task;
        task.data = iobuf;
        task.done = done;
        return _node.apply(task);
    }

private:

    // TODO(chenzhangyi01): replace _fd_mutex with DBD.
    mutable bthread_mutex_t _fd_mutex;
    SharedFD* _fd;
    bthread::ExecutionQueueId<SharedFD*> _sync_queue;
    boost::atomic<bool> _is_leader;
};

class BlockServiceImpl : public BlockServiceAdaptor {
public:
    ~BlockServiceImpl() {
    }
    int init() {
        if (_block_map.init(FLAGS_block_num * 2) != 0) {
            LOG(ERROR) << "Fail to init _block_map";
            return -1;
        }
        base::EndPoint addr;
        base::str2endpoint(FLAGS_ip_and_port.c_str(), &addr);
        if (base::IP_ANY == addr.ip) {
            addr.ip = base::get_host_ip();
        }
        // init peers
        std::vector<raft::PeerId> peers;
        const char* the_string_to_split = FLAGS_peers.c_str();
        for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
            raft::PeerId peer(std::string(s.field(), s.length()));
            peers.push_back(peer);
        }
        for (int i = 0; i < FLAGS_block_num; ++i) {
            std::string name;
            base::string_printf(&name, "block_%d", i);
            Block* block = new Block(name, raft::PeerId(addr, 0));
            std::string prefix;
            base::string_printf(&prefix, "local://data/block_%d", i);
            raft::NodeOptions node_options;
            node_options.election_timeout_ms = base::fast_rand_in(5000, 20000);
            node_options.fsm = block;
            node_options.initial_conf = raft::Configuration(peers); // bootstrap need
            node_options.snapshot_interval_s = 
                    base::fast_rand_in(FLAGS_snapshot_interval, 
                                       FLAGS_snapshot_interval * 2);
            std::string log_uri;
            if (FLAGS_use_rocksdb) {
                base::string_printf(&log_uri, "rocksdb://data/logs?id=%s", name.c_str());
            } else {
                log_uri = prefix + "/log";
            }
            node_options.log_uri = log_uri;
            node_options.stable_uri = prefix + "/stable";
            node_options.snapshot_uri = prefix + "/snapshot";
            if (block->init(node_options) != 0) {
                LOG(ERROR) << "Fail to init block_" << i;
                return -1;
            }
            _block_map[i] = block;
        }
        return 0;
    }
    void Write(::google::protobuf::RpcController* controller,
               const ::baidu::ebs::WriteRequest* request,
               ::baidu::ebs::AckResponse* response,
               ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        Block** block = _block_map.seek(request->block_id());
        if (block == NULL) {
            response->set_error_code(EINVAL);
            response->set_error_message("No such block");
            return;
        }
        return (*block)->write(controller, request, response, 
                               done_guard.release());
    }
    void Read(::google::protobuf::RpcController* controller,
              const ::baidu::ebs::ReadRequest* request,
              ::baidu::ebs::ReadResponse* response,
              ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        Block** block = _block_map.seek(request->block_id());
        if (block == NULL) {
            response->set_error_code(EINVAL);
            return;
        }
        return (*block)->read(controller, request, response,
                              done_guard.release());
    }
    void GetLeader(::google::protobuf::RpcController* controller,
                   const ::baidu::ebs::GetLeaderRequest* request,
                   ::baidu::ebs::GetLeaderResponse* response,
                   ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        Block** block = _block_map.seek(request->block_id());
        if (block == NULL) {
            controller->SetFailed("No such block");
            return;
        }
        response->set_leader_addr(base::endpoint2str((*block)->leader()).c_str());
    }

    void shutdown() {
    }

    void join() {
    }
    
private:
    base::FlatMap<uint64_t, Block*> _block_map;
};

}  // namespace example

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    options.async = false;
    options.process_name = "block_server";
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());

    // add service
    baidu::rpc::Server server;
    // init raft and server
    if (0 != raft::add_service(&server, FLAGS_ip_and_port.c_str())) {
        LOG(FATAL) << "Fail to init raft";
        return -1;
    }

    example::BlockServiceImpl service;
    if (service.init() != 0) {
        LOG(FATAL) << "Fail to init service";
        return -1;
    }

    if (server.AddService(&service, baidu::rpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "FAil to add block service";
        return -1;
    }

    if (server.Start(FLAGS_ip_and_port.c_str(), NULL) != 0) {
        usleep(10000);
        LOG(FATAL) << "Fail to start server";
        return -1;
    }

    LOG(INFO) << "BlockServer is started at " << FLAGS_ip_and_port;

    server.RunUntilAskedToQuit();
    service.shutdown();
    service.join();

    return 0;
}

