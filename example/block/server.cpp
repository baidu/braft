// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

#include <sys/types.h>                  // O_CREAT
#include <fcntl.h>                      // open
#include <gflags/gflags.h>              // DEFINE_*
#include <butil/sys_byteorder.h>        // butil::NetToHost32
#include <brpc/controller.h>            // brpc::Controller
#include <brpc/server.h>                // brpc::Server
#include <braft/raft.h>                 // braft::Node braft::StateMachine
#include <braft/storage.h>              // braft::SnapshotWriter
#include <braft/util.h>                 // braft::AsyncClosureGuard
#include "block.pb.h"                   // BlockService

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8200, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "Block", "Id of the replication group");

namespace example {
class Block;

// Implements Closure which encloses RPC stuff
class BlockClosure : public braft::Closure {
public:
    BlockClosure(Block* block, 
                 const BlockRequest* request,
                 BlockResponse* response,
                 butil::IOBuf* data,
                 google::protobuf::Closure* done)
        : _block(block)
        , _request(request)
        , _response(response)
        , _data(data)   
        , _done(done) {}
    ~BlockClosure() {}

    const BlockRequest* request() const { return _request; }
    BlockResponse* response() const { return _response; }
    void Run();
    butil::IOBuf* data() const { return _data; }

private:
    // Disable explicitly delete
    Block* _block;
    const BlockRequest* _request;
    BlockResponse* _response;
    butil::IOBuf* _data;
    google::protobuf::Closure* _done;
};

// Implementation of example::Block as a braft::StateMachine.
class Block : public braft::StateMachine {
public:
    Block()
        : _node(NULL)
        , _leader_term(-1)
        , _fd(NULL)
    {}
    ~Block() {
        delete _node;
    }

    // Starts this node
    int start() {
        if (!butil::CreateDirectory(butil::FilePath(FLAGS_data_path))) {
            LOG(ERROR) << "Fail to create directory " << FLAGS_data_path;
            return -1;
        }
        std::string data_path = FLAGS_data_path + "/data";
        int fd = ::open(data_path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            PLOG(ERROR) << "Fail to open " << data_path;
            return -1;
        }
        _fd = new SharedFD(fd);
        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        std::string prefix = "local://" + FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    // Impelements Service methods
    void write(const BlockRequest* request,
               BlockResponse* response,
               butil::IOBuf* data,
               google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // Serialize request to the replicated write-ahead-log so that all the
        // peers in the group receive this request as well.
        // Notice that _value can't be modified in this routine otherwise it
        // will be inconsistent with others in this group.
        
        // Serialize request to IOBuf
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            return redirect(response);
        }
        butil::IOBuf log;
        const uint32_t meta_size_raw = butil::HostToNet32(request->ByteSize());
        log.append(&meta_size_raw, sizeof(uint32_t));
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }
        log.append(*data);
        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new BlockClosure(this, request, response,
                                     data, done_guard.release());
        if (FLAGS_check_term) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }

    void read(const BlockRequest *request, BlockResponse* response,
              butil::IOBuf* buf) {
        // In consideration of consistency. GetRequest to follower should be 
        // rejected.
        if (!is_leader()) {
            // This node is a follower or it's not up-to-date. Redirect to
            // the leader if possible.
            return redirect(response);
        }

        if (request->offset() < 0) {
            response->set_success(false);
            return;
        }

        // This is the leader and is up-to-date. It's safe to respond client
        scoped_fd fd = get_fd();
        butil::IOPortal portal;
        const ssize_t nr = braft::file_pread(
                &portal, fd->fd(), request->offset(), request->size());
        if (nr < 0) {
            // Some disk error occurred, shutdown this node and another leader
            // will be elected
            PLOG(ERROR) << "Fail to read from fd=" << fd->fd();
            _node->shutdown(NULL);
            response->set_success(false);
            return;
        }
        buf->swap(portal);
        if (buf->length() < (size_t)request->size()) {
            buf->resize(request->size());
        }
        response->set_success(true);
    }

    bool is_leader() const 
    { return _leader_term.load(butil::memory_order_acquire) > 0; }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

private:
    class SharedFD : public butil::RefCountedThreadSafe<SharedFD> {
    public:
        explicit SharedFD(int fd) : _fd(fd) {}
        int fd() const { return _fd; }
    private:
    friend class butil::RefCountedThreadSafe<SharedFD>;
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
        return _fd;
    }
friend class BlockClosure;

    void redirect(BlockResponse* response) {
        response->set_success(false);
        if (_node) {
            braft::PeerId leader = _node->leader_id();
            if (!leader.is_empty()) {
                response->set_redirect(leader.to_string());
            }
        }
    }

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter) {
        // A batch of tasks are committed, which must be processed through 
        // |iter|
        for (; iter.valid(); iter.next()) {
            BlockResponse* response = NULL;
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine
            braft::AsyncClosureGuard closure_guard(iter.done());
            butil::IOBuf data;
            off_t offset = 0;
            if (iter.done()) {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                BlockClosure* c = dynamic_cast<BlockClosure*>(iter.done());
                offset = c->request()->offset();
                data.swap(*(c->data()));
                response = c->response();
            } else {
                // Have to parse BlockRequest from this log.
                uint32_t meta_size = 0;
                butil::IOBuf saved_log = iter.data();
                saved_log.cutn(&meta_size, sizeof(uint32_t));
                // Remember that meta_size is in network order which hould be
                // covert to host order
                meta_size = butil::NetToHost32(meta_size);
                butil::IOBuf meta;
                saved_log.cutn(&meta, meta_size);
                butil::IOBufAsZeroCopyInputStream wrapper(meta);
                BlockRequest request;
                CHECK(request.ParseFromZeroCopyStream(&wrapper));
                data.swap(saved_log);
                offset = request.offset();
            }

            const ssize_t nw = braft::file_pwrite(data, _fd->fd(), offset);
            if (nw < 0) {
                PLOG(ERROR) << "Fail to write to fd=" << _fd->fd();
                if (response) {
                    response->set_success(false);
                }
                // Let raft run this closure.
                closure_guard.release();
                // Some disk error occurred, notify raft and never apply any data
                // ever after
                iter.set_error_and_rollback();
                return;
            }

            if (response) {
                response->set_success(true);
            }

            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "Write " << data.size() << " bytes"
                    << " from offset=" << offset
                    << " at log_index=" << iter.index();
        }
    }

    struct SnapshotArg {
        scoped_fd fd;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static int link_overwrite(const char* old_path, const char* new_path) {
        if (::unlink(new_path) < 0 && errno != ENOENT) {
            PLOG(ERROR) << "Fail to unlink " << new_path;
            return -1;
        }
        return ::link(old_path, new_path);
    }

    static void *save_snapshot(void* arg) {
        SnapshotArg* sa = (SnapshotArg*) arg;
        std::unique_ptr<SnapshotArg> arg_guard(sa);
        // Serialize StateMachine to the snapshot
        brpc::ClosureGuard done_guard(sa->done);
        std::string snapshot_path = sa->writer->get_path() + "/data";
        // Sync buffered data before
        int rc = 0;
        LOG(INFO) << "Saving snapshot to " << snapshot_path;
        for (; (rc = ::fdatasync(sa->fd->fd())) < 0 && errno == EINTR;) {}
        if (rc < 0) {
            sa->done->status().set_error(EIO, "Fail to sync fd=%d : %m",
                                         sa->fd->fd());
            return NULL;
        }
        std::string data_path = FLAGS_data_path + "/data";
        if (link_overwrite(data_path.c_str(), snapshot_path.c_str()) != 0) {
            sa->done->status().set_error(EIO, "Fail to link data : %m");
            return NULL;
        }
        
        // Snapshot is a set of files in raft. Add the only file into the
        // writer here.
        if (sa->writer->add_file("data") != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer");
            return NULL;
        }
        return NULL;
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotArg* arg = new SnapshotArg;
        arg->fd = _fd;
        arg->writer = writer;
        arg->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        // Load snasphot from reader, replacing the running StateMachine
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        if (reader->get_file_meta("data", NULL) != 0) {
            LOG(ERROR) << "Fail to find `data' on " << reader->get_path();
            return -1;
        }
        // reset fd
        _fd = NULL;
        std::string snapshot_path = reader->get_path() + "/data";
        std::string data_path = FLAGS_data_path + "/data";
        if (link_overwrite(snapshot_path.c_str(), data_path.c_str()) != 0) {
            PLOG(ERROR) << "Fail to link data";
            return -1;
        }
        // Reopen this file
        int fd = ::open(data_path.c_str(), O_RDWR, 0644);
        if (fd < 0) {
            PLOG(ERROR) << "Fail to open " << data_path;
            return -1;
        }
        _fd = new SharedFD(fd);
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }
    // end of @braft::StateMachine

private:
    mutable butil::Mutex _fd_mutex;
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    scoped_fd _fd;
};

void BlockClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<BlockClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    // Try redirect if this request failed.
    _block->redirect(_response);
}

// Implements example::BlockService if you are using brpc.
class BlockServiceImpl : public BlockService {
public:
    explicit BlockServiceImpl(Block* block) : _block(block) {}
    void write(::google::protobuf::RpcController* controller,
               const ::example::BlockRequest* request,
               ::example::BlockResponse* response,
               ::google::protobuf::Closure* done) {
        brpc::Controller* cntl = (brpc::Controller*)controller;
        return _block->write(request, response,
                             &cntl->request_attachment(), done);
    }
    void read(::google::protobuf::RpcController* controller,
              const ::example::BlockRequest* request,
              ::example::BlockResponse* response,
              ::google::protobuf::Closure* done) {
        brpc::Controller* cntl = (brpc::Controller*)controller;
        brpc::ClosureGuard done_guard(done);
        return _block->read(request, response, &cntl->response_attachment());
    }
private:
    Block* _block;
};

}  // namespace example

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    example::Block block;
    example::BlockServiceImpl service(&block);

    // Add your service into RPC rerver
    if (server.AddService(&service, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Block is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice that default options of server are used here. Check out details 
    // from the doc of brpc if you would like change some options
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start Block
    if (block.start() != 0) {
        LOG(ERROR) << "Fail to start Block";
        return -1;
    }

    LOG(INFO) << "Block service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }
    LOG(INFO) << "Block service is going to quit";

    // Stop block before server
    block.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    block.join();
    server.Join();
    return 0;
}
