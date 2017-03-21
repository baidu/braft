// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
//         YangWu(yangwu@baidu.com)
// Date: 2017/02/21 13:45:46

#include <boost/atomic.hpp>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <base/comlog_sink.h>
#include <base/file_util.h>
#include <base/files/file_enumerator.h>
#include <bthread.h>
#include <bthread_unstable.h>
#include <baidu/rpc/controller.h>
#include <baidu/rpc/server.h>
#include <raft/util.h>
#include <raft/storage.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "state_machine.h"
#include "cli_service.h"
#include "db.pb.h"

DEFINE_string(ip_and_port, "0.0.0.0:8000", "server listen address");
DEFINE_string(name, "test", "Name of the raft group");
DEFINE_string(peers, "", "cluster peer set");
DEFINE_int32(snapshot_interval, 10, "Interval between each snapshot");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election after no message received from leader in such time");

namespace example {

bool copy_snapshot(const std::string& from_path, const std::string& to_path) {
    struct stat from_stat;
    if (stat(from_path.c_str(), &from_stat) < 0 || !S_ISDIR(from_stat.st_mode)) {
        DLOG(WARNING) << "stat " << from_path << " failed";
        return false;
    }
    if (!base::CreateDirectory(base::FilePath(to_path))) {
        DLOG(WARNING) << "CreateDirectory " << to_path << " failed";
        return false;
    }

    base::FileEnumerator dir_enum(base::FilePath(from_path),
                                  false, base::FileEnumerator::FILES);
    for (base::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
        std::string src_file(from_path);
        std::string dst_file(to_path);
        base::string_appendf(&src_file, "/%s", name.BaseName().value().c_str());
        base::string_appendf(&dst_file, "/%s", name.BaseName().value().c_str());

        if (0 != link(src_file.c_str(), dst_file.c_str())) {
            if (!base::CopyFile(base::FilePath(src_file), base::FilePath(dst_file))) {
                DLOG(WARNING) << "copy " << src_file << " to " << dst_file << " failed";
                return false;
            }
        }
    }

    return true;
}

struct DbClosure : public raft::Closure {
    DbClosure(baidu::rpc::Controller* cntl_, PutResponse* response_,
              google::protobuf::Closure* done_)
        : cntl(cntl_), response(response_), done(done_) {}
    void Run() {
        if (!status().ok()) {
            cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
        }
        done->Run();
        delete this;
    }

    baidu::rpc::Controller* cntl;
    PutResponse* response;
    google::protobuf::Closure* done;
};

class DbStateMachine : public CommonStateMachine {
public:
    DbStateMachine(const raft::GroupId& group_id, const raft::PeerId& peer_id) 
        : CommonStateMachine(group_id, peer_id), _is_leader(false), _db(NULL) {
    }

    virtual ~DbStateMachine() {}

    int get(const std::string& key,
            GetResponse* response,
            ::google::protobuf::RpcController* controller) {
        baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
        if (!_is_leader.load(boost::memory_order_acquire)) {
            LOG(NOTICE) << "this is not leader, get request, key:" << key;
            cntl->SetFailed(baidu::rpc::SYS_EPERM, "not leader");
            return -1;
        }

        CHECK(_db != NULL);

        std::string value;
        rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) {
            LOG(NOTICE) << "this is leader, get request, key:" << key << " value:" << value; 
            response->set_value(value);
        } else {
            LOG(WARNING) << "get failed, key:" << key;
            cntl->SetFailed(baidu::rpc::SYS_EINVAL, status.ToString().c_str());
        }

        return 0;
    }

    // FSM method
    void on_apply(raft::Iterator& iter) {
        LOG(INFO) << "on_apply";

        for (; iter.valid(); iter.next()) {
            raft::Closure* done = iter.done();
            baidu::rpc::ClosureGuard done_guard(done);
            const base::IOBuf& data = iter.data();

            fsm_put(static_cast<DbClosure*>(done), data);

            if (done) {
                raft::run_closure_in_bthread_nosig(done_guard.release());
            }
        }
        bthread_flush();
    }

    void on_shutdown() {
        LOG(NOTICE) << "on_shutdown";
        // FIXME: should be more elegant
        exit(0);
    }

    void on_error(const raft::Error& e) {
        LOG(NOTICE) << "on_error " << e;
        // FIXME: should be more elegant
        exit(0);
    }

    void on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
        CHECK(_db != NULL);

        LOG(NOTICE) << "on_snapshot_save";
        baidu::rpc::ClosureGuard done_guard(done);

        std::string snapshot_path = writer->get_path() + "/rocksdb_snapshot";

        // 使用rocksdb的checkpoint特性实现snapshot
        rocksdb::Checkpoint* checkpoint = NULL;
        rocksdb::Status status = rocksdb::Checkpoint::Create(_db, &checkpoint);
        if (!status.ok()) {
            LOG(WARNING) << "Checkpoint Create failed, msg:" << status.ToString();
            return;
        }

        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
        status = checkpoint->CreateCheckpoint(snapshot_path);
        if (!status.ok()) {
            LOG(WARNING) << "Checkpoint CreateCheckpoint failed, msg:" << status.ToString();
            return;
        }

        // list出所有的文件 加到snapshot中
        base::FileEnumerator dir_enum(base::FilePath(snapshot_path),
                                      false, base::FileEnumerator::FILES);
        for (base::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
            std::string file_name = "rocksdb_snapshot/" + name.BaseName().value();
            writer->add_file(file_name);
        }
    }

    int on_snapshot_load(raft::SnapshotReader* reader) {
        LOG(NOTICE) << "on_snapshot_load";

        if (_db != NULL) {
            delete _db;
            _db = NULL;
        }

        std::string snapshot_path = reader->get_path();
        snapshot_path.append("/rocksdb_snapshot");

        // 先删除原来的db 使用snapshot和wal还原一个最新的db
        std::string db_path = "./data/rocksdb_data";
        if (!base::DeleteFile(base::FilePath(db_path), true)) {
            LOG(WARNING) << "rm " << db_path << " failed";
            return -1;
        }

        LOG(TRACE) << "rm " << db_path << " success";
        //TODO: try use link instead of copy
        if (!copy_snapshot(snapshot_path, db_path)) {
            LOG(WARNING) << "copy snapshot " << snapshot_path << " to " << db_path << " failed";
            return -1;
        }

        LOG(TRACE) << "copy snapshot " << snapshot_path << " to " << db_path << " success";
        // 重新打开db
        return init_rocksdb();
    }

    // Acutally we don't care now
    void on_leader_start(int64_t term) {
        LOG(NOTICE) << "leader start at term: " << term;
        _is_leader.store(true, boost::memory_order_release);
    }

    void on_leader_stop() {
        LOG(NOTICE) << "leader stop";
        _is_leader.store(false, boost::memory_order_release);
    }

    void apply(base::IOBuf *iobuf, raft::Closure* done) {
        raft::Task task;
        task.data = iobuf;
        task.done = done;
        return _node.apply(task);
    }

    int init_rocksdb() {
        if (_db != NULL) {
            LOG(NOTICE) << "rocksdb already opened";
            return 0;
        }

        std::string db_path = "./data/rocksdb_data";
        if (!base::CreateDirectory(base::FilePath(db_path))) {
            DLOG(WARNING) << "CreateDirectory " << db_path << " failed";
            return -1;
        }

        rocksdb::Options options;
        options.create_if_missing = false;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &_db);
        if (!status.ok()) {
            LOG(WARNING) << "open rocksdb " << db_path << " failed, msg: " << status.ToString();
            return -1;
        }

        LOG(NOTICE) << "rocksdb open success!";
        return 0;
    }

private:
    void fsm_put(DbClosure* done, const base::IOBuf& data) {
        base::IOBufAsZeroCopyInputStream wrapper(data);
        PutRequest req;

        if (!req.ParseFromZeroCopyStream(&wrapper)) {
            if (done) {
                done->status().set_error(baidu::rpc::EREQUEST,
                                "Fail to parse buffer");
            }
            LOG(WARNING) << "Fail to parse PutRequest";
            return;
        }

        LOG(INFO) << "get put request:" << req.ShortDebugString();

        // WriteOptions not need WAL, use raft's WAL
        rocksdb::WriteOptions options;
        options.disableWAL = true;
        rocksdb::Status status = _db->Put(options, req.key(), req.value());
        if (!status.ok()) {
            if (done) {
                done->status().set_error(baidu::rpc::SYS_EINVAL, status.ToString());
            }
            LOG(WARNING) << "Put failed, key:" << req.key() << " value:" << req.value();
        } else {
            LOG(INFO) << "Put success, key:" << req.key() << " value:" << req.value();
        }
    }

private:
    boost::atomic<bool> _is_leader;
    rocksdb::DB* _db;
};

class DbServiceImpl : public DbService {
public:
    explicit DbServiceImpl(DbStateMachine* state_machine) : _state_machine(state_machine) {}

    // rpc method
    virtual void get(::google::protobuf::RpcController* controller,
                       const GetRequest* request,
                       GetResponse* response,
                       ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        _state_machine->get(request->key(), response, controller);
    }

    virtual void put(::google::protobuf::RpcController* controller,
                       const PutRequest* request,
                       PutResponse* response,
                       ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;

        base::IOBuf data;
        base::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(baidu::rpc::EREQUEST, "Fail to serialize request");
            return;
        }

        DbClosure* c = new DbClosure(cntl, response, done_guard.release());
        return _state_machine->apply(&data, c);
    }

private:
    DbStateMachine* _state_machine;
};

}  // namespace example

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    //options.async = true;
    options.process_name = "db_server";
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());

    // add service
    baidu::rpc::Server server;
    if (raft::add_service(&server, FLAGS_ip_and_port.c_str()) != 0) {
        LOG(FATAL) << "Fail to init raft";
        return -1;
    }

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }

    base::EndPoint addr;
    base::str2endpoint(FLAGS_ip_and_port.c_str(), &addr);
    if (base::IP_ANY == addr.ip) {
        addr.ip = base::get_host_ip();
    }

    example::DbStateMachine* state_machine =
        new example::DbStateMachine(FLAGS_name, raft::PeerId(addr, 0));
    raft::NodeOptions node_options;
    node_options.election_timeout_ms = FLAGS_election_timeout_ms;
    node_options.fsm = state_machine;
    node_options.initial_conf = raft::Configuration(peers); // bootstrap need
    node_options.snapshot_interval_s = FLAGS_snapshot_interval;
    node_options.log_uri = "local://./data/log";
    node_options.stable_uri = "local://./data/stable";
    node_options.snapshot_uri = "local://./data/snapshot";

    //删除rocksdb从空DB启动
    std::string db_path = "./data/rocksdb_data";
    if (!base::DeleteFile(base::FilePath(db_path), true)) {
        LOG(WARNING) << "rm " << db_path << " failed";
        return -1;
    }
    // 打开rocksdb
    // init_rocksdb MUST before Node::init, maybe single node become leader
    // and restore log but db not inited
    if (state_machine->init_rocksdb() != 0) {
        LOG(WARNING) << "init_rocksdb failed";
        return -1;
    }

    // 初始化state_machine
    // init will call on_snapshot_load if has snapshot,
    // rocksdb restore from raft's snapshot, then restore log
    if (0 != state_machine->init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(INFO) << "init Node success";

    example::DbServiceImpl service(state_machine);
    if (0 != server.AddService(&service, 
                baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }
    example::CliServiceImpl cli_service_impl(state_machine);
    if (0 != server.AddService(&cli_service_impl, 
                baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }

    if (server.Start(FLAGS_ip_and_port.c_str(), NULL) != 0) {
        LOG(FATAL) << "Fail to start server";
        return -1;
    }
    LOG(INFO) << "Wait until server stopped";
    server.RunUntilAskedToQuit();
    LOG(INFO) << "DbServer is going to quit";

    return 0;
}
