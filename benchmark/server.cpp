// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/08/03 14:04:11

#include <bthread_unstable.h>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <base/comlog_sink.h>
#include <bthread.h>
#include <baidu/rpc/controller.h>
#include <baidu/rpc/server.h>
#include <raft/raft.h>
#include <raft/util.h>
#include "log.pb.h"

DEFINE_int32(map_capacity, 1024, "Initial capicity of value map");
DEFINE_string(ip_and_port, "0.0.0.0:8000", "server listen address");
DEFINE_string(name, "test", "Name of the raft group");
DEFINE_string(peers, "", "cluster peer set");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election after no message received from leader in such time");

namespace benchmark {

struct LogClosure : public raft::Closure {
    void Run() {
        if (!status().ok()) {
            cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
        }
        done->Run();
        delete this;
    }
    baidu::rpc::Controller* cntl;
    google::protobuf::Message* response;
    google::protobuf::Closure* done;
};

class Log : public raft::StateMachine {
public:
    Log(const raft::GroupId& group_id, const raft::PeerId& peer_id) 
        : _node(group_id, peer_id)
    {}

    virtual ~Log() {}

    int init(const raft::NodeOptions& node_options) {
        return _node.init(node_options);
    }

    // FSM method
    void on_apply(raft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            if (iter.done()) {
                raft::run_closure_in_bthread_nosig(iter.done());
            }
        }
        bthread_flush();
    }

    void shutdown() { return _node.shutdown(NULL); }

    void on_shutdown() { }
    void join() { return _node.join(); }

    void on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
        done->Run();
    }

    int on_snapshot_load(raft::SnapshotReader* /*reader*/) {
        return 0;
    }

    void apply(base::IOBuf *iobuf, raft::Closure* done) {
        raft::Task task;
        task.data = iobuf;
        task.done = done;
        return _node.apply(task);
    }

    base::EndPoint leader() {
        return _node.leader_id().addr;
    }

private:
    raft::Node _node;
};

class LogServiceImpl : public LogService {
public:

    explicit LogServiceImpl(Log* log) : _log(log) {}

    void write(::google::protobuf::RpcController* controller,
                       const ::benchmark::WriteRequest* request,
                       ::benchmark::WriteResponse* response,
                       ::google::protobuf::Closure* done) {

        baidu::rpc::ClosureGuard done_guard(done);
        baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;

        base::IOBuf data;
        data.swap(cntl->request_attachment());
        LogClosure* c = new LogClosure;
        c->cntl = cntl;
        c->response = response;
        c->done = done_guard.release();
        return _log->apply(&data, c);
    }
    void leader(::google::protobuf::RpcController* controller,
                const ::benchmark::GetLeaderRequest* /*request*/,
                ::benchmark::GetLeaderResponse* response,
                ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        base::EndPoint leader_addr = _log->leader();
        if (leader_addr != base::EndPoint()) {
            response->set_success(true);
            response->set_leader_addr(base::endpoint2str(leader_addr).c_str());
        } else {
            response->set_success(false);
        }
    }

private:
    Log* _log;
};

}  // namespace benchmark

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    //options.async = true;
    options.process_name = "log_server";
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
    // init counter
    benchmark::Log log(FLAGS_name, raft::PeerId(addr, 0));
    raft::NodeOptions node_options;
    node_options.election_timeout_ms = FLAGS_election_timeout_ms;
    node_options.fsm = &log;
    node_options.initial_conf = raft::Configuration(peers); // bootstrap need
    node_options.snapshot_interval_s = FLAGS_snapshot_interval;
    node_options.log_uri = "local://./data/log";
    node_options.stable_uri = "local://./data/stable";
    node_options.snapshot_uri = "local://./data/snapshot";

    if (0 != log.init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(INFO) << "init Node success";

    benchmark::LogServiceImpl service(&log);
    if (0 != server.AddService(&service, 
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
    LOG(INFO) << "AtomicServer is going to quit";
    log.shutdown();
    log.join();

    return 0;
}
