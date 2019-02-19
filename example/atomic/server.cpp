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

#include <fstream>
#include <bthread/bthread.h>
#include <gflags/gflags.h>
#include <butil/containers/flat_map.h>
#include <butil/logging.h>
#include <bthread/bthread.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>

#include "atomic.pb.h"

DEFINE_bool(allow_absent_key, false, "Cas succeeds if the key is absent while "
                                     " exptected value is exactly 0");
DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(map_capacity, 1024, "Initial capicity of value map");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "Atomic", "Id of the replication group");

namespace example {

class Atomic;
// Implements Closure which encloses RPC stuff
class AtomicClosure : public braft::Closure {
public:
    AtomicClosure(Atomic* atomic,
                  const google::protobuf::Message* request,
                  AtomicResponse* response,
                  google::protobuf::Closure* done)
        : _atomic(atomic)
        , _request(request)
        , _response(response)
        , _done(done)
    {}

    void Run();
    const google::protobuf::Message* request() const { return _request; }
    AtomicResponse* response() const { return _response; }

private:
    Atomic* _atomic;
    const google::protobuf::Message* _request;
    AtomicResponse* _response;
    google::protobuf::Closure* _done;
};

// Implementation of example::Atomic as a braft::StateMachine.
class Atomic : public braft::StateMachine {
public:
    // Define types for different operation
    enum AtomicOpType {
        OP_UNKNOWN = 0,
        OP_GET = 1,
        OP_EXCHANGE = 2,
        OP_CAS = 3,
    };

    Atomic()
        : _node(NULL)
        , _leader_term(-1)
    {
        CHECK_EQ(0, _value_map.init(FLAGS_map_capacity));
    }

    ~Atomic() {
        delete _node;
    }

    // Starts this node
    int start() {
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

    // Impelements service methods
    void get(const ::example::GetRequest* request,
             ::example::AtomicResponse* response,
             ::google::protobuf::Closure* done) {
        return apply(OP_GET, request, response, done);
    }

    void exchange(const ::example::ExchangeRequest* request,
                  ::example::AtomicResponse* response,
                  ::google::protobuf::Closure* done) {
        return apply(OP_EXCHANGE, request, response, done);
    }

    void compare_exchange(const ::example::CompareExchangeRequest* request,
                          ::example::AtomicResponse* response,
                          ::google::protobuf::Closure* done) {
        return apply(OP_CAS, request, response, done);
    }

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
friend class AtomicClosure;

    void apply(AtomicOpType type, const google::protobuf::Message* request,
               AtomicResponse* response, google::protobuf::Closure* done) {
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
        log.push_back((uint8_t)type);
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }
        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new AtomicClosure(this, request, response,
                                      done_guard.release());
        if (FLAGS_check_term) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }

    void redirect(AtomicResponse* response) {
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
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine.
            braft::AsyncClosureGuard done_guard(iter.done());

            // Parse data
            butil::IOBuf data = iter.data();
            // Fetch the type of operation from the leading byte.
            uint8_t type = OP_UNKNOWN;
            data.cutn(&type, sizeof(uint8_t));

            AtomicClosure* c = NULL;
            if (iter.done()) {
                c = dynamic_cast<AtomicClosure*>(iter.done());
            }

            const google::protobuf::Message* request = c ? c->request() : NULL;
            AtomicResponse r;
            AtomicResponse* response = c ? c->response() : &r;
            const char* op = NULL;
            // Execute the operation according to type
            switch (type) {
            case OP_GET:
                op = "get";
                get_value(data, request, response);
                break;
            case OP_EXCHANGE:
                op = "exchange";
                exchange(data, request, response);
                break;
            case OP_CAS:
                op = "cas";
                cas(data, request, response);
                break;
            default:
                CHECK(false) << "Unknown type=" << static_cast<int>(type);
                break;
            }

            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "Handled operation " << op 
                    << " on id=" << response->id()
                    << " at log_index=" << iter.index()
                    << " success=" << response->success()
                    << " old_value=" << response->old_value()
                    << " new_value=" << response->new_value();
        }
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {

        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotClosure* sc = new SnapshotClosure;
        sc->values.reserve(_value_map.size());
        sc->writer = writer;
        sc->done = done;
        for (ValueMap::const_iterator 
                it = _value_map.begin(); it != _value_map.end(); ++it) {
            sc->values.push_back(std::make_pair(it->first, it->second));
        }

        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, sc);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        CHECK_EQ(-1, _leader_term) << "Leader is not supposed to load snapshot";
        _value_map.clear();
        std::string snapshot_path = reader->get_path();
        snapshot_path.append("/data");
        std::ifstream is(snapshot_path.c_str());
        int64_t id = 0;
        int64_t value = 0;
        while (is >> id >> value) {
            _value_map[id] = value;
        }
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
    
    void get_value(const butil::IOBuf& data,
                   const google::protobuf::Message* request,
                   AtomicResponse* response) {
        int64_t id = 0;
        if (request) {
            // This task is applied by this node, get value from this
            // closure to avoid additional parsing.
            id = dynamic_cast<const GetRequest*>(request)->id();
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            GetRequest req;
            CHECK(req.ParseFromZeroCopyStream(&wrapper));
            id = req.id();
        }
        int64_t* const v = _value_map.seek(id);
        response->set_success(true);
        response->set_id(id);
        response->set_old_value(v ? *v : 0);
        response->set_new_value(v ? *v : 0);
    }

    void exchange(const butil::IOBuf& data,
                  const google::protobuf::Message* request,
                  AtomicResponse* response) {
        int64_t id = 0;
        int64_t value = 0;
        if (request) {
            // This task is applied by this node, get value from this
            // closure to avoid additional parsing.
            const ExchangeRequest* req
                    = dynamic_cast<const ExchangeRequest*>(request);
            id = req->id();
            value = req->value();
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            ExchangeRequest req;
            CHECK(req.ParseFromZeroCopyStream(&wrapper));
            id = req.id();
            value = req.value();
        }
        int64_t& old_value = _value_map[id];
        response->set_success(true);
        response->set_id(id);
        response->set_old_value(old_value);
        response->set_new_value(value);
        old_value = value;
    }

    void cas(const butil::IOBuf& data,
                  const google::protobuf::Message* request,
                  AtomicResponse* response) {
        int64_t id = 0;
        int64_t value = 0;
        int64_t expected = 0;
        if (request) {
            // This task is applied by this node, get value from this
            // closure to avoid additional parsing.
            const CompareExchangeRequest* req =
                    dynamic_cast<const CompareExchangeRequest*>(request);
            id = req->id();
            value = req->new_value();
            expected = req->expected_value();
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            CompareExchangeRequest req;
            CHECK(req.ParseFromZeroCopyStream(&wrapper));
            id = req.id();
            value = req.new_value();
            expected = req.expected_value();
        }
        int64_t& old_value = _value_map[id];
        response->set_old_value(old_value);
        response->set_id(id);
        if (old_value != expected) {
            response->set_success(false);
            response->set_new_value(old_value);
        } else {
            response->set_success(true);
            response->set_new_value(value);
            old_value = value;
        }
    }

    static void* save_snapshot(void* arg) {
        SnapshotClosure* sc = (SnapshotClosure*)arg;
        std::unique_ptr<SnapshotClosure> sc_guard(sc);
        brpc::ClosureGuard done_guard(sc->done);
        std::string snapshot_path = sc->writer->get_path();
        snapshot_path.append("/data");
        std::ofstream os(snapshot_path.c_str());
        for (size_t i = 0; i < sc->values.size(); ++i) {
            os << sc->values[i].first << ' ' << sc->values[i].second << '\n';
        }
        CHECK_EQ(0, sc->writer->add_file("data"));
        return NULL;
    }

    typedef butil::FlatMap<int64_t, int64_t> ValueMap;

    struct SnapshotClosure {
        std::vector<std::pair<int64_t, int64_t> > values;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    ValueMap _value_map;
};

void AtomicClosure::Run() {
    std::unique_ptr<AtomicClosure> self_guard(this);
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    // Try redirect if this request failed.
    _atomic->redirect(_response);
}

// Implements example::AtomicService if you are using brpc.
class AtomicServiceImpl : public AtomicService {
public:

    explicit AtomicServiceImpl(Atomic* atomic) : _atomic(atomic) {}
    ~AtomicServiceImpl() {}

    void get(::google::protobuf::RpcController* controller,
             const ::example::GetRequest* request,
             ::example::AtomicResponse* response,
             ::google::protobuf::Closure* done) {
        return _atomic->get(request, response, done);
    }
    void exchange(::google::protobuf::RpcController* controller,
                  const ::example::ExchangeRequest* request,
                  ::example::AtomicResponse* response,
                  ::google::protobuf::Closure* done) {
        return _atomic->exchange(request, response, done);
    }
    void compare_exchange(::google::protobuf::RpcController* controller,
                          const ::example::CompareExchangeRequest* request,
                          ::example::AtomicResponse* response,
                          ::google::protobuf::Closure* done) {
        return _atomic->compare_exchange(request, response, done);
    }

private:
    Atomic* _atomic;
};

}  // namespace example

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    example::Atomic atomic;
    example::AtomicServiceImpl service(&atomic);

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

    // It's recommended to start the server before Atomic is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start Atomic;
    if (atomic.start() != 0) {
        LOG(ERROR) << "Fail to start Atomic";
        return -1;
    }

    LOG(INFO) << "Atomic service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "Atomic service is going to quit";

    // Stop counter before server
    atomic.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    atomic.join();
    server.Join();
    return 0;
}
