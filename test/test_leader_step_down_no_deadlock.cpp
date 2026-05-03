// Reproduces a push_rq deadlock during step_down when many closures are
// pending in ClosureQueue. Submit rate >> FSM rate makes closures pile up;
// an AppendEntries with a higher term then triggers step_down, which calls
// push_rq while holding NodeImpl._mutex. Workers blocked on _mutex cannot
// drain _rq, so push_rq spins forever and the RPC never returns.

#include <gtest/gtest.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unistd.h>

#include <braft/raft.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <butil/endpoint.h>
#include <butil/iobuf.h>
#include "braft/raft.pb.h"

// ── Shared base ───────────────────────────────────────────────────────────────

class StepDownTestBase : public testing::Test {
protected:
  brpc::Server server_;
  std::shared_ptr<braft::Node> node_;
  std::string self_addr_;
  std::string group_id_;
  int concurrency_ = 0;
  int kSubmitters_ = 0;

  void setup_server(int port) {
    self_addr_ = "127.0.0.1:" + std::to_string(port);
    ASSERT_EQ(0, braft::add_service(&server_, port));
    ASSERT_EQ(0, server_.Start(port, nullptr));
    LOG(INFO) << "Server listening at " << self_addr_;
  }

  void setup_node(int port, const std::string &data_dir,
                 const std::string &group_id, braft::StateMachine *fsm) {
    group_id_ = group_id;
    setup_server(port);

    butil::EndPoint addr;
    ASSERT_EQ(0, butil::str2endpoint(self_addr_.c_str(), &addr));

    braft::NodeOptions opts;
    opts.election_timeout_ms = 1000;
    opts.fsm = fsm;
    opts.node_owns_fsm = true;
    opts.snapshot_interval_s = -1;
    ASSERT_EQ(0, opts.initial_conf.parse_from(self_addr_ + ":0"));
    opts.log_uri = "local://" + data_dir + "/log";
    opts.raft_meta_uri = "local://" + data_dir + "/raft_meta";
    opts.snapshot_uri = "local://" + data_dir + "/snapshot";

    node_ = std::make_shared<braft::Node>(group_id, braft::PeerId(addr));
    ASSERT_EQ(0, node_->init(opts));
    ASSERT_TRUE(node_->is_leader()) << "did not become leader";

    bthread_setconcurrency(8);
    concurrency_ = bthread_getconcurrency();
    kSubmitters_ = 2 * concurrency_;
    LOG(INFO) << "bthread concurrency=" << concurrency_
              << " submitters=" << kSubmitters_;
  }

  // Fires step_down via a fake AppendEntries and waits up to 10s.
  // Returns true if the RPC completed (no deadlock).
  // On timeout the rpc_thread is detached; caller must not attempt cleanup.
  bool fire_step_down() {
    brpc::Channel chan;
    brpc::ChannelOptions copts;
    copts.protocol = "baidu_std";
    copts.timeout_ms = 30000;
    copts.connect_timeout_ms = 5000;
    if (chan.Init(self_addr_.c_str(), &copts) != 0) {
      return false;
    }

    braft::AppendEntriesRequest req;
    req.set_group_id(group_id_);
    req.set_server_id("127.0.0.1:0:0:0");
    req.set_peer_id(self_addr_ + ":0:0");
    req.set_term(99999);
    req.set_prev_log_term(99998);
    req.set_prev_log_index(0);
    req.set_committed_index(0);

    braft::AppendEntriesResponse resp;
    brpc::Controller cntl;
    braft::RaftService_Stub stub(&chan);

    std::atomic<bool> rpc_done{false};
    std::thread rpc_thread([&]() {
      stub.append_entries(&cntl, &req, &resp, nullptr);
      rpc_done = true;
    });

    for (int i = 0; i < 100 && !rpc_done; ++i) {
      ::usleep(100000);
    } // wait 10s

    if (!rpc_done) {
      rpc_thread.detach();
      return false;
    }
    rpc_thread.join();
    return true;
  }

  void teardown_node() {
    node_->shutdown(nullptr);
    node_->join();
    server_.Stop(0);
    server_.Join();
  }
};

// Blocking FSM: on_apply() parks on a condvar until release() is called.
// While parked, closures pile up in ballot_box's ClosureQueue.
class SlowFSM : public braft::StateMachine {
public:
    void on_apply(braft::Iterator& iter) override {
        {
            std::unique_lock<std::mutex> lk(_m);
            _cv.wait(lk, [this]{ return _released.load(); });
        }
        for (; iter.valid(); iter.next()) {}
    }
    void on_leader_start(int64_t _term) override  {}
    void on_leader_stop(const butil::Status& _status) override {}
    void on_shutdown() override {}

    void release() {
        { std::lock_guard<std::mutex> lk(_m); _released = true; }
        _cv.notify_all();
    }
private:
    std::mutex              _m;
    std::condition_variable _cv;
    std::atomic<bool>       _released{false};
};

class NopClosure : public braft::Closure {
public:
    NopClosure(std::shared_ptr<std::atomic<int>> pending, std::shared_ptr<braft::Node> node)
        : _pending(std::move(pending)), _node(std::move(node)) { _pending->fetch_add(1); }
    void Run() override {
        braft::NodeStatus st;
        _node->get_status(&st);
        _pending->fetch_sub(1);
        delete this;
    }
private:
    std::shared_ptr<std::atomic<int>> _pending;
    std::shared_ptr<braft::Node>      _node;
};

struct ApplyArg {
    std::shared_ptr<braft::Node>      node;
    std::shared_ptr<std::atomic<bool>> stop;
    std::shared_ptr<std::atomic<int>>  pending;
};

static void* apply_fn(void* arg) {
    auto* a = static_cast<ApplyArg*>(arg);
    const int kPendingTarget = 10000;
    while (!a->stop->load() && a->pending->load() < kPendingTarget) {
        if (!a->node->is_leader()) { bthread_usleep(1000); continue; }
        butil::IOBuf data; data.append("x");
        braft::Task task;
        task.data = &data;
        task.done = new NopClosure(a->pending, a->node);
        a->node->apply(task);
        bthread_usleep(100);
    }
    return nullptr;
}

class LeaderStepDownNoDeadlockTest : public StepDownTestBase {
protected:
    void SetUp()    override { ::system("rm -rf /tmp/cqd_data /tmp/sad_data"); }
    void TearDown() override { ::system("rm -rf /tmp/cqd_data /tmp/sad_data"); }
};

// Deadlock mechanism: NopClosure::Run() calls get_status() which acquires
// _mutex. step_down holds _mutex while calling push_rq to spawn these closures;
// when workers pick them up and block in get_status(), push_rq can't drain.
TEST_F(LeaderStepDownNoDeadlockTest, StepDownWithPendingClosures) {
    auto* fsm = new SlowFSM;
    setup_node(20082, "/tmp/cqd_data", "cqd_group", fsm);

    auto stop_flag = std::make_shared<std::atomic<bool>>(false);
    std::vector<std::shared_ptr<std::atomic<int>>> pendings;
    std::vector<ApplyArg> args;
    pendings.reserve(kSubmitters_);
    args.reserve(kSubmitters_);
    for (int i = 0; i < kSubmitters_; ++i) {
        pendings.emplace_back(std::make_shared<std::atomic<int>>(0));
        args.push_back({node_, stop_flag, pendings.back()});
    }
    std::vector<bthread_t> bthreads;
    for (int i = 0; i < kSubmitters_; ++i) {
        bthread_t t; bthread_start_background(&t, nullptr, apply_fn, &args[i]);
        bthreads.push_back(t);
    }

    // Each submitter exits once its per-thread pending count hits the target.
    // FSM is parked on a condvar so nothing drains; pending counters only grow.
    for (auto t : bthreads) { bthread_join(t, nullptr); }

    // Release FSM concurrently with step_down to race drain against _mutex hold.
    fsm->release();

    EXPECT_TRUE(fire_step_down())
        << "DEADLOCK: step_down did not complete within 10s. "
           "push_rq is spinning with workers blocked on NodeImpl._mutex.";
    if (HasFailure()) { exit(1); }

    stop_flag->store(true);
    teardown_node();
}

// FSM is simply slow (usleep per entry) until set_fast() is called.
class SimpleFSM : public braft::StateMachine {
public:
    void on_apply(braft::Iterator& iter) override {
        for (; iter.valid(); iter.next()) {
            if (!_fast.load()) { ::usleep(500); }  // ~2000 entries/sec until disabled
        }
    }
    void on_leader_start(int64_t _term) override  {}
    void on_leader_stop(const butil::Status& _status) override {}
    void on_shutdown() override {}

    void set_fast() { _fast.store(true); }
private:
    std::atomic<bool> _fast{false};
};

struct SlowApplyArg {
    std::shared_ptr<braft::Node>       node;
    std::shared_ptr<std::atomic<bool>> stop;
    std::shared_ptr<std::atomic<int>>  pending;  // per-thread
};

class SimpleClosure : public braft::Closure {
public:
    explicit SimpleClosure(std::shared_ptr<std::atomic<int>> pending) : _pending(std::move(pending)) {
        _pending->fetch_add(1);
    }
    void Run() override { _pending->fetch_sub(1); delete this; }
private:
    std::shared_ptr<std::atomic<int>> _pending;
};

static void* slow_apply_fn(void* arg) {
    auto* a = static_cast<SlowApplyArg*>(arg);
    while (!a->stop->load()) {
        if (!a->node->is_leader()) { bthread_usleep(1000); continue; }
        butil::IOBuf data; data.append("x");
        braft::Task task;
        task.data = &data;
        task.done = new SimpleClosure(a->pending);
        a->node->apply(task);
        bthread_usleep(100);
    }
    return nullptr;
}

// Apply() caller bthreads keep running when step_down fires. This creates another deadlock:
// step_down holds node._mutex while calling push_rq; apply() callers block on node._mutex.
TEST_F(LeaderStepDownNoDeadlockTest, StepDownWithConcurrentApply) {
    auto* fsm = new SimpleFSM;
    setup_node(20083, "/tmp/sad_data", "sad_group", fsm);

    auto stop_flag = std::make_shared<std::atomic<bool>>(false);
    std::vector<std::shared_ptr<std::atomic<int>>> pendings;
    std::vector<SlowApplyArg> args;
    pendings.reserve(kSubmitters_);
    args.reserve(kSubmitters_);
    for (int i = 0; i < kSubmitters_; ++i) {
        pendings.emplace_back(std::make_shared<std::atomic<int>>(0));
        args.push_back({node_, stop_flag, pendings.back()});
    }

    std::vector<bthread_t> bthreads;
    for (int i = 0; i < kSubmitters_; ++i) {
        bthread_t t; bthread_start_background(&t, nullptr, slow_apply_fn, &args[i]);
        bthreads.push_back(t);
    }

    // Deterministic readiness gate: every submitter must have kPerThreadTarget
    // closures pending before we fire step_down (replaces ::sleep(2)).
    const int kPerThreadTarget = 5000;
    bool ready = false;
    while (!ready) {
        ready = true;
        for (auto& p : pendings) {
            if (p->load() < kPerThreadTarget) { ready = false; break; }
        }
        if (!ready) { bthread_usleep(1000); }
    }

    // Submitters and get_status bthreads remain active during step_down —
    // they keep workers contending on _mutex, matching the production pattern.
    EXPECT_TRUE(fire_step_down())
        << "DEADLOCK: step_down did not complete within 10s. "
           "push_rq is spinning with workers blocked on NodeImpl._mutex.";
    if (HasFailure()) { exit(1); }

    // Step_down succeeded — let the FSM drain its committed backlog at full speed.
    fsm->set_fast();

    stop_flag->store(true);
    for (auto t : bthreads) { bthread_join(t, nullptr); }
    teardown_node();
}
