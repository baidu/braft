**braft 本身并不提供server功能**， 你可以将braft集成到包括brpc在内的任意编程框架中，本文主要是阐述如何在分布式Server中使用braft来构建高可用系统。具体业务如何实现一个Server，本文不在展开。

# Example

[server-side code](../../example/counter/server.cpp) of Counter

# 注册并且启动Server

braft需要运行在具体的brpc server里面你可以让braft和你的业务共享同样的端口， 也可以将braft启动到不同的端口中.

brpc允许一个端口上注册多个逻辑Service,  如果你的Service同样运行在brpc Server里面，你可以管理brpc Server并且调用以下任意一个接口将braft相关的Service加入到你的Server中。这样能让braft和你的业务跑在同样的端口里面， 降低运维的复杂度。如果对brpc Server的使用不是非常了解， 可以先查看[wiki](https://github.com/brpc/brpc/blob/master/docs/cn/server.md)页面. **注意: 如果你提供的是对外网用户暴露的服务，不要让braft跑在相同的端口上。**

```cpp
// Attach raft services to |server|, this makes the raft services share the same
// listen address with the user services.
//
// NOTE: Now we only allow the backing Server to be started with a specific
// listen address, if the Server is going to be started from a range of ports, 
// the behavior is undefined.
// Returns 0 on success, -1 otherwise.
int add_service(brpc::Server* server, const butil::EndPoint& listen_addr);
int add_service(brpc::Server* server, int port);
int add_service(brpc::Server* server, const char* const butil::EndPoint& listen_addr);
```

- **调用这些接口之前不要启动server， 否则相关的Service将无法加入到这个server中. 导致调用失败.**
- **启动这个server的端口需要和add_service传入的端口一致， 不然会导致这个节点无法正常收发RPC请求.**

# 实现业务状态机

你需要继承braft::StateMachine并且实现里面的接口

```cpp

#include <braft/raft.h>

// NOTE: All the interfaces are not required to be thread safe and they are 
// called sequentially, saying that every single method will block all the 
// following ones.
class YourStateMachineImple : public braft::StateMachine {
protected:
    // on_apply是*必须*实现的
    // on_apply会在一条或者多条日志被多数节点持久化之后调用， 通知用户将这些日志所表示的操作应用到业务状态机中.
    // 通过iter, 可以从遍历所有未处理但是已经提交的日志， 如果你的状态机支持批量更新，可以一次性获取多
    // 条日志提高状态机的吞吐.
    // 
    void on_apply(braft::Iterator& iter) {
        // A batch of tasks are committed, which must be processed through 
        // |iter|
        for (; iter.valid(); iter.next()) {
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine.
            braft::AsyncClosureGuard closure_guard(iter.done());
            // Parse operation from iter.data() and execute this operation
            // op = parse(iter.data());
            // result = process(op)
          
            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "Exeucted operation " << op
                    << " and the result is " << result
                    << " at log_index=" << iter.index();
        }
    }
    // 当这个braft节点被shutdown之后， 当所有的操作都结束， 会调用on_shutdown, 来通知用户这个状态机不再被使用。
    // 这时候你可以安全的释放一些资源了.
    virtual void on_shutdown() {
        // Cleanup resources you'd like
    }
```

## iterator

通过braft::iterator你可以遍历从所有有的任务

```cpp
class Iterator {
    // Move to the next task.
    void next();
    // Return a unique and monotonically increasing identifier of the current 
    // task:
    //  - Uniqueness guarantees that committed tasks in different peers with 
    //    the same index are always the same and kept unchanged.
    //  - Monotonicity guarantees that for any index pair i, j (i < j), task 
    //    at index |i| must be applied before task at index |j| in all the 
    //    peers from the group.
    int64_t index() const;
    // Returns the term of the leader which to task was applied to.
    int64_t term() const;
    // Return the data whose content is the same as what was passed to
    // Node::apply in the leader node.
    const butil::IOBuf& data() const;
    // If done() is non-NULL, you must call done()->Run() after applying this
    // task no matter this operation succeeds or fails, otherwise the
    // corresponding resources would leak.
    //
    // If this task is proposed by this Node when it was the leader of this 
    // group and the leadership has not changed before this point, done() is 
    // exactly what was passed to Node::apply which may stand for some 
    // continuation (such as respond to the client) after updating the 
    // StateMachine with the given task. Otherweise done() must be NULL.
    Closure* done() const;
    // Return true this iterator is currently references to a valid task, false
    // otherwise, indicating that the iterator has reached the end of this
    // batch of tasks or some error has occurred
    bool valid() const;
    // Invoked when some critical error occurred. And we will consider the last 
    // |ntail| tasks (starting from the last iterated one) as not applied. After
    // this point, no further changes on the StateMachine as well as the Node 
    // would be allowed and you should try to repair this replica or just drop 
    // it.
    //
    // If |st| is not NULL, it should describe the detail of the error.
    void set_error_and_rollback(size_t ntail = 1, const butil::Status* st = NULL);
};
```

# 构造braft::Node

一个Node代表了一个RAFT实例， Node的ID由两个部分组成:

- GroupId: 为一个string, 表示这个复制组的ID.
- PeerId, 结构是一个[EndPoint](https://github.com/brpc/brpc/blob/master/src/butil/endpoint.h)表示对外服务的端口, 外加一个index(默认为0). 其中index的作用是让不同的副本能运行在同一个进程内, 在下面几个场景中，这个值不能忽略:

```cpp
Node(const GroupId& group_id, const PeerId& peer_id);
```

启动这个节点:

```cpp
struct NodeOptions {
    // A follower would become a candidate if it doesn't receive any message 
    // from the leader in |election_timeout_ms| milliseconds
    // Default: 1000 (1s)
    int election_timeout_ms;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds
    // if this was reset as a positive number
    // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
    //
    // Default: 3600 (1 hour)
    int snapshot_interval_s;

    // We will regard a adding peer as caught up if the margin between the
    // last_log_index of this peer and the last_log_index of leader is less than
    // |catchup_margin|
    //
    // Default: 1000
    int catchup_margin;

    // If node is starting from a empty environment (both LogStorage and
    // SnapshotStorage are empty), it would use |initial_conf| as the
    // configuration of the group, otherwise it would load configuration from
    // the existing environment.
    //
    // Default: A empty group
    Configuration initial_conf;

    // The specific StateMachine implemented your business logic, which must be
    // a valid instance.
    StateMachine* fsm;

    // If |node_owns_fsm| is true. |fms| would be destroyed when the backing
    // Node is no longer referenced.
    //
    // Default: false
    bool node_owns_fsm;

    // Describe a specific LogStorage in format ${type}://${parameters}
    std::string log_uri;

    // Describe a specific StableStorage in format ${type}://${parameters}
    std::string raft_meta_uri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    std::string snapshot_uri;
    
    // If enable, duplicate files will be filtered out before copy snapshot from remote
    // to avoid useless transmission. Two files in local and remote are duplicate,
    // only if they has the same filename and the same checksum (stored in file meta).
    // Default: false
    bool filter_before_copy_remote;
    
    // If true, RPCs through raft_cli will be denied.
    // Default: false
    bool disable_cli;
};
class Node {
    int init(const NodeOptions& options);
};
```

* initial_conf只有在这个复制组从空节点启动才会生效，当有snapshot和log里的数据不为空的时候的时候从其中恢复Configuration。initial_conf只用于创建复制组，第一个节点将自己设置进initial_conf，再调用add_peer添加其他节点，其他节点initial_conf设置为空；也可以多个节点同时设置相同的inital_conf(多个节点的ip:port)来同时启动空节点。

* RAFT需要三种不同的持久存储, 分别是:

  * RaftMetaStorage, 用来存放一些RAFT算法自身的状态数据， 比如term, vote_for等信息.
  * LogStorage, 用来存放用户提交的WAL
  * SnapshotStorage, 用来存放用户的Snapshot以及元信息.

  用三个不同的uri来表示, 并且提供了基于本地文件系统的默认实现，type为local, 比如 local://data 就是存放到当前文件夹的data目录，local:///home/disk1/data 就是存放在 /home/disk1/data中。libraft中有默认的local://实现，用户可以根据需要继承实现相应的Storage。

# 将操作提交到复制组

你需要将你的操作序列化成[IOBuf](https://github.com/brpc/brpc/blob/master/src/butil/iobuf.h), 这是一个非连续零拷贝的缓存结构。构造一个Task, 并且向braft::Node提交

```cpp
#include <braft/raft.h>

...
void function(op, callback) {
    butil::IOBuf data;
    serialize(op, &data);
    braft::Task task;
    task.data = &data;
    task.done = make_closure(callback);
    task.expected_term = expected_term;
    return _node->apply(task);
}
```

具体接口

```cpp
struct Task {
    Task() : data(NULL), done(NULL) {}

    // The data applied to StateMachine
    base::IOBuf* data;

    // Continuation when the data is applied to StateMachine or error occurs.
    Closure* done;
 
    // Reject this task if expected_term doesn't match the current term of
    // this Node if the value is not -1
    // Default: -1
    int64_t expected_term;
};
    
// apply task to the replicated-state-machine
//
// About the ownership:
// |task.data|: for the performance consideration, we will take way the 
//              content. If you want keep the content, copy it before call
//              this function
// |task.done|: If the data is successfully committed to the raft group. We
//              will pass the ownership to StateMachine::on_apply.
//              Otherwise we will specify the error and call it.
//
void apply(const Task& task);
```

* **Thread-Safety**: apply是线程安全的，并且实现基本等价于是[wait-free](https://en.wikipedia.org/wiki/Non-blocking_algorithm#Wait-freedom). 这意味着你可以在多线程向同一个Node中提交WAL.


* **apply不一定成功**，如果失败的话会设置done中的status，并回调。on_apply中一定是成功committed的，但是apply的结果在leader发生切换的时候存在[false negative](https://en.wikipedia.org/wiki/False_positives_and_false_negatives#False_negative_error), 即框架通知这次WAL写失败了， 但最终相同内容的日志被新的leader确认提交并且通知到StateMachine. 这个时候通常客户端会重试(超时一般也是这么处理的), 所以一般需要确保日志所代表的操作是[幂等](https://en.wikipedia.org/wiki/Idempotence)的

* 不同的日志处理结果是独立的, **一个线程**连续提交了A,B两个日志， 那么以下组合都有可能发生:

  * A 和 B都成功
  * A 和 B都失败
  * A 成功 B失败
  * A 失败 B成功

  当A, B都成功的时候， 他们在日志中的顺序会和提交顺序严格保证一致.

* 由于apply是异步的，有可能某个节点在term1是leader，apply了一条log，但是中间发生了主从切换，在很短的时间内这个节点又变为term3的leader，之前apply的日志才开始进行处理，这种情况下要实现严格意义上的复制状态机，需要解决这种ABA问题，可以在apply的时候设置leader当时的term.

raft::Closure是一个特殊的protobuf::Closure的子类， 可以用了标记一次异步调用成功或者失败. 和protobuf::Closure一样， 你需要继承这个类，实现Run接口。 当一次异步调用真正结束之后， Run会被框架调用， 此时你可以通过[status()](https://github.com/brpc/brpc/src/butil/status.h)来确认这次调用是否成功或者失败。

```cpp
// Raft-specific closure which encloses a base::Status to report if the
// operation was successful.
class Closure : public google::protobuf::Closure {
public:
    base::Status& status() { return _st; }
    const base::Status& status() const { return _st; }
};
```

# 监听braft::Node状态变更

StateMachine中还提供了一些接口, 实现这些接口能够监听Node的状态变化，你的系统可以针对这些状态变化实现一些特定的逻辑(比如转发消息给leader节点)

```cpp
class StateMachine {
...
    // Invoked once when the raft node was shut down. Corresponding resources are safe
    // to cleared ever after.
    // Default do nothing
    virtual void on_shutdown();
    // Invoked when the belonging node becomes the leader of the group at |term|
    // Default: Do nothing
    virtual void on_leader_start(int64_t term);
    // Invoked when this node is no longer the leader of the belonging group.
    // |status| describes more details about the reason.
    virtual void on_leader_stop(const butil::Status& status);
    // Invoked when some critical error occurred and this Node stops working 
    // ever after.  
    virtual void on_error(const ::braft::Error& e);
    // Invoked when a configuration has been committed to the group
    virtual void on_configuration_committed(const ::braft::Configuration& conf);
    // Invoked when a follower stops following a leader
    // situations including: 
    // 1. Election timeout is expired. 
    // 2. Received message from a node with higher term
    virtual void on_stop_following(const ::braft::LeaderChangeContext& ctx);
    // Invoked when this node starts to follow a new leader.
    virtual void on_start_following(const ::braft::LeaderChangeContext& ctx);
...
};
```

# 实现Snapshot

在braft中，Snapshot被定义为**在特定持久化存储中的文件集合**, 用户将状态机序列化到一个或者多个文件中， 并且任何节点都能从这些文件中恢复状态机到当时的状态.

Snapshot有两个作用:

- 启动加速， 启动阶段变为加载Snapshot和追加之后日志两个阶段， 而不需要重新执行历史上所有的操作.
- Log Compaction， 在完成Snapshot完成之后， 这个时间之前的日志都可以被删除了， 这样可以减少日志占用的资源.

在braft的中， 可以通过SnapshotReader和SnapshotWriter来控制访问相应的Snapshot.

```cpp
class Snapshot : public butil::Status {
public:
    Snapshot() {}
    virtual ~Snapshot() {}

    // Get the path of the Snapshot
    virtual std::string get_path() = 0;

    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files) = 0;

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename, 
                              ::google::protobuf::Message* file_meta) {
        (void)filename;
        file_meta->Clear();
        return 0;
    }
};

class SnapshotWriter : public Snapshot {
public:
    SnapshotWriter() {}
    virtual ~SnapshotWriter() {}

    // Save the meta information of the snapshot which is used by the raft
    // framework.
    virtual int save_meta(const SnapshotMeta& meta) = 0;

    // Add a file to the snapshot.
    // |file_meta| is an implmentation-defined protobuf message 
    // All the implementation must handle the case that |file_meta| is NULL and
    // no error can be raised.
    // Note that whether the file will be created onto the backing storage is
    // implementation-defined.
    virtual int add_file(const std::string& filename) { 
        return add_file(filename, NULL);
    }

    virtual int add_file(const std::string& filename, 
                         const ::google::protobuf::Message* file_meta) = 0;

    // Remove a file from the snapshot
    // Note that whether the file will be removed from the backing storage is
    // implementation-defined.
    virtual int remove_file(const std::string& filename) = 0;
};

class SnapshotReader : public Snapshot {
public:
    SnapshotReader() {}
    virtual ~SnapshotReader() {}

    // Load meta from 
    virtual int load_meta(SnapshotMeta* meta) = 0;

    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy() = 0;
};
```

 不同业务的Snapshot千差万别，因为SnapshotStorage并没有抽象具体读写Snapshot的接口，而是抽象出SnapshotReader和SnapshotWriter，交由用户扩展具体的snapshot创建和加载逻辑。

**Snapshot创建流程**：

- SnapshotStorage::create创建一个临时的Snapshot，并返回一个SnapshotWriter
- SnapshotWriter将状态数据写入到临时snapshot中
- SnapshotStorage::close来将这个snapshot转为合法的snapshot

**Snapshot读取流程**：

- SnapshotStorage::open打开最近的一个Snapshot，并返回一个SnapshotReader
- SnapshotReader将状态数据从snapshot中恢复出来
- SnapshotStorage::close清理资源

libraft内提供了基于文件列表的LocalSnapshotWriter和LocalSnapshotReader默认实现，具体使用方式为：

- 在fsm的on_snapshot_save回调中，将状态数据写入到本地文件中，然后调用SnapshotWriter::add_file将相应文件加入snapshot meta。
- 在fsm的on_snapshot_load回调中，调用SnapshotReader::list_files获取本地文件列表，按照on_snapshot_save的方式进行解析，恢复状态数据。

实际情况下，用户业务状态机数据的snapshot有下面几种实现方式：

- 状态数据存储使用支持MVCC的存储引擎，创建snapshot之后，再异步迭代snapshot句柄将数据持久化
- 状态数据全内存且数据量不大，直接加锁将数据拷贝出来，再异步将数据持久化
- 定时启动一个离线线程，合并上一次的snapshot和最近的log，生成新的snapshot(需要业务fsm再持久化一份log，可以通过定制logstorage实现raft和fsm共享log)
- fork子进程，在子进程中遍历状态数据并进行持久化(多线程程序实现中需要避免死锁)

对于业界一些newsql系统，它们大都使用类rocksdb的lsm tree的存储引擎，支持MVCC。在进行raft snapshot的时候，使用上面的方案1，先创建一个db的snapshot，然后创建一个iterator，遍历并持久化数据。tidb、cockroachdb都是类似的解决方案。

# 控制这个节点

braft::Node可以通过调用api控制也可以通过[braft_cli](./cli.md)来控制, 本章主要说明如何使用api.

## 节点配置变更

在分布式系统中，机器故障，扩容，副本均衡是管理平面需要解决的基本问题，braft提供了几种方式:

* 增加一个节点
* 删除一个节点
* 全量替换现有节点列表

```cpp
// Add a new peer to the raft group. done->Run() would be invoked after this
// operation finishes, describing the detailed result.
void add_peer(const PeerId& peer, Closure* done);

// Remove the peer from the raft group. done->Run() would be invoked after
// this operation finishes, describing the detailed result.
void remove_peer(const PeerId& peer, Closure* done);

// Gracefully change the configuration of the raft group to |new_peers| , done->Run()
// would be invoked after this operation finishes, describing the detailed
// result.
void change_peers(const Configuration& new_peers, Closure* done);
```

节点变更分为几个阶段:

- **追赶阶段**: 如果新的节点配置相对于当前有新增的一个或者多个节点，leader对应的Replicator, 向把最新的snapshot再这个这些中安装，然后开始同步之后的日志。等到所有的新节点数据都追的差不多，就开始进入一下一阶段。
  - 追赶是为了避免新加入的节点数据和集群相差过远而影响集群的可用性. 并不会影响数据安全性.
  - 在追赶阶段完成前， **只有**leader知道这些新节点的存在，这个节点都不会被记入到集群的决策集合中，包括选主和日志提交的判定。追赶阶段任意节点失败，则这次节点变更就会被标记为失败。
- **联合选举阶段**: leader会将旧节点配置和新节点配置写入Log, 在这个阶段之后直道下一个阶段之前，所有的选举和日志同步都需要在**新老节点之间达到多数**。 这里和标准算法有一点不同， 考虑到和之前实现的兼容性，如果这次只变更了一个节点,  则直接进入下一阶段。
- **新配置同步阶段:**  当联合选举日志正式被新旧集群接受之后，leader将新节点配置写入log，之后所有的log和选举只需要在新集群中达成一致。 等待日志提交到**新集群**中的多数节点中之后， 正式完全节点变更。
- **清理阶段**: leader会将多余的Replicator(如果有)关闭，特别如果当leader本身已经从节点配置中被移除，这时候leader会执行stepdown并且唤醒一个合适的节点触发选举。

> 当考虑节点删除的时候， 情况会变得有些复杂, 由于判断成功提交的节点数量变少， 可能会出现在前面的日志没有成功提交的情况下， 后面的日志已经被判断已经提交。 这时候为了状态机的操作有序性， 即使之前的日志还未提交， 我们也会强制判断为成功.
>
> 举个例子:
>
> - 当前集群为 (A, B, **C, D**), 其中**C D**属于故障， 由于多数节点处于故障阶段, 存在10条还未被提交的日志(A B 已经写入， **C D** 未写入), 这时候发起操作，将D从集群中删除， 这条日志的成功判定条件变为在(A, B, **C**)， 这时候只需要A, B都成功写入这条日志即可认为这个日志已经成功提交， 但是之前还存在10条未写入日志. 这时候我们会强制认为之前的10条已经成功提交.
> - 这个case比较极端， 通常这个情况下leader都会step down， 集群会进入无主状态， 需要至少修复CD中的一个节点之后集群才能正常提供服务。

## 重置节点列表

当多数节点故障的时候，是不能通过add_peer/remove_peer/change_peers进行节点变更的，这个时候安全的做法是等待多数节点恢复，能够保证数据安全。如果业务追求服务的可用性，放弃数据安全性的话，可以使用reset_peers飞线设置复制组Configuration。

```cpp
// Reset the configuration of this node individually, without any repliation
// to other peers before this node beomes the leader. This function is
// supposed to be inovoked when the majority of the replication group are
// dead and you'd like to revive the service in the consideration of
// availability.
// Notice that neither consistency nor consensus are guaranteed in this
// case, BE CAREFULE when dealing with this method.
butil::Status reset_peers(const Configuration& new_peers);
```

reset_peer之后，新的Configuration的节点会开始重新选主，当新的leader选主成功之后，会写一条新Configuration的Log，这条Log写成功之后，reset_peer才算成功。如果中间又发生了失败的话，外部需要重新选取peers并发起reset_peers。

**不建议使用reset_peers**，reset_peers会破坏raft对数据一致性的保证，而且可能会造成脑裂。例如，{A B C D E}组成的复制组G，其中{C D E}故障，将{A B} set_peer成功恢复复制组G'，{C D E}又重新启动它们也会形成一个复制组G''，这样复制组G中会存在两个Leader，且{A B}这两个复制组中都存在，其中的follower会接收两个leader的AppendEntries，当前只检测term和index，可能会导致其上数据错乱。

```cpp
// Add a new peer to the raft group when the current configuration matches
// |old_peers|. done->Run() would be invoked after this operation finishes,
// describing the detailed result.
void add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done);
```

## 转移Leader

```
// Try transferring leadership to |peer|.
// If peer is ANY_PEER, a proper follower will be chosen as the leader the
// the next term.
// Returns 0 on success, -1 otherwise.
int transfer_leadership_to(const PeerId& peer);
```

在一些场景中，我们会需要外部强制将leader切换到另外的节点， 比如:

- 主节点要重启， 这时候发起一次主迁移能够减少集群的不可服务时间
- 主节点所在的机器过于繁忙， 我们需要迁移到另外一个相对空闲的机器中.
- 复制组跨IDC部署， 我们希望主节点存在于离Client延时最小的集群中.

braft实现了主迁移算法， 这个算法包含如下步骤:

1. 主停止写入， 这时候所有的apply会报错.
2. 继续向所有的follower同步日志， 当发现目标节点的日志已经和主一样多之后， 向对应节点发起一个TimeoutNow RPC
3. 节点收到TimeoutNowRequest之后，  直接变为Candidate, 增加term，并开始进入选主
4. 主收到TimeoutNowResponse之后， 开始step down.
5. 如果在election_timeout_ms时间内主没有step down， 会取消主迁移操作， 开始重新接受写入请求.

# 查看节点状态

braft中在Node启动之后，会在http://${your_server_endpoint}/raft_stat中列出当前这个进程上Node的列表，及其每个Node的内部状态。

其中包括：

| 字段                   | 说明                                       |
| -------------------- | ---------------------------------------- |
| state                | 节点状态，包括LEADER/FOLLOWER/CANDIDATE         |
| term                 | 当前term                                   |
| conf_index           | 上一个Configuration产生的log index             |
| peers                | 当前Configuration中节点列表                     |
| leader               | 当前Configuration中Leader节点                 |
| election_timer       | 选主定时器，FOLLOWER状态下启用                      |
| vote_timer           | 投票定时器，CANDIDATE状态下启用                     |
| stepdown_timer       | 主切从定时器，LEADER状态下启用                       |
| snapshot_timer       | 快照定时器                                    |
| storage              | log storage中first log index和last log index |
| disk_index           | 持久化的最后一个log index                        |
| known_applied_index  | fsm已经apply的最后一个log index                 |
| last_log_id          | 最后一条内存log信息（log先写内存再批量刷disk）             |
| state_machine        | fsm状态，包括IDLE/COMMITTED/SNAPSHOT_SAVE/SNAPSHOT_LOAD/LEADER_STOP/ERROR |
| last_committed_index | 已经committed的最大log index                  |
| last_snapshot_index  | 上一次snapshot中包含的最后一条log index             |
| last_snapshot_term   | 上一次snapshot中包含的最后一条log的term              |
| snapshot_status      | snapshot状态，包括：LOADING/DOWNLOADING/SAVING/IDLE，其中LOADING和DOWNLOADING会显示snapshot uri和snapshot meta |

# flags配置项

raft中有很多flags配置项，运行中可以通过http://endpoint/flags查看，具体如下：

| flags名                         | 说明                         |
| ------------------------------ | -------------------------- |
| raft_sync                      | 是否开启sync                   |
| raft_max_append_buffer_size    | log manager中内存缓存大小         |
| raft_leader_batch              | log manager中最大batch合并      |
| raft_max_entries_size          | AppendEntries包含entries最大数量 |
| raft_max_body_size             | AppendEntris最大body大小       |
| raft_max_segment_size          | 单个logsegment大小             |
| raft_max_byte_count_per_rpc    | snapshot每次rpc下载大小          |
| raft_apply_batch               | apply的时候最大batch数量          |
| raft_election_heartbeat_factor | election超时与heartbeat超时的比例  |
