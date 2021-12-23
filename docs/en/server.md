**braft itself does not provide server functions**. You can integrate braft into any programming framework including brpc. This article mainly explains how to use braft in a distributed server to build a highly available system. How to implement a server for a specific business is not in this article.

# Example

[server-side code](../../example/counter/server.cpp) of Counter

# Register and start Server

Braft needs to run in a specific brpc server. You can make braft and your business share the same port, or you can start braft to a different port.

brpc allows multiple logical services to be registered on a port. If your Service is also running in brpc Server, you can manage brpc Server and call any of the following interfaces to add braft-related services to your Server. This allows the braft and your business to run on the same port, reducing the complexity of operation and maintenance. If you are not very familiar with the use of brpc Server, you can check the [wiki](https://github.com/brpc/brpc/blob/master/docs/cn/server.md) page first. **Note: If you provide It is a service exposed to users on the Internet. Don't let the braft run on the same port. **

``` cpp
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

- **Do not start the server before calling these interfaces, otherwise the related Service will not be added to the server. This will cause the call to fail.**
- **The port used to start this server must be the same as the port passed in by add_service, otherwise this node will not be able to send and receive RPC requests normally.**

# Implement business state machine

You need to inherit braft::StateMachine and implement the interface inside

``` cpp

#include <braft/raft.h>

// NOTE: All the interfaces are not required to be thread safe and they are
// called sequentially, saying that every single method will block all the
// following ones.
class YourStateMachineImple : public braft::StateMachine {
protected:
// on_apply is *must* implement
// on_apply will be called after one or more logs are persisted by most nodes to notify the user to apply the operations represented by these logs to the business state machine.
// Through iter, you can traverse all unprocessed but submitted logs. If your state machine supports batch updates, you can get more at one time
// A log improves the throughput of the state machine.
//
void on_apply(braft::Iterator& iter) {
// A batch of tasks are committed, which must be processed through
// | iter |
for (; iter.valid(); iter.next()) {
// This guard helps invoke iter.done()->Run() asynchronously to
// avoid that callback blocks the StateMachine.
braft::AsyncClosureGuard closure_guard(iter.done());
// Parse operation from iter.data() and execute this operation
// op = parse (iter.data ());
// result = process(on)

            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "Exeucted operation " << op
                    << " and the result is " << result
                    << " at log_index=" << iter.index();
        }
    }
    // After the braft node is shut down, when all operations are completed, on_shutdown will be called to notify the user that this state machine is no longer in use.
    // At this time you can safely release some resources.
    virtual void on_shutdown() {
        // Cleanup resources you'd like
    }
```

## iterator

Through braft::iterator you can traverse all tasks

``` cpp
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

# Construct braft::Node

A Node represents a RAFT instance, and the Node ID consists of two parts:

- GroupId: is a string, which represents the ID of this replication group.
- PeerId, the structure is an [EndPoint](https://github.com/brpc/brpc/blob/master/src/butil/endpoint.h) represents the port of the external service, plus an index (default is 0). Among them The function of index is to allow different copies to run in the same process. In the following scenarios, this value cannot be ignored:

``` cpp
Node(const GroupId& group_id, const PeerId& peer_id);
```

Start this node:

``` cpp
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
    std :: string log_uri;

    // Describe a specific StableStorage in format ${type}://${parameters}
    std :: string raft_meta_uri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    std :: string snapshot_uri;
    
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

* initial_conf will only take effect when the replication group is started from an empty node, and the Configuration will be restored when the data in the snapshot and log is not empty. initial_conf is only used to create a replication group. The first node sets itself into initial_conf, and then calls add_peer to add other nodes. The initial_conf of other nodes is set to empty; multiple nodes can also set the same inital_conf (ip:port of multiple nodes). ) To start an empty node at the same time.

* RAFT requires three different types of persistent storage, namely:

  * RaftMetaStorage, used to store some state data of the RAFT algorithm itself, such as term, vote_for and other information.
  * LogStorage, used to store WAL submitted by users
  * SnapshotStorage, used to store the user's Snapshot and meta-information.

  It is represented by three different uris, and a default implementation based on the local file system is provided. The type is local, for example, local://data is the data directory stored in the current folder, local:///home/disk1/data It is stored in /home/disk1/data. There is a default local:// implementation in libraft, and users can inherit and implement the corresponding Storage as needed.

# Submit the operation to the replication group

You need to serialize your operations into [IOBuf](https://github.com/brpc/brpc/blob/master/src/butil/iobuf.h), which is a non-continuous zero-copy cache structure. Construct a Task and submit it to braft::Node

``` cpp
#include <braft/raft.h>

...
void function(on, callback) {
butyl :: IOBuf data;
serialize(op, &data);
braft::Task task;
task.data = &data;
task.done = make_closure(callback);
task.expected_term = expected_term;
return _node->apply(task);
}
```

Specific interface

``` cpp
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

* **Thread-Safety**: apply is thread-safe, and the implementation is basically equivalent to [wait-free](https://en.wikipedia.org/wiki/Non-blocking_algorithm#Wait-freedom). This means So you can submit WAL to the same Node in multiple threads.


* **apply may not be successful**, if it fails, the status in done will be set and callback. On_apply must be successfully committed, but the result of apply exists when the leader is switched [false negative](https://en.wikipedia.org/wiki/False_positives_and_false_negatives#False_negative_error), that is, the framework informs that this WAL write failed. , But in the end the log with the same content is confirmed and submitted by the new leader and notified to StateMachine. At this time, the client will usually retry (timeout is usually handled in this way), so it is generally necessary to ensure that the operation represented by the log is [idempotent]( https://en.wikipedia.org/wiki/Idempotence)

* Different log processing results are independent, **a thread** consecutively submits two logs A and B, then the following combinations may occur:

  * Both A and B are successful
  * Both A and B failed
  * A succeeds B fails
  * A failed and B succeeded

  When A and B are successful, their order in the log will be strictly consistent with the submission order.

* Since apply is asynchronous, it is possible that a node is the leader at term1, and a log is applied, but a master-slave switch occurs in the middle. In a short period of time, the node becomes the leader of term3. The log of the previous application is only Start processing. In this case, to implement a replication state machine in the strict sense, you need to solve this ABA problem. You can set the leader's current term when applying.

raft::Closure is a special subclass of protobuf::Closure, which can be used to mark the success or failure of an asynchronous call. Like protobuf::Closure, you need to inherit this class and implement the Run interface. When an asynchronous call really ends, Run will be called by the framework. At this point, you can confirm the call by [status()](https://github.com/brpc/brpc/src/butil/status.h) Whether it succeeded or failed.

``` cpp
// Raft-specific closure which encloses a base::Status to report if the
// operation was successful.
class Closure : public google::protobuf::Closure {
public:
base::Status& status() { return _st; }
const base::Status& status() const { return _st; }
};
```

# Monitor braft::Node status changes

StateMachine also provides some interfaces. Implementing these interfaces can monitor the state changes of Node. Your system can implement some specific logic for these state changes (such as forwarding messages to the leader node)

``` cpp
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

# Implement Snapshot

In braft, Snapshot is defined as a collection of files in a specific persistent storage. The user serializes the state machine to one or more files, and any node can restore the state machine from these files to the time status.

Snapshot has two functions:

- Start acceleration, the startup phase becomes the two phases of loading the snapshot and appending the log, without the need to re-execute all the operations in the history.
- Log Compaction, after the snapshot is completed, the logs before this time can be deleted, which can reduce the resources occupied by the logs.

In braft, you can use SnapshotReader and SnapshotWriter to control access to the corresponding Snapshot.

``` cpp
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
                              :: google :: protobuf :: Message * file_meta) {
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
                         const :: google :: protobuf :: Message * file_meta) = 0;

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
    virtual std :: string generate_uri_for_copy () = 0;
};
```

Snapshots of different businesses are very different, because SnapshotStorage does not abstract specific read and write Snapshot interfaces, but abstract SnapshotReader and SnapshotWriter, and let users extend the specific snapshot creation and loading logic.

**Snapshot creation process**:

- SnapshotStorage::create creates a temporary Snapshot and returns a SnapshotWriter
- SnapshotWriter writes state data to temporary snapshot
- SnapshotStorage::close to convert this snapshot into a legal snapshot

**Snapshot reading process**:

- SnapshotStorage::open opens the most recent Snapshot and returns a SnapshotReader
- SnapshotReader restores the state data from the snapshot
- SnapshotStorage::close to clean up resources

The default implementation of LocalSnapshotWriter and LocalSnapshotReader based on file list is provided in libraft. The specific usage is:

- In the on_snapshot_save callback of fsm, write the status data to a local file, and then call SnapshotWriter::add_file to add the corresponding file to the snapshot meta.
- In the on_snapshot_load callback of fsm, call SnapshotReader::list_files to get the local file list, parse it in the on_snapshot_save method, and restore the status data.

In actual situations, the snapshot of the user's business state machine data can be implemented in the following ways:

- State data storage uses a storage engine that supports MVCC. After the snapshot is created, the snapshot handle is asynchronously iterated to persist the data
- State data is in full memory and the amount of data is not large, directly lock the data to copy out, and then asynchronously persist the data
- Start an offline thread regularly, merge the last snapshot and the latest log, and generate a new snapshot (the business fsm is required to persist a log, and the raft and fsm can be shared by customizing logstorage)
- Fork the child process, traverse the state data in the child process and persist (need to avoid deadlock in the implementation of multi-threaded programs)

For some newsql systems in the industry, most of them use rocksdb-like lsm tree storage engines and support MVCC. When doing a raft snapshot, using the above scheme 1, first create a snapshot of the db, and then create an iterator to traverse and persist the data. Tidb and cockroachdb are similar solutions.

# Control this node

braft::Node can be controlled by calling api or via [braft_cli](./cli.md), this chapter mainly explains how to use api.

## Node configuration changes

In a distributed system, machine failure, capacity expansion, and replica balance are the basic problems that the management plane needs to solve. Braft provides several ways:

* Add a node
* Delete a node
* Replace the existing node list in full

``` cpp
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

Node changes are divided into several stages:

- **Catch-up phase**: If the new node configuration is relative to the current one or more new nodes, the Replicator corresponding to the leader will install the latest snapshot in these, and then start the log after synchronization. When all the new node data is almost the same, we will enter the next stage.
- The catch-up is to prevent the data of the newly added node from being too far away from the cluster and affecting the availability of the cluster. It does not affect data security.
- Before the catch-up phase is completed, **only** leader knows the existence of these new nodes, and this node will not be included in the cluster's decision set, including the decision of leader selection and log submission. If any node fails in the catch-up phase, the node change will be marked as failed.
- **Joint election phase**: The leader will write the old node configuration and the new node configuration to the Log. After this phase and before the next phase, all elections and log synchronization need to be reached between the old and new nodes. most**. This is a bit different from the standard algorithm. Considering the compatibility with the previous implementation, if only one node is changed this time, it will go directly to the next stage.
- **New configuration synchronization phase:** When the joint election log is officially accepted by the old and new clusters, the leader writes the new node configuration to the log. After that, all logs and elections only need to be agreed in the new cluster. After waiting for the log to be submitted to most nodes in the **new cluster**, the node change is officially completed.
- **Cleanup phase**: The leader will shut down redundant Replicators (if any), especially if the leader itself has been removed from the node configuration, at this time the leader will execute stepdown and wake up a suitable node to trigger the election.

> When considering node deletion, the situation will become a little more complicated. As the number of nodes that are judged to be successfully submitted is reduced, it may appear that the previous log has not been successfully submitted, and the subsequent log has been judged to have been submitted. At this time, for the orderly operation of the state machine, even if the previous log has not been submitted, we will force the judgment to be successful.
>
> For example:
>
>-The current cluster is (A, B, **C, D**), where **CD** is a failure. Since most nodes are in the failure stage, there are 10 uncommitted logs (AB has been written, **CD** not written), at this time, initiate an operation to delete D from the cluster. The successful judgment condition of this log becomes (A, B, **C**), and only A, If B has successfully written this log, it can be considered that this log has been successfully submitted, but there are still 10 unwritten logs. At this time, we will force the previous 10 to be submitted successfully.
>-This case is more extreme. In this case, the leader will step down and the cluster will enter an unowned state. The cluster needs to repair at least one node in the CD before the cluster can provide services normally.

## Reset node list

When most nodes fail, node changes cannot be made through add_peer/remove_peer/change_peers. At this time, the safe way is to wait for most nodes to recover to ensure data security. If the business pursues service availability and gives up data security, you can use reset_peers to set the replication group Configuration.

``` cpp
// Reset the configuration of this node individually, without any repliation
// to other peers before this node beomes the leader. This function is
// supposed to be inovoked when the majority of the replication group are
// dead and you'd like to revive the service in the consideration of
// availability.
// Notice that neither consistency nor consensus are guaranteed in this
// case, BE CAREFULE when dealing with this method.
butil::Status reset_peers(const Configuration& new_peers);
```

After reset_peer, the node of the new Configuration will start to re-elect the leader. When the new leader selects the leader successfully, a log of the new Configuration will be written. After the log is successfully written, reset_peer is considered successful. If another failure occurs in the middle, the outside needs to reselect peers and initiate reset_peers.

**It is not recommended to use reset_peers**, reset_peers will destroy raft's guarantee of data consistency, and may cause split brain. For example, the replication group G composed of {ABCDE}, where {CDE} fails, {AB} set_peer is successfully restored to the replication group G', and {CDE} restarts them to form a replication group G'', so that the replication group G There will be two Leaders in the two replication groups, and both {AB} exist in the two replication groups. The follower will receive the AppendEntries of the two leaders. Currently, only term and index are detected, which may cause data confusion on them.

``` cpp
// Add a new peer to the raft group when the current configuration matches
// |old_peers|. done->Run() would be invoked after this operation finishes,
// describing the detailed result.
void add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done);
```

## Transfer Leader

```
// Try transferring leadership to |peer|.
// If peer is ANY_PEER, a proper follower will be chosen as the leader the
// the next term.
// Returns 0 on success, -1 otherwise.
int transfer_leadership_to(const PeerId& peer);
```

In some scenarios, we will need to externally force the leader to switch to another node, such as:

- The master node needs to be restarted. Initiating a master migration at this time can reduce the unserviceable time of the cluster
- The machine where the master node is located is too busy, and we need to migrate to another relatively idle machine.
- The replication group is deployed across IDCs. We hope that the master node exists in the cluster with the smallest delay from the Client.

Braft implements the main migration algorithm, which includes the following steps:

1. The master stops writing, and all apply will report errors at this time.
2. Continue to synchronize logs with all followers. When it is found that the target node has as many logs as the master, initiate a TimeoutNow RPC to the corresponding node
3. After the node receives the TimeoutNowRequest, it directly becomes Candidate, adds the term, and starts to enter the election
4. After the master receives TimeoutNowResponse, it starts to step down.
5. If the master does not step down within the election_timeout_ms time, the master migration operation will be cancelled and the write request will be accepted again.

# View node status

After Node is started in braft, it will list the current list of Nodes in this process and the internal status of each Node in http://${your_server_endpoint}/raft_stat.

These include:

| Field | Description |
| -------------------- | ---------------------------------------- |
| state | Node state, including LEADER/FOLLOWER/CANDIDATE |
| term | current term |
| conf_index | log index generated by the previous Configuration |
| peers | Node list in current Configuration |
| leader | Leader node in current Configuration |
| election_timer | Election timer, enabled in FOLLOWER state |
| vote_timer | Voting timer, enabled in CANDIDATE state |
| stepdown_timer | Master-switch-slave timer, enabled in LEADER state |
| snapshot_timer | Snapshot timer |
| storage              | log storage中first log index和last log index |
| disk_index | Last log index of persistence |
| known_applied_index | The last log index that fsm has applied |
| last_log_id | The last memory log information (log writes to memory first and then batches disk) |
| state_machine        | fsm状态，包括IDLE/COMMITTED/SNAPSHOT_SAVE/SNAPSHOT_LOAD/LEADER_STOP/ERROR |
| last_committed_index | The largest log index that has been committed |
| last_snapshot_index | The last log index contained in the last snapshot |
| last_snapshot_term | Term of the last log contained in the last snapshot |
| snapshot_status | snapshot status, including: LOADING/DOWNLOADING/SAVING/IDLE, where LOADING and DOWNLOADING will display snapshot uri and snapshot meta |

# flagsConfiguration items

There are many flags configuration items in raft, which can be viewed through http://endpoint/flags during operation, as follows:

| flags name | description |
| ------------------------------ | -------------------------- |
| raft_sync | Whether to enable sync |
| raft_max_append_buffer_size | Memory buffer size in log manager |
| raft_leader_batch | The largest batch merge in log manager |
| raft_max_entries_size | AppendEntries contains the maximum number of entries |
| raft_max_body_size | AppendEntris maximum body size |
| raft_max_segment_size | Single logsegment size |
| raft_max_byte_count_per_rpc | snapshot download size of each rpc |
| raft_apply_batch | Maximum batch number when applying |
| raft_election_heartbeat_factor | Ratio of election timeout to heartbeat timeout |