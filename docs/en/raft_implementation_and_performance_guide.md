# braft: Raft Implementation & Performance Tuning Guide

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Leader Election](#2-leader-election)
3. [Log Replication](#3-log-replication)
4. [Log Storage](#4-log-storage)
5. [Snapshot Mechanism](#5-snapshot-mechanism)
6. [Membership Changes](#6-membership-changes)
7. [State Machine Application](#7-state-machine-application)
8. [RPC Layer](#8-rpc-layer)
9. [Concurrency Design](#9-concurrency-design)
10. [Performance Tuning Guide](#10-performance-tuning-guide)

---

## 1. Architecture Overview

braft is an industrial-grade C++ implementation of the Raft consensus algorithm built on top of **brpc** (Baidu's RPC framework). It provides a replicated state machine with high throughput and low latency.

The central class is `NodeImpl` (`src/braft/node.h:87`), which orchestrates all Raft components:

```
                  +-----------------------------------+
                  |           NodeImpl                 |
                  |  (Leader / Follower / Candidate)   |
                  +---------+-------------------------+
                            |
          +-----------------+----------------------+
          |                 |                      |
   +------v------+  +------v------+  +------------v-----------+
   | LogManager  |  | BallotBox   |  | ReplicatorGroup        |
   | (WAL)       |  | (Quorum)    |  | (per-follower RPCs)    |
   +------+------+  +------+------+  +------------------------+
          |                |
   +------v------+  +------v------+
   |SegmentLog   |  | FSMCaller   |
   |Storage      |  | (Apply to   |
   |(on-disk)    |  |  StateMachine|
   +-------------+  +-------------+
```

### Key Components

| Component | File | Responsibility |
|---|---|---|
| `NodeImpl` | `src/braft/node.h:87` | Main Raft node: state transitions, election, apply |
| `Replicator` | `src/braft/replicator.h:82` | Per-follower log replication thread |
| `ReplicatorGroup` | `src/braft/replicator.h:282` | Manages all Replicators for followers |
| `BallotBox` | `src/braft/ballot_box.h:51` | Quorum counting for commit decisions |
| `LogManager` | `src/braft/log_manager.h` | In-memory log index + WAL management |
| `SegmentLogStorage` | `src/braft/log.h:139` | Segmented append-only file log storage |
| `FSMCaller` | `src/braft/fsm_caller.h:106` | Applies committed entries to user StateMachine |
| `SnapshotExecutor` | `src/braft/snapshot_executor.h` | Snapshot creation, installation, and transfer |
| `LeaderLease` / `FollowerLease` | `src/braft/lease.h:24,64` | Lease-based linearizable reads |
| `ConfigurationManager` | `src/braft/configuration_manager.h` | Tracks membership change history |

---

## 2. Leader Election

### 2.1 Pre-Vote Phase (Disruption Prevention)

braft implements **Pre-Vote** (from the Raft dissertation, Section 9.6) to prevent disruptive candidates from unnecessarily incrementing terms. Before starting a real election, a candidate sends `pre_vote` RPCs (`raft.proto:95`) without incrementing its term.

The flow in `NodeImpl`:
1. **Election timeout fires** -> `handle_election_timeout()` (`node.h:181`)
2. Calls `pre_vote()` (`node.h:277`) -- sends `RequestVoteRequest` via the `pre_vote` RPC
3. If a pre-vote quorum is achieved -> calls `elect_self()` (`node.h:282`)
4. `elect_self()` increments `_current_term`, votes for self, sends real `request_vote` RPCs
5. Votes are tracked in `VoteBallotCtx` (`node.h:448`) which wraps a `Ballot` object
6. When quorum is reached (`Ballot::granted()`) -> `become_leader()` (`node.h:260`)

### 2.2 Election Timers

braft uses four timers, all extending `RepeatedTimerTask`:

- **ElectionTimer** (`node.h:60`): Fires when a follower hasn't heard from the leader. Randomized via `random_timeout()` (`node.cpp:124`) to avoid split votes.
- **VoteTimer** (`node.h:66`): Detects if a candidate's election is stuck.
- **StepdownTimer** (`node.h:72`): Leader checks if a majority of followers are still reachable.
- **SnapshotTimer** (`node.h:77`): Periodic snapshot creation.

The heartbeat interval is derived from the election timeout:
```cpp
// node.cpp:130
heartbeat_timeout = election_timeout / raft_election_heartbeat_factor  // default factor=10
```

### 2.3 Disrupted Leader Detection

braft extends standard Raft with **disrupted leader** detection (`RequestVoteRequest.disrupted_leader` at `raft.proto:31`). When a follower receives a vote request and currently has a valid leader, it reports the leader as "disrupted." This helps handle asymmetric network partitions where a leader can reach some followers but not others.

### 2.4 Leader Lease

`LeaderLease` (`lease.h:24`) allows the leader to serve linearizable reads without an extra round of RPCs. The leader tracks the last time it confirmed a majority of followers were active (`_last_active_timestamp`). If within an election timeout window, reads can be served directly. `FollowerLease` (`lease.h:64`) tracks on the follower side whether the leader's lease is still valid before granting votes.

---

## 3. Log Replication

### 3.1 Write Path (Leader Side)

When a client calls `NodeImpl::apply()` (`node.h:134`):

1. The task is submitted to an **ExecutionQueue** (`_apply_queue`, `node.h:525`), a lock-free bthread-based queue
2. Tasks are **batched** -- up to `raft_apply_batch` (default 32) tasks are consumed at once in `execute_applying_tasks()` (`node.h:304`)
3. For each batch:
   - Log entries are appended to `LogManager` (which writes to the WAL via `SegmentLogStorage`)
   - A `Ballot` is appended to `BallotBox` for quorum tracking
   - `ReplicatorGroup` is notified to start replicating

### 3.2 Replicator (Per-Follower Replication)

Each follower gets its own `Replicator` (`replicator.h:82`), which runs in its own bthread. Key mechanics:

- **`_next_index`** (`replicator.h:240`): Tracks the next log index to send to this follower
- **`_send_entries()`** (`replicator.h:156`): Fills an `AppendEntriesRequest` with up to `raft_max_entries_size` (default 1024) entries, capped by `raft_max_body_size` (512KB)
- **Pipeline support**: `_append_entries_in_fly` (`replicator.h:250`) is a deque of `FlyingAppendEntriesRpc`, allowing up to `raft_max_parallel_append_entries_rpc_num` concurrent AppendEntries RPCs to the same follower
- **Heartbeats** are separate from data replication (`_send_empty_entries(true)` at `replicator.h:155`)

The Replicator state machine (`replicator.h:132`):
```
IDLE -> APPENDING_ENTRIES -> (response) -> IDLE
  |                                         ^
  +---> INSTALLING_SNAPSHOT ----------------+
  |
  +---> BLOCKING (on error, with backoff) --+
```

### 3.3 Quorum Commit (BallotBox)

`BallotBox` (`ballot_box.h:51`) tracks quorum votes:

1. When a Replicator receives a successful `AppendEntriesResponse`, it calls `BallotBox::commit_at(first_log_index, last_log_index, peer)` (`ballot_box.h:60`)
2. Each entry has a `Ballot` in `_pending_meta_queue` -- each `commit_at` grants the ballot
3. When a ballot reaches quorum (majority), `_last_committed_index` advances
4. `FSMCaller::on_committed()` is triggered to apply committed entries

### 3.4 Follower Side

When a follower receives `AppendEntriesRequest`:
1. `handle_append_entries_request()` (`node.h:164`) validates term, prev_log consistency
2. Entries are appended to local log storage
3. If `committed_index` in the request is ahead, `BallotBox::set_last_committed_index()` triggers FSM apply

**Out-of-order cache** (`node.h:410`): When `raft_enable_append_entries_cache=true`, followers can buffer out-of-order AppendEntries RPCs (from pipelining) and process them in order.

---

## 4. Log Storage

### 4.1 Segmented Append-Only Log

`SegmentLogStorage` (`log.h:139`) uses a segmented file design:

```
storage_path/
  log_meta                          # Metadata (first_log_index)
  log_000000000001-000000001000     # Closed segment (indices 1-1000)
  log_000000001001-000000002000     # Closed segment (indices 1001-2000)
  log_inprogress_000000002001       # Open segment (currently writing)
```

- **One open segment** for writes, rotated when it exceeds `raft_max_segment_size` (default 8MB)
- **All indices are in memory** (`_offset_and_term` vector in `Segment`, `log.h:129`) -- no disk seek needed for lookups
- **Entry format** (`log.cpp:78-92`): 64-bit term + 8-bit type + 8-bit checksum_type + 16-bit reserved + 32-bit data_len + 32-bit checksum + data
- **fsync policy**: Controlled by `raft_sync` (default true), `raft_sync_policy` (0=immediate, 1=by_bytes), `raft_sync_per_bytes`

### 4.2 Metadata Storage

`RaftMetaStorage` (`raft_meta.h`) persists `current_term` and `voted_for`:
- `FileBasedSingleMetaStorage`: One file per node
- `MixedMetaStorage`: Batched writes for multi-group scenarios (`raft_meta_write_batch`, default 128)

---

## 5. Snapshot Mechanism

### 5.1 Snapshot Creation

`SnapshotExecutor` (`snapshot_executor.h`) manages the lifecycle:
1. `SnapshotTimer` fires periodically (or manual trigger via `NodeImpl::snapshot()`)
2. `FSMCaller::on_snapshot_save()` calls the user's `StateMachine::on_snapshot_save()`
3. Snapshot metadata records `last_included_index`, `last_included_term`, and current configuration
4. Log entries up to the snapshot point can be truncated

### 5.2 Snapshot Transfer

When a follower is too far behind:
1. `Replicator::_install_snapshot()` sends `InstallSnapshotRequest` with a URI pointing to the snapshot files
2. The follower downloads files via `RemoteFileCopier` using brpc's file service
3. Bandwidth is throttled by `ThroughputSnapshotThrottle` (`snapshot_throttle.h`)
4. Controlled by flags: `raft_max_byte_count_per_rpc` (128KB chunks), `raft_enable_throttle_when_install_snapshot`, `raft_max_install_snapshot_tasks_num`

---

## 6. Membership Changes

braft implements **Joint Consensus** (two-phase membership change):

`ConfigurationCtx` (`node.h:331`) tracks stages:
```
STAGE_NONE -> STAGE_CATCHING_UP -> STAGE_JOINT -> STAGE_STABLE
```

1. **CATCHING_UP**: New peer starts replicating to catch up
2. **STAGE_JOINT**: A joint configuration entry C_{old,new} is committed. During this phase, both old and new quorums must agree (both `_peers` and `_old_peers` in `Ballot`, `ballot.h`)
3. **STAGE_STABLE**: The new configuration C_{new} is committed. Old servers can be safely removed

Configuration entries are stored as log entries with type `ENTRY_TYPE_CONFIGURATION` and both `peers` and `old_peers` fields.

---

## 7. State Machine Application

`FSMCaller` (`fsm_caller.h:106`) applies committed entries asynchronously:

- Uses a bthread `ExecutionQueue` for single-threaded, ordered execution
- Batches up to `raft_fsm_caller_commit_batch` (512) entries per apply round
- Exposes an `IteratorImpl` (`fsm_caller.h:42`) to the user's `StateMachine::on_apply()` -- the user iterates over committed entries and applies them

---

## 8. RPC Layer

Built entirely on brpc (`raft.proto`), the `RaftService` exposes 5 RPCs:

| RPC | Purpose |
|---|---|
| `pre_vote` | Pre-election check without term increment |
| `request_vote` | Actual vote request |
| `append_entries` | Log replication + heartbeat |
| `install_snapshot` | Snapshot transfer initiation |
| `timeout_now` | Leadership transfer (forces immediate election) |

Entry data is sent in brpc's **attachment** (zero-copy IOBuf), not in the protobuf body -- only `EntryMeta` is serialized in protobuf.

---

## 9. Concurrency Design

braft's concurrency model is noteworthy:

- **bthread (M:N threads)**: Lightweight coroutine-like threads from brpc, much cheaper than pthreads
- **ExecutionQueue**: Lock-free MPSC queue for batching (used in both apply path and FSM caller)
- **Per-Replicator threads**: Each follower has its own Replicator bthread -- no contention between followers
- **`BAIDU_CACHELINE_ALIGNMENT`**: Applied to `NodeImpl`, `Replicator`, `Segment`, `FSMCaller` to prevent false sharing
- **Atomic operations**: `_last_committed_index`, `_last_applied_index`, `_last_log_index` use atomics to avoid locks on hot reads
- **Parallel leader write + replication**: The leader writes to its own disk AND sends to Replicators concurrently (not sequentially)

---

## 10. Performance Tuning Guide

### A. Tuning Existing Configuration Flags

#### A.1 Enable Pipelining (Biggest Single Improvement)

```
--raft_max_parallel_append_entries_rpc_num=N  (default: 1)
--raft_enable_append_entries_cache=true       (default: false)
```

Setting `N > 1` allows multiple in-flight AppendEntries RPCs per follower, hiding network round-trip latency. Must enable the cache so followers can handle out-of-order arrivals. This can dramatically improve throughput on high-latency networks.

#### A.2 Increase Batch Sizes

```
--raft_max_entries_size=2048        (default: 1024, entries per RPC)
--raft_max_body_size=1048576        (default: 512KB, bytes per RPC)
--raft_apply_batch=64               (default: 32, tasks batched before writing)
--raft_leader_batch=512             (default: 256, leader IO batch)
--raft_fsm_caller_commit_batch=1024 (default: 512)
```

Larger batches amortize per-entry overhead (syscalls, serialization, fsync).

#### A.3 Relax fsync Policy

```
--raft_sync=false                  (default: true -- DANGER: data loss risk on crash)
--raft_sync_policy=1               (default: 0=immediate, 1=by_bytes)
--raft_sync_per_bytes=1048576      (default: INT32_MAX, bytes between syncs)
```

If your hardware has battery-backed write cache or you can tolerate data loss on crash, relaxing fsync is the single biggest throughput lever. `raft_sync_policy=1` with a reasonable `raft_sync_per_bytes` gives a middle ground.

#### A.4 Increase Segment Size

```
--raft_max_segment_size=67108864   (default: 8MB)
```

Larger segments reduce the frequency of segment rotation (close + rename + create), reducing file-system overhead.

#### A.5 Snapshot Tuning

```
--raft_do_snapshot_min_index_gap=10000  (default: 1, too aggressive)
```

Avoid taking snapshots too frequently. Each snapshot competes for disk I/O.

#### A.6 Enable Leader Lease for Reads

```
--raft_enable_leader_lease=true    (default: false)
```

Allows the leader to serve linearizable reads without an extra quorum check, reducing read latency significantly.

### B. Architectural Improvements (Code Changes)

#### B.1 Batch fsync (Group Commit)

**Current**: Each `Segment::append()` call can trigger an individual `fsync()`.
**Improvement**: Accumulate multiple entries and fsync once per batch. braft partially supports this via `raft_sync_policy=1`, but a true group commit where the leader batches all entries from a single apply round into one fsync would be more effective.

This is partially implemented via `raft_max_append_buffer_size` (256KB) in `log_manager.cpp:483`, but could be made more aggressive.

#### B.2 Asynchronous/Parallel fsync on Leader

**Current**: The leader writes to its log and replicates to followers concurrently (good). But the leader's local fsync is on the critical path.
**Improvement**: Treat the leader as just another "voter" -- start replication immediately (before local fsync completes), and count the leader's vote only when its fsync finishes. This way, if 2 of 3 followers respond before the leader's fsync, the entry can be committed immediately. This is the approach etcd/raft uses.

#### B.3 Follower Reads

**Current**: `LeaderLease` exists but all reads go through the leader.
**Improvement**: Implement **follower reads** -- followers can serve stale reads within a bounded staleness window, offloading the leader entirely.

#### B.4 Witness/Learner Replicas

**Current**: All replicas participate in quorum.
**Improvement**: Add "witness" nodes that participate in voting but don't store the full log/state machine. This allows maintaining fault tolerance with fewer full replicas, reducing the total disk I/O for a given replication factor.

#### B.5 Log Storage: Use io_uring or O_DIRECT

**Current**: Standard `write()` + `fsync()` system calls.
**Improvement**: On modern Linux (5.1+), use `io_uring` for asynchronous I/O to avoid syscall overhead. Alternatively, use `O_DIRECT` to bypass the page cache (relevant when the working set is large and cache pollution is a problem).

#### B.6 Pre-allocate Log Segments

**Current**: Segments grow as entries are written.
**Improvement**: Pre-allocate segment files (e.g., `fallocate()` the full `raft_max_segment_size`) so that writes don't trigger file system metadata updates. This removes inode allocation from the write path.

#### B.7 Zero-Copy Improvements

braft already uses brpc's `IOBuf` (scatter-gather I/O) to avoid copies in the RPC layer. Further improvements:
- Ensure `writev()` is used when writing multiple entries to a segment (batch write)
- Use `sendfile()` or `splice()` for snapshot transfers instead of read-into-buffer-then-write

#### B.8 Separate WAL and State Machine I/O

**Current**: WAL and snapshot typically share the same disk.
**Improvement**: Put WAL on a dedicated fast disk (NVMe) and snapshots/state machine on a separate disk. This prevents snapshot I/O from interfering with latency-sensitive WAL writes. braft already supports separate paths via `NodeOptions` (`log_uri` and `snapshot_uri`).

#### B.9 Compress Log Entries

For large payloads, compressing entries (e.g., LZ4, which is fast enough for inline use) before writing and replicating can reduce both disk I/O and network bandwidth. This could be added as an optional codec in `Segment::append()` and `Replicator::_prepare_entry()`.

#### B.10 Adaptive Batching

**Current**: Fixed batch sizes via gflags.
**Improvement**: Dynamically adjust batch sizes based on current load. Under high load, increase batch sizes to maximize throughput; under low load, reduce batching to minimize latency. The `ExecutionQueue` already provides natural batching -- the key is tuning the drain frequency.

### C. Impact Summary

| Optimization | Throughput Impact | Latency Impact | Risk |
|---|---|---|---|
| Pipeline replication (N>1) | High | Medium | Low |
| Increase batch sizes | High | Slight increase | Low |
| Relax fsync | Very High | Very High | Data loss on crash |
| Group commit / batch fsync | High | Medium | Low |
| Parallel leader fsync | Medium | Medium | Moderate (correctness) |
| Leader lease reads | -- | Very High for reads | Low |
| Follower reads | -- | High for reads | Stale reads |
| io_uring / O_DIRECT | Medium | Medium | Portability |
| Pre-allocate segments | Low-Medium | Low-Medium | Low |
| Log compression | Medium (large payloads) | Slight CPU cost | Low |

### D. Quick-Win Recommendations

The quickest wins for most deployments:

1. **Enable pipelining** (`raft_max_parallel_append_entries_rpc_num > 1`)
2. **Increase batch sizes** (apply_batch, leader_batch, max_entries_size)
3. **Enable leader lease** (`raft_enable_leader_lease=true`)

For write-heavy workloads, **group commit** and **relaxed fsync** provide the largest gains.

### E. Complete Flag Reference

| Flag | Default | File | Description |
|---|---|---|---|
| `raft_max_election_delay_ms` | 1000 | node.cpp:39 | Max random election delay |
| `raft_election_heartbeat_factor` | 10 | node.cpp:129 | Election:heartbeat timeout ratio |
| `raft_step_down_when_vote_timedout` | true | node.cpp:43 | Candidate steps down on timeout |
| `raft_enable_append_entries_cache` | false | node.cpp:47 | Cache out-of-order AppendEntries |
| `raft_max_append_entries_cache_size` | 8 | node.cpp:52 | Max cached out-of-order RPCs |
| `raft_apply_batch` | 32 | node.cpp:645 | Max tasks per apply batch |
| `raft_rpc_channel_connect_timeout_ms` | 200 | node.cpp:64 | RPC connection timeout |
| `raft_max_entries_size` | 1024 | replicator.cpp:32 | Max entries per AppendEntries |
| `raft_max_parallel_append_entries_rpc_num` | 1 | replicator.cpp:36 | Parallel replication RPCs |
| `raft_max_body_size` | 512KB | replicator.cpp:41 | Max RPC body size |
| `raft_retry_replicate_interval_ms` | 1000 | replicator.cpp:45 | Retry interval on failure |
| `raft_leader_batch` | 256 | log_manager.cpp:30 | Leader IO batch size |
| `raft_max_append_buffer_size` | 256KB | log_manager.cpp:483 | Append buffer size |
| `raft_fsm_caller_commit_batch` | 512 | fsm_caller.cpp:37 | FSM apply batch size |
| `raft_sync` | true | storage.cpp:31 | Call fsync on write |
| `raft_sync_meta` | false | storage.cpp:41 | Sync metadata separately |
| `raft_sync_per_bytes` | INT32_MAX | storage.cpp:33 | Bytes between syncs |
| `raft_sync_policy` | 0 | storage.cpp:38 | 0=immediate, 1=by_bytes |
| `raft_max_segment_size` | 8MB | log.cpp:48 | Max segment file size |
| `raft_sync_segments` | false | log.cpp:52 | Sync on segment close |
| `raft_do_snapshot_min_index_gap` | 1 | snapshot_executor.cpp:25 | Min gap between snapshots |
| `raft_max_byte_count_per_rpc` | 128KB | remote_file_copier.cpp:33 | Snapshot chunk size |
| `raft_enable_throttle_when_install_snapshot` | true | remote_file_copier.cpp:40 | Throttle snapshot transfer |
| `raft_max_install_snapshot_tasks_num` | 1000 | snapshot_throttle.cpp:32 | Max concurrent snapshot tasks |
| `raft_meta_write_batch` | 128 | raft_meta.cpp:31 | Metadata batch writes |
| `raft_enable_leader_lease` | false | lease.cpp:23 | Enable leader lease |
