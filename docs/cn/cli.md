braft提供了一系列API用来控制复制主或者具体节点, 可以选择在程序了调用[API](../../src/braft/cli.h)或者使用[braft_cli](../../tools/braft_cli.cpp)来给节点发远程控制命令

# API

```cpp
// Add a new peer into the replicating group which consists of |conf|.
// Returns OK on success, error information otherwise.
butil::Status add_peer(const GroupId& group_id, const Configuration& conf,
                       const PeerId& peer_id, const CliOptions& options);
// Remove a peer from the replicating group which consists of |conf|.
// Returns OK on success, error information otherwise.
butil::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                          const PeerId& peer_id, const CliOptions& options);
// Gracefully change the peers of the replication group.
butil::Status change_peers(const GroupId& group_id, const Configuration& conf, 
                           const Configuration& new_peers,
                           const CliOptions& options);
// Transfer the leader of the replication group to the target peer
butil::Status transfer_leader(const GroupId& group_id, const Configuration& conf,
                              const PeerId& peer, const CliOptions& options);
// Reset the peer set of the target peer
butil::Status reset_peer(const GroupId& group_id, const PeerId& peer_id,
                         const Configuration& new_conf,
                         const CliOptions& options);
// Ask the peer to dump a snapshot immediately.
butil::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                       const CliOptions& options);
```

# braft_cli

braft_cli提供了命令行工具, 作用和API类似

```shell
braft_cli: Usage: braft_cli [Command] [OPTIONS...]
Command:
  add_peer --group=$group_id --peer=$adding_peer --conf=$current_conf
  remove_peer --group=$group_id --peer=$removing_peer --conf=$current_conf
  change_peers --group=$group_id --conf=$current_conf --new_peers=$new_peers
  reset_peer --group=$group_id --peer==$target_peer --new_peers=$new_peers
  snapshot --group=$group_id --peer=$target_peer
  transfer_leader --group=$group_id --peer=$target_leader --conf=$current_conf
```
