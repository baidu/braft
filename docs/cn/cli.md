braft提供了一系列API用来控制复制主或者具体节点, 可以选择在程序了调用[API](../../src/braft/cli.h)或者使用[braft_cli](../../tools/braft_cli.cpp)来给节点发远程控制命令

#API

```cpp
// Add a new peer into the replicating group which consists of |conf|.
// Returns OK on success, error infomation otherwise.
butil::Status add_peer(const GroupId& group_id, const Configuration& conf,
                       const PeerId& peer_id, const CliOptions& options);

// Remove the peer from the replicating group which consists of |conf|.
// Returns OK on success, error infomation otherwise.
butil::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                          const PeerId& peer_id, const CliOptions& options);

// Reset the configuration of the given peer
butil::Status set_peer(const GroupId& group_id, const PeerId& peer_id,
                       const Configuration& new_conf, const CliOptions& options);

// Trigger snapshot of the peer
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
  set_peer --group=$group_id --peer==$target_peer --conf=$target_conf
  snapshot --group=$group_id --peer=$target_peer
```