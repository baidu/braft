braft并不能直接被任何client访问， 本文主要是说明一个能访问braft节点的client需要那些要素。

# Example

[client side code](../../example/counter/client.cpp) of Counter

# 总体流程

要访问braft的主节点，需要做这么一些事情:

* 需要知道这个复制组有哪些节点， 这个可以通过配置列表，记录在dns，或者提供某些naming service如集群的master，redis, zookeeper, etcd等。
* 查询Leader位置
* 感知Leader变化
* 向Leader发起RPC.

## RouteTable

braft提供了[RouteTable](../../src/braft/route_table.h)功能，命名空间在braft::rtb, 可以帮助你的进程记录和追踪某个节点的主节点位置, 包含以下功能

```cpp
// Update configuration of group in route table
int update_configuration(const GroupId& group, const Configuration& conf);
int update_configuration(const GroupId& group, const std::string& conf_str);
// Get the cached leader of group.
// Returns:
//  0 : success
//  1 : Not sure about the leader
//  -1, otherwise
int select_leader(const GroupId& group, PeerId* leader);
// Update leader
int update_leader(const GroupId& group, const PeerId& leader);
int update_leader(const GroupId& group, const std::string& leader_str);
// Blocking the thread until query_leader finishes
butil::Status refresh_leader(const GroupId& group, int timeout_ms);
// Remove this group from route table
int remove_group(const GroupId& group);
```
