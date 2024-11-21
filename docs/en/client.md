# CLIENT

Braft cannot be directly accessed by any client. This article mainly explains what elements are needed for a client that can access the braft node.

# Example

[client side code](../../example/counter/client.cpp) of Counter

# Overall process

To access the master node of the braft, you need to do some things:

* Need to know which nodes the replication group has. This can be recorded in dns through the configuration list, or provide certain naming services such as cluster master, redis, zookeeper, etcd.
* Query Leader location
* Perceive Leader change
* Initiate RPC to Leader.

## RouteTable

Braft provides the [RouteTable](../../src/braft/route_table.h) function, the namespace is in braft::rtb, which can help your process record and track the location of the master node of a node, including the following functions

``` cpp
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