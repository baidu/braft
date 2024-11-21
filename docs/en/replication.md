# REPLICATION

Chain replication is the most widely used replication model. After the data is replicated to all nodes, it responds to the client successfully. In the development process of chain replication, a variety of improved versions have been developed from the basic chain replication to improve replication delay.

## Basic chain copy

In the most primitive chain replication, starting from client writing, one node is required to write and then forward to the next node. In basic chain replication, the Tail node is the last update, and strong consistent replication can only be achieved by reading this Tail node.

The delay of the entire replication process is:

```
rtt1/2 + io1 + rtt2/2 + io2 + rtt3/2 + io3 + rtt4/2
```

In basic chain replication, slow IO on any node will cause replication stalls.



![img](../images/chain_rep.png)

## Improve chain copy

In the basic chain replication, the replication process requires each node to complete IO in turn. Among them, IO takes a lot of time, so there are many improved versions to optimize the replication delay. The replication model in hdfs is a typical improved chain replication, that is, each node only needs to wait until the downstream node has successfully written, and after the local write is successful, it can Ack upstream, and the Head node will reply to the Client with the successful write. , Read the Head node to achieve strong consistent replication.

The delay of the entire replication process is:

```
rtt1 + Max(io1, rtt2 + Max(io2, rtt3 + io3))
```

Although the improved chain replication improves the delay, it still requires all nodes to complete the writing before they can reply to the Client. Any slow IO on any node will cause replication to stall.

![img](../images/chain_rep2.png)

# Tree copy

In view of chain replication, all nodes need to complete writing before they can respond to the Client. Later, the industry has developed some other replication schemes, among which tree replication is the most typical one. The client writes to the primary first, and then the primary forwards to the secondary. If the primary write is successful and at least one secondary write is successful, it will perform an Ack to the client. There are several different implementations for the timing of Primary writing to the local: Primary writing to local and then copying to Secondary; Primary writing to local and copying to Secondary at the same time; Primary copying to Secondary first, and writing to the secondary after one of the responses Into the local.

Here we choose the way that Primary writes to local and replicates to Secondary at the same time with the best latency, where the latency of the replication process is:

```
rtt1 + Max(io1, Min(rtt2 + io2, rtt3 + io3))
```

Compared with chain replication, tree replication does not require all nodes to be successfully written, but only requires that most nodes including the primary node are successfully written. Therefore, only the IO of the primary node is slow to cause replication stalls.

In fact, tree replication can also be optimized. When the Primary receives two Secondary Secondary writes, it will respond to the Client. The Primary will no longer wait for unfinished writes, but directly apply the changes to the business, so that the Client can pass Primary reads the latest changes. Through this optimization, the slow replication lag problem of Primary node IO can be solved.

![img](../images/tree_rep.png)

# Distribution copy

In the above replication models, there is more or less the problem of slow nodes. Therefore, in some scenarios where replication delays are pursued, a replication solution that completely solves the slow nodes is required. Distributed replication is one of them. The nodes in the distribution and replication are all peer-to-peer, and the Client directly distributes and writes to each node, and there is no communication replication between the nodes. As long as the writing to most nodes is successful, the writing is judged to be successful.

The delay of the entire replication process is:

```
Median(rtt1 + io1, rtt2 + io2, rtt3 + io3)
```

Distributed replication solves the problem of slow nodes very well, but it also has great limitations, mainly facing two problems:

- You cannot have two Writers at the same time. Multiple Writers update at the same time is a paxos problem. Therefore, the external system generally selects a unique Writer to avoid multiple writes at the same time and ensure data consistency.
- Another problem is how to read the latest data. Because the nodes are all peer-to-peer and do not communicate with each other. Reader only reads all nodes, and can determine which nodes return data is valid and inefficient according to the version or index returned by the node; or the Writer selects the node that returned successfully last time for reading.

![img](../images/all_rep.png)

# Summarize

Do a simple comparison of the above several replication models, you can make corresponding choices according to the business model.

- Distributed copying has advantages only in the case of single writers, and the scope of use is relatively limited.
- Chain replication has higher throughput, but higher latency and there is no way to avoid slow nodes.
- Tree replication is a good compromise between throughput and delay.

| | Throughput | Latency | Slow Nodes | Multiple Writes | Real-time Reading                                               |
| --- | --- | --- | --- | --- |-----------------------------------------------------------------|
| Chain replication | 1 * Network card bandwidth | High | All nodes | Support | Head node or Tail node                                          |
| Tree replication | 1/2 * NIC bandwidth | Medium | Primary node | Support | Primary node                                                    |
| Distributed replication| 1/3 * Network card bandwidth| Low| None| Not supported| Read all nodes select majority, or be determined by Writer node |