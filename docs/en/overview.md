# OVERVIEW

# Distributed consistency

Distributed consensus is the most basic problem in a distributed system. It is used to ensure the reliability and disaster tolerance of a distributed system. Simply put, it is how to reach agreement on a certain value among multiple machines, and when the agreement is reached, this value can remain unchanged no matter what failures occur between these machines afterwards.

In the abstract definition, all processes in a distributed system must determine a value v. If the system satisfies the following properties, it can be considered that it solves the distributed consistency problem, which are:

- Termination: All normal processes will determine the specific value of v, and there will be no processes that have been circulating.
- Validity: The value v'determined by any normal process, then v'must be submitted by a certain process. For example, random number generators do not satisfy this property.
- Agreement: The values ​​selected for all normal processes are the same.

# Consistency state machine

For an infinitely growing sequence a[1, 2, 3…], if for any integer i, the value of a[i] satisfies distributed consistency, the system meets the requirements of a consistent state machine.

Basically all systems will operate continuously. At this time, it is not enough to agree on a specific value alone. In order to ensure the consistency of all copies of the real system, the operation is usually converted to [write-ahead-log](https://en.wikipedia.org/wiki/Write-ahead_logging)(WAL for short). Then let the system All copies are consistent with the WAL, so that each process executes the operations in the WAL in order to ensure that the final state is consistent.

![img](../images/distributed_state_machine.png)

# RAFT

RAFT is a new and easy-to-understand distributed consensus replication protocol, proposed by Diego Ongaro and John Ousterhout of Stanford University [http://wiki.baidu.com/download/attachments/142056196/raft.pdf?version= 1&modificationDate=1457941130000&api=v2), as the central coordination component in the [RAMCloud](https://ramcloud.atlassian.net/wiki/display/RAM/RAMCloud) project. Raft is a Leader-Based Multi-Paxos variant. Compared with Paxos, Zab, View Stamped Replication and other protocols, it provides a more complete and clear protocol description, as well as a clear description of node additions and deletions.

As a replication state machine, Raft is the core and most basic component of a distributed system. It provides commands for orderly replication and execution among multiple nodes. When the initial states of multiple nodes are consistent, it ensures that the states are consistent between nodes. The system can process normally as long as most nodes survive. It allows message delay, discarding and disorder, but does not allow message tampering (non-Byzantine scenario).

![img](../images/raft.png)

Raft can solve the CP in distributed theory, that is, consistency and partition tolerance, but it cannot solve the problem of Available. It contains some common functions in distributed systems:

- Leader Election
- Log Replication
- Membership Change
- Log Compaction

# What RAFT can do

The consistent state machine provided by RAFT can solve the problems of replication, repair, node management, etc., greatly simplifying the design and implementation of current distributed systems, allowing developers to focus only on business logic and abstract it into a corresponding state Machine. Based on this framework, many distributed applications can be built:

- Distributed lock service, such as Zookeeper
- Distributed storage systems, such as distributed message queues, distributed block systems, distributed file systems, distributed table systems, etc.
- Highly reliable meta-information management, such as HA of various Master modules

# Why do BRAFT

Many of the current company's systems either have a single point of issue, or have replication data security issues, or replication consistency issues, or replication delay issues. These problems make it difficult to develop and maintain many systems, and often affect business development. The RAFT algorithm can solve the above problems to a large extent. The high-performance CP replication framework can solve the problems of replication consistency and delay. For availability, we can make some consistency compromises in the design, and provide multiple copy reads to achieve high Available.

Since the RAFT protocol came out in 2013, a lot of [implementations](http://raft.github.io/) have emerged in the community, but most of them are experimental and lack functions such as Membership Changes and Log Compaction. A few more reliable implementations are part of the specific Service implementation, and are not encapsulated into a common basic library form. Most of the RAFT implementations use a threaded network model, that is, the connection between a peer is maintained by one thread, and the processing of multi-threaded calls is relatively rough, which is not suitable for maintaining a large number of RAFT replication instances in one process.

A good RAFT algorithm implementation can shield details from the upper layer, free developers from complex exception handling, focus on their own business logic, and build a distributed system like writing a stand-alone program. Although the RAFT algorithm itself is known to be easy to understand, it still has to face complex exception handling and concurrent events to achieve correctness. All race conditon and ABA problems must be properly resolved, and at the same time, sufficient performance must be ensured. While braft guarantees correctness and high performance, it also needs to ensure that the interface is sufficiently simple and easy to use.

# Supported features of BRAFT

* Leader election.
* Replication and recovery.
* Snapshot and log compaction.
* Membership management.
* Fully concurrent replication.
* Fault tolerance.
* Asymmetric network partition tolerance.
* Workaround when quorate peers are dead.