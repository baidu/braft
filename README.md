A fully-functional industrial-grade C++ implementation of [RAFT consensus algorithm](https://raft.github.io/) based on [brpc](https://github.com/brpc/brpc). 

braft is an approach of [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication). It is widely used inside Baidu to build highly available distributed systems, such as:

* Various kinds of distributed storage: block storage, file system, object storage, NoSQL, etc.
* Highly avaiable MySQL cluster.
* NewSQL and distributed transaction.
* Meta Service for managing clusters, virtual machines and containers.

braft is designed and implemented for high workload and high throughput scenarios. And it makes efforts to reduce overhead of latency. With braft you can easily build a distributed system just like writing a standalone service by defining your business logic as a state machine, and it's also high performanced.

# Getting Started

* braft is built up brpc and bthread, build [brpc](https://github.com/brpc/brpc/blob/master/docs/cn/getting_started.md) before build brpc

* Compile brpc with cmake

  ```shell
  $ mkdir build && cd build && cmake .. && make
  ```

* Play braft with [examples](./example)

#Docs

* Read [overview](./docs/cn/overview.md) to know what you can do with braft.
* Read benchmark to have a quick look about performance of braft
* Consensus Protocols:
  * raft
  * paxos
  * Zab
* ​
* Client

  