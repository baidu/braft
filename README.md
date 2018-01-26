A fully-functional industrial-grade C++ implementation of [RAFT consensus algorithm](https://raft.github.io/) based on [brpc](https://github.com/brpc/brpc). 

braft is an approach of [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication). It is widely used inside Baidu to build highly available distributed systems, such as:

* Various kinds of distributed storage: block, file, object, NoSQL, etc.
* Highly available MySQL cluster.
* Distributed transaction and NewSQL system.
* Meta services in cloud computing to manage a large scale of clusters, virtual machines and containers.

braft is designed and implemented for high workload and high throughput scenarios. And it makes efforts to reduce overhead of latency. With braft you can easily build a distributed system just like writing a standalone server by defining your business logic as a [finite state machine](https://en.wikipedia.org/wiki/Finite-state_machine), and it's high performance as well.

# Getting Started

* Build [brpc](https://github.com/brpc/brpc/blob/master/docs/cn/getting_started.md) since braft is built upon [brpc](https://github.com/brpc/brpc/blob/master/docs/en/overview.md) and [bthread](https://github.com/brpc/brpc/blob/master/docs/cn/bthread.md)

* Compile braft with cmake

  ```shell
  $ mkdir build && cd build && cmake .. && make
  ```

* Play braft with [examples](./example).

# Docs

* Read [overview](./docs/cn/overview.md) to know what you can do with braft.
* Read [benchmark](./docs/cn/benchmark.md) to have a quick view about performance of braft
* [Build Service based on braft](./docs/cn/server.md)
* [Access Service baesd on braft](./docs/cn/client.md)
* [Cli tools](./docs/cn/cli.md)
* [Replication Model](./docs/cn/replication.md)
* Consensus protocol:
  * [RAFT](./docs/cn/raft_protocol.md)
  * [Paxos](./docs/cn/paxos_protocol.md)
  * [ZAB](./docs/cn/zab_protocol.md)
  * [QJM](./docs/cn/qjm.md)

