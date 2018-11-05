[![Build Status](https://travis-ci.org/brpc/braft.svg?branch=master)](https://travis-ci.org/brpc/braft)

---

An industrial-grade C++ implementation of [RAFT consensus algorithm](https://raft.github.io/) and [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication) based on [brpc](https://github.com/brpc/brpc). braft is designed and implemented for scenarios demanding for high workload and low overhead of latency, with the consideration for easy-to-understand concepts so that engineers inside Baidu can build their own distributed systems individually and correctly.

It's widely used inside Baidu to build highly-available systems, such as:
* Storage systems: Key-Value, Block, Object, File ...
* SQL storages: HA MySQL cluster, distributed transactions, NewSQL systems ...
* Meta services: Various master modules, Lock services ...

# Getting Started

* Build [brpc](https://github.com/brpc/brpc/blob/master/docs/cn/getting_started.md) which is the main dependency of braft.

* Compile braft with cmake

  ```shell
  $ mkdir bld && cd bld && cmake .. && make
  ```

* Play braft with [examples](./example).

# Docs

* Read [overview](./docs/cn/overview.md) to know what you can do with braft.
* Read [benchmark](./docs/cn/benchmark.md) to have a quick view about performance of braft
* [Build Service based on braft](./docs/cn/server.md)
* [Access Service based on braft](./docs/cn/client.md)
* [Cli tools](./docs/cn/cli.md)
* [Replication Model](./docs/cn/replication.md)
* Consensus protocol:
  * [RAFT](./docs/cn/raft_protocol.md)
  * [Paxos](./docs/cn/paxos_protocol.md)
  * [ZAB](./docs/cn/zab_protocol.md)
  * [QJM](./docs/cn/qjm.md)

