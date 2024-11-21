[![Build Status](https://travis-ci.org/baidu/braft.svg?branch=master)](https://travis-ci.org/baidu/braft)

---

# Overview
An industrial-grade C++ implementation of [RAFT consensus algorithm](https://raft.github.io/) and [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication) based on [brpc](https://github.com/brpc/brpc). braft is designed and implemented for scenarios demanding for high workload and low overhead of latency, with the consideration for easy-to-understand concepts so that engineers inside Baidu can build their own distributed systems individually and correctly.

It's widely used inside Baidu to build highly-available systems, such as:
* Storage systems: Key-Value, Block, Object, File ...
* SQL storages: HA MySQL cluster, distributed transactions, NewSQL systems ...
* Meta services: Various master modules, Lock services ...

# Getting Started

* Build [brpc](https://github.com/brpc/brpc/blob/master/docs/en/getting_started.md) which is the main dependency of braft.

* Compile braft with cmake
  
  ```shell
  $ mkdir bld && cd bld && cmake .. && make
  ```

* Play braft with [examples](./example).

* Installing from vcpkg
  
  You can download and install `braft` using the [vcpkg](https://github.com/Microsoft/vcpkg) dependency manager:
  ```sh
  git clone https://github.com/Microsoft/vcpkg.git
  cd vcpkg
  ./bootstrap-vcpkg.sh
  ./vcpkg integrate install
  ./vcpkg install braft
  ```
  The `braft` port in vcpkg is kept up to date by Microsoft team members and community contributors. If the version is out of date, please [create an issue or pull   request](https://github.com/Microsoft/vcpkg) on the vcpkg repository.

# Docs

* Read [overview](./docs/en/overview.md) to know what you can do with braft.
* Read [benchmark](./docs/en/benchmark.md) to have a quick view about performance of braft
* [Build Service based on braft](./docs/en/server.md)
* [Access Service based on braft](./docs/en/client.md)
* [Cli tools](./docs/en/cli.md)
* [Replication Model](./docs/en/replication.md)
* Consensus protocol:
  * [RAFT](./docs/en/raft_protocol.md)
  * [Paxos](./docs/en/paxos_protocol.md)
  * [ZAB](./docs/en/zab_protocol.md)
  * [QJM](./docs/en/qjm.md)

# Discussion

* Add Weixin id ***zhengpf__87*** or ***xiongk_2049*** with a verification message '**braft**', then you will be invited into the discussion group. 
