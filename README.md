[![Build Status](https://travis-ci.org/baidu/braft.svg?branch=master)](https://travis-ci.org/baidu/braft)

---

An industrial-grade C++ implementation of [RAFT consensus algorithm](https://raft.github.io/) and [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication) based on [brpc](https://github.com/brpc/brpc). braft is designed and implemented for scenarios demanding for high workload and low overhead of latency, with the consideration for easy-to-understand concepts so that engineers inside Baidu can build their own distributed systems individually and correctly.

It's widely used inside Baidu to build highly-available systems, such as:
* Storage systems: Key-Value, Block, Object, File ...
* SQL storages: HA MySQL cluster, distributed transactions, NewSQL systems ...
* Meta services: Various master modules, Lock services ...

# Getting Started

1. braft mainly depends on brpc, which depends on openssl, protobuffer,
	leveldb, gflags and glog, make sure you have all the dependencies installed
	before building braft

2. Build [brpc](https://github.com/brpc/brpc/blob/master/docs/cn/getting_started.md),
	once brpc built successfully you are just one small step away from building
	braft successfully

3. Compile braft with cmake, **remember to change paths to deps respectively**,
	the build result is in folder named `output`

	```shell
	$ mkdir build-dir && cd build-dir
	$ cmake .. \
		-DOPENSSL_ROOT_DIR:PATH=path/to/openssl \
		-DPROTOBUF_INCLUDE_PATH:PATH=path/to/protobuffer/include/ \
		-DPROTOBUF_LIB:PATH=path/to/protobuffer/lib/libprotobuf.a \
		-DLEVELDB_INCLUDE_PATH:PATH=path/to/leveldb/include \
		-DLEVELDB_LIB:PATH=path/to/leveldb/libs/libleveldb.a \
		-DGFLAGS_INCLUDE_PATH:PATH=path/to/gflags/include \
		-DGFLAGS_LIB:PATH=path/to/gflags/libs/libgflags.a \
		-DBRPC_INCLUDE_PATH:PATH=path/to/brpc/include \
		-DBRPC_LIB:PATH=path/to/brpc/lib/libbrpc.a \
		-DBRPC_WITH_GLOG=ON \
		-DGLOG_INCLUDE_PATH:PATH=path/to/glog/include \
		-DGLOG_LIB:PATH=path/to/glog/libs/libglog.a
	```

	in which:
	* glog is optional, but it's no harm to build with it
	* If there is any confusion about openssl, check [this issue](https://github.com/apache/incubator-brpc/pull/770)
		for more detail

4. Play braft with [examples](./example).

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

