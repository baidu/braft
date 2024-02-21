

# Foreword

Almost all frameworks, modules, and libraries will regard high performance as one of the most important tags (of course, braft is no exception). However, developers often only stay on the digital surface of throughput or QPS for performance understanding, and performance testing has become a game of trying to brush numbers, regardless of whether the scene meets the actual application scenario. There are two common methods of "increasing performance numbers":

* **Batch:** The system proactively waits for the number of requests to reach a certain number or waits for a timeout period to synthesize a request and send it to the back-end system. Depending on the value of batch_size / request_size, the "QPS" can be increased by tens to hundreds of times (This point will be explained in detail later).
* **Client unrestricted asynchronous sending**: This keeps the system in a high load state without any waiting, avoiding some system calls and thread synchronization overhead.

Although these designs can run good benchmark data, they completely deviate from the actual application scenarios. Take batch as an example. The problem with batch is that it does not essentially guide how to improve the QPS of the system. In the performance test, the concurrency is usually very high, and there will be a steady stream of requests coming in, so each request does not need to wait long to meet the batch size condition, but in real scenarios, the concurrency is not so high , This will cause each request to have to "wait" a set value, latency cannot reach the optimal solution. At this time, engineers are often immersed in tuning parameters such as optimizing timeouts and batch sizes, thereby ignoring the truly meaningful things such as analyzing system bottlenecks. In addition, with the gradual improvement of hardware performance, the delay of the network and the disk itself is getting shorter and shorter. This mechanism cannot take into account the delay under low load and the throughput of high load.

In braft, we mainly used the following methods to improve performance:

- The data stream is fully concurrent. The leader writes to the local disk and replicates data to the follower is fully concurrent.
- Improve locality as much as possible, and give full play to the role of caches at different levels
- Isolate access to different hardware as much as possible, and improve throughput through pipelines
- Reduce the size of the lock critical section as much as possible, and use lock-free/wait-free algorithms on the critical path.

# testing scenarios

The performance of different user state machines varies greatly. In this scenario, we first strip off the impact of the user state machine. Multiple threads from the client write logs of a certain size to the server through synchronous RPC, and the server calls the libraft interface to submit logs to the replication group. When the server receives the event that the raft log is successfully submitted, it replies to the client and collects the throughput and delay data on the client. The comparison group is that fio writes to the local disk in the same thread number order. Through this test, we hope to see that, After using raft, compared to writing to local disks, how much performance data the application will sacrifice in exchange for higher data security.

If there is no special instructions, the default client thread is 100, and fsync is turned on by default for writing files.

## vs FIO (2016.3)

fio test command (where -bs=xxx will be replaced with the size corresponding to the test):

```shell
./fio -filename=./10G.data -iodepth 100 -thread -rw=write -ioengine=libaio --direct=1 -sync=1 -bs=512 -size=10G -numjobs=1 -runtime=120- group_reporting -name=mytest
```



**hardware information**

CPU: 12 cores, Intel(R) Xeon(R) CPU E5-2620 v2 @ 2.10GHz

Disk: LENOVO SAS 300G, random write IOPS~=800, sequential write throughput~=200M/s

Network card: 10 Gigabit network card, multi-queue network card is not turned on

Number of copies: 3

![img](../images/benchmark0.png)

## vs Logcabin(2017.2)

Since there are few RAFT implementations that can be used alone in the industry, we have made some adjustments to Logcabin, adding the rpc service and replying to the client immediately after the log synchronization is completed (the patch part will be open source soon). And we have counted the QPS on the client side and the specific data As shown below.

![img](../images/benchmark.png)