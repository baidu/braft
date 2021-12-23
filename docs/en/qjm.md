# Introduction to QJM Protocol

QJM is an introduction to QuorumJournalManager, and is the default HA scheme for namenode in Hadoop V2. The qjm solution is simple, with only two components: journal node and libqjm. The qjm solution is not responsible for the selection of the master, and the selection of the master is implemented externally, for example, based on zookeeper. libqjm is responsible for the reading and writing of journal data, including the consistency recovery of journal under abnormal conditions; journalnode is responsible for the storage of log data.

![img](../images/qjm_overview.png)

QJM's Recovery algorithm is a basic paxos implementation, used to negotiate and confirm the content of the latest in progress log segment (the actual operation is to confirm the txid range in the log segment), because the master has been selected, the edit log written can be in accordance with Multi -Paxos optimizes, skips the Prepare phase in basic paxos and directly enters the Accept phase, that is, writes n+1 nodes to 2n+1 nodes and returns success, which reduces the delay of writing data content agreement. The delay is: median(Latency @JNs).

The above description is very simple, but there are many very interesting questions below:

1. When a JN receives edit but other nodes do not receive it, what should I do if the namenode hangs?
2. How to deal with "split brain" when both namenodes declare that they are active?
3. How to recover from an inconsistent state when multiple nodes hang up during the writing process?
4. Namenode hangs during writing or finialized process, how to recover from inconsistent state?
5. If the standy namenode hangs during the failover process, how to recover from the inconsistent state?

The QJM distributed protocol is based on the implementation of paxos, which can solve the above problems and ensure the consistency of data between nodes. Specifically, use Multi-Paxos to commit each batch of edit logs, and use Paxos to perform recovery when a standy namenode is used for failover.

<http://blog.cloudera.com/blog/2012/10/quorum-based-journaling-in-cdh4-1/>

The overall QJM process:

1. Fencing prior writers
2. Recovering in-progress logs
3. Start a new log segment
4. Write edits
5. Finalize log segment
6. Go to step 3

## Fencing

Fencing is the antidote to the "split brain" problem in distributed systems. The new active namenode can ensure that the old active namenode will not modify the system meta-information. The key to Fencing in QJM is *epoch number*, which has the following attributes:

- When a writer becomes active, an epoch number needs to be assigned
- Each epoch number is unique, and two writers will not have the same epoch number
- Epoch number defines the order of the writer, the larger the epoch number, the newer the writer

The realization process of Epoch number is as follows:

- Before starting to write the edit log, QJM first generates an epoch number
- QJM sends a newEpoch request to all journalNodes, and brings the generated epoch number. Only when most JournalNodes respond successfully, it is considered successful
- JournalNode receives newEpoch and persists it to the local lastPromisedEpoch
- Any RPC that modifies the edit log needs to bring the epoch number
- When JournalNode processes non-newEpoch requests, it checks the epoch number in the request and the local lastPromisedEpoch. If the epoch in the request is small, reject it directly; if the epoch in the request is large, update the local lastPromisedEpoch. In this way, even if a JN hangs up and there is a new writer in the process, the lastPromisedEpoch can be updated. [There is no master to join the node and set the latest epoch number]



The above strategies can guarantee that once QJM receives a majority of successful responses from newEpoch(N), then any writer with an epoch less than N will not successfully write to the majority node. In this way, the split-brain problem of the two namenodes can be solved.

The lastPromisedEpoch above is mainly used for fencing and affects AcceptedEpoch (promisedEpoch in each round will definitely increase, and when preparing, take the then-promisedEpoch as AcceptEpoch). In addition to lastPromisedEpoch, JournalNode also needs a lastWriterEpoch, which is used to compare the unfinialized edits data version during recovery.

There is a problem in the above process that is not explained clearly, that is how QJM sets the initial epoch number, there is a process for generating epoch:

1. QJM sends getJournalState() to all JournalNodes, and journalNode returns the local lastPromisedEpoch.
2. When QJM receives most responses, select the maximum value and add 1 as proposedEpoch.
3. QJM sends newEpoch (proposedEpoch) to all JournalNodes. The JournalNode is compared with the local lastPromisedEpoch. If the proposed is large, the local lastPromisedEpoch is updated and success is returned, otherwise it fails.
4. If QJM receives a majority of responses, it sets the epoch number to proposedEpoch; otherwise, it throws an exception and terminates the startup.

## Recovery

### Invariant

Before discussing recovery, some system-dependent invariants should be discussed:

- Once the log is finialized, it is not unfinialized
- If a segment starts with N, it must contain finialized segments ending with N-1 on most nodes
- If a finialized segment ends with N, it must contain finialized segments ending with N on most nodes

These system-dependent invariants will affect the strategy selected by the recovery source later.

### Recovery algorithm

When a new writer is started, the previous writer may still leave some log segments in the in progress state. The new writer needs to recover and finialize the log segment in progress before writing the new edit log. Therefore, the job of recovery is to ensure that each JN agrees on the logsegment in progress and finialize. The Recovery algorithm is a Paxos process that uses the principle that the minority obeys the majority and the latter agrees with the former to ensure data consistency in progress.

The specific Recovery algorithm is as follows:

- Determining which segment to recover:

Each JN will return the transaction id of the latest log segment when responding to newEpoch.

- PrepareRecovery RPC:

QJM sends a PrepareRecovery request to each JN to obtain the status of the last log segment, including the length and whether it is finialized. If the journal id status on JN is accepted, then the epoch of the last accepted writer is returned.

This request and response correspond to the Prepare (Phase 1a) and Promise (Phase 1b) of paxos

- AcceptRecovery RPC:

After QJM receives the response from PrepareRecovery, the new writer selects a JN as the source for synchronization, which must contain previously committed transactions. The AcceptRecovery RPC contains the status of the segment and the URL of the source.

AcceptRecovery corresponds to Phase 2a in Paxos, usually called Accept.

When JN receives AcceptRecovery, it performs the following operations:

1. Log Synchronization: If there is no local or the length is different, download the corresponding log segment from the source to the local.
2. Persist recovery metadata: JN persists the status of the segment (the start and end id and status of the segment) and the epoch of the current writer to the disk, so that when the PrepareRecovery request of the same segment id is requested, these metadata are responded to.

- Finalize segment:

At this time, most JournalNodes already have the same log segment, and all have persistent recovery metadata, even if there is PrepareRecovery later, they will eventually get the same result. Finally, simply call FinializeLogSegment.

### Recovery Source selection

From the above Recovery algorithm, it can be seen that the core of data security is that after receiving the PrepareRecovery response, QJM selects the Recovery Source. The selection rules are as follows:

1. If a JN does not have a corresponding log segment, it cannot be used as a source
2. If a JN has a corresponding finialized log segment, it means that the last round of Recovery has reached the Finalize segment stage or Recovery is not needed. This JN should be used as source
3. If there are multiple JNs with corresponding in progress log segments, they need to be compared as follows:

a) For each JN, take the maximum value of lastWriterEpoch and AcceptEpoch in the PrepareRecovery response as maxSeenEpoch

b) If there is a JN whose maxSeenEpoch is greater than other JNs, then select this JN as the source

c) If maxSeenEpoch is the same, select JN with larger transaction id (have more edit log) as the source

[Discussion] Why choose the largest transaction id here instead of choosing the smallest value among the longest n+1 copies like AFS? Because the logSync and log two interfaces are provided in hdfs, log is an asynchronous interface. It may happen that the user has been replied but not written to the backend. At this time, try to restore more edit logs to reduce log loss. risk.

## Write Edits

When a batch of edits starts to be written, QJM will send a request to all JN nodes at the same time, and JN will perform the following operations when receiving the journal request:

1. Check whether the epoch number in the request is consistent with lastPromisedEpoch
2. Check whether the txid of edits in the request is empty or out of order with the local txid
3. Write journal to local disk and sync
4. Return success

QJM will wait for the response from the majority of JNs, and return success only if the majority of JN nodes respond successfully. If a JN hangs up or fails to return, it will be marked as "out of sync", and subsequent journal requests for the current log segment will no longer be sent to it.

## Read Edits

Only the finialized log segment can be read in QJM, because QJM has ensured that the content of the finialized log segment has been agreed on most JNs. If you want to read the log segment in progress, although the first half of the log segment data may have been agreed, the overall content has not been agreed. QJM does not know the location of the consensus point and needs to read most of the JN, according to the size of epoch and txid It can be determined whether the data has reached agreement on most JNs, which is similar to the Recovery algorithm flow, and the implementation is more complicated.

When currently QJM is reading, it first sends a getEditLogManifest() to all JNs to obtain the finialized segment view, JN exposes the http interface to obtain the finialized log segment data, and QJM establishes RedundantEditLogInputStreams for reading.