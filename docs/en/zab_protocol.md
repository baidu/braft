# ZAB PROTOCOL

[ZAB](attachments/164310517/252150191.pdf) is the abbreviation of Zookeeper Atomic Broadcast protocol, which is an atomic broadcast protocol that supports crash recovery specially designed for the distributed coordination service Zookeeper. Its implementation is similar to that of Raft, which constructs a tree-shaped master-slave mode architecture to maintain data consistency among replicas in the cluster. A single main process is used to accept and process all client requests, and ZAB's atomic broadcast protocol is used to broadcast service status changes to other replica processes in the form of proposal. The ZAB protocol guarantees that only one master process in a cluster can broadcast proposals at any time, and all proposals are guaranteed to be submitted in an orderly manner among replicas.

All transaction requests in ZooKeeper are processed by a master server, that is, the leader. The other servers are followers. The leader converts the client's transaction requests into transaction proposals, and distributes the proposal to all other followers in the cluster, and then the leader waits for the follower Feedback, when more than half of the followers (>=N/2+1) feedback information, Leader will broadcast Commit information to Followers in the cluster again, and Commit means submitting the previous proposal.

# Introduction to ZAB Agreement

## state

Each process in the ZAB protocol has three states:

- **Looking**: The system is in the election state when it is just started or after the leader crashes
- **Following**: The status of the Follower node, and the Follower and Leader are in the data synchronization phase
- **Leading**: The state of the leader, there is a leader in the current cluster as the main process

When ZooKeeper starts, the initial state of all nodes is Looking. At this time, the cluster will try to elect a Leader node, and the elected Leader node will switch to the Leading state; when the node finds that the leader has been elected in the cluster, the node will switch to the Following state, and then Keep in sync with the Leader node; when the follower node loses contact with the leader, the follower node will switch to the Looking state and start a new round of elections; in the entire life cycle of ZooKeeper, each node will continuously switch between the Looking, Following, and Leading states .

## Persistent data

- **history**: The log of the transaction proposal received by the current node
- **acceptedEpoch**: The leader’s proposal that the follower has accepted to change the epoch’s NEWEPOCH
- **currentEpoch**: the current epoch
- **lastZxid**: The zxid of the most recently received proposal in history (the largest)

> In the Zxid design of the ZAB protocol transaction number, Zxid is a 64-bit number, of which the lower 32 bits is a simple monotonically increasing counter. For each transaction request of the client, the counter is incremented by 1; while the upper 32 bits represent The number of the leader's period epoch. Each time a new leader server is elected, the ZXID of the largest transaction in its local log will be taken from the leader server, and the epoch value will be read from it, and then incremented by 1, as a new epoch , And count the lower 32 bits from 0.
>
> epoch: Similar to the term in Raft, it indicates the version of the leader, and each time the leader changes, it will be upgraded, so that it can be used to rejoin the old leader before the fence crashes and recovers.

## Agreement process

The ZAB protocol mainly includes two processes: message broadcasting and crash recovery, which can be further subdivided into discovery, synchronization, and broadcast phases. Each distributed process that makes up the ZAB protocol executes these three phases cyclically. . After each node is started, it enters the discovery phase, which is actually a process of selecting the master. If the master selection is successful, it enters the leader state, otherwise, it may find that the current leader joins the cluster. Once the node is abnormal, it will enter the discovery phase.

![img](../images/zab_stats.jpg)

### information

The above are the three core workflows of the ZAB protocol. The messages exchanged between the processes during the whole process are explained as follows:

- CEPOCH: The follower process sends the epoch value of the last transaction Proposal that it has processed to the quasi-Leader
- NEWEPOCH: The quasi-Leader process generates a new round of epoch value according to the received epoch of each process
- ACK-E: The Follower process feeds back the NEWEPOCH message sent by the quasi-Leader process
- NEWLEADER: The quasi-Leader process determines its own leadership position and sends NEWLEADER messages to each process
- ACK-LD: The follower process feeds back the NEWLEADER message sent by the leader process
- COMMIT-LD: require the follower process to submit the corresponding historical proposal
- PROPOSAL: Leader process generates a Proposal for client transaction request
- ACK: The follower process feeds back the PROPOSAL message sent by the leader process
- COMMIT: Leader sends a COMMIT message, requiring all processes to submit transaction PROPOSE

### Phase 0: Leader election

The nodes are in the election phase at the beginning, as long as one node gets more than half of the votes of the nodes, it can be elected as the quasi-leader. Only after reaching the Phase 3 quasi-leader will it become the real leader. The purpose of this stage is to select a quasi-leader and then enter the next stage.

The agreement does not specify a detailed election algorithm. We will refer to the Fast Leader Election used in the implementation later.

### Phase 1: Discovery

At this stage, the leader is the election process, which is used to elect a quasi-Leader process among multiple distributed processes. At this time, the quasi-leader may not be able to successfully elect the leader, and any node failure will trigger the leader election process to join the cluster.

The implementation of zab is not based on RPC, but a socket-oriented approach. After the election starts, the node will establish a connection with other nodes, and send its own epoch and lastZxid, and other nodes reply currentEpoch (cepoch). The quasi-Leader collects Follower's cepoch, generates newEpoch according to max(cepoch)+1, and then notifies other nodes to update its acceptEpoch. During the newEpoch notification process, Follower will feed back its historical transaction proposal collection. In this way, the quasi-leader can determine which of the most nodes in the current cluster has the latest data.

![img](../images/zab_discovery.png)

### Phase 2: Synchronization

The synchronization phase mainly uses the latest proposal history obtained by the leader in the previous phase to synchronize all replicas in the cluster. The quasi-Leader first determines the latest data node with the historical proposal collection of each follower node discovered in the Discovery stage, and then synchronizes with it.

After the quasi-Leader completes the latest data synchronization, it will notify other nodes that it has been elected as the new leader through NewLeader and synchronize data to it. Only when the quorums are completed synchronously, the quasi-leader will become the real leader. A follower will only accept proposals with a zxid greater than its own lastZxid.

![img](../images/zab_sync.png)

**Here, only when the quasi-Leader synchronizes the data of the majority of nodes to a consistent state, the NewLeader will be sent to the majority of nodes, and the majority of nodes will update their local currentEpoch. This is different from the way the term node increases by itself in raft. **

### Phase 3: Broadcast

At this stage, the leader can provide services to the outside world and broadcast messages. The process of data synchronization is similar to a 2PC. The Leader generates a transaction proposal from the client's request and sends it to Followers. After most followers respond, the Leader sends Commit to all Followers for submission.

![img](../images/zab_broadcast.jpg)

The specific algorithm is described as follows: e'is the new epoch, Q is the quorum set, and f is the follower.

![img](../images/zab_broadcast_algo.png)

# Protocol implementation

The implementation of the Java version of the protocol is somewhat different from the definition above. Fast Leader Election (FLE) is used in the election phase, which includes the discovery responsibilities of Phase 1. Because FLE will elect the node with the latest proposal history as the leader, this saves the step of discovering the latest proposal. The actual implementation merges Phase 1 and Phase 2 into the Recovery Phase. Therefore, the realization of ZAB has only three stages:

- Fast Leader Election
- Recovery Phase
- Broadcast Phase

## Fast Leader Election

FastLeaderElection is the implementation of the standard Fast Paxos. It first proposes to all servers that it wants to become the leader. When other servers receive the proposal, it resolves the conflict between epoch and zxid, accepts the other party's proposal, and then sends the other party a message that the proposal is completed. . The FastLeaderElection algorithm collects votes from other nodes through asynchronous communication, and at the same time, performs different processing according to the current state of the voter when analyzing the votes, so as to speed up the leader election process. FLE will elect the node with the latest proposal history (the largest lastZxid) as the leader, which saves the step of discovering the latest proposal. This is based on the premise that the node with the latest proposal also has the latest submission record.

**Conditions for becoming a leader**

- Choose the largest epoch
- epochs are equal, choose the one with the largest zxid
- Both epoch and zxid are equal, choose the one with the largest server id (that is, myid in zoo.cfg we configure)

Nodes vote for themselves by default at the beginning of the election. When receiving votes from other nodes, they will change their votes according to the above conditions and resend the votes to other nodes. When a node has more than half of the votes, the node will set its own The status is leading, and other nodes will set their status to following.

![img](../images/zab_fast_election.png)

### Data recovery phase

Each ZooKeeper Server reads the current disk data (transaction log) and obtains the largest zxid.

### Send votes

Each ZooKeeper Server participating in the voting sends its recommended Leader to other Servers. This protocol includes several parts of data:

- The recommended Leader id. In the initial stage, for the first time, all servers voted to elect themselves as Leaders.
- The maximum zxid value on the recommended node. The larger the value, the newer the data of the server.
- logicalclock. This value increases from 0, and each election corresponds to a value, that is, in the same election, this value is the same. The larger the value, the newer the election process.
- The state of the machine. Including LOOKING, FOLLOWING, OBSERVING, LEADING.

### Processing votes

After each server sends its own data to other servers, it also has to accept votes from other servers and do some processing.

**If Sender's status is LOOKING**

- If the sent logicalclock is greater than the current logicalclock. It means that this is an updated election, the logicalclock of the machine needs to be updated, and the votes that have been collected are cleared at the same time, because the data is no longer valid. Then determine whether you need to update your election situation. First judge the zxid, the bigger zxid wins; if the leader id is the same, the bigger wins.
- If the sent logicalclock is smaller than the current logicalclock. It means that the other party is in an earlier election process and only needs to send the data of the local machine.
- If the sent logicalclock is equal to the current logicalclock. Update the votes according to the received zxid and leader id, and then broadcast it.

After the server has processed the votes, it may be necessary to update the status of the server:

- Determine whether the server has collected the election status of all servers. If you set your own role (FOLLOWING or LEADER) based on the results of the election, then withdraw from the election.
- If you have not received the election status of not all servers, you can also judge whether the election leader updated after the above process is supported by more than half of the servers. If it is, then try to receive the data within 200ms. If there is no heart data coming, it means that everyone has agreed with the result. At this time, set the role and withdraw from the election.

**If Sender's status is FOLLOWING or LEADER**

- If the LogicalClock is the same, save the data to recvset. If Sender claims to be the leader, then judge whether more than half of the servers elect it, if it is to set the role and withdraw from the election.
- Otherwise, this is a message inconsistent with the current LogicalClock, indicating that there has been an election result in another election process, so the election result is added to the OutOfElection set, and the election is judged based on OutOfElection whether the election can be ended, and if so Save the LogicalClock, update the role, and withdraw from the election.

## Recovery Phase

At this stage, the followers send their lastZixd to the leader, and the leader decides how to synchronize data based on the lastZixd. The implementation here is different from the previous Phase 2: Follower will suspend the proposal after L.lastCommittedZxid when it receives the TRUNC instruction, and will accept the new proposal when it receives the DIFF instruction.

The Leader in ZAB will use a variety of synchronization strategies based on the follower data:

- SNAP: If the follower data is too old, the leader will send a snapshot SNAP command to the follower to synchronize the data
- DIFF: Leader sends the proposal DIFF data from Follower.lastZxid to Leader.lastZxid to Follower to synchronize data
- TRUNC: When Follower.lastZxid is greater than Leader.lastZxid, Leader sends TRUNC instruction from Leader.lastZxid to Follower.lastZxid to let Follower discard the data

SNAP and DIFF are used to ensure the consistency of committed data on the Follower nodes in the cluster, and TRUNC is used to discard data that has been processed but not yet committed. Follower receives SNAP/DIFF/TRUNC instructions to synchronize data with ZXID, and sends ACK-LD to Leader after successful synchronization, and Leader will add it to the list of available Followers.

![img](../images/zab_recovery_algo.png)

The specific message flow is as follows:

![img](../images/zab_recovery_phase.png)
