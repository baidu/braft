#Paxos Introduction

The problem solved by the Paxos algorithm is how a distributed system can agree on a certain value (resolution). A typical scenario is that in a distributed database system, if the initial state of each node is the same, and each node performs the same sequence of operations, then they can finally get a consistent state. In order to ensure that each node executes the same command sequence, a "consistency algorithm" needs to be executed on each instruction to ensure that the instructions seen by each node are consistent. A general consensus algorithm can be applied in many scenarios and is an important issue in distributed computing.

The two core principles to ensure consistency in Paxos are:

- The minority obeys the majority
- The latter agrees with the former

Paxos is divided into two categories:

- Basic Paxos: Also called single decree paxos, each time one or more nodes vote for a variable to determine the value of the variable.
- Multi Paxos: through a series of Basic Paxos instances to determine the value of a series of variables to achieve a Log

Paxos mainly meets the following two requirements:

- Safety: A variable can only be determined with a value, and a variable can only be learned after the value is determined
- Liveness: Some proposals will eventually be accepted; after a proposal is accepted, it will eventually be learned

The solution of Liveness is actually relatively simple. Each Proposer introduces a random timeout rollback, so that a certain Proposer can make a proposal first and successfully submit to the majority, then the problem of livelock can be solved.

Only the corresponding Proposer in Paxos knows the value of the proposal. If other Proposers want to read the value of the proposal, they also need to execute the proposal of Basic Paxos to get the value of the proposal.

# Basic Paxos

## Components

Paxos mainly includes two components: Proposer and Acceptor. Proposer actively initiates voting, and Acceptor passively receives votes and stores the value of the proposal. In the actual system, each Paxos Server contains these two components.

## Problem

In the Paxos proposal process, in order to solve the acceptor crash, the quorum response from multiple acceptors is required to be considered successful, so that after the response, as long as one node survives the proposal value, it is valid. However, multiple acceptors will have problems with Split Votes and Conflict Chose.

### Split Votes

If the Acceptor only accepts the first proposal value and considers that multiple Proposers propose a proposal at the same time, then it is possible that no Proposer will get a majority response.

![img](../images/split_votes.png)

At this time, the Acceptor needs to allow multiple different values ​​to be received to solve the Split Votes problem.

### Conflicting Choices

In order to solve Split Votes, the Acceptor can accept multiple different values. If the Acceptor wants to accept each proposal, then different Proposers may propose different values, which may be chosen, destroying the principle that each proposal has only one value.

![img](../images/conflict_choices.png)

At this time, the 2-phase protocol needs to be adopted. For the value that has been chosen, the subsequent proposal must propose the same value.

![img](../images/2pc_choice.png)

As shown in Figure S3, S1’s proposal should be rejected, so as to ensure that S5’s proposal is successful and S1’s proposal fails due to conflicts. The proposals need to be sorted so that the Acceptor can reject the old proposals.

### Proposal Number

Proposal can be uniquely marked by Proposal Number, which is convenient for Acceptor to sort.

A simple definition of Proposal Number is:

![img](../images/proposal_number.png)

Proposer saves maxRound, which represents the largest Round Number currently seen. Each time a Proposal is re-initiated, maxRound is increased, and the ServerId is spliced ​​to form the Proposal Number.

Proposer needs to persist maxRound to ensure that the previous maxRound will not be reused after the downtime and avoid generating the same Proposal Number.

## Flow

Paxos execution process includes two stages: Prepare and Accept. Among them, the Prepare stage is used to block the old proposals that are currently unfinished, and to discover the currently selected proposals (if the proposals have been completed or partially completed), the Accept process is used to actually submit the proposal, and the Acceptor needs to persist the proposal, but it needs The principle of ensuring that each proposal number only accepts one proposal. The specific process is as follows:
![img](../images/flow.png)

## Example

Let's discuss the actual operation of paxos for several situations. The most common is that the previous proposal has been completed, and then a proposer initiates a proposal later. Except for the change of the proposal number, the proposal value has not changed:
![img](../images/pp1.png)
When multiple proposers initiate proposals concurrently or the last proposer terminates abnormally, the proposal will be partially completed. If the new proposer sees the proposal value of the last proposer in the prepare phase, it will use it as its own proposal value, so that even The concurrent proposal of two proposers can still ensure that both proposers are successful and the values ​​are the same:
![img](../images/pp2.png)
In the case of the above concurrent proposal, if the new proposer does not see the proposal value of the previous proposal in the prepare stage, it will submit its own new proposal value, so that the old proposer will fail and accept the new proposal value:
![img](../images/pp3.png)

# Multi Paxos

Lamport did not describe the details of Multi Paxos in the paper. Intuitively, Multi Paxos adds an Index to each proposal to form a sequential Basic Paxos instance stream.

When receiving a request from the Client, the processing flow is as follows:

1. Find the first LogEntry without chosen
2. Run Basic Paxos for the Index corresponding to this LogEntry to make a proposal
3. Does Prepare return acceptedValue?
    1. Yes: finish choosing acceptedValue, skip to 1 to continue processing
    2. No: Use Client's Value for Accept

![img](../images/multi_paxos.png)

It can be seen from the above process that each proposal requires 2 RTTs in the optimal situation. When multiple nodes make proposals at the same time, the contention for index will be more serious, which will cause Split Votes. In order to solve Split Votes, the node needs to perform a random timeout rollback, so that the write delay will increase. To solve this problem, the following solutions are generally adopted:

1. Choose a Leader, there is only one Proposer at any time, which can avoid conflicts and reduce delay
2. Eliminate most of the Prepare, you only need to prepare the entire Log once, and most of the subsequent Logs can be chosen through an Accept

## Leader Election

For Leader Election, Lamport proposed a simple way: Let the node with the largest ServerId become the Leader.

Each node periodically sends heartbeats to other nodes. If a node does not receive the heartbeat of the node with the largest ServerId within 2T, it becomes a leader.

Leader will receive Client's request and act as Proposer and Acceptor; other nodes reject Client's request and only act as Acceptor.

## Eliminating Prepares

Before discussing the elimination of Prepare requests, let's discuss the role of Prepare:

1. Shield the old proposal: reject the old proposal, the scope is an Index
2. Return the value that may be chosen: When multiple Proposers perform concurrent Proposal, the new Proposal needs to ensure that the same value is proposed

In the Multi-Paxos process, to eliminate Prepare is to eliminate most of Prepare, but Prepare is still needed:

1. Block the old Proposal: Reject the old Proposal proposal, but the domain name is the entire Log, not a single Index
2. Return the value that may be chosen: Return the acceptedValue of the largest Proposal Number, and return noMoreAccepted when the following Proposal has no acceptedValue.

Once such an acceptor replies to Prepare with noMoreAccepted, Proposer does not need to send Prepare to it.

If Proposer gets the noMoreAccepted of most acceptors, the proposals behind Proposer do not need to send Prepare, so that each LogEntry only needs 1 RTT Accept to choose.

## Other

After selecting the master and eliminating Prepare, Multi Paxos is still not complete and needs to be resolved:

- Full Replication: All nodes need to replicate to get all Logs
- All Server Know about chosen value: All nodes need to know which values ​​in the Log are chosen
For Full Replication, the Leader can always retry the Accept request to ensure that the data on the Acceptor is as up-to-date as possible.

In order to notify each node of Chosen's Value, we have added some content:

- Each LogEntry has an acceptedProposal, which indicates the proposal number. Once it is Chosen, set it to ∞.
- Each Server maintains a firstUnChosenIndex, indicating the location of the first LogEntry that has not been Chosen.

When the Proposer sends an Accept request to the Acceptor, bring firstUnChosenIndex, so that when the Acceptor receives the Accept request, it can update the range of Chosen Value in the local Log:

- i <request.firstUnChosenIndex
- acceptedProposal[i] = request.proposal

The several methods discussed above are that during the leader’s survival period, the leader ensures that its proposal value is copied and Chosen as much as possible on all nodes. It is necessary to consider that after the leader fails, the new leader needs to make the previous leader as much as possible but not completed. Data is copied and Chosen.

- Acceptor returns its firstUnChosenIndex as Accept's response to Proposer
- Proposer judges if Acceptor.firstUnChosenIndex <local.firstUnChosenIndex, then asynchronously sends Success RPC.

Success(index, v) is used to notify the Acceptor that Chosen’s Value has been reached:

- acceptedValue[index] = v
- acceptedProposal[index]=∞

Acceptor returns firstUnChosenIndex to Proposer, and Proposer continues to send Success requests to notify other Chosen's Value.