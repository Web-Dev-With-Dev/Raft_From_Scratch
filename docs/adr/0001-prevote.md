# ADR 0001: PreVote Protocol Extension (§9.6)

## Status
Accepted

## Context

In basic Raft ([Ongaro & Ousterhout, 2014, §5.2](https://raft.github.io/raft.pdf)), when a follower's randomized election timer expires without receiving heartbeats or granting a vote, it immediately increments its `currentTerm`, transitions to `Candidate`, votes for itself, and broadcasts `RequestVote` RPCs to all peers.

While simple, this baseline protocol is vulnerable to the **Disruptive Server Problem** ([Ongaro, 2014, §9.6](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)):
1. Consider a 5-node cluster with leader $N_1$ running at term 1.
2. Suppose follower $N_5$ is network-partitioned away from the cluster.
3. While disconnected, $N_5$'s election timer repeatedly expires. Since it receives no heartbeats, $N_5$ continuously increments its term:
   $$1 \to 2 \to 3 \to \dots \to 15$$
4. When the network partition heals, $N_5$ reconnects and broadcasts `RequestVote` with term 15.
5. Per Raft Figure 2 rule *"If RPC request or response contains term $T > currentTerm$: set $currentTerm = T$, convert to follower"*, the healthy leader $N_1$ (and all other followers) observe $T = 15 > 1$ and are forced to immediately step down to `Follower`.
6. $N_5$ cannot actually win an election because its log is stale compared to the rest of the cluster (Raft §5.4.1 log completeness check). However, by forcing $N_1$ to step down, it unnecessarily disrupts cluster availability, aborts in-flight client writes, and triggers a spurious cluster-wide election.

## Decision

We introduce a speculative **PreVote phase** (§9.6) prior to transitioning to `Candidate` or incrementing `currentTerm`:

1. **State Machine Addition**:
   - Introduce `raft.PreCandidate` role.
   - When an election timer fires on a follower, the node transitions to `PreCandidate` instead of `Candidate`.
   - The node does **not** increment `currentTerm` or persist state to disk.

2. **PreVote RPC Broadcast**:
   - The node calculates a speculative term: $T_{spec} = \text{currentTerm} + 1$.
   - The node broadcasts a `RequestVoteArgs` with `PreVote = true` and `Term = T_{spec}`.

3. **PreVote Evaluation Rules (Receiver)**:
   - When `PreVote == true`, the receiver verifies:
     a. **Log Up-To-Date (§5.4.1)**: The candidate's log must be at least as up-to-date as the receiver's log.
     b. **Term Relevance**: The speculative term $T_{spec} \ge \text{currentTerm}$.
     c. **Leader Stickiness (§9.6)**: The receiver rejects the pre-vote if it has heard an `AppendEntries` or heartbeat from a valid leader within the minimum election timeout ($\Delta t < \text{minElectionTimeout}$).
   - **Crucial Invariant**: The receiver **does not update its `currentTerm` or `votedFor`** when evaluating a pre-vote request, regardless of $T_{spec}$.

4. **Transition to Real Election**:
   - If the pre-candidate receives pre-vote grants from a strict majority ($\lfloor N/2 \rfloor + 1$) of nodes, it transitions to `Candidate`, increments `currentTerm`, persists state, and initiates a standard Raft election.
   - If the pre-candidate fails to gather majority pre-votes before its election timer fires again, it starts another pre-vote cycle or reverts to `Follower`.

```
                    +-------------------+
                    |     Follower      |
                    +---------+---------+
                              | Election Timeout fires
                              v
                    +-------------------+
                    |   PreCandidate    | <---+
                    | (term NOT changed)|     | PreVote rejected
                    +----+---------+----+     | (timer expires)
                         |         |          |
         Majority Grants |         +----------+
                         v
                    +-------------------+
                    |     Candidate     |
                    | (term incremented)|
                    +-------------------+
```

## What PreVote Protects Against

1. **Disruptive Reconnecting Servers**:
   - An isolated or partitioned node whose election timer fires repeatedly cannot inflate its term counter.
   - Upon reconnecting, its pre-vote requests are rejected by peers because it lacks up-to-date logs and peers are receiving heartbeats from a healthy leader.
   - The healthy leader is never forced to step down.

2. **Transient Network Blips and Asymmetric Partitions**:
   - A node that momentarily loses connectivity cannot single-handedly bump cluster terms and force leader re-elections.
   - A node must prove it can communicate with a majority before triggering state transitions on other nodes.

3. **Spurious Disk Write Amplification**:
   - Because `currentTerm` and `votedFor` are not modified during PreVote, partitioned nodes do not trigger repetitive disk fsyncs on persistent state during disconnection loops.

## Consequences & Trade-offs

- **Slightly Longer Time-to-Elect on Actual Leader Failure**:
  - In the event of a real leader crash, an election requires two round-trips: one RTT for PreVote + one RTT for RequestVote.
  - Given standard local network RTTs ($< 1\,\text{ms}$), this additional delay is negligible compared to the election timeout duration ($150\text{--}300\,\text{ms}$).
- **Configuration**:
  - Configurable via `SetPreVote(bool)` and `WithPreVote(bool)`. Enabled by default (`true`) for production safety.

