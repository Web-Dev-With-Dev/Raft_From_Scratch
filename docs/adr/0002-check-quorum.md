# ADR 0002: CheckQuorum Leadership Lease Verification (§6.2)

## Status
Accepted

## Context

In basic Raft ([Ongaro & Ousterhout, 2014, §5.2](https://raft.github.io/raft.pdf)), a leader remains in the `Leader` state indefinitely until it explicitly receives an RPC request or response carrying a higher term ($T > \text{currentTerm}$).

This rule creates a critical vulnerability when network partitions occur:
1. Consider a 5-node cluster with nodes $\{N_1, N_2, N_3, N_4, N_5\}$, where $N_1$ is leader.
2. A network partition divides the cluster into two segments:
   - **Minority partition**: $\{N_1, N_2\}$
   - **Majority partition**: $\{N_3, N_4, N_5\}$
3. The majority partition detects the missing heartbeats, times out, and elects a new leader (e.g., $N_3$ at term 2).
4. If the partition is asymmetric or completely isolates the two partitions, $N_1$ in the minority partition receives no traffic from $\{N_3, N_4, N_5\}$.
5. As a result, $N_1$ never sees a message with term 2. **$N_1$ continues to believe it is the legitimate leader** indefinitely:
   - $N_1$ continues accepting client read queries (potentially returning stale, dirty data if lease-read checks do not verify quorum).
   - $N_1$ continues accepting client write operations, buffering them in its log, and holding client connections open while waiting for acknowledgments that will never arrive.
   - When the partition finally heals, multiple nodes claiming leadership flood the network, increasing post-partition stabilization latency.

## Decision

We implement the **CheckQuorum mechanism** ([Ongaro, 2014, §6.2](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)):

1. **Leader Contact Tracking**:
   - The leader tracks the last communication time for each peer: `lastHeardFrom[peerID] = time.Now()`.
   - Timestamps are updated whenever an affirmative response (heartbeat ACK or `AppendEntries` reply) is received from a follower.

2. **Periodic Quorum Evaluation**:
   - A periodic ticker (or check timer) fires every election timeout window ($T_{\text{check}} = \text{minElectionTimeout}$).
   - The leader computes the active peer count:
     $$\text{activeCount} = 1 \; (\text{self}) + \sum_{p \in \text{Peers}} \mathbb{I}\left( \text{Now}() - \text{lastHeardFrom}[p] < \text{electionTimeout} \right)$$
   - The leader checks whether `activeCount >= majority` where $\text{majority} = \lfloor N/2 \rfloor + 1$.

3. **Autonomous Step-Down**:
   - If `activeCount < majority`, the leader realizes it has lost connectivity to a quorum.
   - The leader voluntarily demotes itself to `Follower` via `becomeFollowerLocked(currentTerm)` **without waiting to observe a higher term**.
   - It stops its heartbeat ticker, clears its peer ack tracking, and starts its election timer.

```
       Leader Heartbeat Cycle
                 |
                 v
       +--------------------+
       |  Evaluate Contact  |
       |  with all Peers    |
       +---------+----------+
                 |
       activeCount >= majority?
             /        \
       YES  /          \  NO
           v            v
    +------------+  +--------------------------------+
    | Maintain   |  | Step Down to Follower (§6.2)   |
    | Leadership |  | - Stop heartbeat ticker        |
    +------------+  | - Transition state to Follower |
                    | - Reset election timer         |
                    +--------------------------------+
```

## What CheckQuorum Protects Against

1. **Zombie / Isolated Leaders**:
   - A partitioned leader automatically detects isolation within a single election timeout and relinquishes leadership.
   - Prevents stale leaders from lingering in split-brain states indefinitely.

2. **Stale Reads & Client Connection Starvation**:
   - Minority leaders cannot indefinitely hold client connections open.
   - Fast demotion returns errors or redirection hints to clients promptly, allowing clients to reconnect to the surviving majority.

3. **Accelerated Cluster Convergence Upon Partition Heal**:
   - Because the isolated former leader has already demoted itself to `Follower`, it does not compete or fight for leadership when network links resume.
   - Reintegration into the majority leader's stream is immediate and clean.

## Critical Interaction Invariant: CheckQuorum + Leader Stickiness

When `CheckQuorum` and `Stickiness` are both enabled, a subtle deadlock could occur upon partition heal if timestamps are handled incorrectly:
- **The Pitfall**: If demoting a leader (or updating a follower) refreshes the node's heartbeat timestamp (`lastHeartbeat = Now()`), the node falsely assumes an active leader lease is held. When the partition heals, nodes in the former minority partition reject all incoming `RequestVote` / `PreVote` requests from the true leader or candidate because they think they have an active leader.
- **The Invariant**: Only **authentic `AppendEntries` or heartbeat messages received from an active, recognized leader** update `lastLeaderHeartbeat`. Demotions, term bumps, and vote grants never fabricate a leader lease.

## Consequences & Trade-offs

- **Subtle Heartbeat Overhead**:
  - The leader must maintain consistent periodic heartbeats and track per-peer reply times.
  - Heartbeat interval must remain substantially shorter than the election timeout (e.g. $20\text{--}50\,\text{ms}$ vs $150\text{--}300\,\text{ms}$) to avoid false-positive step-downs caused by transient network jitter.
- **Configuration**:
  - Configurable via `SetCheckQuorum(bool)` and `WithCheckQuorum(bool)`. Enabled by default (`true`) for production safety.

