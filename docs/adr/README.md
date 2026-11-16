# Architecture Decision Records (ADR)

This directory documents key architectural decisions, safety protocols, and consensus extensions implemented in this Raft system.

## Index

| ADR | Title | Raft Section | Key Threat Mitigated | Status |
|:---:|---|:---:|---|:---:|
| [ADR 0001](file:///c:/Users/devgo/Desktop/Raft/docs/adr/0001-prevote.md) | **PreVote Protocol Extension** | §9.6 | Disruptive reconnecting servers & term inflation | Accepted |
| [ADR 0002](file:///c:/Users/devgo/Desktop/Raft/docs/adr/0002-check-quorum.md) | **CheckQuorum Leadership Lease Verification** | §6.2 | Zombie minority leaders & stale reads | Accepted |

---

## Defense-in-Depth Comparison

Standard Raft ([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)) ensures safety (Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety), but vanilla Raft leaves availability and operational stability vulnerable to asymmetric network partitions and transient blips.

Our implementation employs a three-pillar defense-in-depth architecture:

| Mechanism | Direction of Protection | Threat Model | Resolution / Defense |
|---|---|---|---|
| **PreVote** (§9.6) | **Partitioned Follower $\to$ Healthy Leader** | An isolated node repeatedly increments its term, reconnects, and forces a healthy leader to step down despite having a stale log. | Node must gather majority pre-votes speculatively *before* incrementing its term or starting a real election. |
| **CheckQuorum** (§6.2) | **Partitioned Leader $\to$ Cluster** | A leader isolated in a minority partition continues believing it is leader indefinitely because it never receives messages with a higher term. | Leader monitors contact timestamps from peers and autonomously steps down to Follower if it loses majority contact. |
| **Leader Stickiness** (§9.6) | **Stale Node $\to$ Active Leader Lease** | Network blips or lagging followers broadcast vote requests while a legitimate leader is actively heartbeating. | Followers ignore vote requests if they heard an authentic heartbeat from a valid leader within `minElectionTimeout`. |

```
                       +-----------------------------------+
                       |          Raft Node State          |
                       +-----------------+-----------------+
                                         |
               +-------------------------+-------------------------+
               | (When Leader)                                     | (When Follower)
               v                                                   v
     +-------------------+                               +-------------------+
     |    CheckQuorum    |                               |      PreVote      |
     |      (§6.2)       |                               |      (§9.6)       |
     +---------+---------+                               +---------+---------+
               | Lost majority                                     | Election timeout
               v contact                                           v fires
     [Autonomously demote                                [Gather speculative
      to Follower]                                        majority pre-votes
                                                          without term bump]
                                                                   |
                                                                   v
                                                         +-------------------+
                                                         |  Leader Stickiness|
                                                         |      (§9.6)       |
                                                         +-------------------+
                                                         [Reject vote if
                                                          active leader lease
                                                          is held]
```

