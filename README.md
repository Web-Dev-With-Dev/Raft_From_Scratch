# Raft Distributed Consensus

## Goal

Build a distributed key-value database implementing the Raft consensus algorithm.

---

## Components

### Client

Sends requests to the leader.

### Leader

Receives client requests and replicates logs.

### Followers

Receive replicated logs and acknowledge them.

### RPC Layer

Responsible for communication between nodes.

### Persistent Store

Stores:

- Current Term
- VotedFor
- Log Entries
- Snapshots

---

## Project Structure

cmd/
raft/
rpc/
server/
client/
store/

---

## Cluster Configuration

config.json

{
  "nodes": [
    {
      "id":1,
      "address":"localhost:8001"
    },
    {
      "id":2,
      "address":"localhost:8002"
    },
    {
      "id":3,
      "address":"localhost:8003"
    }
  ]
}

---

## Architecture & Design Notes

- [Why Single-Threaded Event Loop Avoids Raft Locking Bugs](docs/design/single-threaded-event-loop.md) - In-depth technical note explaining the actor-style event loop architecture, race prevention, and deadlock elimination.

## Features & Implementation Status

- [x] **Core Raft State Machine**: Follower, Candidate, and Leader states with randomized election timers.
- [x] **Single-Threaded Event Loop**: Actor-style concurrency model eliminating mutex lock contention and deadlocks.
- [x] **RPC Layer**: High-performance gRPC communication with connection pooling, keepalive, and retry backoff.
- [x] **Leader Election & Heartbeats**: Quorum-based elections, split-vote mitigation, and periodic heartbeats.
- [x] **State Persistence**: Pluggable `Storage` interface with atomic `Persist()` points at exact Raft specification transitions (`currentTerm`, `votedFor`, and log entries).
- [x] **Observability**: Live `/status` debug HTTP endpoint on every node and terminal cluster status monitor.
- [ ] Log Replication & State Machine Application (In Progress)
- [ ] Persistent Storage Engine (Month 4)
- [ ] Snapshotting & Compaction
- [ ] Cluster Membership Changes
