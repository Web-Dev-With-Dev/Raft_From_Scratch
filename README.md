# Raft Distributed Consensus Engine

A production-grade, fault-tolerant distributed consensus implementation in Go based on the Raft algorithm ([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)). 

This implementation prioritizes correctness, formal protocol invariants, and deterministic concurrency by utilizing an actor-style single-threaded event loop, high-performance gRPC communication, pluggable state persistence, and built-in cluster observability.

---

## Table of Contents

- [Overview](#overview)
- [Architecture and Concurrency Model](#architecture-and-concurrency-model)
- [Core Invariants and Features](#core-invariants-and-features)
- [Repository Structure](#repository-structure)
- [Cluster Configuration](#cluster-configuration)
- [Building and Running](#building-and-running)
- [Cluster Observability and Monitoring](#cluster-observability-and-monitoring)
- [Testing and Verification](#testing-and-verification)
- [Deterministic Simulation & Reproducing Failures](#deterministic-simulation--reproducing-failures)
- [Protocol Conformance and Roadmap](#protocol-conformance-and-roadmap)
- [References](#references)

---

## Overview

Raft is a consensus algorithm designed to manage replicated logs in a fault-tolerant manner. It decomposes consensus into distinct subproblems: leader election, log replication, and safety.

Rather than relying on widespread multi-threaded mutex synchronization across RPC handlers, this implementation isolates consensus state transitions inside a single-threaded event loop. All state mutations (`currentTerm`, `votedFor`, node roles, and timer lifecycles) are serialized, eliminating race conditions, lock inversion deadlocks, and atomicity gaps during network I/O.

---

## Architecture and Concurrency Model

### Single-Threaded Event Loop (Actor Pattern)

A primary failure mode in multi-threaded Raft implementations is holding a mutex during outbound RPC calls or dropping a mutex across network boundaries, which exposes intermediate or stale states.

In this architecture, incoming network requests, timer expirations, and control signals are transformed into discrete events dispatched over Go channels into a central event loop (`s.run()`):

```
+---------------------------------------------------------------------------------+
|                                gRPC Worker Pool                                 |
|  RequestVote()                  AppendEntries()               /status HTTP      |
+---------+------------------------------+---------------------------+------------+
          |                              |                           |
          | (Event Channel)              | (Event Channel)           | (Read Lock)
          v                              v                           v
+-------------------+          +--------------------+       +-----------------+
|  requestVoteCh    |          |  appendEntriesCh   |       |  /status API    |
+---------+---------+          +---------+----------+       +--------+--------+
          \                              |                          /
           \                             |                         /
            +----------------------------v------------------------+
            |                                                     |
            |              Single-Threaded Event Loop             |
            |                       (s.run)                       |
            |                                                     |
            |    select {                                         |
            |    case <-s.ElectionTimer.C:                        |
            |        s.handleElectionTimeout()                    |
            |    case <-s.heartbeatTimer.C:                       |
            |        s.handleHeartbeatTick()                      |
            |    case ev := <-s.requestVoteCh:                    |
            |        ev.ResCh <- s.processRequestVote(ev.Args)    |
            |    case ev := <-s.appendEntriesCh:                  |
            |        ev.ResCh <- s.processAppendEntries(ev.Args)  |
            |    case act := <-s.stateActionCh:                   |
            |        act.fn(); act.resCh <- struct{}{}            |
            |    case <-s.stopCh:                                 |
            |        return                                       |
            |    }                                                |
            +-----------------------------------------------------+
                                         |
                                         v
                         [ Guaranteed Serial Mutations ]
                         - s.PersistentState (currentTerm, votedFor)
                         - s.State (Follower, Candidate, Leader)
                         - s.Storage.Persist() checkpoints
```

### Concurrency Guarantees

- **No Lock Inversion**: Network I/O and consensus state mutation are strictly decoupled. The event loop never executes synchronous blocking network operations while mutating local state.
- **Linearizable State Mutations**: Transitions between `Follower`, `Candidate`, and `Leader` execute sequentially without intermediate state exposure.
- **Strict Persistence Timing**: In accordance with Raft §5.2 and §5.6, state mutations (`currentTerm`, `votedFor`) are written to the storage interface synchronously *before* responding to peer RPCs.
- **Zero Goroutine Leaks**: All channel operations select on cancellation contexts (`ctx.Done()`) and lifecycle termination signals (`stopCh`), ensuring clean teardown under any load.

For an in-depth analysis, read the technical design note:
- [Why Single-Threaded Event Loop Avoids Raft Locking Bugs](docs/design/single-threaded-event-loop.md)

---

## Core Invariants and Features

- **State Machine**: Full implementation of Follower, Candidate, and Leader states with strict role transition validation.
- **Randomized Election Timers**: Randomized timeouts (150ms to 300ms default) using high-resolution monotonic timers to prevent split-vote deadlocks.
- **gRPC Transport Layer**: Protocol Buffer definitions (`proto/raft.proto`) backed by an active connection pool (`rpc.ClientPool`), supporting automatic reconnects, exponential backoff, and HTTP/2 keepalives.
- **Heartbeat Mechanism**: Automatic ticker activation upon election to Leader, broadcasting periodic heartbeats to maintain authority and prevent peer timeouts.
- **Atomic Persistence**: Formal `store.Storage` contract (`Persist()`, `Load()`, `Close()`) with in-memory deep-copy reference implementation and planned pluggable disk-backed engine.
- **Cluster Observability**: Dedicated `/status` HTTP debug interface per node and a real-time polling monitor for terminal inspection.

---

## Repository Structure

```
.
|-- Makefile                   # Build, test, and cluster orchestration targets
|-- cluster.json               # Cluster topology specification (node IDs and addresses)
|-- cmd/
|   `-- node/
|       `-- main.go            # Node binary entrypoint and lifecycle coordinator
|-- config/
|   |-- config.go              # Configuration parsing, validation, and peer resolution
|   `-- config_test.go         # Configuration unit tests
|-- docs/
|   `-- design/
|       `-- single-threaded-event-loop.md  # Concurrency and locking design note
|-- logger/
|   |-- logger.go              # Structured logging utility with node ID prefixes
|   `-- logger_test.go         # Logger formatting tests
|-- proto/
|   |-- raft.proto             # Protocol Buffer service and message definitions
|   |-- raft.pb.go             # Protobuf generated structures
|   `-- raft_grpc.pb.go        # gRPC client and server stubs
|-- raft/
|   |-- types.go               # Protocol domain types (NodeID, Term, State, etc.)
|   |-- election_timer.go      # Randomized timer abstractions
|   `-- election_timer_test.go # Monotonic bound and reset tests
|-- rpc/
|   |-- client.go              # Single peer gRPC connection wrapper
|   |-- client_pool.go         # Multi-peer pool manager with retry policies
|   |-- client_pool_test.go    # Pool connection and retry test suite
|   `-- types.go               # Internal RPC argument and reply structures
|-- scripts/
|   |-- cluster.sh             # Linux/macOS multi-node launcher and stopper
|   |-- cluster.ps1            # Windows PowerShell multi-node launcher
|   |-- poll_status.go         # Cross-platform live status polling tool
|   |-- poll_status.sh         # Shell wrapper for status monitoring
|   `-- poll_status.ps1        # PowerShell wrapper for status monitoring
|-- server/
|   |-- server.go              # Main Raft node server, event loop, and RPC handlers
|   `-- server_test.go         # Comprehensive unit, race, and leak test suite
`-- store/
    |-- storage.go             # Storage interface and in-memory engine
    `-- storage_test.go        # Persistence and deep-copy isolation tests
```

---

## Cluster Configuration

A cluster is defined using a JSON topology file (default: `cluster.json`):

```json
{
  "nodes": [
    {
      "id": 1,
      "address": "127.0.0.1:8001"
    },
    {
      "id": 2,
      "address": "127.0.0.1:8002"
    },
    {
      "id": 3,
      "address": "127.0.0.1:8003"
    }
  ]
}
```

Each node automatically reads the configuration file, binds to its designated address, and computes the list of remote peers.

---

## Building and Running

### Prerequisites

- Go 1.22 or higher
- Protocol Buffers compiler (`protoc`) and Go plugins (only required when modifying `proto/raft.proto`)
- Make (optional, for automation commands)

### Compilation

Build the node binary:

```bash
# Using Makefile
make build

# Or directly via Go toolchain
go build -o bin/node.exe ./cmd/node
```

### Running Individual Nodes

Nodes can be started individually by passing flags:

```bash
# Start Node 1 (gRPC on 127.0.0.1:8001, debug HTTP on 127.0.0.1:9001)
./bin/node -id 1 -config cluster.json

# Start Node 2 (gRPC on 127.0.0.1:8002, debug HTTP on 127.0.0.1:9002)
./bin/node -id 2 -config cluster.json

# Start Node 3 (gRPC on 127.0.0.1:8003, debug HTTP on 127.0.0.1:9003)
./bin/node -id 3 -config cluster.json
```

#### Command-Line Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-id` | `1` | Integer ID of this node within the cluster topology. |
| `-config` | `cluster.json` | Path to the cluster topology JSON file. |
| `-status-addr` | `127.0.0.1:9000+ID` | Address for debug HTTP endpoint. Set to `disabled` to turn off. |

### Running a Local Cluster

#### Using Make / Bash (Linux/macOS)
```bash
# Start a 3-node cluster in the background
make run-cluster

# Stop all background cluster processes
make stop-cluster
```

#### Using PowerShell (Windows)
```powershell
# Start a 3-node cluster
.\scripts\cluster.ps1 start

# Check running cluster processes
.\scripts\cluster.ps1 status

# Stop the cluster
.\scripts\cluster.ps1 stop
```

---

## Cluster Observability and Monitoring

### Debug HTTP Endpoint (`/status`)

Each node exposes its current consensus state via HTTP GET on port `9000 + ID`:

```bash
curl http://127.0.0.1:9001/status
```

Response payload:
```json
{
  "id": 1,
  "term": 4,
  "state": "Leader",
  "votedFor": 1,
  "logLength": 0
}
```

### Real-Time Terminal Status Monitor

A dedicated utility polls all nodes concurrently and displays a formatted tabular summary every second:

```bash
# Run the Go monitor tool
go run scripts/poll_status.go

# Or run via shell / PowerShell scripts
./scripts/poll_status.sh
.\scripts\poll_status.ps1
```

Example monitor output:

```
[2026-11-14 12:15:00] Polling 3 node status endpoint(s)...
+---------+------+-----------+----------+------------+---------+
| NODE ID | TERM | STATE     | VOTEDFOR | LOG LENGTH | STATUS  |
+---------+------+-----------+----------+------------+---------+
| 1       | 4    | Leader    | 1        | 0          | ONLINE  |
| 2       | 4    | Follower  | 1        | 0          | ONLINE  |
| 3       | 4    | Follower  | 1        | 0          | ONLINE  |
+---------+------+-----------+----------+------------+---------+
Summary: 3/3 online | Leader: Node 1 (Term 4)
```

---

## Testing and Verification

The test suite covers unit behavior, randomized timer jitter, client pool retry semantics, persistence contracts, and concurrency safety.

### Running Tests

```bash
# Run all tests
go test -v ./...

# Run tests with the Go data race detector
go test -race ./...

# Force uncached execution
go test -race -count=1 ./...
```

### Goroutine Leak Verification

A dedicated leak test (`TestServer_NoGoroutineLeak` in `server/server_test.go`) initializes and tears down gRPC listeners, HTTP servers, and internal event loops across multiple cycles, asserting via `runtime.NumGoroutine()` that all goroutines terminate cleanly without lingering tasks.

---

## Deterministic Simulation & Reproducing Failures

The cluster includes a fully deterministic discrete-event simulation engine driven by `FakeClock` and `SimNetwork`. All node election timers, randomized timeout jitter, message delays, and network packet drops are derived from a single integer seed.

Because execution is free from real wall-clock timing variations, **any failure discovered during parallel fuzzing can be reproduced with 100% bit-for-bit fidelity**.

### Running Simulation Scenarios (`simrunner` CLI)

Run scenario sweeps across thousands of seeds in parallel using the `simrunner` CLI:

```bash
# Run 1,000 seeds on the basic election scenario in parallel
go run ./cmd/simrunner -scenario election -n 1000

# Run a specific seed range with custom worker concurrency
go run ./cmd/simrunner -scenario crash -from 1 -to 500 -workers 8

# List available scenarios (election, crash, partition)
go run ./cmd/simrunner -list
```

#### CLI Flags Reference

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-scenario` | `election` | Target scenario: `election`, `crash`, `partition` |
| `-n` | `1000` | Number of seeds to evaluate starting from `-from` |
| `-from` | `1` | First seed in the test sequence |
| `-to` | `0` | Optional explicit upper-bound seed (overrides `-n`) |
| `-workers` | `runtime.NumCPU()` | Number of parallel worker goroutines |
| `-trace-dir` | `./traces` | Directory where failure traces are saved |
| `-seed` | `0` | Run a single seed in verbose mode with event logging |
| `-replay` | `""` | Path to a trace JSON file to replay |
| `-list` | `false` | Display all registered scenarios |

### Failure Detection and Trace Generation

When an invariant violation or unexpected condition occurs (such as an election safety violation where multiple leaders are elected in the same term, or a failover timeout), the runner:

1. Immediately outputs the failing seed number, scenario name, and error description.
2. Emits the exact replay command for instant reproduction.
3. Serializes a comprehensive diagnostic trace to `./traces/<scenario>_seed_<seed>.json`.

Example failure output:
```text
[FAIL] Seed: 472 | Error: election safety violated: multiple leaders in term 2 (nodes 1 and 3)
       Trace:  ./traces/election_seed_472.json
       Replay: go run ./cmd/simrunner -scenario election -seed 472
```

#### Trace File Structure

Saved trace files (`./traces/*.json`) contain:
- Execution metadata (scenario name, seed, duration, status, error).
- Final node statuses (`ID`, `State`, `Term`, `VotedFor`, `IsKilled`).
- Complete chronological consensus event history from each node's `EventLog` ring buffer.
- The exact CLI command to replay the trial.

### Reproducing a Failure

There are three ways to reproduce and diagnose a failing seed:

#### 1. Replay by Seed via CLI (Recommended)

Run `simrunner` with the `-seed` flag. This executes the scenario deterministically for that single seed and prints the full post-mortem consensus event log for every node:

```bash
go run ./cmd/simrunner -scenario election -seed 472
```

Sample output:
```text
=== Running Scenario "election" (Seed: 472) ===
[FAIL] Seed 472 failed: election safety violated: multiple leaders in term 2 (nodes 1 and 3)

--- Node 1 [Leader] (Term=2, VotedFor=1, Events=4) ---
  [18:00:00.120] Term=0 server_started     server node 1 initialized
  [18:00:00.135] Term=1 election_started   started election for term 1
  [18:00:00.150] Term=2 election_started   started election for term 2
  [18:00:00.165] Term=2 state_transition   transitioned to Leader for term 2

--- Node 2 [Follower] (Term=2, VotedFor=1, Events=3) ---
  [18:00:00.120] Term=0 server_started     server node 2 initialized
  [18:00:00.155] Term=2 vote_granted       granted vote to candidate 1 for term 2

--- Node 3 [Leader] (Term=2, VotedFor=3, Events=4) ---
  [18:00:00.120] Term=0 server_started     server node 3 initialized
  [18:00:00.150] Term=2 election_started   started election for term 2
  [18:00:00.165] Term=2 state_transition   transitioned to Leader for term 2
```

#### 2. Replay from a Trace File

If a failure was captured during automated CI runs, download the trace JSON and replay it directly:

```bash
go run ./cmd/simrunner -replay ./traces/election_seed_472.json
```

#### 3. Reproduce in a Go Unit Test

You can write or invoke a targeted Go test with the specific seed in `server/scenario_test.go`:

```go
func TestReproduce_Seed472(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    trace, err := server.RunScenario(ctx, "election", 472, "./traces")
    if err != nil {
        t.Fatalf("seed 472 failed as expected: %v (trace saved to %s)", err, trace.ReplayCmd)
    }
}
```

Run with standard Go testing tools and debugger support:
```bash
go test -v -run TestReproduce_Seed472 ./server
```

---

## Protocol Conformance and Roadmap

| Capability | Specification Section | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Node Roles and Transitions** | Raft §5.1, §5.2 | Implemented | Follower, Candidate, and Leader states |
| **Randomized Election Timers** | Raft §5.2 | Implemented | 150ms-300ms randomized bounds |
| **Leader Election and Quorum** | Raft §5.2 | Implemented | Majority vote collection and step-down |
| **Heartbeat Broadcasting** | Raft §5.2 | Implemented | Automatic periodic ticker on election |
| **State Persistence** | Raft §5.2, §5.6 | Implemented | Invoked on `currentTerm` and `votedFor` changes |
| **HTTP Debug State Surface** | Operational | Implemented | `/status` endpoint on all active nodes |
| **Log Replication Engine** | Raft §5.3 | In Progress | Consistency checks and log matching |
| **Commit Index and State Machine** | Raft §5.3, §5.4 | In Progress | Linearizable state machine application |
| **Persistent Disk Storage Engine** | Raft §5.6 | Planned | WAL / append-only file storage implementation |
| **Log Compaction and Snapshots** | Raft §7 | Planned | State snapshots and `InstallSnapshot` RPC |
| **Cluster Membership Changes** | Raft §6 | Planned | Joint consensus configuration transitions |

---

## References

1. Ongaro, D., & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm*. USENIX Annual Technical Conference (ATC). [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)
2. Raft Protocol Information and Interactive Visualizations: [https://raft.github.io/](https://raft.github.io/)
3. Architecture Design Note: [Why Single-Threaded Event Loop Avoids Raft Locking Bugs](docs/design/single-threaded-event-loop.md)

---

## License

This project is licensed under the MIT License.
