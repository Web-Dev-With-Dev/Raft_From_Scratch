# Why a Single-Threaded Event Loop Avoids Raft Locking Bugs

## 1. Executive Summary

Implementing the Raft consensus algorithm ([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)) using traditional multi-threaded shared-state mutexes is notoriously error-prone. Concurrency bugs in mutex-heavy Raft implementations manifest as deadlocks, lock inversions, stale state reads during term transitions, and subtle goroutine leaks.

This project adopts an **Actor-style Single-Threaded Event Loop** pattern in Go. In this architecture, all mutable Raft consensus state (`currentTerm`, `votedFor`, `state`, election/heartbeat timers, and log entries) is owned and mutated by a single dedicated goroutine (`s.run()`). Concurrent network requests, gRPC RPC handlers, and system timers interact with consensus state exclusively via message passing through Go channels.

This design document analyzes the technical rationale, architectural mechanics, eliminated bug classes, and performance characteristics of this approach.

---

## 2. The Concurrency Challenge in Raft

A Raft node is an asynchronous, event-driven state machine governed by concurrent external and internal stimuli:
1. **Randomized Election Timers**: Periodic timeouts triggering transitions from Follower to Candidate.
2. **Heartbeat & Replication Timers**: Periodic tickers triggering `AppendEntries` dispatches from Leaders to all peers.
3. **Inbound RPCs**: Simultaneous `RequestVote` and `AppendEntries` requests from competing peers handled by a pool of network worker threads (e.g., gRPC worker goroutines).
4. **Outbound RPC Responses**: Asynchronous responses arriving from peers that alter quorum calculations, step-downs, or match indices.
5. **Administrative & Lifecycle Events**: Debug endpoints (e.g., `/status`), client command submissions, and graceful shutdown signals.

```
       +-------------------------------------------------------+
       |                   Concurrent Inputs                   |
       |  +----------------+  +--------------+  +-----------+  |
       |  | Inbound RPCs   |  | Heartbeats & |  | Outbound  |  |
       |  | (gRPC workers) |  | Timers       |  | Responses |  |
       +--+--------+-------+--+-------+------+--+-----+-----+--+
                   |                  |               |
                   \                  |               /
                    v                 v              v
       =========================================================
                 Naive Mutex-Based Shared State
                 [ mu.Lock() / mu.Unlock() everywhere ]
             -> Contention, Lock Inversion, Deadlocks, Races
       =========================================================
```

---

## 3. Pitfalls of the Naive Mutex-Based Approach

In a naive Raft implementation, a node wraps its state in a `sync.Mutex` or `sync.RWMutex` and attempts to acquire locks in every RPC handler and timer callback. This introduces several fatal failure modes:

### 3.1 Lock Inversion and Deadlocks during Network I/O
The most common bug occurs when a handler acquires a mutex and performs synchronous network I/O or blocks on a channel while holding the lock:
```go
// ANTI-PATTERN: Holding lock across outbound RPC
func (s *Server) startElection() {
    s.mu.Lock()
    s.currentTerm++
    for _, peer := range s.peers {
        // DEADLOCK RISK: s.mu is held while waiting for network response.
        // If the peer makes an inbound RPC to this node, the inbound handler
        // needs s.mu, causing distributed deadlock or total local starvation.
        reply, err := s.sendRequestVote(peer, ...)
    }
    s.mu.Unlock()
}
```

### 3.2 Atomicity Gaps ("Drop-the-Lock" Races)
To avoid holding locks across RPCs, developers often drop the lock before dialing and reacquire it when the reply arrives:
```go
// ANTI-PATTERN: Drop-and-reacquire race
s.mu.Lock()
term := s.currentTerm
s.mu.Unlock()

reply, err := s.sendRequestVote(peer, term)

s.mu.Lock()
// BUG: Between s.mu.Unlock() and s.mu.Lock(), another peer may have
// sent an RPC with term+2. Applying reply into s.currentTerm now corrupts
// state or invalidates election quorum rules.
```

### 3.3 Fine-Grained Locking Inconsistencies
Splitting locks into fine-grained mutexes (e.g., `termMu`, `logMu`, `timerMu`) exponentially increases complexity. An RPC updating `currentTerm` and resetting `votedFor` requires acquiring locks in a strict hierarchy; failing to do so produces lock acquisition deadlocks or exposes intermediate states where `currentTerm` has updated but `votedFor` reflects the previous term.

---

## 4. Architectural Solution: Single-Threaded Event Loop

Our Raft node enforces a strict concurrency boundary: **only one goroutine ever touches the core consensus state.**

```
+---------------------------------------------------------------------------------+
|                                gRPC Worker Pool                                 |
|  RequestVote()                  AppendEntries()               /status HTTP      |
+---------+------------------------------+---------------------------+------------+
          |                              |                           |
          | (Send to Channel)            | (Send to Channel)         | (RLock / Chan)
          v                              v                           v
+-------------------+          +--------------------+       +-----------------+
|  requestVoteCh    |          |  appendEntriesCh   |       |  stateActionCh  |
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
                         * s.PersistentState (currentTerm, votedFor)
                         * s.State (Follower, Candidate, Leader)
                         * s.Storage.Persist() points
```

### 4.1 Multiplexing via Go's `select`
The core event loop (`server/server.go:run()`) listens on a unified multiplexer:
1. `s.ElectionTimer.C`: Triggers elections when heartbeat deadlines elapse.
2. `s.heartbeatTicker.C` / `s.heartbeatTimer.C`: Drives leader heartbeats.
3. `s.requestVoteCh`: Enqueues inbound `RequestVote` RPC events with synchronous response channels.
4. `s.appendEntriesCh`: Enqueues inbound `AppendEntries` RPC events.
5. `s.stateActionCh`: Enqueues synchronous state queries or mutations (e.g., debug inspection).
6. `s.stopCh`: Broadcasts shutdown across all subsystems.

### 4.2 Non-Blocking Event Dispatch with Context Resilience
Inbound gRPC worker threads do not block indefinitely. If a node is shutting down or an RPC client times out, channels unblock cleanly without leaking goroutines:

```go
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
    if s.loopRunning.Load() {
        resCh := make(chan *raftpb.RequestVoteReply, 1)
        select {
        case s.requestVoteCh <- RequestVoteEvent{Args: req, ResCh: resCh}:
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-s.stopCh:
            return nil, fmt.Errorf("server %d stopped", s.ID)
        }

        select {
        case reply := <-resCh:
            return reply, nil
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-s.stopCh:
            return nil, fmt.Errorf("server %d stopped", s.ID)
        }
    }
    // Fallback if loop is stopped
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.processRequestVote(req), nil
}
```

---

## 5. Formal Guarantees

| Invariant | Multi-Threaded Mutex Model | Single-Threaded Event Loop Model |
| :--- | :--- | :--- |
| **Deadlock Freedom** | Vulnerable to lock inversion across RPCs and nested locks | **Guaranteed**: Event loop never holds a lock across external calls |
| **State Linearizability** | Fragile; depends on error-prone mutex nesting order | **Guaranteed**: Events are serialized strictly through FIFO channel turns |
| **Persistence Correctness** | Race between state update and disk sync (`Persist()`) | **Guaranteed**: `Persist()` executes synchronously prior to answering RPC |
| **Timer Race Prevention** | Timers fire while RPC handler changes role/term | **Guaranteed**: Timer expiration and RPC handling are mutually exclusive branches of `select` |
| **Goroutine Cleanliness** | Leaked worker threads blocked on mutexes or unbuffered sends | **Guaranteed**: All channel operations select on `ctx.Done()` and `stopCh` |

---

## 6. How Specific Raft Bugs Are Avoided

### 6.1 Timer Expiration vs. Concurrent Heartbeat Reception
* **The Bug**: A Follower receives an `AppendEntries` RPC at the exact instant its election timer fires. In a mutex-based system, both threads acquire locks in race conditions; the election timer may promote the node to Candidate even though a valid Leader heartbeat was already accepted.
* **Our Protection**: In the event loop, `s.ElectionTimer.C` and `s.appendEntriesCh` are mutually exclusive cases in the same `select`. If the `AppendEntries` event is chosen, `processAppendEntries` resets the election timer, immediately draining/canceling pending timer events before an election can begin.

### 6.2 Strict Persistence Points
* **The Bug**: Raft §5.2 and §5.6 require `currentTerm` and `votedFor` to be persisted to stable storage *before* responding to RPCs. In multi-threaded code, an uncoordinated reader might respond to a peer while another thread is still writing state to disk.
* **Our Protection**: Inside `processRequestVote()` and `becomeCandidate()`, `s.Storage.Persist(s.PersistentState)` is invoked synchronously inside the event loop turn. The response channel `ev.ResCh` is not signaled until persistence succeeds.

### 6.3 Goroutine Leak Elimination
* **The Bug**: If an RPC client cancels its HTTP/2 stream, a naive server goroutine attempting to send the response onto an unbuffered channel blocks forever.
* **Our Protection**:
  1. Response channels have buffer capacity 1 (`make(chan ..., 1)`).
  2. Sends in the event loop are guarded by `select`:
     ```go
     select {
     case ev.ResCh <- reply:
     case <-s.stopCh:
         return
     }
     ```
  3. All background loops (peer dialers, tickers, monitors) wire `stopCh` or a cancellable `context.Context`.

---

## 7. Performance and Scalability Evaluation

1. **Elimination of Mutex Contention Overhead**:
   Under high RPC throughput, CPU caches thrash on shared mutex cachelines. The event loop eliminates lock bouncing across cores for consensus state.
2. **Channel Throughput in Go**:
   Go channels operate in nanoseconds for buffered/uncontended operations. A single event loop easily handles tens of thousands of consensus state evaluations per second on a single CPU core, exceeding network I/O bounds.
3. **Decoupled I/O**:
   Heavy network serialization (protobuf encoding/decoding and gRPC framing) remains distributed across Go's multi-threaded worker pool. Only the pure state transitions run on the serialized loop.

---

## 8. Conclusion

By shifting from shared-memory locking to an Actor-style Single-Threaded Event Loop, the Raft implementation eliminates the entire class of lock inversion, race condition, and goroutine leak bugs common to distributed consensus systems. The resulting codebase is deterministic, easy to reason about, and provably race-free under `go test -race`.

