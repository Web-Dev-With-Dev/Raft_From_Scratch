// Package server provides the production-grade Raft consensus server implementation,
// in-process event-loop concurrency orchestration, and testing harness infrastructure.
//
// # Single-Threaded Event Loop Architecture
//
// To prevent concurrency bugs, race conditions, and lock-ordering deadlocks, all core Raft
// state mutations are processed sequentially on a single dedicated event-loop goroutine.
// External and asynchronous events are delivered via Go channels into the run() select loop:
//
//	+-----------------------+     +-----------------------+
//	| Incoming RequestVote  |     | Incoming AppendEntry  |
//	+-----------------------+     +-----------------------+
//	           |                              |
//	           v                              v
//	     requestVoteCh                 appendEntriesCh
//	           \                              /
//	            \                            /
//	             v                          v
//	       +--------------------------------------+
//	       |       Single Raft Event Loop         | <--- Election Timer / Check Quorum
//	       |            s.run()                   | <--- Heartbeat Ticker
//	       +--------------------------------------+
//	             ^                          ^
//	            /                            \
//	           /                              \
//	     voteResultCh                  heartbeatResultCh
//	           ^                              ^
//	           |                              |
//	+-----------------------+     +-----------------------+
//	| Outbound Vote Replies |     | Outbound Heartbeat Res|
//	+-----------------------+     +-----------------------+
//
// Event channels include:
//   - requestVoteCh: Delivers incoming RequestVote gRPC requests with synchronous response channels.
//   - appendEntriesCh: Delivers incoming AppendEntries gRPC requests with synchronous response channels.
//   - voteResultCh: Streams results from parallel RequestVote goroutines back to the candidate.
//   - heartbeatResultCh: Streams results from parallel leader AppendEntries heartbeats back to the leader.
//   - stateActionCh: Multiplexes thread-safe synchronized closures from external inspection APIs.
//   - s.ElectionTimer.C: Delivers randomized election timeout triggers to begin candidate campaigns.
//   - s.heartbeatTimer.C: Delivers periodic leader heartbeat ticks to maintain authority.
//   - checkQuorumTimer.C: Evaluates majority quorum contact per §6.2 for leader leasing.
//
// # RPC Flow Lifecycle
//
//  1. Election Campaign (Candidate):
//     When an election timeout fires on a Follower or Candidate, it transitions to Candidate,
//     increments term, votes for self, and launches parallel goroutines dispatching RequestVote
//     RPCs to all peers. Each worker goroutine enforces RPCTimeout() and posts a RequestVoteResult
//     to voteResultCh. When a majority of votes are granted within the term, the node transitions to Leader.
//
//  2. Leadership & Heartbeats (Leader):
//     Upon becoming Leader, the server arms a high-frequency heartbeat ticker (default 20-50ms).
//     On each tick, it dispatches empty AppendEntries RPCs in parallel to all peers. When peers
//     reply with higher terms, the leader immediately steps down to Follower.
//
//  3. Leader Leasing & Quorum Checking (§6.2):
//     Under check-quorum leasing, the leader tracks round-trip heartbeats received from peers.
//     Every check-quorum interval, if the leader fails to establish contact with a majority quorum,
//     it immediately steps down to Follower to eliminate stale minority leaders during partitions.
//
// # Observability & Diagnostics
//
// Every Server instance maintains:
//   - Atomic Consensus Counters: Tracks total elections started, votes granted, votes denied,
//     and leader heartbeats sent.
//   - Circular Event Log (Ring Buffer): A thread-safe, fixed-capacity ring buffer capturing
//     timestamped state transitions, election starts, vote decisions, and lifecycle events
//     for post-mortem debugging.
//   - HTTP Debug Endpoints: Serves `/status` (JSON cluster state, counters, recent events)
//     and `/events` (full chronological event history array) via HTTP.
package server
