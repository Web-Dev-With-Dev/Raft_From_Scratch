// Package raft implements core consensus types, state machine role definitions,
// election timing primitives, and algorithmic rules for the Raft consensus protocol
// as described in Ongaro & Ousterhout (2014), "In Search of an Understandable Consensus Algorithm".
//
// # Consensus State Machine
//
// A Raft node operates in one of three mutually exclusive roles (Figure 4):
//
//		                 [Times out, starts election]
//		            +------------------------------------+
//		            |                                    |
//		            v                                    |
//		     +---------------+   Majority votes    +------------+
//		     |   Candidate   | ------------------> |   Leader   |
//		     +---------------+                     +------------+
//		       ^           |                         |
//		       |           | Discovers higher term   | Discovers higher term
//		       |           v or steps down           v or loses quorum (§6.2)
//		       |     +---------------+               |
//		       +---- |   Follower    | <-------------+
//		             +---------------+
//
//	  - Follower: The initial baseline state for all nodes upon boot or restart. Followers are
//	    completely passive; they issue no RPCs independently and only respond to incoming
//	    RequestVote and AppendEntries RPCs. If a follower receives no valid communication
//	    within a randomized election timeout window, it transitions to Candidate.
//
//	  - Candidate: A node actively campaigning for cluster leadership. Upon transition, the candidate
//	    increments currentTerm, votes for itself, resets its election timer, and broadcasts
//	    RequestVote RPCs in parallel to all peers. If it collects votes from a majority of nodes
//	    (floor(N/2) + 1), it transitions to Leader. If it discovers a leader or peer with term
//	    T > currentTerm, it steps down to Follower. If another election timeout elapses without
//	    a majority or step-down (split vote), it increments the term and starts a new election.
//
//	  - Leader: Manages log replication, serves client requests, and maintains authority by
//	    periodically dispatching empty AppendEntries RPCs (heartbeats) to all peers. Leaders
//	    never time out on elections. Under check-quorum leasing (§6.2), a leader unable to
//	    communicate with a majority quorum over an election timeout window immediately abdicates
//	    and steps down to Follower.
//
// # RPC Flow Architecture
//
// Consensus coordination relies on two fundamental remote procedure calls:
//
//  1. RequestVote (Figure 2 Rules for RequestVote):
//     Initiated in parallel by Candidates during elections.
//     - Rule 1: Receiver replies false if req.Term < receiver.CurrentTerm (§5.1).
//     - Rule 2: If votedFor is null (-1) or candidateId, and the candidate's log is at least
//     as up-to-date as the receiver's log (§5.4), grant vote.
//     - Follower resets its election timer upon granting a vote (§5.2) to mitigate split votes.
//
//  2. AppendEntries (Figure 2 Rules for AppendEntries):
//     Initiated exclusively by Leaders for heartbeats (empty entries) and log replication.
//     - Rule 1: Receiver replies false if req.Term < receiver.CurrentTerm (§5.1).
//     - Rule 2: Reply false if log doesn't contain an entry at prevLogIndex whose term
//     matches prevLogTerm (§5.3).
//     - Receiver resets election timer on receiving a valid heartbeat from current/higher term leader.
//
// # Core Invariants
//
// The implementation enforces the five fundamental Raft safety invariants:
//
//   - Election Safety: At most one leader can be elected in any given term (§5.2).
//   - Leader Append-Only: A leader never overwrites or truncates its own log entries (§5.3).
//   - Log Matching: If two logs contain an entry with the same index and term, then the logs
//     are identical in all entries up through the given index (§5.3).
//   - Leader Completeness: If a log entry is committed in a given term, that entry will be
//     present in the logs of the leaders for all higher-numbered terms (§5.4).
//   - State Machine Safety: If a server has applied a log entry at a given index to its
//     state machine, no other server will ever apply a different log entry for the same index (§5.4.3).
package raft
