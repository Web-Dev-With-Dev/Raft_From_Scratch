package server

import (
	"testing"

	"pgregory.net/rapid"
	"raft/proto"
	"raft/raft"
)

// TestVoteHandling_IdempotentSameCandidate verifies that when the same candidate
// re-asks for a vote in the same term, it receives the exact same answer.
func TestVoteHandling_IdempotentSameCandidate(t *testing.T) {
	srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, 1)
	srv.State = raft.Follower
	srv.PersistentState.VotedFor = -1

	req := &raftpb.RequestVoteArgs{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	reply1 := srv.processRequestVote(req)
	if !reply1.VoteGranted {
		t.Fatalf("first vote request: expected VoteGranted=true, got false")
	}

	reply2 := srv.processRequestVote(req)
	if !reply2.VoteGranted {
		t.Fatalf("second vote request: expected idempotent VoteGranted=true, got false")
	}

	if srv.PersistentState.VotedFor != 2 {
		t.Fatalf("expected votedFor=2, got %d", srv.PersistentState.VotedFor)
	}
}

// TestVoteHandling_IdempotentDeniedCandidate verifies that a candidate whose request
// was denied (e.g. stale log) gets denied again when re-asking in the same term.
func TestVoteHandling_IdempotentDeniedCandidate(t *testing.T) {
	srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, 2)
	srv.State = raft.Follower
	srv.PersistentState.VotedFor = -1
	_ = srv.Log.Append(raft.LogEntry{Term: 2, Command: "x"})

	req := &raftpb.RequestVoteArgs{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 1,
		LastLogTerm:  1,
	}

	reply1 := srv.processRequestVote(req)
	if reply1.VoteGranted {
		t.Fatalf("expected first vote request to be denied for stale log")
	}

	reply2 := srv.processRequestVote(req)
	if reply2.VoteGranted {
		t.Fatalf("expected duplicate vote request to be denied idempotently")
	}
}

// TestVoteHandling_NeverGrantTwoDifferentCandidatesInOneTerm verifies the votedFor invariant:
// A server never grants its vote to two different candidates in the same term (§5.2).
func TestVoteHandling_NeverGrantTwoDifferentCandidatesInOneTerm(t *testing.T) {
	srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, 1)
	srv.State = raft.Follower
	srv.PersistentState.VotedFor = -1

	req2 := &raftpb.RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
	if !srv.processRequestVote(req2).VoteGranted {
		t.Fatalf("expected vote granted to candidate 2")
	}

	req3 := &raftpb.RequestVoteArgs{Term: 1, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 0}
	if srv.processRequestVote(req3).VoteGranted {
		t.Fatalf("votedFor invariant violated: vote granted to candidate 3 after candidate 2 in term 1")
	}

	if !srv.processRequestVote(req2).VoteGranted {
		t.Fatalf("expected candidate 2 to still get true on retry")
	}
	if srv.processRequestVote(req3).VoteGranted {
		t.Fatalf("votedFor invariant violated: candidate 3 received vote on retry")
	}
}

// TestProperty_VoteHandling_IdempotencyAndVotedForInvariant verifies with rapid
// that across arbitrary sequences of candidates asking and re-asking in a term:
// 1. At most one candidate is granted a vote in the term.
// 2. Any candidate re-asking in the same term gets the exact same answer.
func TestProperty_VoteHandling_IdempotencyAndVotedForInvariant(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		term := raft.Term(rapid.Uint64Range(1, 50).Draw(rt, "term"))
		srv := newTestCandidateServer(1, []raft.NodeID{2, 3, 4, 5}, term)
		srv.State = raft.Follower
		srv.PersistentState.VotedFor = -1

		numRequests := rapid.IntRange(5, 20).Draw(rt, "num_requests")
		history := make(map[int64]bool)

		for i := 0; i < numRequests; i++ {
			candID := int64(rapid.IntRange(2, 5).Draw(rt, "cand_id"))
			req := &raftpb.RequestVoteArgs{Term: uint64(term), CandidateId: candID, LastLogIndex: 0, LastLogTerm: 0}
			reply := srv.processRequestVote(req)

			if prior, exists := history[candID]; exists {
				if reply.VoteGranted != prior {
					rt.Fatalf("idempotency violated for candidate %d: prior=%v, current=%v", candID, prior, reply.VoteGranted)
				}
			}
			history[candID] = reply.VoteGranted

			grantedCount := 0
			for _, granted := range history {
				if granted {
					grantedCount++
				}
			}
			if grantedCount > 1 {
				rt.Fatalf("votedFor invariant violated: %d different candidates granted votes in term %d", grantedCount, term)
			}
		}
	})
}

