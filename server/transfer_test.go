package server

import (
	"context"
	"testing"
	"time"

	raftpb "raft/proto"
	"raft/raft"
)

// TestTimeoutNow_StartsElectionAndBypassesStickiness verifies that receiving
// a TimeoutNow RPC causes the target node to immediately start an election,
// bypassing leader stickiness (§3.10).
func TestTimeoutNow_StartsElectionAndBypassesStickiness(t *testing.T) {
	cluster, leader, initialTerm := setupHealthyClusterWithLeader(t)

	f2 := cluster.GetNode(2)
	// Follower 2 has an active leader lease from recently received heartbeats
	if !f2.hasActiveLeaderLocked() {
		t.Fatalf("expected Follower 2 to have active leader lease before TimeoutNow")
	}

	reply, err := f2.TimeoutNow(context.Background(), &raftpb.TimeoutNowArgs{
		Term:     uint64(initialTerm),
		LeaderId: int64(leader.ID),
	})
	if err != nil {
		t.Fatalf("unexpected error from TimeoutNow: %v", err)
	}
	if !reply.Success {
		t.Fatalf("expected TimeoutNow to succeed")
	}
	if reply.Term != uint64(initialTerm+1) {
		t.Fatalf("expected reply term %d, got %d", initialTerm+1, reply.Term)
	}

	// Target 2 must have immediately transitioned to Candidate (or already won and became Leader) at term initialTerm + 1
	if state := f2.GetState(); state != raft.Candidate && state != raft.Leader {
		t.Fatalf("expected Node 2 to become Candidate or Leader, got %s", state)
	}
	if f2.Status().Term != initialTerm+1 {
		t.Fatalf("expected Node 2 term %d, got %d", initialTerm+1, f2.Status().Term)
	}
	if f2.Status().VotedFor != 2 {
		t.Fatalf("expected Node 2 to vote for self (2), got %d", f2.Status().VotedFor)
	}
}

// TestTransfer_LeaderTransfersToFollower_Succeeds verifies that calling Transfer
// on the leader triggers TimeoutNow on the target, electing the target as the new leader.
func TestTransfer_LeaderTransfersToFollower_Succeeds(t *testing.T) {
	cluster, leader, initialTerm := setupHealthyClusterWithLeader(t)

	// Transfer leadership from Node 1 to Node 2
	if err := leader.Transfer(2); err != nil {
		t.Fatalf("Transfer(2) failed: %v", err)
	}

	// Advance simulated network/clock to deliver RequestVote and process election
	cluster.Advance(80 * time.Millisecond)

	newLeader, newTerm := cluster.Leader()
	if newLeader == nil {
		t.Fatalf("expected a new leader after transfer, but none found")
	}
	if newLeader.ID != 2 {
		t.Fatalf("expected Node 2 to become leader, got Node %d", newLeader.ID)
	}
	if newTerm != initialTerm+1 {
		t.Fatalf("expected new term %d, got %d", initialTerm+1, newTerm)
	}

	// Old leader Node 1 must have stepped down to Follower
	if leader.GetState() != raft.Follower {
		t.Fatalf("expected old leader Node 1 to step down to Follower, got %s", leader.GetState())
	}
}

// TestTransfer_BypassesFollowerStickiness verifies that followers grant their vote
// to the transfer target even though their active leader lease is unexpired.
func TestTransfer_BypassesFollowerStickiness(t *testing.T) {
	cluster, _, initialTerm := setupHealthyClusterWithLeader(t)

	// Heartbeats just flowed: Node 3 has active leader lease
	f3 := cluster.GetNode(3)
	staleReq := &raftpb.RequestVoteArgs{
		Term:               uint64(initialTerm + 1),
		CandidateId:        99,
		LeadershipTransfer: false,
	}
	if f3.processRequestVote(staleReq).VoteGranted {
		t.Fatalf("follower 3 must deny vote without LeadershipTransfer during active lease")
	}

	// Initiate transfer to Node 2 via cluster helper
	if err := cluster.Transfer(2); err != nil {
		t.Fatalf("cluster.Transfer(2) failed: %v", err)
	}

	cluster.Advance(80 * time.Millisecond)

	newLeader, newTerm := cluster.Leader()
	if newLeader == nil || newLeader.ID != 2 {
		t.Fatalf("expected Node 2 to win leadership despite follower stickiness")
	}
	if newTerm != initialTerm+1 {
		t.Fatalf("expected term %d, got %d", initialTerm+1, newTerm)
	}
}

// TestTransfer_ErrorCases verifies error handling for Transfer and TimeoutNow.
func TestTransfer_ErrorCases(t *testing.T) {
	cluster, leader, initialTerm := setupHealthyClusterWithLeader(t)

	// Calling Transfer on a follower must return an error
	f2 := cluster.GetNode(2)
	if err := f2.Transfer(3); err == nil {
		t.Fatalf("expected error transferring from follower, got nil")
	}

	// Transfer to self is a no-op returning nil
	if err := leader.Transfer(leader.ID); err != nil {
		t.Fatalf("expected Transfer to self to return nil, got %v", err)
	}

	// Transfer to an unknown peer must return an error
	if err := leader.Transfer(999); err == nil {
		t.Fatalf("expected error transferring to unknown peer 999, got nil")
	}

	// TimeoutNow with stale term must be rejected
	staleReply, err := f2.TimeoutNow(context.Background(), &raftpb.TimeoutNowArgs{
		Term:     uint64(initialTerm - 1),
		LeaderId: int64(leader.ID),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if staleReply.Success {
		t.Fatalf("expected TimeoutNow with stale term to be rejected")
	}
}
