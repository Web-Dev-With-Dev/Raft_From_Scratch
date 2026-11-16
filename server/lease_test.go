package server

import (
	"testing"
	"time"

	raftpb "raft/proto"
	"raft/raft"
)

func setupHealthyClusterWithLeader(t *testing.T) (*Cluster, *Server, raft.Term) {
	t.Helper()
	minTimeout, maxTimeout := 100*time.Millisecond, 200*time.Millisecond
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(42)
	simNet.SetClock(clk)

	cluster := NewCluster(t, 3,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(42),
		WithElectionTimeout(minTimeout, maxTimeout),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithPreVote(true),
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader == nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}

	// Advance cluster so heartbeats are actively flowing to all followers
	cluster.Advance(60 * time.Millisecond)
	return cluster, leader, initialTerm
}

// TestLeaderLease_StaleViewCannotDeposeHealthyLeader verifies §9.6:
// Followers reject (pre)votes if they heard from a current leader within the minimum
// election timeout, preventing stale/inflated candidates from deposing healthy leaders.
func TestLeaderLease_StaleViewCannotDeposeHealthyLeader(t *testing.T) {
	cluster, leader, initialTerm := setupHealthyClusterWithLeader(t)

	// Node 3 has stale view / high term and attempts real RequestVote without LeadershipTransfer
	staleReq := &raftpb.RequestVoteArgs{
		Term:               uint64(initialTerm + 10),
		CandidateId:        3,
		LastLogIndex:       1,
		LastLogTerm:        uint64(initialTerm),
		PreVote:            false,
		LeadershipTransfer: false,
	}

	// Follower Node 2 has active leader lease; must reject vote and NOT step down or update term
	f2 := cluster.GetNode(2)
	reply2 := f2.processRequestVote(staleReq)
	if reply2.VoteGranted {
		t.Fatalf("follower Node 2 must deny RequestVote while active leader lease is held")
	}
	if f2.Status().Term != initialTerm {
		t.Fatalf("follower Node 2 term must NOT update; expected %d, got %d", initialTerm, f2.Status().Term)
	}

	// Pre-vote rejection: both follower with lease and active leader deny pre-vote from stale candidate
	stalePreReq := &raftpb.RequestVoteArgs{
		Term:               uint64(initialTerm + 1),
		CandidateId:        3,
		LastLogIndex:       1,
		LastLogTerm:        uint64(initialTerm),
		PreVote:            true,
		LeadershipTransfer: false,
	}
	if f2.processRequestVote(stalePreReq).VoteGranted {
		t.Fatalf("follower Node 2 must deny PreVote while active leader lease is held")
	}
	if leader.processRequestVote(stalePreReq).VoteGranted {
		t.Fatalf("leader Node 1 must deny PreVote while actively leading")
	}

	// End-to-end election attempt: Node 3 attempts to start an election to depose the leader.
	// Because followers and leader reject its vote, Node 3 cannot gather quorum.
	cluster.GetNode(3).StartElection()
	cluster.Advance(50 * time.Millisecond)

	// Healthy leader Node 1 must NOT be deposed and must maintain active leadership
	if leader.GetState() != raft.Leader {
		t.Fatalf("healthy leader Node 1 was deposed; got %s, want %s", leader.GetState(), raft.Leader)
	}
	if leader.Status().Term != initialTerm {
		t.Fatalf("healthy leader term must remain %d, got %d", initialTerm, leader.Status().Term)
	}
	if cluster.GetNode(3).GetState() == raft.Leader {
		t.Fatalf("stale Node 3 must not become leader")
	}
}

// TestLeaderLease_LeadershipTransferException verifies that explicit leadership-transfer
// requests bypass the active leader lease check (§6.4, §9.6).
func TestLeaderLease_LeadershipTransferException(t *testing.T) {
	cluster, _, initialTerm := setupHealthyClusterWithLeader(t)
	f2 := cluster.GetNode(2)

	// Designee Node 3 sends RequestVote with LeadershipTransfer: true
	transferReq := &raftpb.RequestVoteArgs{
		Term:               uint64(initialTerm + 1),
		CandidateId:        3,
		LastLogIndex:       0,
		LastLogTerm:        0,
		PreVote:            false,
		LeadershipTransfer: true,
	}

	reply := f2.processRequestVote(transferReq)
	if !reply.VoteGranted {
		t.Fatalf("follower Node 2 MUST grant vote when LeadershipTransfer is true")
	}
	if f2.Status().Term != initialTerm+1 {
		t.Fatalf("follower Node 2 must update term to %d, got %d", initialTerm+1, f2.Status().Term)
	}
	if f2.Status().VotedFor != 3 {
		t.Fatalf("follower Node 2 must record vote for candidate 3, got %d", f2.Status().VotedFor)
	}
}

// TestLeaderLease_PreVoteRejectionAndTransferException verifies that PreVote respects
// active leader lease rejection and the LeadershipTransfer bypass exception.
func TestLeaderLease_PreVoteRejectionAndTransferException(t *testing.T) {
	cluster, _, initialTerm := setupHealthyClusterWithLeader(t)
	f2 := cluster.GetNode(2)

	preVoteReq := &raftpb.RequestVoteArgs{
		Term:               uint64(initialTerm + 1),
		CandidateId:        3,
		LastLogIndex:       0,
		LastLogTerm:        0,
		PreVote:            true,
		LeadershipTransfer: false,
	}

	// Denied with active leader lease
	if f2.processRequestVote(preVoteReq).VoteGranted {
		t.Fatalf("PreVote must be denied during active leader lease")
	}

	// Granted with LeadershipTransfer exception
	preVoteReq.LeadershipTransfer = true
	if !f2.processRequestVote(preVoteReq).VoteGranted {
		t.Fatalf("PreVote must be granted when LeadershipTransfer is true")
	}
}
