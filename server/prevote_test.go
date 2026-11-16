package server

import (
	"testing"
	"time"

	raftpb "raft/proto"
	"raft/raft"
)

func TestPreVote_PreCandidateDoesNotIncrementTerm(t *testing.T) {
	clk := raft.NewFakeClock()
	srv := NewServer(1, []Peer{{ID: 2}, {ID: 3}}, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    -1,
	}, nil)
	srv.SetClock(clk)
	srv.SetPreVote(true)

	// Explicitly initiate pre-vote
	srv.startPreVoteLocked()

	if srv.GetState() != raft.PreCandidate {
		t.Fatalf("expected state PreCandidate, got %s", srv.GetState())
	}
	if srv.Status().Term != 5 {
		t.Fatalf("pre-candidate MUST NOT increment term; expected 5, got %d", srv.Status().Term)
	}
	if srv.Status().VotedFor != -1 {
		t.Fatalf("pre-candidate MUST NOT set votedFor; expected -1, got %d", srv.Status().VotedFor)
	}
	if !srv.PreVotesReceived()[1] {
		t.Fatalf("pre-candidate should record vote for itself in pre-vote phase")
	}
}

func TestPreVote_ReceiverDoesNotMutateState(t *testing.T) {
	srv := NewServer(1, []Peer{{ID: 2}}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1,
	}, nil)

	// Receiver gets pre-vote for speculative term 3
	req := &raftpb.RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
		PreVote:      true,
	}

	reply := srv.processRequestVote(req)
	if !reply.VoteGranted {
		t.Fatalf("expected pre-vote to be granted")
	}
	if reply.Term != 2 {
		t.Fatalf("expected reply term to be receiver's current term (2), got %d", reply.Term)
	}

	// Invariant: term and votedFor must NOT change
	if srv.Status().Term != 2 {
		t.Fatalf("receiver term MUST NOT change; expected 2, got %d", srv.Status().Term)
	}
	if srv.Status().VotedFor != -1 {
		t.Fatalf("receiver votedFor MUST NOT change; expected -1, got %d", srv.Status().VotedFor)
	}
}

func TestPreVote_ReceiverDeniesStaleTerm(t *testing.T) {
	srv := NewServer(1, []Peer{{ID: 2}}, &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    -1,
	}, nil)

	// Candidate proposes term 4 < currentTerm 5
	req := &raftpb.RequestVoteArgs{
		Term:         4,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
		PreVote:      true,
	}

	reply := srv.processRequestVote(req)
	if reply.VoteGranted {
		t.Fatalf("expected pre-vote to be denied for stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestPreVote_ReceiverDeniesStaleLog(t *testing.T) {
	srv := NewServer(1, []Peer{{ID: 2}}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1,
		Log: []raft.LogEntry{
			{Index: 1, Term: 2, Command: []byte("cmd1")},
		},
	}, nil)

	// Candidate has empty log
	req := &raftpb.RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
		PreVote:      true,
	}

	reply := srv.processRequestVote(req)
	if reply.VoteGranted {
		t.Fatalf("expected pre-vote to be denied for stale log")
	}
}

func TestPreVote_ReceiverDeniesWhenActiveLeader(t *testing.T) {
	clk := raft.NewFakeClock()
	srv := NewServer(1, []Peer{{ID: 2}, {ID: 3}}, &raft.PersistentState{
		CurrentTerm: 2,
		VotedFor:    -1,
	}, nil)
	srv.SetClock(clk)

	// Simulate receiving a heartbeat from leader 3
	srv.HandleHeartbeat(3, 2)

	// Disconnected candidate 2 asks for pre-vote within min election timeout
	req := &raftpb.RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
		PreVote:      true,
	}

	reply := srv.processRequestVote(req)
	if reply.VoteGranted {
		t.Fatalf("expected pre-vote to be denied while receiver has active leader")
	}

	// Advance clock past minimum election timeout (e.g. 350ms)
	clk.Advance(350 * time.Millisecond)

	reply2 := srv.processRequestVote(req)
	if !reply2.VoteGranted {
		t.Fatalf("expected pre-vote to be granted after active leader lease expires")
	}
}

func TestPreVote_StartsRealElectionOnlyOnMajority(t *testing.T) {
	srv := NewServer(1, []Peer{{ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}}, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
	}, nil)
	srv.SetPreVote(true)

	// Initiate pre-vote: node 1 votes for itself (1/5)
	srv.startPreVoteLocked()
	if srv.GetState() != raft.PreCandidate {
		t.Fatalf("expected state PreCandidate, got %s", srv.GetState())
	}

	// Receive pre-vote from peer 2 (2/5 votes; minority)
	srv.handleVoteResultLocked(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 2,
		PreVote:      true,
		Reply:        &raft.RequestVoteReply{Term: 1, VoteGranted: true},
	})

	if srv.GetState() != raft.PreCandidate {
		t.Fatalf("should remain PreCandidate on minority (2/5); got %s", srv.GetState())
	}
	if srv.Status().Term != 1 {
		t.Fatalf("term should still be 1; got %d", srv.Status().Term)
	}

	// Receive pre-vote from peer 3 (3/5 votes; quorum majority achieved)
	srv.handleVoteResultLocked(RequestVoteResult{
		PeerID:       3,
		ElectionTerm: 2,
		PreVote:      true,
		Reply:        &raft.RequestVoteReply{Term: 1, VoteGranted: true},
	})

	// Now it MUST transition to Candidate, increment term to 2, and vote for self
	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected state Candidate after majority pre-votes, got %s", srv.GetState())
	}
	if srv.Status().Term != 2 {
		t.Fatalf("expected term 2 after starting real election, got %d", srv.Status().Term)
	}
	if srv.Status().VotedFor != 1 {
		t.Fatalf("expected votedFor 1 after starting real election, got %d", srv.Status().VotedFor)
	}
}

func runElectionAndFailover(t *testing.T, preVote bool) {
	t.Helper()
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(42)
	simNet.SetClock(clk)

	cluster := NewCluster(t, 3,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(42),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithPreVote(preVote),
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader == nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader (preVote=%v): %v", preVote, err)
	}

	cluster.Advance(60 * time.Millisecond)

	cluster.Kill(leader.ID)
	var newLeader *Server
	elected := cluster.AdvanceUntil(3*time.Second, func() bool {
		l, _ := cluster.Leader()
		if l != nil && l.ID != leader.ID {
			newLeader = l
			return true
		}
		return false
	})
	if !elected || newLeader == nil {
		t.Fatalf("failover election failed (preVote=%v)", preVote)
	}
	if newLeader.Status().Term <= initialTerm {
		t.Fatalf("expected new leader term > %d, got %d", initialTerm, newLeader.Status().Term)
	}
}

// TestRegression_NormalElections_PreVoteOnAndOff verifies that normal leader election
// and failover re-election operate seamlessly with PreVote enabled or disabled.
func TestRegression_NormalElections_PreVoteOnAndOff(t *testing.T) {
	t.Run("PreVote_Off", func(t *testing.T) {
		runElectionAndFailover(t, false)
	})
	t.Run("PreVote_On", func(t *testing.T) {
		runElectionAndFailover(t, true)
	})
}
