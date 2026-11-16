package server

import (
	"testing"
	"time"

	"raft/raft"
)

func newTestCandidateServer(id raft.NodeID, peerIDs []raft.NodeID, initialTerm raft.Term) *Server {
	peers := make([]Peer, len(peerIDs))
	for i, pid := range peerIDs {
		peers[i] = Peer{ID: pid, Address: "127.0.0.1:0"}
	}
	clk := raft.NewFakeClock()
	srv := NewServer(id, peers, &raft.PersistentState{
		CurrentTerm: initialTerm,
		VotedFor:    id,
	}, nil)
	srv.SetClock(clk)
	srv.State = raft.Candidate
	srv.votesReceived = map[raft.NodeID]bool{id: true}
	return srv
}

func TestDelayedVote_ArrivesAfterNodeBecameFollower(t *testing.T) {
	srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, 1)

	// Node 1 receives valid AppendEntries from leader 2, reverting to Follower
	ok, _ := srv.HandleHeartbeat(2, 1)
	if !ok || srv.GetState() != raft.Follower {
		t.Fatalf("expected transition to Follower, got %s", srv.GetState())
	}

	// Delayed vote arrives from peer 3 for Term 1
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       3,
		ElectionTerm: 1,
		Reply:        &raft.RequestVoteReply{Term: 1, VoteGranted: true},
	})

	if srv.GetState() != raft.Follower {
		t.Fatalf("expected node to remain Follower, got %s", srv.GetState())
	}
	if len(srv.VotesReceived()) != 0 {
		t.Fatalf("expected empty votesReceived for Follower, got %v", srv.VotesReceived())
	}
}

func TestDelayedVote_ArrivesAfterNewElectionStarted(t *testing.T) {
	srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, 1)

	// Election timeout: node starts new election for Term 2
	srv.becomeCandidateLocked()
	if srv.PersistentState.CurrentTerm != 2 || srv.GetState() != raft.Candidate {
		t.Fatalf("expected Candidate in term 2, got %s in term %d", srv.GetState(), srv.PersistentState.CurrentTerm)
	}

	// Delayed vote arrives from peer 2 for previous Term 1
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 1,
		Reply:        &raft.RequestVoteReply{Term: 1, VoteGranted: true},
	})
	if srv.GetState() != raft.Candidate || len(srv.VotesReceived()) != 1 {
		t.Fatalf("expected vote from term 1 ignored; state=%s, votes=%v", srv.GetState(), srv.VotesReceived())
	}

	// Delayed vote with mismatched reply term arrives
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       3,
		ElectionTerm: 2,
		Reply:        &raft.RequestVoteReply{Term: 1, VoteGranted: true},
	})
	if srv.GetState() != raft.Candidate || len(srv.VotesReceived()) != 1 {
		t.Fatalf("expected vote with reply.Term < currentTerm ignored; state=%s, votes=%v", srv.GetState(), srv.VotesReceived())
	}

	// Legitimate vote for Term 2 arrives from peer 2
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 2,
		Reply:        &raft.RequestVoteReply{Term: 2, VoteGranted: true},
	})
	if srv.GetState() != raft.Leader {
		t.Fatalf("expected node to achieve majority and become Leader in term 2, got %s", srv.GetState())
	}
}

func TestDelayedVote_NeverBecomesLeaderOnOldTermVotes(t *testing.T) {
	tests := []struct {
		name         string
		initialState raft.RaftState
		currentTerm  raft.Term
		electionTerm raft.Term
		replyTerm    raft.Term
		voteGranted  bool
		wantState    raft.RaftState
		wantLeader   bool
	}{
		{"stale election and reply term", raft.Candidate, 5, 4, 4, true, raft.Candidate, false},
		{"matching election term but stale reply term", raft.Candidate, 5, 5, 4, true, raft.Candidate, false},
		{"stale election term but matching reply term", raft.Candidate, 5, 4, 5, true, raft.Candidate, false},
		{"higher reply term steps down to follower", raft.Candidate, 5, 5, 6, true, raft.Follower, false},
		{"vote arrived while follower", raft.Follower, 5, 5, 5, true, raft.Follower, false},
		{"vote denied", raft.Candidate, 5, 5, 5, false, raft.Candidate, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, tc.currentTerm)
			srv.State = tc.initialState
			if tc.initialState == raft.Follower {
				srv.votesReceived = make(map[raft.NodeID]bool)
			}

			srv.HandleVoteResult(RequestVoteResult{
				PeerID:       2,
				ElectionTerm: tc.electionTerm,
				Reply:        &raft.RequestVoteReply{Term: tc.replyTerm, VoteGranted: tc.voteGranted},
			})

			if srv.GetState() != tc.wantState {
				t.Fatalf("got state %s; want %s", srv.GetState(), tc.wantState)
			}
			if (srv.GetState() == raft.Leader) != tc.wantLeader {
				t.Fatalf("unexpected leader status; got %v, want %v", srv.GetState() == raft.Leader, tc.wantLeader)
			}
		})
	}
}

func TestDelayedVote_EventLoopStaleVoteIgnored(t *testing.T) {
	srv := newTestCandidateServer(1, []raft.NodeID{2, 3}, 1)
	srv.StartEventLoop()
	defer srv.Stop()

	// Cause server to start election 2 via state action
	resCh := make(chan struct{}, 1)
	srv.stateActionCh <- stateAction{
		fn: func() {
			srv.becomeCandidateLocked() // Now Term 2
		},
		resCh: resCh,
	}
	<-resCh

	// Send delayed vote from term 1 through event loop channel
	srv.HandleVoteResult(RequestVoteResult{
		PeerID:       2,
		ElectionTerm: 1,
		Reply:        &raft.RequestVoteReply{Term: 1, VoteGranted: true},
	})

	time.Sleep(20 * time.Millisecond)

	if srv.GetState() != raft.Candidate {
		t.Fatalf("expected candidate to ignore delayed vote in event loop, got state %s", srv.GetState())
	}
	if len(srv.VotesReceived()) != 1 {
		t.Fatalf("expected only 1 vote (self), got %d: %v", len(srv.VotesReceived()), srv.VotesReceived())
	}
}
