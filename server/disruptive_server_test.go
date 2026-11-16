package server

import (
	"testing"
	"time"

	"raft/raft"
)

func setupDisruptiveCluster(t *testing.T, preVote bool) (*Cluster, *Server, raft.Term) {
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
		WithPreVote(preVote),
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader == nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}
	cluster.Advance(60 * time.Millisecond)
	return cluster, leader, initialTerm
}

// TestDisruptiveServer_Regression verifies the disruptive server problem (§4.2.3, §9.6):
// 1. Follower times out repeatedly during partition. With Pre-Vote (§9.6), it cannot gather
//    majority pre-votes and does NOT inflate its term.
// 2. Partition heals; healthy leader maintaining active quorum is NOT forced to step down.
func TestDisruptiveServer_Regression(t *testing.T) {
	cluster, leader, initialTerm := setupDisruptiveCluster(t, true)
	isolatedID := raft.NodeID(3)

	cluster.Partition([]raft.NodeID{1, 2}, []raft.NodeID{isolatedID})
	cluster.Advance(600 * time.Millisecond)

	isolatedNode := cluster.GetNode(isolatedID)
	isolatedTerm := isolatedNode.Status().Term
	if isolatedTerm != initialTerm {
		t.Fatalf("expected isolated follower term NOT to inflate with Pre-Vote, got %d, want %d",
			isolatedTerm, initialTerm)
	}

	if leader.GetState() != raft.Leader || leader.Status().Term != initialTerm {
		t.Fatalf("expected Node 1 to remain leader in term %d before heal, got %s term %d",
			initialTerm, leader.GetState(), leader.Status().Term)
	}

	cluster.Heal()
	cluster.Advance(100 * time.Millisecond)

	t.Logf("DISRUPTIVE SERVER TRACE: isolated follower %d term after heal: %d (initial term: %d)",
		isolatedID, isolatedNode.Status().Term, initialTerm)
	t.Logf("DISRUPTIVE SERVER TRACE: healthy leader %d state after heal: %s (term: %d)",
		leader.ID, leader.GetState(), leader.Status().Term)

	// Invariant: Healthy leader maintaining active quorum must NOT be forced to step down by an isolated node.
	if leader.GetState() != raft.Leader {
		t.Fatalf("REGRESSION ('disruptive server'): healthy leader Node %d stepped down to %s; expected to remain Leader",
			leader.ID, leader.GetState())
	}
	if leader.Status().Term != initialTerm {
		t.Fatalf("expected leader term to remain %d, got %d", initialTerm, leader.Status().Term)
	}
}

// TestDay15_DisruptiveServer verifies Day 15's disruptive server prevention (§4.2.3, §9.6).
func TestDay15_DisruptiveServer(t *testing.T) {
	TestDisruptiveServer_Regression(t)
}

// TestDisruptiveServer_WithoutPreVote_StepsDown verifies why standard Raft Figure 2 suffers from
// the disruptive server vulnerability when Pre-Vote is not active.
func TestDisruptiveServer_WithoutPreVote_StepsDown(t *testing.T) {
	cluster, leader, initialTerm := setupDisruptiveCluster(t, false)
	isolatedID := raft.NodeID(3)

	cluster.Partition([]raft.NodeID{1, 2}, []raft.NodeID{isolatedID})
	cluster.Advance(600 * time.Millisecond)

	isolatedNode := cluster.GetNode(isolatedID)
	isolatedTerm := isolatedNode.Status().Term
	if isolatedTerm <= initialTerm+1 {
		t.Fatalf("expected isolated follower to inflate term without Pre-Vote, got %d", isolatedTerm)
	}

	cluster.Heal()
	cluster.Advance(100 * time.Millisecond)

	// Without Pre-Vote, higher term on isolated node forces healthy leader to step down
	if leader.GetState() == raft.Leader && leader.Status().Term == initialTerm {
		t.Fatalf("without Pre-Vote, expected leader to be disrupted by inflated term %d", isolatedTerm)
	}
}
