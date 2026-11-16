package server

import (
	"testing"
	"time"

	"raft/raft"
)

func setupDisruptiveCluster(t *testing.T) (*Cluster, *Server, raft.Term) {
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
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader == nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}
	cluster.Advance(60 * time.Millisecond)
	return cluster, leader, initialTerm
}

func isolateAndInflateFollower(t *testing.T, cluster *Cluster, isolatedID raft.NodeID, initialTerm raft.Term) raft.Term {
	t.Helper()
	cluster.Partition([]raft.NodeID{1, 2}, []raft.NodeID{isolatedID})
	cluster.Advance(600 * time.Millisecond)

	isolatedNode := cluster.GetNode(isolatedID)
	isolatedTerm := isolatedNode.Status().Term
	if isolatedTerm <= initialTerm+1 {
		t.Fatalf("expected isolated follower to inflate term above %d, got %d", initialTerm+1, isolatedTerm)
	}
	return isolatedTerm
}

// TestDisruptiveServer_Regression verifies the disruptive server problem (§4.2.3, §9.6):
// 1. Follower times out repeatedly during partition and inflates term.
// 2. Partition heals; higher term forces the healthy leader to step down.
// 3. Fails now in standard Raft; will pass later with Pre-Vote.
func TestDisruptiveServer_Regression(t *testing.T) {
	cluster, leader, initialTerm := setupDisruptiveCluster(t)
	isolatedID := raft.NodeID(3)

	isolatedTerm := isolateAndInflateFollower(t, cluster, isolatedID, initialTerm)

	if leader.GetState() != raft.Leader || leader.Status().Term != initialTerm {
		t.Fatalf("expected Node 1 to remain leader in term %d before heal, got %s term %d",
			initialTerm, leader.GetState(), leader.Status().Term)
	}

	cluster.Heal()
	cluster.Advance(100 * time.Millisecond)

	t.Logf("DISRUPTIVE SERVER TRACE: isolated follower %d inflated term to %d (initial term: %d)",
		isolatedID, isolatedTerm, initialTerm)
	t.Logf("DISRUPTIVE SERVER TRACE: healthy leader %d state after heal: %s (term: %d)",
		leader.ID, leader.GetState(), leader.Status().Term)

	// REGRESSION INVARIANT ('disruptive server'):
	// Healthy leader maintaining active quorum must NOT be forced to step down by an isolated node.
	// FAILS NOW (standard Raft steps down); PASSES LATER (Pre-Vote prevents term inflation).
	if leader.GetState() != raft.Leader {
		t.Fatalf("REGRESSION ('disruptive server'): healthy leader Node %d was forced to step down to %s by isolated follower Node %d with inflated term %d; expected leader to remain Leader (Pre-Vote required)",
			leader.ID, leader.GetState(), isolatedID, isolatedTerm)
	}
}
