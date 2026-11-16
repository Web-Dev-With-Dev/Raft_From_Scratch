package server

import (
	"testing"
	"time"

	"raft/raft"
)

// TestCheckQuorum_MinorityPartition_StepsDownOnItsOwn verifies §6.2:
// A leader in a minority partition loses majority contact and steps down to Follower
// entirely on its own, without seeing a higher term or message from other nodes.
func TestCheckQuorum_MinorityPartition_StepsDownOnItsOwn(t *testing.T) {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(42)
	simNet.SetClock(clk)

	minTimeout, maxTimeout := 100*time.Millisecond, 200*time.Millisecond
	cluster := NewCluster(t, 3,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(42),
		WithElectionTimeout(minTimeout, maxTimeout),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithCheckQuorum(true),
		WithPreVote(true),
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader == nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}

	// Advance cluster so heartbeats flow to all followers and lastHeardFrom is updated
	cluster.Advance(60 * time.Millisecond)

	lastHeard := leader.LastHeardFrom()
	if len(lastHeard) < 2 {
		t.Fatalf("expected leader to track last-heard-from for all peers, got %d", len(lastHeard))
	}

	// Partition leader 1 into minority: {1} vs {2, 3}
	cluster.Partition([]raft.NodeID{1}, []raft.NodeID{2, 3})

	// Advance past election timeout without letting leader see any higher terms
	cluster.Advance(150 * time.Millisecond)

	// Invariant: Leader must step down to Follower on its own due to quorum loss
	if leader.GetState() != raft.Follower {
		t.Fatalf("expected leader in minority partition to step down to Follower; got %s", leader.GetState())
	}

	// Invariant: Leader stepped down without seeing a higher term
	if leader.Status().Term != initialTerm {
		t.Fatalf("expected leader term to remain %d without seeing higher term; got %d", initialTerm, leader.Status().Term)
	}

	// Invariant: Heartbeat ticker must be stopped after stepping down
	if leader.GetHeartbeatTicker() != nil {
		t.Fatalf("expected heartbeat ticker to be nil after leader step down")
	}

	// Verify EventQuorumLoss was logged
	hasQuorumLossEvent := false
	for _, ev := range leader.EventLog().Entries() {
		if ev.Type == EventQuorumLoss {
			hasQuorumLossEvent = true
			break
		}
	}
	if !hasQuorumLossEvent {
		t.Fatalf("expected EventQuorumLoss in leader event log")
	}
}

// TestCheckQuorum_MajorityPartition_NeverStepsDownSpuriously_500Seeds verifies §6.2:
// A leader maintaining contact with a majority never spuriously steps down across 500 random seeds.
func TestCheckQuorum_MajorityPartition_NeverStepsDownSpuriously_500Seeds(t *testing.T) {
	for seed := 1; seed <= 500; seed++ {
		func() {
			clk := raft.NewFakeClock()
			simNet := NewSimNetwork(int64(seed))
			simNet.SetClock(clk)

			minTimeout, maxTimeout := 100*time.Millisecond, 200*time.Millisecond
			cluster := NewCluster(t, 3,
				WithClock(clk),
				WithSimNetwork(simNet),
				WithRandSeed(int64(seed)),
				WithElectionTimeout(minTimeout, maxTimeout),
				WithHeartbeatInterval(20*time.Millisecond),
				WithRPCTimeout(40*time.Millisecond),
				WithCheckQuorum(true),
				WithPreVote(true),
			)
			defer cluster.Stop()

			cluster.GetNode(1).StartElection()
			leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
			if err != nil || leader == nil || leader.ID != 1 {
				t.Fatalf("seed %d: failed to elect initial leader: %v", seed, err)
			}

			// Advance cluster to establish active heartbeats
			cluster.Advance(60 * time.Millisecond)

			// Partition node 3 away; leader 1 maintains a majority with node 2 ({1, 2} of 3)
			cluster.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3})

			// Advance across multiple election timeouts (300ms = 15 heartbeat cycles)
			cluster.Advance(300 * time.Millisecond)

			// Invariant: Leader maintaining majority contact must NEVER step down spuriously
			if leader.GetState() != raft.Leader {
				t.Fatalf("seed %d: leader with majority spuriously stepped down to %s", seed, leader.GetState())
			}
			if leader.Status().Term != initialTerm {
				t.Fatalf("seed %d: leader term changed unexpectedly from %d to %d", seed, initialTerm, leader.Status().Term)
			}
		}()
	}
}
