package server

import (
	"context"
	"testing"
	"time"

	"raft/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func assertClusterFollowers(t testing.TB, cluster *Cluster, except raft.NodeID) {
	for _, id := range cluster.NodeIDs() {
		node := cluster.GetNode(id)
		if id != except && !node.IsKilled() {
			if st := node.GetState(); st != raft.Follower {
				t.Fatalf("expected node %d to be Follower, got %s", id, st)
			}
		}
	}
}

func verifySimFailoverBounds(t testing.TB, elapsed, minTimeout, maxTimeout time.Duration) {
	if elapsed < minTimeout/2 {
		t.Fatalf("time-to-new-leader %v unexpectedly faster than minTimeout/2 (%v)", elapsed, minTimeout/2)
	}
	maxAllowed := 3*maxTimeout + 300*time.Millisecond
	if elapsed > maxAllowed {
		t.Fatalf("time-to-new-leader %v exceeded upper bound %v", elapsed, maxAllowed)
	}
}

// TestHarness_Day20_CrashScenario verifies Day 20's leader crash and failover
// scenario using FakeClock + SimNetwork with zero wall-clock sleep waits.
func TestHarness_Day20_CrashScenario(t *testing.T) {
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

	wallStart := time.Now()
	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}

	cluster.Advance(60 * time.Millisecond)
	assertClusterFollowers(t, cluster, leader.ID)

	killSimTime := clk.Now()
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
		t.Fatalf("surviving nodes failed to elect a new leader")
	}

	simTimeToNewLeader := clk.Now().Sub(killSimTime)
	verifySimFailoverBounds(t, simTimeToNewLeader, minTimeout, maxTimeout)
	if time.Since(wallStart) > 250*time.Millisecond {
		t.Fatalf("real wall-clock time (%v) should be practically zero", time.Since(wallStart))
	}

	cluster.Advance(60 * time.Millisecond)
	assertClusterFollowers(t, cluster, newLeader.ID)
	if newLeader.Status().Term <= initialTerm {
		t.Fatalf("expected new leader term > %d, got %d", initialTerm, newLeader.Status().Term)
	}
}

// TestHarness_Network_Partition_OldLeaderIsolatedWithMinority_MustNotKeepActingAsLeader verifies that:
// When a network partition isolates the leader with a minority of nodes (e.g., 2 nodes out of 5),
// the leader loses quorum and must NOT keep acting as leader (§6.2 Check Quorum).
func TestHarness_Network_Partition_OldLeaderIsolatedWithMinority_MustNotKeepActingAsLeader(t *testing.T) {
	minTimeout, maxTimeout := 100*time.Millisecond, 200*time.Millisecond
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(101)
	simNet.SetClock(clk)

	cluster := NewCluster(t, 5,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithElectionTimeout(minTimeout, maxTimeout),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithCheckQuorum(true),
		WithCheckQuorumInterval(100*time.Millisecond),
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}

	cluster.Advance(60 * time.Millisecond)
	cluster.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3, 4, 5})

	steppedDown := cluster.AdvanceUntil(2*time.Second, func() bool {
		return cluster.GetNode(1).GetState() != raft.Leader
	})
	if !steppedDown {
		t.Fatalf("isolated leader Node 1 with minority must step down to Follower")
	}

	var majorityLeader *Server
	elected := cluster.AdvanceUntil(3*time.Second, func() bool {
		for _, id := range []raft.NodeID{3, 4, 5} {
			node := cluster.GetNode(id)
			if node.GetState() == raft.Leader && node.Status().Term > initialTerm {
				majorityLeader = node
				return true
			}
		}
		return false
	})
	if !elected || majorityLeader == nil {
		t.Fatalf("majority partition {3, 4, 5} failed to elect a new leader")
	}
	if cluster.GetNode(1).GetState() == raft.Leader {
		t.Fatalf("Node 1 should not be leader after partition")
	}
}

// TestHarness_Network_Partition_Heals_OldLeaderStepsDownOnHigherTerm verifies that:
// When partition heals, the isolated old leader steps down on seeing the higher term (§5.1).
func TestHarness_Network_Partition_Heals_OldLeaderStepsDownOnHigherTerm(t *testing.T) {
	minTimeout, maxTimeout := 100*time.Millisecond, 200*time.Millisecond
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(202)
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
	if err != nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}

	cluster.Advance(60 * time.Millisecond)
	cluster.Partition([]raft.NodeID{1}, []raft.NodeID{2, 3})

	var majorityLeader *Server
	elected := cluster.AdvanceUntil(3*time.Second, func() bool {
		for _, id := range []raft.NodeID{2, 3} {
			node := cluster.GetNode(id)
			if node.GetState() == raft.Leader && node.Status().Term > initialTerm {
				majorityLeader = node
				return true
			}
		}
		return false
	})
	if !elected || majorityLeader == nil {
		t.Fatalf("majority partition failed to elect new leader")
	}

	higherTerm := majorityLeader.Status().Term
	cluster.Heal()

	steppedDown := cluster.AdvanceUntil(2*time.Second, func() bool {
		return cluster.GetNode(1).GetState() == raft.Follower && cluster.GetNode(1).Status().Term >= higherTerm
	})
	if !steppedDown {
		t.Fatalf("old leader Node 1 failed to step down after heal")
	}
}

// TestHarness_Cluster5_Tolerates2NodeFailures_StillElectsLeader verifies that:
// In a 5-node Raft cluster, up to 2 simultaneous node failures are tolerated.
func TestHarness_Cluster5_Tolerates2NodeFailures_StillElectsLeader(t *testing.T) {
	minTimeout, maxTimeout := 100*time.Millisecond, 200*time.Millisecond
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(303)
	simNet.SetClock(clk)

	cluster := NewCluster(t, 5,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithElectionTimeout(minTimeout, maxTimeout),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
	)

	cluster.GetNode(1).StartElection()
	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader.ID != 1 {
		t.Fatalf("failed to elect initial leader: %v", err)
	}

	cluster.Advance(60 * time.Millisecond)
	killSimTime := clk.Now()
	cluster.Kill(1)
	cluster.Kill(2)

	var newLeader *Server
	elected := cluster.AdvanceUntil(3*time.Second, func() bool {
		for _, id := range []raft.NodeID{3, 4, 5} {
			node := cluster.GetNode(id)
			if node.GetState() == raft.Leader && node.Status().Term > initialTerm {
				newLeader = node
				return true
			}
		}
		return false
	})
	if !elected || newLeader == nil {
		t.Fatalf("5-node cluster failed to elect new leader after 2 failures")
	}

	simTimeToNewLeader := clk.Now().Sub(killSimTime)
	verifySimFailoverBounds(t, simTimeToNewLeader, minTimeout, maxTimeout)
	cluster.Advance(50 * time.Millisecond)
	assertClusterFollowers(t, cluster, newLeader.ID)
}

// TestHarness_Determinism_NoWallClockDrift confirms that simulation runs
// successfully elect valid leaders and maintain Raft invariants across multiple trials,
// completely unaffected by real wall-clock time or goroutine scheduling order.
func TestHarness_Determinism_NoWallClockDrift(t *testing.T) {
	wallStart := time.Now()
	runSimulation := func(seed int64) (raft.NodeID, raft.Term, time.Duration) {
		clk := raft.NewFakeClock()
		simNet := NewSimNetwork(seed)
		simNet.SetClock(clk)

		cluster := NewCluster(t, 3,
			WithClock(clk),
			WithSimNetwork(simNet),
			WithRandSeed(seed),
			WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
			WithHeartbeatInterval(20*time.Millisecond),
			WithRPCTimeout(40*time.Millisecond),
		)
		defer cluster.Stop()

		cluster.GetNode(1).StartElection()
		leader, _, err := cluster.WaitLeader(2 * time.Second)
		if err != nil || leader == nil {
			t.Fatalf("failed initial election in simulation: %v", err)
		}
		cluster.Advance(60 * time.Millisecond)

		killTime := clk.Now()
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
			t.Fatalf("failed to elect new leader in trial")
		}
		return newLeader.ID, newLeader.Status().Term, clk.Now().Sub(killTime)
	}

	for trial := 1; trial <= 5; trial++ {
		leader, term, duration := runSimulation(int64(trial * 100))
		if leader == 1 {
			t.Fatalf("trial %d: killed leader 1 was re-elected", trial)
		}
		if term <= 1 {
			t.Fatalf("trial %d: expected new leader term > 1, got %d", trial, term)
		}
		if duration < 50*time.Millisecond || duration > 2*time.Second {
			t.Fatalf("trial %d: failover duration %v outside [50ms, 2s]", trial, duration)
		}
	}

	if time.Since(wallStart) > 500*time.Millisecond {
		t.Fatalf("total real wall-clock time (%v) exceeded 500ms", time.Since(wallStart))
	}
}

// TestHarness_Network_DropMessage verifies that dropped messages prevent delivery,
// and that Undrop / ClearDrops restores normal communication.
func TestHarness_Network_DropMessage(t *testing.T) {
	cluster := NewCluster(t, 2, WithAutoStart(false))

	net := cluster.Network
	node1 := cluster.GetNode(1)
	node2 := cluster.GetNode(2)

	// Drop messages from 1 to 2
	net.Drop(1, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Sending RequestVote from 1 to 2 should be dropped
	_, err := node1.SendRequestVote(ctx, Peer{ID: 2}, &raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err == nil {
		t.Fatalf("expected error when sending dropped message, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable on dropped message, got %v", err)
	}

	if net.TotalDropped() == 0 {
		t.Fatalf("expected TotalDropped() > 0, got %d", net.TotalDropped())
	}

	// Reverse direction (2 to 1) is not dropped
	reply, err := node2.SendRequestVote(ctx, Peer{ID: 1}, &raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("reverse direction should succeed, got error: %v", err)
	}
	if reply == nil {
		t.Fatalf("expected non-nil reply for allowed message")
	}

	// Undrop 1 to 2
	net.Undrop(1, 2)

	reply2, err := node1.SendRequestVote(ctx, Peer{ID: 2}, &raft.RequestVoteArgs{
		Term:         2,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("after Undrop, message should succeed, got: %v", err)
	}
	if reply2 == nil {
		t.Fatalf("expected non-nil reply after Undrop")
	}
}

// TestHarness_Network_DelayMessage verifies artificial message latency injection
// and context cancellation when delay exceeds deadline.
func TestHarness_Network_DelayMessage(t *testing.T) {
	cluster := NewCluster(t, 2, WithAutoStart(false))

	net := cluster.Network
	node1 := cluster.GetNode(1)

	// Set 60ms delay from 1 to 2
	net.SetDelay(1, 2, 60*time.Millisecond)

	// Test 1: Call with ample deadline (150ms) -> succeeds, but takes >= 55ms
	ctx1, cancel1 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel1()

	start := time.Now()
	reply, err := node1.SendRequestVote(ctx1, Peer{ID: 2}, &raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected call to succeed, got: %v", err)
	}
	if reply == nil {
		t.Fatalf("expected non-nil reply")
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected call to take at least 50ms due to delay, took: %v", elapsed)
	}

	// Test 2: Call with short deadline (20ms) -> times out due to 60ms delay
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()

	_, err2 := node1.SendRequestVote(ctx2, Peer{ID: 2}, &raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err2 == nil {
		t.Fatalf("expected context deadline exceeded error, got nil")
	}

	// Clear delays
	net.ClearDelays()

	start3 := time.Now()
	ctx3, cancel3 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel3()

	_, err3 := node1.SendRequestVote(ctx3, Peer{ID: 2}, &raft.RequestVoteArgs{
		Term:         2,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	elapsed3 := time.Since(start3)

	if err3 != nil {
		t.Fatalf("expected success after ClearDelays, got: %v", err3)
	}
	if elapsed3 >= 40*time.Millisecond {
		t.Fatalf("expected fast response without delay, took %v", elapsed3)
	}
}

// TestHarness_Network_CustomMessageFilter verifies arbitrary custom filter predicates.
func TestHarness_Network_CustomMessageFilter(t *testing.T) {
	cluster := NewCluster(t, 2, WithAutoStart(false))

	net := cluster.Network
	node1 := cluster.GetNode(1)

	// Filter that drops only RequestVote RPCs but permits AppendEntries
	net.AddFilter(func(src, dst raft.NodeID, msgType RPCMessageType, msg any) bool {
		return msgType == RPCRequestVote
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// RequestVote should be dropped by filter
	_, err := node1.SendRequestVote(ctx, Peer{ID: 2}, &raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err == nil {
		t.Fatalf("expected RequestVote to be dropped by filter")
	}

	// AppendEntries should pass through filter
	aeReply, err := node1.SendAppendEntries(ctx, Peer{ID: 2}, &raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: 1,
	})
	if err != nil {
		t.Fatalf("expected AppendEntries to pass filter, got: %v", err)
	}
	if aeReply == nil {
		t.Fatalf("expected non-nil AppendEntries reply")
	}
}
