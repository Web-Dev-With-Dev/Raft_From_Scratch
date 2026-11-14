package server

import (
	"context"
	"testing"
	"time"

	"raft/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestHarness_Day20_CrashScenario verifies Day 20's leader crash and failover
// scenario using the in-process test harness and in-memory fake network.
func TestHarness_Day20_CrashScenario(t *testing.T) {
	// Raft §5.2: Leader failure and re-election.
	// 1. Setup a 3-node in-process cluster with in-memory fake network.
	// 2. Elect Node 1 as initial leader in Term 1.
	// 3. Mid-test, abruptly kill the leader process (simulating crash).
	// 4. Remaining followers detect leader silence via election timeout and elect a single new leader.
	// 5. Measure and log time-to-new-leader (should be roughly 1-3x election timeout).

	minTimeout := 100 * time.Millisecond
	maxTimeout := 200 * time.Millisecond
	heartbeatInterval := 20 * time.Millisecond

	cluster := NewCluster(t, 3,
		WithElectionTimeout(minTimeout, maxTimeout),
		WithHeartbeatInterval(heartbeatInterval),
		WithRPCTimeout(40*time.Millisecond),
	)

	// Elect initial leader (trigger election on Node 1)
	cluster.GetNode(1).StartElection()

	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect initial leader: %v", err)
	}
	if leader.ID != 1 {
		t.Fatalf("expected Node 1 to become initial leader, got Node %d", leader.ID)
	}
	if initialTerm < 1 {
		t.Fatalf("expected initial leader term >= 1, got %d", initialTerm)
	}

	// Allow heartbeats to stabilize followers
	time.Sleep(60 * time.Millisecond)

	for _, id := range cluster.NodeIDs() {
		if id != leader.ID {
			if st := cluster.GetNode(id).GetState(); st != raft.Follower {
				t.Fatalf("expected node %d to be Follower, got %s", id, st)
			}
		}
	}

	// 3. Abruptly kill the leader process mid-test
	killTime := time.Now()
	cluster.Kill(leader.ID)

	if !leader.IsKilled() {
		t.Fatalf("expected leader.IsKilled() == true after Kill()")
	}

	// 4. Poll surviving nodes (Node 2 and Node 3) for new leader election
	reElectionDeadline := time.Now().Add(3 * time.Second)
	var newLeader *Server
	for time.Now().Before(reElectionDeadline) {
		l, _ := cluster.Leader()
		if l != nil && l.ID != leader.ID {
			newLeader = l
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	timeToNewLeader := time.Since(killTime)

	if newLeader == nil {
		t.Fatalf("surviving nodes failed to elect a new leader within %v (node 2: %s, node 3: %s)",
			time.Since(killTime), cluster.GetNode(2).GetState(), cluster.GetNode(3).GetState())
	}

	newTerm := newLeader.Status().Term

	// 5. Measure and log time-to-new-leader
	t.Logf("================================================================================")
	t.Logf("DAY 20 IN-MEMORY HARNESS CRASH FAILOVER MEASUREMENT:")
	t.Logf("Leader Node %d killed at: %v", leader.ID, killTime.Format("15:04:05.000"))
	t.Logf("New Leader elected: Node %d at term %d", newLeader.ID, newTerm)
	t.Logf("Time to elect new leader: %v (%.2f ms)", timeToNewLeader, float64(timeToNewLeader.Microseconds())/1000.0)
	t.Logf("Configured Election Timeout Range: [%v, %v]", minTimeout, maxTimeout)
	t.Logf("Configured Heartbeat Interval: %v", heartbeatInterval)
	t.Logf("Ratio to Min Election Timeout: %.2fx", float64(timeToNewLeader)/float64(minTimeout))
	t.Logf("Ratio to Max Election Timeout: %.2fx", float64(timeToNewLeader)/float64(maxTimeout))
	t.Logf("================================================================================")

	// Assertions
	if newLeader.ID == leader.ID {
		t.Fatalf("new leader cannot be the killed node %d", leader.ID)
	}
	if newTerm <= initialTerm {
		t.Fatalf("expected new leader term > %d, got %d", initialTerm, newTerm)
	}

	// Allow heartbeats from new leader to stabilize the surviving follower
	time.Sleep(60 * time.Millisecond)

	for _, id := range cluster.NodeIDs() {
		node := cluster.GetNode(id)
		if id == leader.ID {
			if !node.IsKilled() {
				t.Fatalf("killed leader %d should report IsKilled() == true", id)
			}
		} else if id != newLeader.ID {
			if node.GetState() != raft.Follower {
				t.Fatalf("surviving node %d should be Follower, got %s", id, node.GetState())
			}
		}
	}

	// Verify bounds (~1-3x election timeout)
	if timeToNewLeader < minTimeout/2 {
		t.Fatalf("time-to-new-leader %v unexpectedly faster than minTimeout/2 (%v)", timeToNewLeader, minTimeout/2)
	}
	maxAllowed := 3*maxTimeout + 300*time.Millisecond
	if timeToNewLeader > maxAllowed {
		t.Fatalf("time-to-new-leader %v exceeded upper bound %v (~3x timeout)", timeToNewLeader, maxAllowed)
	}
}

// TestHarness_Network_Partition_MajorityElectsLeader_MinorityStepsDown verifies:
// 1. A 3-node cluster elects Node 1 leader.
// 2. Partitioning {1} (minority) from {2, 3} (majority) causes {2, 3} to elect a new leader.
// 3. Healing the partition causes old leader Node 1 to step down to Follower.
func TestHarness_Network_Partition_MajorityElectsLeader_MinorityStepsDown(t *testing.T) {
	minTimeout := 100 * time.Millisecond
	maxTimeout := 200 * time.Millisecond
	heartbeatInterval := 20 * time.Millisecond

	cluster := NewCluster(t, 3,
		WithElectionTimeout(minTimeout, maxTimeout),
		WithHeartbeatInterval(heartbeatInterval),
		WithRPCTimeout(40*time.Millisecond),
	)

	// Elect Node 1 as initial leader
	cluster.GetNode(1).StartElection()

	leader, initialTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect initial leader: %v", err)
	}
	if leader.ID != 1 {
		t.Fatalf("expected Node 1 leader, got %d", leader.ID)
	}

	time.Sleep(60 * time.Millisecond)

	// Partition: {1} is isolated from {2, 3}
	cluster.Partition([]raft.NodeID{1}, []raft.NodeID{2, 3})

	if !cluster.Network.IsPartitioned(1, 2) || !cluster.Network.IsPartitioned(2, 1) {
		t.Fatalf("expected nodes 1 and 2 to be partitioned")
	}
	if !cluster.Network.IsPartitioned(1, 3) || !cluster.Network.IsPartitioned(3, 1) {
		t.Fatalf("expected nodes 1 and 3 to be partitioned")
	}
	if cluster.Network.IsPartitioned(2, 3) || cluster.Network.IsPartitioned(3, 2) {
		t.Fatalf("nodes 2 and 3 must NOT be partitioned from each other")
	}

	// Majority partition {2, 3} should time out and elect a new leader
	reElectionDeadline := time.Now().Add(3 * time.Second)
	var majorityLeader *Server
	for time.Now().Before(reElectionDeadline) {
		for _, id := range []raft.NodeID{2, 3} {
			node := cluster.GetNode(id)
			if node.GetState() == raft.Leader && node.Status().Term > initialTerm {
				majorityLeader = node
				break
			}
		}
		if majorityLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if majorityLeader == nil {
		t.Fatalf("majority partition {2, 3} failed to elect a new leader (node 2: %s, node 3: %s)",
			cluster.GetNode(2).GetState(), cluster.GetNode(3).GetState())
	}

	higherTerm := majorityLeader.Status().Term
	t.Logf("Majority partition {2, 3} elected Node %d in term %d (initial term was %d)",
		majorityLeader.ID, higherTerm, initialTerm)

	if higherTerm <= initialTerm {
		t.Fatalf("expected majority leader term > %d, got %d", initialTerm, higherTerm)
	}

	// Now heal the partition
	cluster.Heal()

	if cluster.Network.IsPartitioned(1, 2) || cluster.Network.IsPartitioned(1, 3) {
		t.Fatalf("expected all partitions to be cleared after Heal()")
	}

	// Old leader (Node 1) receives heartbeats from new leader, observes higher term, and must step down to Follower
	stepDownDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(stepDownDeadline) {
		if cluster.GetNode(1).GetState() == raft.Follower {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if cluster.GetNode(1).GetState() != raft.Follower {
		t.Fatalf("expected old leader Node 1 to step down to Follower after partition heal, got %s",
			cluster.GetNode(1).GetState())
	}
	if cluster.GetNode(1).Status().Term < higherTerm {
		t.Fatalf("expected Node 1 term to update to >= %d, got %d",
			higherTerm, cluster.GetNode(1).Status().Term)
	}

	t.Logf("Partition healed: Node 1 successfully stepped down to Follower at term %d",
		cluster.GetNode(1).Status().Term)
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
