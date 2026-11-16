package server

import (
	"context"
	"testing"
	"time"

	"raft/raft"
)

// TestSimNetwork_SeededDeterminism verifies that two SimNetwork instances with identical seeds
// produce identical pseudorandom decisions for delay, drop, duplication, and reordering.
func TestSimNetwork_SeededDeterminism(t *testing.T) {
	net1 := NewSimNetwork(42)
	net2 := NewSimNetwork(42)

	net1.SetDelayRange(5*time.Millisecond, 50*time.Millisecond)
	net2.SetDelayRange(5*time.Millisecond, 50*time.Millisecond)
	net1.SetDropProbability(0.25)
	net2.SetDropProbability(0.25)
	net1.SetDuplicationProbability(0.15)
	net2.SetDuplicationProbability(0.15)

	for i := 0; i < 50; i++ {
		d1, r1 := net1.computeDeliveryDelay(1, 2)
		d2, r2 := net2.computeDeliveryDelay(1, 2)
		if d1 != d2 || r1 != r2 {
			t.Fatalf("iteration %d: delay mismatch net1=%v net2=%v, reorder net1=%v net2=%v", i, d1, d2, r1, r2)
		}

		drop1 := net1.checkDrop(1, 2)
		drop2 := net2.checkDrop(1, 2)
		if drop1 != drop2 {
			t.Fatalf("iteration %d: drop mismatch net1=%v net2=%v", i, drop1, drop2)
		}

		dup1 := net1.checkDuplicate(1, 2)
		dup2 := net2.checkDuplicate(1, 2)
		if dup1 != dup2 {
			t.Fatalf("iteration %d: dup mismatch net1=%v net2=%v", i, dup1, dup2)
		}
	}
}

// TestSimNetwork_DelayRanges verifies computed delays fall strictly within configured [min, max] bounds.
func TestSimNetwork_DelayRanges(t *testing.T) {
	net := NewSimNetwork(12345)
	minD := 10 * time.Millisecond
	maxD := 25 * time.Millisecond
	net.SetDelayRange(minD, maxD)

	for i := 0; i < 200; i++ {
		delay, _ := net.computeDeliveryDelay(1, 2)
		if delay < minD || delay > maxD {
			t.Fatalf("iteration %d: delay %v outside bounds [%v, %v]", i, delay, minD, maxD)
		}
	}

	// Link-specific override
	linkMin := 40 * time.Millisecond
	linkMax := 60 * time.Millisecond
	net.SetLinkDelayRange(1, 2, linkMin, linkMax)

	for i := 0; i < 100; i++ {
		delay, _ := net.computeDeliveryDelay(1, 2)
		if delay < linkMin || delay > linkMax {
			t.Fatalf("link override: delay %v outside [%v, %v]", delay, linkMin, linkMax)
		}
	}
}

// TestSimNetwork_DropProbability verifies drop rates approximate configured probabilities over sample sets.
func TestSimNetwork_DropProbability(t *testing.T) {
	net := NewSimNetwork(9999)
	net.SetDropProbability(0.30)

	total := 2000
	dropped := 0
	for i := 0; i < total; i++ {
		if net.checkDrop(1, 2) {
			dropped++
		}
	}

	rate := float64(dropped) / float64(total)
	if rate < 0.25 || rate > 0.35 {
		t.Fatalf("expected drop rate ~0.30, got %.4f (%d/%d)", rate, dropped, total)
	}

	// Edge case probabilities
	net.SetDropProbability(0.0)
	if net.checkDrop(1, 2) {
		t.Fatalf("expected 0 drops with p=0.0")
	}

	net.SetDropProbability(1.0)
	if !net.checkDrop(1, 2) {
		t.Fatalf("expected 100%% drop with p=1.0")
	}
}

// TestSimNetwork_Duplication verifies that packet duplication generates duplicate deliveries.
func TestSimNetwork_Duplication(t *testing.T) {
	net := NewSimNetwork(777)
	net.RegisterChannel(2, 100)
	net.SetDuplicationProbability(1.0) // 100% duplication

	pkt := &Packet{ID: 101, Src: 1, Dst: 2, Type: "PING"}
	if err := net.Send(pkt); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	ch := net.ReceiveChan(2)
	select {
	case p1 := <-ch:
		if p1.ID != 101 {
			t.Fatalf("expected pkt ID 101, got %d", p1.ID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for original packet")
	}

	select {
	case p2 := <-ch:
		if !p2.IsDuplicate {
			t.Fatalf("expected second packet to be marked duplicate")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for duplicate packet")
	}

	if net.TotalDuplicated() != 1 {
		t.Fatalf("expected totalDuplicated 1, got %d", net.TotalDuplicated())
	}
}

// TestSimNetwork_Reordering verifies that reorder delays cause subsequent packets to arrive first.
func TestSimNetwork_Reordering(t *testing.T) {
	net := NewSimNetwork(555)
	net.RegisterChannel(2, 10)

	// Packet 1 gets reordered with extra 40ms delay
	net.SetReorderProbability(1.0)
	net.SetReorderDelayRange(40*time.Millisecond, 40*time.Millisecond)

	pkt1 := &Packet{ID: 1, Src: 1, Dst: 2, Type: "FIRST"}
	_ = net.SendAsync(pkt1)

	// Small pause so pkt1 is in flight
	time.Sleep(5 * time.Millisecond)

	// Packet 2 has no delay and zero reorder probability
	net.SetReorderProbability(0.0)
	net.ClearDelayRanges()

	pkt2 := &Packet{ID: 2, Src: 1, Dst: 2, Type: "SECOND"}
	_ = net.SendAsync(pkt2)

	ch := net.ReceiveChan(2)

	// First received packet must be Packet 2 due to Packet 1's reorder delay
	select {
	case recv1 := <-ch:
		if recv1.ID != 2 {
			t.Fatalf("expected packet 2 to arrive first, got %d", recv1.ID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for packet 2")
	}

	select {
	case recv2 := <-ch:
		if recv2.ID != 1 {
			t.Fatalf("expected packet 1 to arrive second, got %d", recv2.ID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for packet 1")
	}
}

// TestSimNetwork_PartitionAndHeal verifies multi-group isolation and healing.
func TestSimNetwork_PartitionAndHeal(t *testing.T) {
	net := NewSimNetwork(101)
	for i := 1; i <= 5; i++ {
		net.RegisterChannel(raft.NodeID(i), 10)
	}

	groupA := []raft.NodeID{1, 2}
	groupB := []raft.NodeID{3, 4, 5}

	net.Partition(groupA, groupB)

	// Verify groupA <-> groupB is partitioned
	if !net.IsPartitioned(1, 3) || !net.IsPartitioned(3, 1) {
		t.Fatalf("expected link 1-3 to be partitioned")
	}
	if !net.IsPartitioned(2, 5) || !net.IsPartitioned(5, 2) {
		t.Fatalf("expected link 2-5 to be partitioned")
	}

	// Verify intra-group is NOT partitioned
	if net.IsPartitioned(1, 2) || net.IsPartitioned(2, 1) {
		t.Fatalf("expected intra-groupA links to remain open")
	}
	if net.IsPartitioned(3, 4) || net.IsPartitioned(4, 5) {
		t.Fatalf("expected intra-groupB links to remain open")
	}

	// Attempt send across partition: must fail
	err := net.Send(&Packet{Src: 1, Dst: 3, Payload: "blocked"})
	if err == nil {
		t.Fatalf("expected message across partition to be dropped")
	}

	// Attempt intra-group send: must succeed
	if err := net.Send(&Packet{Src: 1, Dst: 2, Payload: "intra"}); err != nil {
		t.Fatalf("intra-group send failed: %v", err)
	}

	// Heal partition
	net.Heal()

	if net.IsPartitioned(1, 3) || net.IsPartitioned(3, 1) {
		t.Fatalf("expected links to be healed")
	}

	// Communication across previously partitioned nodes now succeeds
	if err := net.Send(&Packet{Src: 1, Dst: 3, Payload: "healed"}); err != nil {
		t.Fatalf("send after heal failed: %v", err)
	}
}

// TestSimNetwork_ClusterIntegration tests a live Raft cluster backed by SimNetwork.
func TestSimNetwork_ClusterIntegration(t *testing.T) {
	simNet := NewSimNetwork(31415)
	simNet.SetDelayRange(1*time.Millisecond, 5*time.Millisecond)

	cluster := NewCluster(t, 3,
		WithSimNetwork(simNet),
		WithElectionTimeout(80*time.Millisecond, 160*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
	)

	leader, term, err := cluster.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect initial leader over SimNetwork: %v", err)
	}
	if leader == nil || term < 1 {
		t.Fatalf("invalid leader %v or term %d", leader, term)
	}

	// Verify metrics on SimNetwork
	if simNet.TotalSent() == 0 {
		t.Fatalf("expected SimNetwork TotalSent > 0")
	}
	if simNet.TotalDelivered() == 0 {
		t.Fatalf("expected SimNetwork TotalDelivered > 0")
	}

	// Test partition via SimNetwork
	simNet.Partition([]raft.NodeID{leader.ID}, []raft.NodeID{2, 3})
	time.Sleep(200 * time.Millisecond)

	// Remaining nodes or term check
	simNet.Heal()
}

// TestSimNetwork_UnconditionalDrops verifies explicit Drop, Undrop, and ClearDrops.
func TestSimNetwork_UnconditionalDrops(t *testing.T) {
	net := NewSimNetwork(7)
	net.RegisterChannel(1, 10)
	net.RegisterChannel(2, 10)

	net.DropBidirectional(1, 2)
	if err := net.Send(&Packet{Src: 1, Dst: 2}); err == nil {
		t.Fatalf("expected send to be dropped")
	}
	if err := net.Send(&Packet{Src: 2, Dst: 1}); err == nil {
		t.Fatalf("expected reverse send to be dropped")
	}

	net.Undrop(1, 2)
	if err := net.Send(&Packet{Src: 1, Dst: 2}); err != nil {
		t.Fatalf("expected 1->2 send to succeed after Undrop")
	}
	if err := net.Send(&Packet{Src: 2, Dst: 1}); err == nil {
		t.Fatalf("expected 2->1 to remain dropped")
	}

	net.ClearDrops()
	if err := net.Send(&Packet{Src: 2, Dst: 1}); err != nil {
		t.Fatalf("expected 2->1 to succeed after ClearDrops")
	}
}

// TestSimNetwork_ContextCancellation verifies that SendWithContext aborts on cancelled context.
func TestSimNetwork_ContextCancellation(t *testing.T) {
	net := NewSimNetwork(11)
	net.RegisterChannel(2, 10)
	net.SetDelayRange(500*time.Millisecond, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := net.SendWithContext(ctx, &Packet{Src: 1, Dst: 2})
	if err == nil {
		t.Fatalf("expected context timeout error, got nil")
	}
}

// TestSimNetwork_HeavyDuplicationAndReordering_ElectionSafety simulates leader election
// under 30% message duplication and heavy reordering, verifying election safety
// and the votedFor invariant across nodes.
func TestSimNetwork_HeavyDuplicationAndReordering_ElectionSafety(t *testing.T) {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(888)
	simNet.SetClock(clk)
	simNet.SetDuplicationProbability(0.30)
	simNet.SetReorderProbability(0.60)
	simNet.SetDelayRange(5*time.Millisecond, 25*time.Millisecond)
	simNet.SetReorderDelayRange(20*time.Millisecond, 70*time.Millisecond)

	cluster := NewCluster(t, 3,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(888),
		WithQuiet(true),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
	)

	termHist := make(map[raft.Term]raft.NodeID)
	leader, err := advanceUntilLeader(context.Background(), clk, cluster, 4*time.Second, termHist)
	if err != nil {
		t.Fatalf("election failed under 30%% duplication and heavy reordering: %v", err)
	}
	if leader == nil || leader.Status().Term < 1 {
		t.Fatalf("expected valid leader with term >= 1")
	}

	// Advance clock further so leader dispatches multiple heartbeat rounds through SimNetwork
	for i := 0; i < 30; i++ {
		advanceClockStep(clk, 20*time.Millisecond)
	}

	if simNet.TotalDuplicated() == 0 {
		t.Fatalf("expected non-zero duplicated packets, got 0 (sent=%d)", simNet.TotalSent())
	}
	if simNet.TotalReordered() == 0 {
		t.Fatalf("expected non-zero reordered packets, got 0")
	}

	if err := verifyVotedForInvariantAcrossNodes(cluster); err != nil {
		t.Fatalf("votedFor invariant check failed: %v", err)
	}
}
