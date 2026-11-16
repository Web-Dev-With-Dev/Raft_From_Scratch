package server

import (
	"fmt"
	"testing"
	"time"

	"raft/raft"
)

func createFaultCluster(t *testing.T, n int, seed int64) (*Cluster, *GlobalChecker) {
	t.Helper()
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(seed)
	simNet.SetClock(clk)
	checker := NewGlobalChecker()

	c := NewCluster(t, n,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(seed),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithPreVote(true),
		WithStickiness(true),
		WithCheckQuorum(true),
		WithGlobalChecker(checker),
		WithQuiet(true),
	)
	return c, checker
}

func runLeaderCrashTest(t *testing.T, n int) {
	c, checker := createFaultCluster(t, n, int64(100+n))
	c.GetNode(1).StartElection()
	leader, initialTerm, err := c.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("N=%d: failed initial election: %v", n, err)
	}
	checker.Assert(t, c)

	c.Kill(leader.ID)
	c.Advance(30 * time.Millisecond)

	newLeader, newTerm, err := c.WaitLeader(2 * time.Second)
	if err != nil || newLeader == nil {
		t.Fatalf("N=%d: liveness failed after leader crash: %v", n, err)
	}
	if newLeader.ID == leader.ID {
		t.Fatalf("N=%d: killed leader %d re-elected", n, leader.ID)
	}
	if newTerm <= initialTerm {
		t.Fatalf("N=%d: expected new term > %d, got %d", n, initialTerm, newTerm)
	}
	checker.Assert(t, c)
}

func runFollowerCrashTest(t *testing.T, n int) {
	c, checker := createFaultCluster(t, n, int64(200+n))
	c.GetNode(1).StartElection()
	leader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("N=%d: failed initial election: %v", n, err)
	}
	checker.Assert(t, c)

	var followerID raft.NodeID
	for _, id := range c.NodeIDs() {
		if id != leader.ID {
			followerID = id
			break
		}
	}
	c.Kill(followerID)
	c.Advance(200 * time.Millisecond)

	activeLeader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || activeLeader == nil {
		t.Fatalf("N=%d: liveness failed after follower crash: %v", n, err)
	}
	checker.Assert(t, c)
}

func runMinorityPartitionTest(t *testing.T, n int) {
	c, checker := createFaultCluster(t, n, int64(300+n))
	c.GetNode(1).StartElection()
	leader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("N=%d: failed initial election: %v", n, err)
	}

	minSize := (n - 1) / 2
	minSet := []raft.NodeID{leader.ID}
	for _, id := range c.NodeIDs() {
		if len(minSet) >= minSize {
			break
		}
		if id != leader.ID {
			minSet = append(minSet, id)
		}
	}
	minMap := make(map[raft.NodeID]bool)
	for _, id := range minSet {
		minMap[id] = true
	}
	majSet := make([]raft.NodeID, 0, n-len(minSet))
	for _, id := range c.NodeIDs() {
		if !minMap[id] {
			majSet = append(majSet, id)
		}
	}

	c.Partition(minSet, majSet)
	c.Advance(150 * time.Millisecond)

	newLeader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || newLeader == nil || minMap[newLeader.ID] {
		t.Fatalf("N=%d: liveness failed in majority partition: %v", n, err)
	}
	checker.Assert(t, c)

	c.Heal()
	c.Advance(200 * time.Millisecond)
	if _, _, err := c.WaitLeader(2 * time.Second); err != nil {
		t.Fatalf("N=%d: convergence failed after heal: %v", n, err)
	}
	checker.Assert(t, c)
}

func runMajorityPartitionTest(t *testing.T, n int) {
	c, checker := createFaultCluster(t, n, int64(400+n))
	majSize := (n / 2) + 1
	allIDs := c.NodeIDs()
	majSet := allIDs[:majSize]
	minSet := allIDs[majSize:]

	c.Partition(majSet, minSet)
	c.GetNode(majSet[0]).StartElection()

	leader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("N=%d: majority partition failed to elect leader: %v", n, err)
	}
	isMajLeader := false
	for _, id := range majSet {
		if leader.ID == id {
			isMajLeader = true
			break
		}
	}
	if !isMajLeader {
		t.Fatalf("N=%d: elected leader %d not in majority set", n, leader.ID)
	}
	checker.Assert(t, c)

	c.Heal()
	c.Advance(200 * time.Millisecond)
	if _, _, err := c.WaitLeader(2 * time.Second); err != nil {
		t.Fatalf("N=%d: convergence failed after heal: %v", n, err)
	}
	checker.Assert(t, c)
}

func runFlappingLinksTest(t *testing.T, n int) {
	c, checker := createFaultCluster(t, n, int64(500+n))
	c.GetNode(1).StartElection()
	_, _, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("N=%d: initial election failed: %v", n, err)
	}

	n1, n2 := c.NodeIDs()[0], c.NodeIDs()[1]
	for i := 0; i < 4; i++ {
		c.Drop(n1, n2)
		c.Drop(n2, n1)
		c.Advance(40 * time.Millisecond)
		c.Heal()
		c.Advance(40 * time.Millisecond)
		checker.Assert(t, c)
	}

	leader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("N=%d: liveness failed after flapping links: %v", n, err)
	}
	checker.Assert(t, c)
}

func runOneSlowNodeTest(t *testing.T, n int) {
	c, checker := createFaultCluster(t, n, int64(600+n))
	allIDs := c.NodeIDs()
	slowNode := allIDs[n-1]

	for _, peerID := range allIDs {
		if peerID != slowNode {
			c.SetDelayBidirectional(slowNode, peerID, 80*time.Millisecond)
		}
	}

	c.GetNode(allIDs[0]).StartElection()
	leader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("N=%d: failed to elect leader with one slow node: %v", n, err)
	}
	checker.Assert(t, c)

	c.Advance(300 * time.Millisecond)
	activeLeader, _, err := c.WaitLeader(1 * time.Second)
	if err != nil || activeLeader == nil {
		t.Fatalf("N=%d: cluster lost leader with slow node: %v", n, err)
	}
	checker.Assert(t, c)
}

// TestFaultMatrix executes all 6 fault types across cluster sizes 3, 5, 7
// (18 test configurations total), asserting liveness and safety invariants.
func TestFaultMatrix(t *testing.T) {
	sizes := []int{3, 5, 7}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			t.Run("LeaderCrash", func(t *testing.T) { runLeaderCrashTest(t, n) })
			t.Run("FollowerCrash", func(t *testing.T) { runFollowerCrashTest(t, n) })
			t.Run("MinorityPartition", func(t *testing.T) { runMinorityPartitionTest(t, n) })
			t.Run("MajorityPartition", func(t *testing.T) { runMajorityPartitionTest(t, n) })
			t.Run("FlappingLinks", func(t *testing.T) { runFlappingLinksTest(t, n) })
			t.Run("OneSlowNode", func(t *testing.T) { runOneSlowNodeTest(t, n) })
		})
	}
}

