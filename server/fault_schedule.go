package server

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"raft/raft"
)

// FaultType specifies the type of chaos fault injected into the cluster.
type FaultType string

const (
	FaultCrash     FaultType = "crash"
	FaultRestart   FaultType = "restart"
	FaultPartition FaultType = "partition"
	FaultHeal      FaultType = "heal"
	FaultClockJump FaultType = "clock_jump"
	FaultDropLink  FaultType = "drop_link"
	FaultAdvance   FaultType = "advance"
)

// FaultStep represents a single deterministic fault event in a schedule.
type FaultStep struct {
	Type     FaultType
	NodeA    raft.NodeID
	NodeB    raft.NodeID
	SetA     []raft.NodeID
	SetB     []raft.NodeID
	Duration time.Duration
}

// FaultSchedule contains the configuration and steps for a randomized chaos run.
type FaultSchedule struct {
	Seed        int64
	ClusterSize int
	Steps       []FaultStep
}

// GenerateFaultSchedule produces a deterministic fault schedule driven strictly by seed.
func GenerateFaultSchedule(seed int64, numSteps int) *FaultSchedule {
	rng := rand.New(rand.NewSource(seed))
	sizes := []int{3, 5, 7}
	clusterSize := sizes[rng.Intn(len(sizes))]

	nodeIDs := make([]raft.NodeID, clusterSize)
	for i := 0; i < clusterSize; i++ {
		nodeIDs[i] = raft.NodeID(i + 1)
	}

	steps := make([]FaultStep, 0, numSteps)
	for s := 0; s < numSteps; s++ {
		actionRoll := rng.Intn(100)
		step := FaultStep{}
		switch {
		case actionRoll < 20: // Crash
			step.Type = FaultCrash
			step.NodeA = nodeIDs[rng.Intn(clusterSize)]
		case actionRoll < 35: // Restart
			step.Type = FaultRestart
			step.NodeA = nodeIDs[rng.Intn(clusterSize)]
		case actionRoll < 55: // Partition
			step.Type = FaultPartition
			step.SetA, step.SetB = partitionNodesDeterministic(rng, nodeIDs)
		case actionRoll < 70: // Heal
			step.Type = FaultHeal
		case actionRoll < 85: // Clock Jump
			step.Type = FaultClockJump
			step.Duration = time.Duration(100+rng.Intn(300)) * time.Millisecond
		case actionRoll < 92: // Drop Link
			step.Type = FaultDropLink
			step.NodeA = nodeIDs[rng.Intn(clusterSize)]
			step.NodeB = nodeIDs[rng.Intn(clusterSize)]
		default: // Normal Advance
			step.Type = FaultAdvance
			step.Duration = time.Duration(15+rng.Intn(40)) * time.Millisecond
		}
		steps = append(steps, step)
	}

	return &FaultSchedule{
		Seed:        seed,
		ClusterSize: clusterSize,
		Steps:       steps,
	}
}

func partitionNodesDeterministic(rng *rand.Rand, nodeIDs []raft.NodeID) ([]raft.NodeID, []raft.NodeID) {
	n := len(nodeIDs)
	perm := rng.Perm(n)
	split := 1 + rng.Intn(n-1)
	setA := make([]raft.NodeID, 0, split)
	setB := make([]raft.NodeID, 0, n-split)
	for i := 0; i < split; i++ {
		setA = append(setA, nodeIDs[perm[i]])
	}
	for i := split; i < n; i++ {
		setB = append(setB, nodeIDs[perm[i]])
	}
	return setA, setB
}

// ExecuteFaultSchedule executes the schedule on an in-memory test cluster,
// enforcing invariants via GlobalChecker after every simulated step.
func ExecuteFaultSchedule(t testing.TB, sched *FaultSchedule) error {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(sched.Seed)
	simNet.SetClock(clk)
	checker := NewGlobalChecker()

	c := NewCluster(t, sched.ClusterSize,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(sched.Seed),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithPreVote(true),
		WithStickiness(true),
		WithCheckQuorum(true),
		WithGlobalChecker(checker),
		WithQuiet(true),
	)
	defer c.Stop()

	// Initial election trigger
	c.GetNode(1).StartElection()
	c.Advance(50 * time.Millisecond)

	if err := applyFaultSteps(c, sched.Steps, sched.ClusterSize); err != nil {
		return fmt.Errorf("seed %d fault injection failed: %w", sched.Seed, err)
	}

	return recoverAndAssertLiveness(c, sched.Seed, checker)
}

func applyFaultSteps(c *Cluster, steps []FaultStep, clusterSize int) error {
	for _, step := range steps {
		switch step.Type {
		case FaultCrash:
			live := c.LiveNodes()
			if len(live) > (clusterSize/2)+1 {
				c.Kill(step.NodeA)
				c.Advance(20 * time.Millisecond)
			}
		case FaultRestart:
			killed := c.KilledNodeIDs()
			if len(killed) > 0 {
				c.Restart(step.NodeA)
				c.Advance(25 * time.Millisecond)
			}
		case FaultPartition:
			if len(step.SetA) > 0 && len(step.SetB) > 0 {
				c.Partition(step.SetA, step.SetB)
				c.Advance(30 * time.Millisecond)
			}
		case FaultHeal:
			c.Heal()
			c.Advance(20 * time.Millisecond)
		case FaultClockJump:
			dur := step.Duration
			if dur <= 0 {
				dur = 150 * time.Millisecond
			}
			c.Advance(dur)
		case FaultDropLink:
			if step.NodeA != step.NodeB {
				c.Drop(step.NodeA, step.NodeB)
				c.Advance(15 * time.Millisecond)
			}
		case FaultAdvance:
			dur := step.Duration
			if dur <= 0 {
				dur = 20 * time.Millisecond
			}
			c.Advance(dur)
		}

		if err := c.CheckerError(); err != nil {
			return fmt.Errorf("safety invariant violated during step %s: %w", step.Type, err)
		}
	}
	return nil
}

func recoverAndAssertLiveness(c *Cluster, seed int64, checker *GlobalChecker) error {
	c.Heal()
	for _, id := range c.KilledNodeIDs() {
		c.Restart(id)
	}
	c.Advance(50 * time.Millisecond)

	leader, _, err := c.WaitLeader(2500 * time.Millisecond)
	if err != nil || leader == nil {
		return fmt.Errorf("seed %d: liveness violated after heal & recovery: %w", seed, err)
	}

	if chkErr := c.CheckerError(); chkErr != nil {
		return fmt.Errorf("seed %d: safety invariant violation post-recovery: %w", seed, chkErr)
	}

	return nil
}

