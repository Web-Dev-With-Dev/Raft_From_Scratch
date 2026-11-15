package server

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft/raft"
)

// TestStress_RandomKillsRestarts_AtMostOneLeaderPerTerm tests cluster stability under
// continuous chaos: random node kills and restarts every few seconds for 2 minutes.
// Invariant asserted continuously (every 5ms): AT MOST ONE LEADER PER TERM.
func TestStress_RandomKillsRestarts_AtMostOneLeaderPerTerm(t *testing.T) {
	duration := 2 * time.Minute
	if env := os.Getenv("RAFT_STRESS_DURATION"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			duration = d
		}
	} else if testing.Short() {
		duration = 10 * time.Second
	}

	t.Logf("Starting Raft chaos stress test for duration: %v", duration)

	// 5-node cluster with check-quorum enabled to ensure clean leader step-downs
	cluster := NewCluster(t, 5,
		WithHeartbeatInterval(20*time.Millisecond),
		WithElectionTimeout(60*time.Millisecond, 120*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithCheckQuorum(true),
		WithCheckQuorumInterval(60*time.Millisecond),
	)
	defer cluster.Stop()

	// 1. Wait for initial leader to emerge
	initLeader, initTerm, err := cluster.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect initial leader: %v", err)
	}
	t.Logf("Initial leader elected: Node %d at term %d", initLeader.GetID(), initTerm)

	// 2. Setup continuous background invariant checker
	// Invariant: At most one leader per term across the whole cluster, both:
	//   a) Instantaneously (no two simultaneous leaders for the same term)
	//   b) Historically (no term is ever led by two distinct node IDs)
	stopChecker := make(chan struct{})
	checkerErrCh := make(chan error, 100)
	var totalChecks uint64
	var checkerWg sync.WaitGroup

	checkerWg.Add(1)
	go func() {
		defer checkerWg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		historicalLeaders := make(map[raft.Term]raft.NodeID)

		for {
			select {
			case <-stopChecker:
				return
			case <-ticker.C:
				atomic.AddUint64(&totalChecks, 1)

				leaders := cluster.Leaders()
				if len(leaders) == 0 {
					continue
				}

				// a) Instantaneous check: multiple leaders claiming the same term
				instantTermMap := make(map[raft.Term][]raft.NodeID)
				for _, l := range leaders {
					instantTermMap[l.Term] = append(instantTermMap[l.Term], l.NodeID)
					if len(instantTermMap[l.Term]) > 1 {
						select {
						case checkerErrCh <- fmt.Errorf(
							"ELECTION SAFETY VIOLATION (instantaneous): multiple simultaneous leaders for term %d: nodes %v",
							l.Term, instantTermMap[l.Term],
						):
						default:
						}
						return
					}
				}

				// b) Historical check: at most one distinct leader ever elected per term
				for _, l := range leaders {
					if prevLeader, exists := historicalLeaders[l.Term]; exists {
						if prevLeader != l.NodeID {
							select {
							case checkerErrCh <- fmt.Errorf(
								"ELECTION SAFETY VIOLATION (historical): term %d has multiple leaders across history: first Node %d, now Node %d",
								l.Term, prevLeader, l.NodeID,
							):
							default:
							}
							return
						}
					} else {
						historicalLeaders[l.Term] = l.NodeID
					}
				}
			}
		}
	}()

	// 3. Chaos Generator: kills and restarts every few seconds
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	startTime := time.Now()
	deadline := startTime.Add(duration)

	var killsCount uint64
	var restartsCount uint64
	var stepCount uint64

	for time.Now().Before(deadline) {
		stepCount++

		// Check if invariant checker detected any violation
		select {
		case chkErr := <-checkerErrCh:
			close(stopChecker)
			checkerWg.Wait()
			t.Fatalf("Stress test failed invariant check: %v", chkErr)
		default:
		}

		live := cluster.LiveNodes()
		killed := cluster.KilledNodeIDs()

		// Decide action:
		// - If <= 2 live nodes, we MUST restart a node to allow quorum (progress)
		// - If all 5 live nodes, we MUST kill a node
		// - Otherwise, random choice between killing and restarting
		action := rng.Intn(3) // 0: kill, 1: restart, 2: both kill and restart
		if len(live) <= 2 {
			action = 1 // restart
		} else if len(killed) == 0 {
			action = 0 // kill
		}

		switch action {
		case 0: // Kill a live node
			if len(live) > 2 {
				// 50% chance to specifically target current leader if alive
				var target raft.NodeID
				currLeader, _ := cluster.Leader()
				if currLeader != nil && rng.Float64() < 0.5 {
					target = currLeader.GetID()
				} else {
					target = live[rng.Intn(len(live))].GetID()
				}
				cluster.Kill(target)
				killsCount++
				t.Logf("[%s] Action #%d: KILLED Node %d (live=%d, killed=%d)",
					time.Since(startTime).Truncate(time.Millisecond), stepCount, target, len(live)-1, len(killed)+1)
			}

		case 1: // Restart a killed node
			if len(killed) > 0 {
				target := killed[rng.Intn(len(killed))]
				cluster.Restart(target)
				restartsCount++
				t.Logf("[%s] Action #%d: RESTARTED Node %d (live=%d, killed=%d)",
					time.Since(startTime).Truncate(time.Millisecond), stepCount, target, len(live)+1, len(killed)-1)
			}

		case 2: // Both kill one and restart another
			if len(live) > 2 && len(killed) > 0 {
				killTarget := live[rng.Intn(len(live))].GetID()
				restartTarget := killed[rng.Intn(len(killed))]
				cluster.Kill(killTarget)
				killsCount++
				cluster.Restart(restartTarget)
				restartsCount++
				t.Logf("[%s] Action #%d: KILLED Node %d & RESTARTED Node %d",
					time.Since(startTime).Truncate(time.Millisecond), stepCount, killTarget, restartTarget)
			}
		}

		// Wait a random duration between 1.0s and 2.5s ("every few seconds")
		// (Or shorter if running under -short)
		sleepDuration := time.Duration(1000+rng.Intn(1500)) * time.Millisecond
		if duration <= 10*time.Second {
			sleepDuration = time.Duration(100+rng.Intn(200)) * time.Millisecond
		}

		time.Sleep(sleepDuration)
	}

	// 4. Teardown and post-chaos cluster stabilization
	t.Logf("Chaos phase completed after %v. Restoring all nodes...", time.Since(startTime).Truncate(time.Millisecond))

	// Restart all currently killed nodes to ensure full cluster can stabilize
	for _, killedID := range cluster.KilledNodeIDs() {
		cluster.Restart(killedID)
	}

	// Wait for a leader to stabilize
	finalLeader, finalTerm, err := cluster.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("failed to stabilize and elect leader after chaos: %v", err)
	}

	// Stop background invariant checker
	close(stopChecker)
	checkerWg.Wait()

	// Final verification of checker channel
	select {
	case chkErr := <-checkerErrCh:
		t.Fatalf("Stress test detected invariant violation during stabilization: %v", chkErr)
	default:
	}

	totalInvariantChecks := atomic.LoadUint64(&totalChecks)

	t.Logf("================================================================================")
	t.Logf("RAFT CHAOS STRESS TEST REPORT")
	t.Logf("================================================================================")
	t.Logf("Total Duration:            %v", time.Since(startTime).Truncate(time.Millisecond))
	t.Logf("Total Chaos Steps:         %d", stepCount)
	t.Logf("Total Nodes Killed:        %d", killsCount)
	t.Logf("Total Nodes Restarted:     %d", restartsCount)
	t.Logf("Total Invariant Checks:    %d (every 5ms)", totalInvariantChecks)
	t.Logf("Final Leader:              Node %d at Term %d", finalLeader.GetID(), finalTerm)
	t.Logf("Invariant Result:          PASSED — AT MOST ONE LEADER PER TERM MAINTAINED 100%%")
	t.Logf("================================================================================")
}

