package server

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func runSeedWorker(t *testing.T, seedCh <-chan int64, failedCount *atomic.Int64, failingSeeds *[]int64, mu *sync.Mutex) {
	for s := range seedCh {
		sched := GenerateFaultSchedule(s, 6)
		if err := ExecuteFaultSchedule(t, sched); err != nil {
			t.Errorf("seed %d failed: %v", s, err)
			failedCount.Add(1)
			mu.Lock()
			*failingSeeds = append(*failingSeeds, s)
			mu.Unlock()
		}
	}
}

// TestRandomFaultSchedule_10000Seeds runs 10,000 randomized fault schedule seeds
// (crashes, restarts, partitions, clock jumps, message drops) with invariants verified
// at every simulated step across cluster sizes 3, 5, 7.
func TestRandomFaultSchedule_10000Seeds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10,000 seeds fault schedule in short mode")
	}

	numSeeds, numWorkers := 10000, runtime.NumCPU()*2
	if numWorkers < 16 {
		numWorkers = 16
	}

	seedCh := make(chan int64, numWorkers*4)
	var wg sync.WaitGroup
	var failedCount atomic.Int64
	var failingSeeds []int64
	var mu sync.Mutex

	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSeedWorker(t, seedCh, &failedCount, &failingSeeds, &mu)
		}()
	}

	for s := int64(1); s <= int64(numSeeds); s++ {
		seedCh <- s
	}
	close(seedCh)
	wg.Wait()

	t.Logf("Completed %d seeds in %v using %d workers (failed: %d, failing seeds: %v)",
		numSeeds, time.Since(start), numWorkers, failedCount.Load(), failingSeeds)

	if failedCount.Load() > 0 {
		t.Fatalf("%d out of %d seeds failed: %v", failedCount.Load(), numSeeds, failingSeeds)
	}
}

// TestRandomFaultSchedule_RegressionSuite runs a permanent regression suite of
// deterministic seeds covering edge-case fault schedules across cluster sizes 3, 5, 7.
func TestRandomFaultSchedule_RegressionSuite(t *testing.T) {
	regressionSeeds := []struct {
		name     string
		seed     int64
		numSteps int
	}{
		{"Seed1_BasicTransitions", 1, 6},
		{"Seed42_DeepPartitions", 42, 8},
		{"Seed101_RepeatedClockJumps", 101, 8},
		{"Seed202_FrequentRestarts", 202, 10},
		{"Seed303_LinkFlapping", 303, 8},
		{"Seed404_ClusterSize7_MultiCrash", 404, 10},
		{"Seed505_RapidClockSkews", 505, 8},
		{"Seed606_AsymmetricPartitions", 606, 10},
		{"Seed707_HighChaos", 707, 12},
		{"Seed808_RecoveryConvergence", 808, 10},
		{"Seed999_ExtremeStress", 999, 12},
	}

	for _, tc := range regressionSeeds {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sched := GenerateFaultSchedule(tc.seed, tc.numSteps)
			if err := ExecuteFaultSchedule(t, sched); err != nil {
				t.Fatalf("regression seed %d (%s) failed: %v", tc.seed, tc.name, err)
			}
		})
	}
}
