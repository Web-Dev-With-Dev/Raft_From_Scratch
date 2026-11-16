package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"raft/raft"
)

// TuningConfig specifies the parameters for an election timeout tuning experiment.
type TuningConfig struct {
	ClusterSize       int
	MinTimeout        time.Duration
	MaxTimeout        time.Duration
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration
	NetworkMinDelay   time.Duration
	NetworkMaxDelay   time.Duration
	Seeds             int
	Workers           int
}

// TuningResult aggregates empirical performance metrics across simulation seeds.
type TuningResult struct {
	Config         TuningConfig
	TotalRuns      int
	SuccessfulRuns int
	SplitVoteRuns  int
	SplitVoteRate  float64 // percentage of runs where term > 1
	ContestedRuns  int     // percentage of runs where > 1 node started an election
	ContestedRate  float64
	AvgTerms       float64
	MeanTimeToLead time.Duration
	P50TimeToLead  time.Duration
	P90TimeToLead  time.Duration
	P99TimeToLead  time.Duration
	MinTimeToLead  time.Duration
	MaxTimeToLead  time.Duration
}

type seedMeasurement struct {
	timeToLeader time.Duration
	leaderTerm   raft.Term
	splitVote    bool
	contested    bool
	err          error
}

func runSingleTuningRun(cfg TuningConfig, seed int64) seedMeasurement {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(seed)
	simNet.SetClock(clk)
	simNet.SetDelayRange(cfg.NetworkMinDelay, cfg.NetworkMaxDelay)

	cluster := NewCluster(nil, cfg.ClusterSize,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(seed),
		WithDefaultGlobalChecker(),
		WithQuiet(true),
		WithElectionTimeout(cfg.MinTimeout, cfg.MaxTimeout),
		WithHeartbeatInterval(cfg.HeartbeatInterval),
		WithRPCTimeout(cfg.RPCTimeout),
	)
	defer cluster.Stop()

	step := 5 * time.Millisecond
	budget := 6 * time.Second
	for elapsed := time.Duration(0); elapsed < budget; elapsed += step {
		if l, _ := cluster.Leader(); l != nil {
			term := l.Status().Term
			var totalElections uint64
			for _, n := range cluster.Nodes() {
				totalElections += n.ElectionsStarted()
			}
			return seedMeasurement{
				timeToLeader: elapsed,
				leaderTerm:   term,
				splitVote:    term > 1,
				contested:    totalElections > 1,
			}
		}
		advanceClockStep(clk, step)
	}
	return seedMeasurement{err: fmt.Errorf("no leader elected within %v", budget)}
}

func aggregateTuningResults(cfg TuningConfig, measurements []seedMeasurement) *TuningResult {
	res := &TuningResult{Config: cfg, TotalRuns: len(measurements)}
	if len(measurements) == 0 {
		return res
	}

	var validDurations []time.Duration
	var totalTerms uint64
	var totalDur time.Duration

	for _, m := range measurements {
		if m.err != nil {
			continue
		}
		res.SuccessfulRuns++
		if m.splitVote {
			res.SplitVoteRuns++
		}
		if m.contested {
			res.ContestedRuns++
		}
		totalTerms += uint64(m.leaderTerm)
		totalDur += m.timeToLeader
		validDurations = append(validDurations, m.timeToLeader)
	}

	if res.SuccessfulRuns == 0 {
		return res
	}

	sort.Slice(validDurations, func(i, j int) bool {
		return validDurations[i] < validDurations[j]
	})

	n := len(validDurations)
	res.SplitVoteRate = (float64(res.SplitVoteRuns) / float64(res.SuccessfulRuns)) * 100.0
	res.ContestedRate = (float64(res.ContestedRuns) / float64(res.SuccessfulRuns)) * 100.0
	res.AvgTerms = float64(totalTerms) / float64(res.SuccessfulRuns)
	res.MeanTimeToLead = time.Duration(int64(totalDur) / int64(n))
	res.MinTimeToLead = validDurations[0]
	res.MaxTimeToLead = validDurations[n-1]
	res.P50TimeToLead = validDurations[n/2]
	res.P90TimeToLead = validDurations[(n*90)/100]
	res.P99TimeToLead = validDurations[(n*99)/100]
	return res
}

// RunTuningExperiment runs an election tuning suite across seeds in parallel.
func RunTuningExperiment(ctx context.Context, cfg TuningConfig) (*TuningResult, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Seeds <= 0 {
		cfg.Seeds = 1000
	}

	measurements := make([]seedMeasurement, cfg.Seeds)
	seedCh := make(chan int64, cfg.Seeds)
	for i := 1; i <= cfg.Seeds; i++ {
		seedCh <- int64(i)
	}
	close(seedCh)

	var wg sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := range seedCh {
				if ctx.Err() != nil {
					return
				}
				measurements[seed-1] = runSingleTuningRun(cfg, seed)
			}
		}()
	}
	wg.Wait()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return aggregateTuningResults(cfg, measurements), nil
}

