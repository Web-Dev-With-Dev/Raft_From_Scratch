package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"raft/raft"
)

// NodeTraceState records the post-run state and consensus events of a single node.
type NodeTraceState struct {
	ID       raft.NodeID `json:"id"`
	State    string      `json:"state"`
	Term     raft.Term   `json:"term"`
	VotedFor raft.NodeID `json:"voted_for"`
	IsKilled bool        `json:"is_killed"`
	Events   []Event     `json:"events"`
}

// ScenarioTrace stores the complete record of a simulation run for deterministic replay and debugging.
type ScenarioTrace struct {
	Scenario  string                         `json:"scenario"`
	Seed      int64                          `json:"seed"`
	Timestamp time.Time                      `json:"timestamp"`
	Duration  time.Duration                  `json:"duration_ns"`
	Status    string                         `json:"status"` // "PASS" or "FAIL"
	Error     string                         `json:"error,omitempty"`
	Nodes     map[raft.NodeID]NodeTraceState `json:"nodes"`
	ReplayCmd string                         `json:"replay_cmd"`
}

// Save serializes the trace to a JSON file in the target directory.
func (st *ScenarioTrace) Save(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create trace dir %q: %w", dir, err)
	}
	filename := fmt.Sprintf("%s_seed_%d.json", st.Scenario, st.Seed)
	path := filepath.Join(dir, filename)

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal trace JSON: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("failed to write trace file %q: %w", path, err)
	}
	return path, nil
}

// buildTrace extracts node statuses and ring-buffer event logs from the cluster.
func buildTrace(cluster *Cluster, scenario string, seed int64, dur time.Duration, runErr error) *ScenarioTrace {
	status := "PASS"
	errMsg := ""
	if runErr != nil {
		status = "FAIL"
		errMsg = runErr.Error()
	}

	nodesTrace := make(map[raft.NodeID]NodeTraceState)
	if cluster != nil {
		for _, srv := range cluster.Nodes() {
			var events []Event
			if srv.EventLog() != nil {
				events = srv.EventLog().Entries()
			}
			nodesTrace[srv.ID] = NodeTraceState{
				ID:       srv.ID,
				State:    srv.GetState().String(),
				Term:     srv.Status().Term,
				VotedFor: srv.Status().VotedFor,
				IsKilled: srv.IsKilled(),
				Events:   events,
			}
		}
	}

	return &ScenarioTrace{
		Scenario:  scenario,
		Seed:      seed,
		Timestamp: time.Now(),
		Duration:  dur,
		Status:    status,
		Error:     errMsg,
		Nodes:     nodesTrace,
		ReplayCmd: fmt.Sprintf("go run ./cmd/simrunner -scenario %s -seed %d", scenario, seed),
	}
}

// CheckLeaderInvariant verifies Raft Election Safety (§5.2): at most one leader per term across all nodes.
func CheckLeaderInvariant(cluster *Cluster, termLeaderHistory map[raft.Term]raft.NodeID) error {
	leaders := cluster.Leaders()
	for i := 0; i < len(leaders); i++ {
		for j := i + 1; j < len(leaders); j++ {
			if leaders[i].Term == leaders[j].Term {
				return fmt.Errorf("election safety violated: multiple leaders in term %d (nodes %d and %d)",
					leaders[i].Term, leaders[i].NodeID, leaders[j].NodeID)
			}
		}
	}
	if termLeaderHistory != nil {
		for _, l := range leaders {
			if prior, exists := termLeaderHistory[l.Term]; exists && prior != l.NodeID {
				return fmt.Errorf("election safety violated: historical conflict for term %d (was %d, now %d)",
					l.Term, prior, l.NodeID)
			}
			termLeaderHistory[l.Term] = l.NodeID
		}
	}
	return nil
}

func newSimCluster(seed int64, n int) (*Cluster, *raft.FakeClock) {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(seed)
	simNet.SetClock(clk)

	cluster := NewCluster(nil, n,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(seed),
		WithQuiet(true),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
	)
	return cluster, clk
}

func advanceClockStep(clk *raft.FakeClock, step time.Duration) {
	clk.Advance(step)
	time.Sleep(50 * time.Microsecond)
}

func advanceUntilLeader(ctx context.Context, clk *raft.FakeClock, cluster *Cluster, budget time.Duration, hist map[raft.Term]raft.NodeID) (*Server, error) {
	step := 5 * time.Millisecond
	for elapsed := time.Duration(0); elapsed < budget; elapsed += step {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := CheckLeaderInvariant(cluster, hist); err != nil {
			return nil, err
		}
		if l, _ := cluster.Leader(); l != nil {
			return l, nil
		}
		advanceClockStep(clk, step)
	}
	return nil, fmt.Errorf("no leader elected within %v budget", budget)
}

func verifyStability(clk *raft.FakeClock, cluster *Cluster, steps int, hist map[raft.Term]raft.NodeID) error {
	step := 5 * time.Millisecond
	for i := 0; i < steps; i++ {
		advanceClockStep(clk, step)
		if err := CheckLeaderInvariant(cluster, hist); err != nil {
			return err
		}
	}
	return nil
}

// RunBasicElectionScenario runs a 3-node election under FakeClock + SimNetwork driven by seed.
func RunBasicElectionScenario(ctx context.Context, seed int64) (*Cluster, error) {
	cluster, clk := newSimCluster(seed, 3)
	termHistory := make(map[raft.Term]raft.NodeID)

	leader, err := advanceUntilLeader(ctx, clk, cluster, 4*time.Second, termHistory)
	if err != nil {
		return cluster, err
	}
	if leader.Status().Term < 1 {
		return cluster, fmt.Errorf("elected leader term must be >= 1, got %d", leader.Status().Term)
	}
	if err := verifyStability(clk, cluster, 20, termHistory); err != nil {
		return cluster, err
	}
	return cluster, nil
}

// RunCrashScenario runs leader election, crashes the leader, and asserts a surviving node becomes leader.
func RunCrashScenario(ctx context.Context, seed int64) (*Cluster, error) {
	cluster, err := RunBasicElectionScenario(ctx, seed)
	if err != nil {
		return cluster, fmt.Errorf("initial election failed: %w", err)
	}

	leader, initialTerm := cluster.Leader()
	if leader == nil {
		return cluster, fmt.Errorf("expected initial leader before crash")
	}

	clk := cluster.FakeClock()
	cluster.Kill(leader.ID)

	termHistory := map[raft.Term]raft.NodeID{initialTerm: leader.ID}
	step := 5 * time.Millisecond
	for elapsed := time.Duration(0); elapsed < 4*time.Second; elapsed += step {
		if ctx != nil && ctx.Err() != nil {
			return cluster, ctx.Err()
		}
		if err := CheckLeaderInvariant(cluster, termHistory); err != nil {
			return cluster, err
		}
		if l, term := cluster.Leader(); l != nil && l.ID != leader.ID && term > initialTerm {
			return cluster, nil
		}
		advanceClockStep(clk, step)
	}
	return cluster, fmt.Errorf("no new leader elected after crashing node %d", leader.ID)
}

// RunPartitionScenario runs an election, partitions into minority/majority, and verifies failover.
func RunPartitionScenario(ctx context.Context, seed int64) (*Cluster, error) {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(seed)
	simNet.SetClock(clk)

	cluster := NewCluster(nil, 5,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(seed),
		WithQuiet(true),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
		WithCheckQuorum(true),
		WithCheckQuorumInterval(100*time.Millisecond),
	)

	termHistory := make(map[raft.Term]raft.NodeID)
	leader, err := advanceUntilLeader(ctx, clk, cluster, 4*time.Second, termHistory)
	if err != nil {
		return cluster, fmt.Errorf("initial 5-node election failed: %w", err)
	}

	initialTerm := leader.Status().Term
	minority, majority := partitionMinorityMajority(cluster, leader)
	cluster.Partition(minority, majority)

	step := 5 * time.Millisecond
	for elapsed := time.Duration(0); elapsed < 4*time.Second; elapsed += step {
		for _, id := range majority {
			node := cluster.GetNode(id)
			if node.GetState() == raft.Leader && node.Status().Term > initialTerm {
				cluster.Heal()
				return cluster, nil
			}
		}
		advanceClockStep(clk, step)
	}
	return cluster, fmt.Errorf("majority partition failed to elect new leader")
}

func partitionMinorityMajority(cluster *Cluster, leader *Server) ([]raft.NodeID, []raft.NodeID) {
	otherID := raft.NodeID(1)
	if leader.ID == 1 {
		otherID = 2
	}
	minority := []raft.NodeID{leader.ID, otherID}
	majority := make([]raft.NodeID, 0, 3)
	for i := raft.NodeID(1); i <= 5; i++ {
		if i != leader.ID && i != otherID {
			majority = append(majority, i)
		}
	}
	return minority, majority
}

// Scenarios maps scenario names to their execution functions.
var Scenarios = map[string]func(ctx context.Context, seed int64) (*Cluster, error){
	"election":  RunBasicElectionScenario,
	"crash":     RunCrashScenario,
	"partition": RunPartitionScenario,
}

// RunScenario executes a scenario by name with the given seed, returning the trace and any error.
func RunScenario(ctx context.Context, name string, seed int64, traceDir string) (*ScenarioTrace, error) {
	fn, ok := Scenarios[name]
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q", name)
	}

	start := time.Now()
	cluster, runErr := fn(ctx, seed)
	dur := time.Since(start)

	trace := buildTrace(cluster, name, seed, dur, runErr)
	if cluster != nil {
		cluster.Stop()
	}

	if runErr != nil && traceDir != "" {
		if path, err := trace.Save(traceDir); err == nil {
			trace.ReplayCmd = fmt.Sprintf("go run ./cmd/simrunner -scenario %s -seed %d (trace: %s)", name, seed, path)
		}
	}
	return trace, runErr
}

// BatchConfig configures parallel execution of a scenario over a range of seeds.
type BatchConfig struct {
	Scenario   string
	StartSeed  int64
	EndSeed    int64
	Workers    int
	TraceDir   string
	OnProgress func(completed, total int, failed int)
	OnFailure  func(seed int64, err error, tracePath string)
}

// BatchResult aggregates the results of a batch scenario execution.
type BatchResult struct {
	Scenario     string           `json:"scenario"`
	Total        int              `json:"total"`
	Passed       int              `json:"passed"`
	Failed       int              `json:"failed"`
	FailingSeeds []int64          `json:"failing_seeds"`
	Duration     time.Duration    `json:"duration_ns"`
	Traces       map[int64]string `json:"traces"`
}

type batchRunState struct {
	mu           sync.Mutex
	completed    atomic.Int64
	failed       atomic.Int64
	failingSeeds []int64
	traces       map[int64]string
	total        int
}

func (s *batchRunState) recordFailure(seed int64, tracePath string, err error, onFail func(int64, error, string)) {
	s.failed.Add(1)
	s.mu.Lock()
	s.failingSeeds = append(s.failingSeeds, seed)
	if tracePath != "" {
		s.traces[seed] = tracePath
	}
	s.mu.Unlock()
	if onFail != nil {
		onFail(seed, err, tracePath)
	}
}

func runBatchWorker(ctx context.Context, cfg BatchConfig, seedCh <-chan int64, state *batchRunState) {
	for seed := range seedCh {
		if ctx.Err() != nil {
			return
		}
		trace, err := RunScenario(ctx, cfg.Scenario, seed, cfg.TraceDir)
		done := int(state.completed.Add(1))
		if err != nil {
			var tracePath string
			if trace != nil && cfg.TraceDir != "" {
				tracePath, _ = trace.Save(cfg.TraceDir)
			}
			state.recordFailure(seed, tracePath, err, cfg.OnFailure)
		}
		if cfg.OnProgress != nil && (done%100 == 0 || done == state.total) {
			cfg.OnProgress(done, state.total, int(state.failed.Load()))
		}
	}
}

// RunBatch runs the scenario over seeds StartSeed..EndSeed in parallel using a worker pool.
func RunBatch(ctx context.Context, cfg BatchConfig) (*BatchResult, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.StartSeed > cfg.EndSeed {
		return nil, fmt.Errorf("start seed (%d) > end seed (%d)", cfg.StartSeed, cfg.EndSeed)
	}

	total := int(cfg.EndSeed - cfg.StartSeed + 1)
	seedCh := make(chan int64, total)
	for s := cfg.StartSeed; s <= cfg.EndSeed; s++ {
		seedCh <- s
	}
	close(seedCh)

	state := &batchRunState{
		traces: make(map[int64]string),
		total:  total,
	}
	start := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runBatchWorker(ctx, cfg, seedCh, state)
		}()
	}

	wg.Wait()
	failCount := int(state.failed.Load())
	return &BatchResult{
		Scenario:     cfg.Scenario,
		Total:        total,
		Passed:       total - failCount,
		Failed:       failCount,
		FailingSeeds: state.failingSeeds,
		Duration:     time.Since(start),
		Traces:       state.traces,
	}, nil
}
