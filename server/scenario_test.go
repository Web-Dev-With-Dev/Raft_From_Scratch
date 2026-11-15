package server

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestScenario_BasicElection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	trace, err := RunScenario(ctx, "election", 42, "")
	if err != nil {
		t.Fatalf("election scenario failed: %v", err)
	}
	if trace.Status != "PASS" {
		t.Fatalf("expected status PASS, got %s", trace.Status)
	}
	if len(trace.Nodes) != 3 {
		t.Fatalf("expected 3 nodes in trace, got %d", len(trace.Nodes))
	}
}

func TestScenario_Crash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	trace, err := RunScenario(ctx, "crash", 101, "")
	if err != nil {
		t.Fatalf("crash scenario failed: %v", err)
	}
	if trace.Status != "PASS" {
		t.Fatalf("expected status PASS, got %s", trace.Status)
	}
}

func TestScenario_Partition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	trace, err := RunScenario(ctx, "partition", 202, "")
	if err != nil {
		t.Fatalf("partition scenario failed: %v", err)
	}
	if trace.Status != "PASS" {
		t.Fatalf("expected status PASS, got %s", trace.Status)
	}
}

func TestScenario_TraceSaveAndReplayCmd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sim_traces_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	trace, err := RunScenario(context.Background(), "election", 777, tmpDir)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	tracePath, err := trace.Save(tmpDir)
	if err != nil {
		t.Fatalf("failed to save trace: %v", err)
	}
	if _, err := os.Stat(tracePath); err != nil {
		t.Fatalf("expected trace file to exist at %q: %v", tracePath, err)
	}
}

func TestScenario_RunBatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sim_batch_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := BatchConfig{
		Scenario:  "election",
		StartSeed: 1,
		EndSeed:   20,
		Workers:   4,
		TraceDir:  tmpDir,
	}

	res, err := RunBatch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunBatch returned error: %v", err)
	}
	if res.Total != 20 {
		t.Fatalf("expected total 20, got %d", res.Total)
	}
	if res.Failed > 0 {
		t.Fatalf("expected 0 failures in first 20 seeds, got %d (seeds: %v)", res.Failed, res.FailingSeeds)
	}
	if res.Passed != 20 {
		t.Fatalf("expected 20 passed, got %d", res.Passed)
	}
}

func TestScenario_FailureSavesTraceAndReplays(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sim_fail_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	Scenarios["test_fail"] = func(ctx context.Context, seed int64) (*Cluster, error) {
		cluster, clk := newSimCluster(seed, 3)
		if seed == 13 {
			return cluster, os.ErrPermission // intentional mock failure
		}
		leader, err := advanceUntilLeader(ctx, clk, cluster, 4*time.Second, nil)
		if err != nil {
			return cluster, err
		}
		_ = leader
		return cluster, nil
	}
	defer delete(Scenarios, "test_fail")

	var failedSeeds []int64
	var tracePaths []string
	cfg := BatchConfig{
		Scenario:  "test_fail",
		StartSeed: 10,
		EndSeed:   15,
		Workers:   2,
		TraceDir:  tmpDir,
		OnFailure: func(seed int64, err error, tracePath string) {
			failedSeeds = append(failedSeeds, seed)
			tracePaths = append(tracePaths, tracePath)
		},
	}

	res, err := RunBatch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected batch error: %v", err)
	}
	if res.Failed != 1 {
		t.Fatalf("expected 1 failure, got %d", res.Failed)
	}
	if len(res.FailingSeeds) != 1 || res.FailingSeeds[0] != 13 {
		t.Fatalf("expected failing seed 13, got %v", res.FailingSeeds)
	}
	if len(tracePaths) != 1 || tracePaths[0] == "" {
		t.Fatalf("expected trace path for seed 13, got %v", tracePaths)
	}
	if _, err := os.Stat(tracePaths[0]); err != nil {
		t.Fatalf("trace file not found on disk at %s: %v", tracePaths[0], err)
	}
}
