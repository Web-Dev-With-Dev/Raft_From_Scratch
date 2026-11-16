package server

import (
	"context"
	"testing"
	"time"
)

func TestTuning_QuickSuite(t *testing.T) {
	cfg := TuningConfig{
		ClusterSize:       3,
		MinTimeout:        100 * time.Millisecond,
		MaxTimeout:        200 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
		RPCTimeout:        30 * time.Millisecond,
		NetworkMinDelay:   2 * time.Millisecond,
		NetworkMaxDelay:   5 * time.Millisecond,
		Seeds:             20,
		Workers:           4,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := RunTuningExperiment(ctx, cfg)
	if err != nil {
		t.Fatalf("tuning experiment failed: %v", err)
	}
	if res.SuccessfulRuns != 20 {
		t.Fatalf("expected 20 successful runs, got %d", res.SuccessfulRuns)
	}
	if res.MeanTimeToLead <= 0 {
		t.Fatalf("expected positive mean time to leader, got %v", res.MeanTimeToLead)
	}
}

