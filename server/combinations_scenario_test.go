package server

import (
	"context"
	"testing"
	"time"
)

// TestScenario_All8Combinations_FullSuite runs the full scenario suite over all 8 combinations
// of PreVote / Stickiness / CheckQuorum, verifying that no combination introduces deadlocks,
// split-brain, or liveness failures (especially CheckQuorum + Stickiness after partition heals).
func TestScenario_All8Combinations_FullSuite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scenarios := []string{"election", "crash", "partition", "varied_logs", "duplication"}
	combinations := AllFeatureCombinations()

	if len(combinations) != 8 {
		t.Fatalf("expected 8 feature combinations, got %d", len(combinations))
	}

	seedBase := int64(100)
	for i, fc := range combinations {
		fc := fc
		comboName := fc.String()
		t.Run(comboName, func(t *testing.T) {
			for _, scName := range scenarios {
				scName := scName
				t.Run(scName, func(t *testing.T) {
					seed := seedBase + int64(i)*10
					trace, err := RunScenarioWithFeatures(ctx, scName, seed, fc, "")
					if err != nil {
						t.Fatalf("scenario %q failed under %s (seed %d): %v",
							scName, comboName, seed, err)
					}
					if trace == nil || trace.Status != "PASS" {
						t.Fatalf("scenario %q did not report pass under %s: %+v",
							scName, comboName, trace)
					}
				})
			}
		})
	}
}

// TestScenario_PartitionHeal_CheckQuorumAndStickiness_NoDeadlock explicitly verifies that
// when CheckQuorum and Stickiness are both enabled, the cluster does NOT deadlock after a partition
// heals and smoothly stabilizes on an elected leader across multiple random seeds.
func TestScenario_PartitionHeal_CheckQuorumAndStickiness_NoDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fc := FeatureConfig{
		PreVote:     true,
		Stickiness:  true,
		CheckQuorum: true,
	}

	seeds := []int64{11, 42, 101, 202, 303, 404, 505, 606, 707, 808}
	for _, seed := range seeds {
		cluster, err := RunPartitionScenarioWithFeatures(ctx, seed, fc)
		if err != nil {
			t.Fatalf("partition heal deadlocked or failed for seed %d under %s: %v", seed, fc, err)
		}
		leader, term := cluster.Leader()
		if leader == nil {
			t.Fatalf("seed %d: expected stable leader after partition heal under %s", seed, fc)
		}
		if term < 2 {
			t.Fatalf("seed %d: expected term >= 2 after failover and heal, got %d", seed, term)
		}
		cluster.Stop()
	}
}
