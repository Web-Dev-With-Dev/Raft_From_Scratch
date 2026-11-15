package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"

	"raft/server"
)

type cliConfig struct {
	scenario string
	from     int64
	to       int64
	seed     int64
	replay   string
	workers  int
	traceDir string
	list     bool
}

func parseFlags() cliConfig {
	scenario := flag.String("scenario", "election", "Scenario to run: election, crash, partition")
	n := flag.Int("n", 1000, "Number of seeds to run starting from -from (default: 1000)")
	from := flag.Int64("from", 1, "First seed to run")
	to := flag.Int64("to", 0, "Last seed to run (overrides -n if set)")
	seed := flag.Int64("seed", 0, "Run a single seed in verbose replay mode")
	replay := flag.String("replay", "", "Path to a trace JSON file to replay")
	workers := flag.Int("workers", runtime.NumCPU(), "Worker concurrency level")
	traceDir := flag.String("trace-dir", "./traces", "Directory to write failure trace files")
	list := flag.Bool("list", false, "List available scenarios")
	flag.Parse()

	endSeed := *to
	if endSeed <= 0 {
		endSeed = *from + int64(*n) - 1
	}

	return cliConfig{
		scenario: *scenario,
		from:     *from,
		to:       endSeed,
		seed:     *seed,
		replay:   *replay,
		workers:  *workers,
		traceDir: *traceDir,
		list:     *list,
	}
}

func printScenarios() {
	fmt.Println("Available Scenarios:")
	keys := make([]string, 0, len(server.Scenarios))
	for k := range server.Scenarios {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  - %s\n", k)
	}
}

func replayTraceFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read trace file %q: %w", path, err)
	}
	var trace server.ScenarioTrace
	if err := json.Unmarshal(data, &trace); err != nil {
		return fmt.Errorf("failed to parse trace JSON: %w", err)
	}
	fmt.Printf("Replaying trace: scenario=%s seed=%d (recorded status: %s)\n",
		trace.Scenario, trace.Seed, trace.Status)
	return runSingleSeed(trace.Scenario, trace.Seed)
}

func runSingleSeed(scenario string, seed int64) error {
	fmt.Printf("=== Running Scenario %q (Seed: %d) ===\n", scenario, seed)
	trace, err := server.RunScenario(context.Background(), scenario, seed, "")
	if err != nil {
		fmt.Printf("[FAIL] Seed %d failed: %v\n", seed, err)
	} else {
		fmt.Printf("[PASS] Seed %d passed in %v\n", seed, trace.Duration)
	}

	for id, node := range trace.Nodes {
		fmt.Printf("\n--- Node %d [%s] (Term=%d, VotedFor=%d, Events=%d) ---\n",
			id, node.State, node.Term, node.VotedFor, len(node.Events))
		for _, ev := range node.Events {
			fmt.Printf("  [%s] Term=%d %-18s %s\n",
				ev.Timestamp.Format("15:04:05.000"), ev.Term, ev.Type, ev.Message)
		}
	}
	return err
}

func printBatchBanner(cfg cliConfig) {
	fmt.Println("==================================================")
	fmt.Println("             Raft Simulation Runner               ")
	fmt.Println("==================================================")
	fmt.Printf("Scenario:  %s\n", cfg.scenario)
	fmt.Printf("Seeds:     %d..%d (%d total)\n", cfg.from, cfg.to, cfg.to-cfg.from+1)
	fmt.Printf("Workers:   %d\n", cfg.workers)
	fmt.Printf("Trace Dir: %s\n", cfg.traceDir)
	fmt.Println("--------------------------------------------------")
}

func printBatchSummary(res *server.BatchResult, traceDir string) {
	fmt.Println("==================================================")
	fmt.Printf("Scenario: %s | Duration: %v\n", res.Scenario, res.Duration.Round(time.Millisecond))
	fmt.Printf("Total:    %d\n", res.Total)
	fmt.Printf("Passed:   %d\n", res.Passed)
	fmt.Printf("Failed:   %d\n", res.Failed)
	fmt.Println("==================================================")

	if res.Failed > 0 {
		fmt.Printf("Failing Seeds (%d): %v\n", res.Failed, res.FailingSeeds)
		fmt.Printf("Trace files saved to: %s\n", traceDir)
		fmt.Println("To replay a failing seed:")
		fmt.Printf("  go run ./cmd/simrunner -scenario %s -seed %d\n", res.Scenario, res.FailingSeeds[0])
	}
}

func runBatchCLI(cfg cliConfig) int {
	printBatchBanner(cfg)

	batchCfg := server.BatchConfig{
		Scenario:  cfg.scenario,
		StartSeed: cfg.from,
		EndSeed:   cfg.to,
		Workers:   cfg.workers,
		TraceDir:  cfg.traceDir,
		OnProgress: func(done, total, failed int) {
			fmt.Printf("Progress: %d/%d completed (%d failed)\n", done, total, failed)
		},
		OnFailure: func(seed int64, err error, tracePath string) {
			fmt.Printf("[FAIL] Seed: %d | Error: %v\n", seed, err)
			if tracePath != "" {
				fmt.Printf("       Trace:  %s\n", tracePath)
				fmt.Printf("       Replay: go run ./cmd/simrunner -scenario %s -seed %d\n", cfg.scenario, seed)
			}
		},
	}

	res, err := server.RunBatch(context.Background(), batchCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Batch run failed: %v\n", err)
		return 1
	}

	printBatchSummary(res, cfg.traceDir)
	if res.Failed > 0 {
		return 1
	}
	return 0
}

func main() {
	cfg := parseFlags()

	if cfg.list {
		printScenarios()
		return
	}
	if cfg.replay != "" {
		if err := replayTraceFile(cfg.replay); err != nil {
			fmt.Fprintf(os.Stderr, "Replay failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if cfg.seed > 0 {
		if err := runSingleSeed(cfg.scenario, cfg.seed); err != nil {
			os.Exit(1)
		}
		return
	}

	os.Exit(runBatchCLI(cfg))
}

