package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"text/tabwriter"
	"time"

	"raft/server"
)

type rangeConfig struct {
	min time.Duration
	max time.Duration
}

func main() {
	seeds := flag.Int("seeds", 1000, "Number of seeds per experiment combination")
	workers := flag.Int("workers", runtime.NumCPU(), "Worker concurrency")
	flag.Parse()

	clusterSizes := []int{3, 5, 7}
	ranges := []rangeConfig{
		{min: 150 * time.Millisecond, max: 300 * time.Millisecond},
		{min: 200 * time.Millisecond, max: 400 * time.Millisecond},
		{min: 300 * time.Millisecond, max: 600 * time.Millisecond},
	}
	heartbeatInterval := 50 * time.Millisecond

	fmt.Printf("Running tuning experiments across %d seeds per configuration (workers=%d)...\n\n", *seeds, *workers)

	var results []*server.TuningResult

	for _, size := range clusterSizes {
		for _, r := range ranges {
			cfg := server.TuningConfig{
				ClusterSize:       size,
				MinTimeout:        r.min,
				MaxTimeout:        r.max,
				HeartbeatInterval: heartbeatInterval,
				RPCTimeout:        40 * time.Millisecond,
				NetworkMinDelay:   2 * time.Millisecond,
				NetworkMaxDelay:   8 * time.Millisecond,
				Seeds:             *seeds,
				Workers:           *workers,
			}

			start := time.Now()
			res, err := server.RunTuningExperiment(context.Background(), cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error running experiment (%d nodes, %v-%v): %v\n", size, r.min, r.max, err)
				os.Exit(1)
			}
			dur := time.Since(start)
			results = append(results, res)

			fmt.Printf("Completed %d nodes [%v - %v] in %v (split-rate: %.1f%%, mean: %v)\n",
				size, r.min, r.max, dur.Round(time.Millisecond), res.SplitVoteRate, res.MeanTimeToLead.Round(time.Millisecond))
		}
	}

	printMarkdownTable(results, heartbeatInterval)
}

func printMarkdownTable(results []*server.TuningResult, hb time.Duration) {
	fmt.Println("\n### Empirical Tuning Results (1,000 Seeds per Configuration)")
	fmt.Printf("*Fixed Parameters: Heartbeat = %v, Network Delay = 2-8ms, RPC Timeout = 40ms*\n\n", hb)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "| Nodes | Timeout Range | Ratio (Tmin/Thb) | Split-Vote Rate | Contested Rate | Mean Time | P50 (Median) | P90 | P99 | Avg Terms |")
	fmt.Fprintln(w, "|:-----:|:-------------:|:----------------:|:---------------:|:--------------:|:---------:|:------------:|:---:|:---:|:---------:|")

	for _, r := range results {
		ratio := float64(r.Config.MinTimeout) / float64(r.Config.HeartbeatInterval)
		fmt.Fprintf(w, "| %d | %v - %v | %.1fx | %.1f%% (%d/%d) | %.1f%% | %v | %v | %v | %v | %.3f |\n",
			r.Config.ClusterSize,
			r.Config.MinTimeout.Round(time.Millisecond),
			r.Config.MaxTimeout.Round(time.Millisecond),
			ratio,
			r.SplitVoteRate, r.SplitVoteRuns, r.TotalRuns,
			r.ContestedRate,
			r.MeanTimeToLead.Round(time.Millisecond),
			r.P50TimeToLead.Round(time.Millisecond),
			r.P90TimeToLead.Round(time.Millisecond),
			r.P99TimeToLead.Round(time.Millisecond),
			r.AvgTerms,
		)
	}
	w.Flush()
}

