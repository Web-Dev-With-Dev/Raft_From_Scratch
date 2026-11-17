package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"raft/raft"
	"raft/server"
)

type timelineConfig struct {
	scenario  string
	seed      int64
	tracePath string
	exportDir string
}

func parseFlags() timelineConfig {
	scenario := flag.String("scenario", "election", "Scenario to run (election, crash, partition, varied_logs, duplication)")
	seed := flag.Int64("seed", 42, "Simulation seed to run and render")
	tracePath := flag.String("trace", "", "Path to existing scenario trace JSON file (skips running simulation if set)")
	exportDir := flag.String("export-dir", "", "Directory to export per-node trace JSON files")
	flag.Parse()

	return timelineConfig{
		scenario:  *scenario,
		seed:      *seed,
		tracePath: *tracePath,
		exportDir: *exportDir,
	}
}

func loadOrRunTrace(cfg timelineConfig) (*server.ScenarioTrace, error) {
	if cfg.tracePath != "" {
		data, err := os.ReadFile(cfg.tracePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read trace file: %w", err)
		}
		var trace server.ScenarioTrace
		if err := json.Unmarshal(data, &trace); err != nil {
			return nil, fmt.Errorf("failed to parse trace JSON: %w", err)
		}
		return &trace, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.RunScenario(ctx, cfg.scenario, cfg.seed, "")
}

type timelineEntry struct {
	t       time.Time
	nodeID  raft.NodeID
	summary string
}

func collectTimelineEntries(trace *server.ScenarioTrace) ([]timelineEntry, []raft.NodeID, time.Time) {
	var entries []timelineEntry
	var nodeIDs []raft.NodeID
	var earliest time.Time

	for id, node := range trace.Nodes {
		nodeIDs = append(nodeIDs, id)
		for _, ev := range node.Events {
			if earliest.IsZero() || ev.Timestamp.Before(earliest) {
				earliest = ev.Timestamp
			}
			summary := formatEventSummary(ev)
			entries = append(entries, timelineEntry{
				t:       ev.Timestamp,
				nodeID:  id,
				summary: summary,
			})
		}
	}

	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	sort.Slice(entries, func(i, j int) bool { return entries[i].t.Before(entries[j].t) })
	return entries, nodeIDs, earliest
}

func formatEventSummary(ev server.Event) string {
	switch ev.Type {
	case server.EventServerStarted:
		return fmt.Sprintf("[INIT] Server started (T:%d)", ev.Term)
	case server.EventStateTransition:
		return fmt.Sprintf("[STATE] %s", ev.Message)
	case server.EventElectionStarted:
		return fmt.Sprintf("[ELECTION] Started (T:%d)", ev.Term)
	case server.EventVoteGranted:
		if self, ok := ev.Details["self_vote"].(bool); ok && self {
			return fmt.Sprintf("[VOTE] Voted for self (T:%d)", ev.Term)
		}
		if pv, ok := ev.Details["pre_vote"].(bool); ok && pv {
			return fmt.Sprintf("[PREVOTE] Granted (T:%d)", ev.Term)
		}
		return fmt.Sprintf("[VOTE] Granted (T:%d)", ev.Term)
	case server.EventPreVoteGranted:
		return fmt.Sprintf("[PREVOTE] Granted (T:%d)", ev.Term)
	case server.EventPreVoteDenied:
		reason, _ := ev.Details["reason"].(string)
		return fmt.Sprintf("[PREVOTE] Denied: %s (T:%d)", reason, ev.Term)
	case server.EventVoteDenied:
		reason, _ := ev.Details["reason"].(string)
		return fmt.Sprintf("[VOTE] Denied: %s (T:%d)", reason, ev.Term)
	case server.EventTermChange:
		return fmt.Sprintf("[TERM] %s", ev.Message)
	case server.EventTimerReset:
		reason, _ := ev.Details["reason"].(string)
		return fmt.Sprintf("[TIMER] Reset (%s)", reason)
	case server.EventTimerTimeout:
		return fmt.Sprintf("[TIMEOUT] Election timer fired (T:%d)", ev.Term)
	case server.EventHeartbeatSent:
		return fmt.Sprintf("[HEARTBEAT] Sent (T:%d)", ev.Term)
	case server.EventHeartbeatReceived:
		leader, _ := ev.Details["leader_id"].(float64)
		return fmt.Sprintf("[HEARTBEAT] Recv L:%d (T:%d)", int(leader), ev.Term)
	case server.EventStepDown:
		reason, _ := ev.Details["reason"].(string)
		return fmt.Sprintf("[STEPDOWN] %s", reason)
	default:
		return fmt.Sprintf("[%s] %s", ev.Type, ev.Message)
	}
}

func renderTimelineHeader(trace *server.ScenarioTrace, nodeIDs []raft.NodeID) {
	colWidth := 30
	totalWidth := 13 + len(nodeIDs)*(colWidth+3)
	banner := strings.Repeat("=", totalWidth)
	sep := strings.Repeat("-", totalWidth)

	fmt.Println(banner)
	fmt.Printf("                   RAFT SCENARIO EXECUTION TIMELINE\n")
	fmt.Printf("  Scenario: %-12s | Seed: %-6d | Status: %-4s | Duration: %v\n",
		trace.Scenario, trace.Seed, trace.Status, trace.Duration.Round(time.Millisecond))
	fmt.Println(banner)

	header := fmt.Sprintf("%-11s |", "Time (+ms)")
	for _, id := range nodeIDs {
		header += fmt.Sprintf(" %-*s |", colWidth, fmt.Sprintf("Node %d", id))
	}
	fmt.Println(header)
	fmt.Println(sep)
}

func renderTimelineRows(entries []timelineEntry, nodeIDs []raft.NodeID, baseTime time.Time) {
	colWidth := 30
	for _, entry := range entries {
		delta := entry.t.Sub(baseTime)
		timeStr := fmt.Sprintf("+%7.2fms", float64(delta.Microseconds())/1000.0)
		row := fmt.Sprintf("%-11s |", timeStr)

		for _, id := range nodeIDs {
			if id == entry.nodeID {
				msg := entry.summary
				if len(msg) > colWidth {
					msg = msg[:colWidth-3] + "..."
				}
				row += fmt.Sprintf(" %-*s |", colWidth, msg)
			} else {
				row += fmt.Sprintf(" %-*s |", colWidth, "")
			}
		}
		fmt.Println(row)
	}
}

func renderMetricsSummary(trace *server.ScenarioTrace, nodeIDs []raft.NodeID) {
	totalWidth := 95
	banner := strings.Repeat("=", totalWidth)
	sep := strings.Repeat("-", totalWidth)

	fmt.Println("\n" + banner)
	fmt.Printf("                                   PER-NODE METRICS SUMMARY\n")
	fmt.Println(banner)
	fmt.Printf("%-6s | %-12s | %-5s | %-14s | %-16s | %s\n",
		"Node", "Final State", "Term", "Elections/hr", "PreVote Win Rate", "Step-Downs (Req/Rep/Quorum/Hb)")
	fmt.Println(sep)

	for _, id := range nodeIDs {
		node := trace.Nodes[id]
		electionsHr := 0.0
		preVoteStr := "N/A (0/0)"
		stepDownsStr := "0 (0/0/0/0)"

		if node.Metrics != nil {
			electionsHr = node.Metrics.ElectionsPerHour
			if node.Metrics.PreVotesStarted > 0 {
				preVoteStr = fmt.Sprintf("%5.1f%% (%d/%d)",
					node.Metrics.PreVoteSuccessRate*100, node.Metrics.PreVotesWon, node.Metrics.PreVotesStarted)
			}
			r := node.Metrics.StepDownReasons
			stepDownsStr = fmt.Sprintf("%d (%d/%d/%d/%d)",
				r.Total(), r.HigherTermRequest, r.HigherTermReply, r.QuorumLoss, r.LeaderHeartbeat)
		}

		fmt.Printf("%-6d | %-12s | %-5d | %14.2f | %-16s | %s\n",
			id, node.State, node.Term, electionsHr, preVoteStr, stepDownsStr)
	}
	fmt.Println(banner)
}

func exportNodeTracesIfRequested(trace *server.ScenarioTrace, dir string) {
	if dir == "" {
		return
	}
	paths, err := trace.ExportNodeTracesJSON(dir)
	if err != nil {
		fmt.Printf("Error exporting per-node traces: %v\n", err)
		return
	}
	fmt.Printf("\nExported %d per-node event trace JSON files to %s:\n", len(paths), dir)
	for id, p := range paths {
		fmt.Printf("  - Node %d: %s\n", id, p)
	}
}

func main() {
	cfg := parseFlags()
	trace, err := loadOrRunTrace(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	entries, nodeIDs, baseTime := collectTimelineEntries(trace)
	renderTimelineHeader(trace, nodeIDs)
	renderTimelineRows(entries, nodeIDs, baseTime)
	renderMetricsSummary(trace, nodeIDs)
	exportNodeTracesIfRequested(trace, cfg.exportDir)
}
