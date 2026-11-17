package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServer_EventLogging_TermChangesAndTimers(t *testing.T) {
	srv := NewServer(1, []Peer{{ID: 2, Address: "localhost:9002"}}, nil, nil)
	defer srv.Stop()

	// Verify initial timer reset and start events
	events := srv.EventLog().Entries()
	if len(events) == 0 {
		t.Fatalf("expected initial events recorded in event log")
	}

	// Trigger election timeout
	srv.mu.Lock()
	srv.handleElectionTimeout(time.Now())
	srv.mu.Unlock()

	entries := srv.EventLog().Entries()
	hasTimeout := false
	hasTermChange := false
	hasSelfVote := false
	hasTimerReset := false

	for _, ev := range entries {
		switch ev.Type {
		case EventTimerTimeout:
			hasTimeout = true
		case EventTermChange:
			hasTermChange = true
		case EventVoteGranted:
			if self, ok := ev.Details["self_vote"].(bool); ok && self {
				hasSelfVote = true
			}
		case EventTimerReset:
			hasTimerReset = true
		}
	}

	if !hasTimeout {
		t.Errorf("expected EventTimerTimeout to be recorded")
	}
	if !hasTermChange {
		t.Errorf("expected EventTermChange to be recorded")
	}
	if !hasSelfVote {
		t.Errorf("expected self-vote EventVoteGranted to be recorded")
	}
	if !hasTimerReset {
		t.Errorf("expected EventTimerReset to be recorded")
	}
}

func TestServer_Metrics_ElectionsPerHour_PreVote_StepDownReasons(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "localhost:9002"},
		{ID: 3, Address: "localhost:9003"},
	}, nil, nil)
	srv.SetPreVote(true)
	defer srv.Stop()

	// 1. Elections per hour
	srv.electionsStarted.Store(5)
	rate := srv.ElectionsPerHour()
	if rate <= 0 {
		t.Errorf("expected positive elections per hour, got %f", rate)
	}

	// 2. PreVote Success Rate
	srv.preVotesStarted.Store(4)
	srv.preVotesWon.Store(3)
	successRate := srv.PreVoteSuccessRate()
	if successRate != 0.75 {
		t.Errorf("expected pre-vote success rate 0.75, got %f", successRate)
	}

	// 3. StepDown Reasons
	srv.stepDowns.record("higher_term_request")
	srv.stepDowns.record("higher_term_reply")
	srv.stepDowns.record("quorum_loss")
	srv.stepDowns.record("leader_heartbeat")

	reasons := srv.StepDownReasons()
	if reasons.HigherTermRequest != 1 || reasons.HigherTermReply != 1 ||
		reasons.QuorumLoss != 1 || reasons.LeaderHeartbeat != 1 {
		t.Errorf("unexpected step down reasons: %+v", reasons)
	}
	if reasons.Total() != 4 {
		t.Errorf("expected total step downs 4, got %d", reasons.Total())
	}

	summary := srv.MetricsSummary()
	if summary.ElectionsStarted != 5 || summary.PreVoteSuccessRate != 0.75 {
		t.Errorf("unexpected metrics summary: %+v", summary)
	}
}

func TestServer_TraceExport_JSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft_trace_export_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	srv := NewServer(1, nil, nil, nil)
	defer srv.Stop()

	// Export single node JSON
	jsonBytes, err := srv.ExportEventTraceJSON()
	if err != nil {
		t.Fatalf("failed to export trace JSON: %v", err)
	}

	var trace NodeEventTrace
	if err := json.Unmarshal(jsonBytes, &trace); err != nil {
		t.Fatalf("failed to parse exported JSON: %v", err)
	}
	if trace.NodeID != 1 {
		t.Errorf("expected node ID 1, got %d", trace.NodeID)
	}
	if len(trace.Events) == 0 {
		t.Errorf("expected recorded events in trace")
	}

	// Export to file
	filePath := filepath.Join(tmpDir, "node_1_trace.json")
	if err := srv.ExportEventTraceJSONFile(filePath); err != nil {
		t.Fatalf("failed to write trace file: %v", err)
	}
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("trace file not created: %v", err)
	}

	// Test scenario trace export
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	scTrace, err := RunScenario(ctx, "election", 42, "")
	if err != nil {
		t.Fatalf("RunScenario failed: %v", err)
	}

	nodeFiles, err := scTrace.ExportNodeTracesJSON(tmpDir)
	if err != nil {
		t.Fatalf("ExportNodeTracesJSON failed: %v", err)
	}
	if len(nodeFiles) != len(scTrace.Nodes) {
		t.Errorf("expected %d node trace files, got %d", len(scTrace.Nodes), len(nodeFiles))
	}
	for id, fPath := range nodeFiles {
		data, err := os.ReadFile(fPath)
		if err != nil {
			t.Fatalf("failed to read node %d trace file %q: %v", id, fPath, err)
		}
		var parsed NodeTraceState
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("failed to parse node %d trace: %v", id, err)
		}
		if parsed.ID != id {
			t.Errorf("expected parsed ID %d, got %d", id, parsed.ID)
		}
	}
}
