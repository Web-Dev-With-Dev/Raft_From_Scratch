package server

import (
	"strings"
	"testing"
	"time"

	"raft/raft"
)

func TestGlobalChecker_ElectionSafety_InstantaneousViolation(t *testing.T) {
	gc := NewGlobalChecker()
	statuses := []NodeStatus{
		{ID: 1, Term: 1, State: raft.Leader.String(), VotedFor: 1},
		{ID: 2, Term: 1, State: raft.Leader.String(), VotedFor: 2},
		{ID: 3, Term: 1, State: raft.Follower.String(), VotedFor: 1},
	}
	err := gc.checkNodeStatusesLocked(statuses)
	if err == nil {
		t.Fatalf("expected instantaneous election safety violation, got nil")
	}
	if !strings.Contains(err.Error(), "election safety violated (instantaneous)") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGlobalChecker_ElectionSafety_HistoricalViolation(t *testing.T) {
	gc := NewGlobalChecker()
	step1 := []NodeStatus{
		{ID: 1, Term: 2, State: raft.Leader.String(), VotedFor: 1},
		{ID: 2, Term: 2, State: raft.Follower.String(), VotedFor: 1},
	}
	if err := gc.checkNodeStatusesLocked(step1); err != nil {
		t.Fatalf("step 1 failed unexpectedly: %v", err)
	}

	step2 := []NodeStatus{
		{ID: 1, Term: 2, State: raft.Follower.String(), VotedFor: 1},
		{ID: 2, Term: 2, State: raft.Leader.String(), VotedFor: 2},
	}
	err := gc.checkNodeStatusesLocked(step2)
	if err == nil {
		t.Fatalf("expected historical election safety violation, got nil")
	}
	if !strings.Contains(err.Error(), "election safety violated (historical)") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGlobalChecker_ElectionSafety_ValidTermSuccession(t *testing.T) {
	gc := NewGlobalChecker()
	step1 := []NodeStatus{
		{ID: 1, Term: 1, State: raft.Leader.String(), VotedFor: 1},
		{ID: 2, Term: 1, State: raft.Follower.String(), VotedFor: 1},
	}
	if err := gc.checkNodeStatusesLocked(step1); err != nil {
		t.Fatalf("step 1 failed: %v", err)
	}

	step2 := []NodeStatus{
		{ID: 1, Term: 2, State: raft.Follower.String(), VotedFor: 2},
		{ID: 2, Term: 2, State: raft.Leader.String(), VotedFor: 2},
	}
	if err := gc.checkNodeStatusesLocked(step2); err != nil {
		t.Fatalf("expected valid succession in different terms, got: %v", err)
	}
}

func TestGlobalChecker_TermMonotonicity_DecreasingTermFails(t *testing.T) {
	gc := NewGlobalChecker()
	step1 := []NodeStatus{{ID: 1, Term: 3, State: raft.Follower.String(), VotedFor: -1}}
	if err := gc.checkNodeStatusesLocked(step1); err != nil {
		t.Fatalf("step 1 failed: %v", err)
	}

	step2 := []NodeStatus{{ID: 1, Term: 2, State: raft.Follower.String(), VotedFor: -1}}
	err := gc.checkNodeStatusesLocked(step2)
	if err == nil {
		t.Fatalf("expected term monotonicity violation, got nil")
	}
	if !strings.Contains(err.Error(), "term monotonicity violated") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGlobalChecker_TermMonotonicity_ValidProgression(t *testing.T) {
	gc := NewGlobalChecker()
	for _, term := range []raft.Term{1, 1, 2, 2, 5, 5, 6} {
		step := []NodeStatus{{ID: 1, Term: term, State: raft.Follower.String(), VotedFor: -1}}
		if err := gc.checkNodeStatusesLocked(step); err != nil {
			t.Fatalf("valid term progression to %d failed: %v", term, err)
		}
	}
}

func TestGlobalChecker_VotedForStability_VoteFlipWithinTermFails(t *testing.T) {
	gc := NewGlobalChecker()
	step1 := []NodeStatus{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: 2}}
	if err := gc.checkNodeStatusesLocked(step1); err != nil {
		t.Fatalf("step 1 failed: %v", err)
	}

	step2 := []NodeStatus{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: 3}}
	err := gc.checkNodeStatusesLocked(step2)
	if err == nil {
		t.Fatalf("expected votedFor flip violation within same term, got nil")
	}
	if !strings.Contains(err.Error(), "votedFor stability violated") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGlobalChecker_VotedForStability_ErasingVoteWithinTermFails(t *testing.T) {
	gc := NewGlobalChecker()
	step1 := []NodeStatus{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: 2}}
	if err := gc.checkNodeStatusesLocked(step1); err != nil {
		t.Fatalf("step 1 failed: %v", err)
	}

	step2 := []NodeStatus{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: -1}}
	err := gc.checkNodeStatusesLocked(step2)
	if err == nil {
		t.Fatalf("expected votedFor erasure violation, got nil")
	}
	if !strings.Contains(err.Error(), "votedFor stability violated") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGlobalChecker_VotedForStability_ValidLifecycle(t *testing.T) {
	gc := NewGlobalChecker()
	// Term 1: uncast (-1) -> cast (2) -> stays (2)
	steps := [][]NodeStatus{
		{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: -1}},
		{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: 2}},
		{{ID: 1, Term: 1, State: raft.Follower.String(), VotedFor: 2}},
		// Term 2: new term resets vote to -1 -> votes for candidate 3 -> stays 3
		{{ID: 1, Term: 2, State: raft.Follower.String(), VotedFor: -1}},
		{{ID: 1, Term: 2, State: raft.Follower.String(), VotedFor: 3}},
		{{ID: 1, Term: 2, State: raft.Follower.String(), VotedFor: 3}},
	}
	for i, s := range steps {
		if err := gc.checkNodeStatusesLocked(s); err != nil {
			t.Fatalf("valid vote step %d failed: %v", i+1, err)
		}
	}
}

func TestGlobalChecker_SimulationSteps_AutomaticExecution(t *testing.T) {
	clk := raft.NewFakeClock()
	simNet := NewSimNetwork(42)
	simNet.SetClock(clk)
	gc := NewGlobalChecker()

	cluster := NewCluster(t, 3,
		WithClock(clk),
		WithSimNetwork(simNet),
		WithRandSeed(42),
		WithGlobalChecker(gc),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
		WithRPCTimeout(40*time.Millisecond),
	)

	cluster.GetNode(1).StartElection()
	leader, _, err := cluster.WaitLeader(2 * time.Second)
	if err != nil || leader == nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	cluster.Advance(60 * time.Millisecond)

	if gc.StepCount() == 0 {
		t.Fatalf("expected global checker to have evaluated simulated steps, got 0")
	}
	if cluster.CheckerError() != nil {
		t.Fatalf("expected zero checker errors, got: %v", cluster.CheckerError())
	}
	if len(gc.LeaderHistory()) == 0 {
		t.Fatalf("expected leader history to record leader, got empty")
	}
}

