package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"raft/proto"
	"raft/raft"
)

func TestServer_Counters_TrackingAndStatusEndpoint(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "127.0.0.1:9002"},
		{ID: 3, Address: "127.0.0.1:9003"},
	}, &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
	}, nil)
	defer srv.Close()

	// Initial counters should all be zero
	if c := srv.ElectionsStarted(); c != 0 {
		t.Fatalf("expected 0 elections started, got %d", c)
	}
	if c := srv.VotesGranted(); c != 0 {
		t.Fatalf("expected 0 votes granted, got %d", c)
	}
	if c := srv.VotesDenied(); c != 0 {
		t.Fatalf("expected 0 votes denied, got %d", c)
	}
	if c := srv.HeartbeatsSent(); c != 0 {
		t.Fatalf("expected 0 heartbeats sent, got %d", c)
	}

	// 1. Trigger election -> electionsStarted should increment to 1
	srv.becomeCandidate()
	if c := srv.ElectionsStarted(); c != 1 {
		t.Fatalf("expected 1 election started, got %d", c)
	}

	// 2. Process RequestVote that is DENIED due to stale term (req term 1 < current term 2)
	reply1 := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if reply1.VoteGranted {
		t.Fatalf("expected vote denied for stale term")
	}
	if c := srv.VotesDenied(); c != 1 {
		t.Fatalf("expected 1 vote denied, got %d", c)
	}

	// 3. Process RequestVote that is DENIED due to candidate log not up to date
	// Force higher term 3 so candidate term is accepted, but receiver has log at index 2 term 3
	srv.mu.Lock()
	srv.PersistentState.CurrentTerm = 3
	srv.PersistentState.VotedFor = -1
	srv.PersistentState.Log = []raft.LogEntry{
		{Term: 3, Index: 1, Command: []byte("cmd1")},
		{Term: 3, Index: 2, Command: []byte("cmd2")},
	}
	srv.mu.Unlock()

	reply2 := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 1, // Stale log index
		LastLogTerm:  2, // Stale log term
	})
	if reply2.VoteGranted {
		t.Fatalf("expected vote denied for stale candidate log")
	}
	if c := srv.VotesDenied(); c != 2 {
		t.Fatalf("expected 2 votes denied, got %d", c)
	}

	// 4. Process RequestVote that is GRANTED
	reply3 := srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         4, // higher term resets votedFor
		CandidateId:  3,
		LastLogIndex: 5,
		LastLogTerm:  4,
	})
	if !reply3.VoteGranted {
		t.Fatalf("expected vote granted for candidate 3")
	}
	if c := srv.VotesGranted(); c != 1 {
		t.Fatalf("expected 1 vote granted, got %d", c)
	}

	// 5. Become Leader and simulate heartbeat ticks -> heartbeatsSent should increment
	srv.becomeLeader()
	srv.handleHeartbeatTick()
	srv.handleHeartbeatTick()
	if c := srv.HeartbeatsSent(); c != 2 {
		t.Fatalf("expected 2 heartbeats sent, got %d", c)
	}

	// Verify Counters() struct snapshot
	counters := srv.Counters()
	if counters.ElectionsStarted != 1 || counters.VotesGranted != 1 || counters.VotesDenied != 2 || counters.HeartbeatsSent != 2 {
		t.Fatalf("unexpected Counters() snapshot: %+v", counters)
	}

	// 6. Verify HTTP GET /status exposes the counters
	handler := srv.HTTPHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	var jsonMap map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &jsonMap); err != nil {
		t.Fatalf("failed to decode /status JSON: %v", err)
	}

	// Flat counter fields
	if v, ok := jsonMap["electionsStarted"].(float64); !ok || uint64(v) != 1 {
		t.Errorf("expected electionsStarted=1, got %v", jsonMap["electionsStarted"])
	}
	if v, ok := jsonMap["votesGranted"].(float64); !ok || uint64(v) != 1 {
		t.Errorf("expected votesGranted=1, got %v", jsonMap["votesGranted"])
	}
	if v, ok := jsonMap["votesDenied"].(float64); !ok || uint64(v) != 2 {
		t.Errorf("expected votesDenied=2, got %v", jsonMap["votesDenied"])
	}
	if v, ok := jsonMap["heartbeatsSent"].(float64); !ok || uint64(v) != 2 {
		t.Errorf("expected heartbeatsSent=2, got %v", jsonMap["heartbeatsSent"])
	}

	// Nested counters map
	cMap, ok := jsonMap["counters"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected 'counters' object in /status response")
	}
	if uint64(cMap["electionsStarted"].(float64)) != 1 ||
		uint64(cMap["votesGranted"].(float64)) != 1 ||
		uint64(cMap["votesDenied"].(float64)) != 2 ||
		uint64(cMap["heartbeatsSent"].(float64)) != 2 {
		t.Errorf("unexpected nested counters object: %+v", cMap)
	}

	// Verify unmarshaling into NodeStatus struct
	var status NodeStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("failed to unmarshal into NodeStatus: %v", err)
	}
	if status.ElectionsStarted != 1 || status.VotesGranted != 1 || status.VotesDenied != 2 || status.HeartbeatsSent != 2 {
		t.Errorf("unexpected NodeStatus counters: %+v", status)
	}
	if len(status.RecentEvents) == 0 {
		t.Errorf("expected RecentEvents in NodeStatus to be non-empty")
	}
}

func TestServer_EventLog_PostMortemDebugging(t *testing.T) {
	srv := NewServer(1, []Peer{
		{ID: 2, Address: "127.0.0.1:9002"},
	}, &raft.PersistentState{
		CurrentTerm: 0,
		VotedFor:    -1,
	}, nil)

	// Verify event log exists and starts with EventServerStarted
	eventLog := srv.EventLog()
	if eventLog == nil {
		t.Fatalf("expected non-nil eventLog on Server")
	}

	entries := eventLog.Entries()
	if len(entries) == 0 {
		t.Fatalf("expected initial event log entries")
	}
	if entries[0].Type != EventServerStarted {
		t.Errorf("expected first event to be %s, got %s", EventServerStarted, entries[0].Type)
	}

	// 1. Transition to Candidate
	srv.becomeCandidate()

	// 2. Grant a vote to candidate 2 in term 2
	srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// 3. Deny a vote to candidate 2 in stale term 1
	srv.processRequestVote(&raftpb.RequestVoteArgs{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// 4. Transition to Leader
	srv.becomeLeader()

	// 5. Send heartbeat
	srv.handleHeartbeatTick()

	// 6. Stop server (simulating crash / shutdown)
	srv.Stop()

	// Post-Mortem Verification: inspect entries after node is stopped
	postMortemEntries := srv.EventLog().Entries()
	if len(postMortemEntries) < 6 {
		t.Fatalf("expected at least 6 recorded events in post-mortem log, got %d", len(postMortemEntries))
	}

	// Check that expected event types were recorded in order
	eventTypes := make([]EventType, len(postMortemEntries))
	for i, ev := range postMortemEntries {
		eventTypes[i] = ev.Type
		if ev.Timestamp.IsZero() {
			t.Errorf("event %d has zero timestamp", i)
		}
		if ev.Index != uint64(i+1) {
			t.Errorf("event %d has unexpected sequence index %d (want %d)", i, ev.Index, i+1)
		}
	}

	// Verify required events appear in timeline
	hasType := func(target EventType) bool {
		for _, et := range eventTypes {
			if et == target {
				return true
			}
		}
		return false
	}

	if !hasType(EventServerStarted) {
		t.Errorf("missing EventServerStarted in post-mortem timeline: %v", eventTypes)
	}
	if !hasType(EventElectionStarted) {
		t.Errorf("missing EventElectionStarted in post-mortem timeline: %v", eventTypes)
	}
	if !hasType(EventVoteGranted) {
		t.Errorf("missing EventVoteGranted in post-mortem timeline: %v", eventTypes)
	}
	if !hasType(EventVoteDenied) {
		t.Errorf("missing EventVoteDenied in post-mortem timeline: %v", eventTypes)
	}
	if !hasType(EventStateTransition) {
		t.Errorf("missing EventStateTransition in post-mortem timeline: %v", eventTypes)
	}
	if !hasType(EventHeartbeatSent) {
		t.Errorf("missing EventHeartbeatSent in post-mortem timeline: %v", eventTypes)
	}
	if !hasType(EventServerStopped) {
		t.Errorf("missing EventServerStopped in post-mortem timeline: %v", eventTypes)
	}

	// Verify HTTP GET /events endpoint
	handler := srv.HTTPHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 from /events, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json, got %q", ct)
	}

	var eventsDecoded []Event
	if err := json.Unmarshal(rec.Body.Bytes(), &eventsDecoded); err != nil {
		t.Fatalf("failed to unmarshal /events JSON: %v", err)
	}
	if len(eventsDecoded) != len(postMortemEntries) {
		t.Errorf("mismatch between /events count (%d) and memory log count (%d)", len(eventsDecoded), len(postMortemEntries))
	}
}

func TestHarness_Cluster_CountersAndEventLogs(t *testing.T) {
	cluster := NewCluster(t, 3,
		WithHeartbeatInterval(20*time.Millisecond),
		WithElectionTimeout(100*time.Millisecond, 200*time.Millisecond),
	)
	defer cluster.Stop()

	// Wait for an initial leader to emerge
	leaderSrv, term, err := cluster.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect initial leader: %v", err)
	}
	leaderID := leaderSrv.GetID()

	// Allow leader to send several heartbeats
	time.Sleep(60 * time.Millisecond)

	leaderNode := cluster.GetNode(leaderID)
	if leaderNode.ElectionsStarted() == 0 {
		t.Fatalf("leader Node %d should have electionsStarted >= 1, got %d", leaderID, leaderNode.ElectionsStarted())
	}
	if leaderNode.HeartbeatsSent() == 0 {
		t.Fatalf("leader Node %d should have heartbeatsSent > 0, got %d", leaderID, leaderNode.HeartbeatsSent())
	}

	// Verify peer nodes granted votes to leader
	for id := raft.NodeID(1); id <= 3; id++ {
		if id == leaderID {
			continue
		}
		peerNode := cluster.GetNode(id)
		if peerNode.VotesGranted() == 0 {
			t.Errorf("peer Node %d should have votesGranted > 0, got %d", id, peerNode.VotesGranted())
		}
	}

	// Post-Mortem Debugging: Kill current leader and inspect its event log
	cluster.Kill(leaderID)

	postMortem := leaderNode.EventLog().Entries()
	if len(postMortem) == 0 {
		t.Fatalf("expected non-empty event log on killed leader")
	}

	t.Logf("--- Killed Leader (Node %d) Post-Mortem Event Log (len=%d) ---", leaderID, len(postMortem))
	for _, ev := range postMortem {
		t.Logf("[%s] #%d Term=%d Type=%s Message=%s",
			ev.Timestamp.Format("15:04:05.000"), ev.Index, ev.Term, ev.Type, ev.Message)
	}

	// Remaining nodes should elect a new leader in higher term
	newLeaderSrv, newTerm, err := cluster.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader after crash: %v", err)
	}
	if newLeaderSrv.GetID() == leaderID {
		t.Fatalf("killed node %d should not be elected leader", leaderID)
	}
	if newTerm <= term {
		t.Fatalf("expected higher term > %d, got %d", term, newTerm)
	}

	t.Logf("New leader elected after crash: Node %d at term %d", newLeaderSrv.GetID(), newTerm)
}
