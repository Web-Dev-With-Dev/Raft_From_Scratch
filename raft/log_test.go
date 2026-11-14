package raft

import "testing"

func TestIsLogUpToDate_TableDriven_RaftPaperCases(t *testing.T) {
	// The Raft paper (§5.4.1 Election restriction):
	// "If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//  If the logs end with the same term, then whichever log is longer is more up-to-date."
	//
	// This yields 4 canonical comparison cases:
	// Case 1: Candidate has higher last log term (cTerm > rTerm) -> Grant (true)
	// Case 2: Candidate has lower last log term (cTerm < rTerm) -> Deny (false)
	// Case 3: Same last log term, candidate log is at least as long (cTerm == rTerm && cIndex >= rIndex) -> Grant (true)
	// Case 4: Same last log term, candidate log is shorter (cTerm == rTerm && cIndex < rIndex) -> Deny (false)

	tests := []struct {
		name          string
		candIndex     LogIndex
		candTerm      Term
		recvIndex     LogIndex
		recvTerm      Term
		expected      bool
		raftPaperCase string
	}{
		// Case 1: Candidate has higher last log term (cTerm > rTerm) -> true
		{
			name:          "Case 1a: Higher candidate term with shorter log",
			candIndex:     2,
			candTerm:      3,
			recvIndex:     10,
			recvTerm:      2,
			expected:      true,
			raftPaperCase: "Case 1: cTerm > rTerm (higher term takes precedence over log length)",
		},
		{
			name:          "Case 1b: Higher candidate term with longer log",
			candIndex:     10,
			candTerm:      3,
			recvIndex:     5,
			recvTerm:      2,
			expected:      true,
			raftPaperCase: "Case 1: cTerm > rTerm",
		},
		{
			name:          "Case 1c: Higher candidate term with empty receiver log",
			candIndex:     1,
			candTerm:      1,
			recvIndex:     0,
			recvTerm:      0,
			expected:      true,
			raftPaperCase: "Case 1: cTerm > rTerm (receiver log empty)",
		},

		// Case 2: Candidate has lower last log term (cTerm < rTerm) -> false
		{
			name:          "Case 2a: Lower candidate term with longer log",
			candIndex:     10,
			candTerm:      2,
			recvIndex:     2,
			recvTerm:      3,
			expected:      false,
			raftPaperCase: "Case 2: cTerm < rTerm (lower term rejected even if log is longer)",
		},
		{
			name:          "Case 2b: Lower candidate term with shorter log",
			candIndex:     2,
			candTerm:      1,
			recvIndex:     5,
			recvTerm:      2,
			expected:      false,
			raftPaperCase: "Case 2: cTerm < rTerm",
		},
		{
			name:          "Case 2c: Empty candidate log with non-empty receiver log",
			candIndex:     0,
			candTerm:      0,
			recvIndex:     1,
			recvTerm:      1,
			expected:      false,
			raftPaperCase: "Case 2: cTerm < rTerm (candidate log empty)",
		},

		// Case 3: Same last log term, candidate log is at least as long (cTerm == rTerm && cIndex >= rIndex) -> true
		{
			name:          "Case 3a: Same term with strictly longer candidate log",
			candIndex:     6,
			candTerm:      2,
			recvIndex:     5,
			recvTerm:      2,
			expected:      true,
			raftPaperCase: "Case 3: cTerm == rTerm && cIndex > rIndex",
		},
		{
			name:          "Case 3b: Same term with identical log length",
			candIndex:     5,
			candTerm:      2,
			recvIndex:     5,
			recvTerm:      2,
			expected:      true,
			raftPaperCase: "Case 3: cTerm == rTerm && cIndex == rIndex",
		},
		{
			name:          "Case 3c: Both logs empty (0 term, 0 index)",
			candIndex:     0,
			candTerm:      0,
			recvIndex:     0,
			recvTerm:      0,
			expected:      true,
			raftPaperCase: "Case 3: cTerm == rTerm && cIndex == rIndex (both empty)",
		},

		// Case 4: Same last log term, candidate log is shorter (cTerm == rTerm && cIndex < rIndex) -> false
		{
			name:          "Case 4a: Same term with strictly shorter candidate log",
			candIndex:     4,
			candTerm:      2,
			recvIndex:     5,
			recvTerm:      2,
			expected:      false,
			raftPaperCase: "Case 4: cTerm == rTerm && cIndex < rIndex",
		},
		{
			name:          "Case 4b: Same term (0) with candidate empty and receiver index 1",
			candIndex:     0,
			candTerm:      0,
			recvIndex:     1,
			recvTerm:      0,
			expected:      false,
			raftPaperCase: "Case 4: cTerm == rTerm && cIndex < rIndex (candidate empty, receiver has entry at term 0)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsLogUpToDate(tc.candIndex, tc.candTerm, tc.recvIndex, tc.recvTerm)
			if got != tc.expected {
				t.Fatalf("[%s] IsLogUpToDate(candIndex=%d, candTerm=%d, recvIndex=%d, recvTerm=%d) = %v; want %v",
					tc.raftPaperCase, tc.candIndex, tc.candTerm, tc.recvIndex, tc.recvTerm, got, tc.expected)
			}
		})
	}
}

func TestPersistentState_LastLogInfo(t *testing.T) {
	// Test nil PersistentState
	var nilState *PersistentState
	index, term := nilState.LastLogInfo()
	if index != 0 || term != 0 {
		t.Fatalf("expected (0, 0) for nil state, got (%d, %d)", index, term)
	}

	// Test empty log
	emptyState := &PersistentState{
		CurrentTerm: 1,
		VotedFor:    -1,
		Log:         []LogEntry{},
	}
	index, term = emptyState.LastLogInfo()
	if index != 0 || term != 0 {
		t.Fatalf("expected (0, 0) for empty log, got (%d, %d)", index, term)
	}

	// Test log with multiple entries
	populatedState := &PersistentState{
		CurrentTerm: 3,
		VotedFor:    -1,
		Log: []LogEntry{
			{Index: 1, Term: 1, Command: "cmd1"},
			{Index: 2, Term: 1, Command: "cmd2"},
			{Index: 3, Term: 2, Command: "cmd3"},
			{Index: 4, Term: 3, Command: "cmd4"},
		},
	}
	index, term = populatedState.LastLogInfo()
	if index != 4 || term != 3 {
		t.Fatalf("expected (4, 3) for populated log, got (%d, %d)", index, term)
	}
}

