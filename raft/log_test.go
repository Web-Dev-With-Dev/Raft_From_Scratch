package raft

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

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

func TestLog_EmptyAndSentinel(t *testing.T) {
	log := NewMemoryLog()

	if log.LastIndex() != 0 || log.LastTerm() != 0 {
		t.Fatalf("expected LastIndex=0, LastTerm=0; got %d, %d", log.LastIndex(), log.LastTerm())
	}

	term, err := log.TermAt(0)
	if err != nil || term != 0 {
		t.Fatalf("expected TermAt(0) = (0, nil); got (%d, %v)", term, err)
	}

	if _, err := log.TermAt(1); !errors.Is(err, ErrIndexOutOfBounds) {
		t.Fatalf("expected ErrIndexOutOfBounds for TermAt(1); got %v", err)
	}

	if err := log.TruncateFrom(0); !errors.Is(err, ErrCannotTruncateSentinel) {
		t.Fatalf("expected ErrCannotTruncateSentinel for TruncateFrom(0); got %v", err)
	}

	if err := log.TruncateFrom(1); err != nil {
		t.Fatalf("expected nil for TruncateFrom(1) on empty log; got %v", err)
	}

	if err := log.TruncateFrom(2); !errors.Is(err, ErrIndexOutOfBounds) {
		t.Fatalf("expected ErrIndexOutOfBounds for TruncateFrom(2); got %v", err)
	}
}

func TestLog_Append_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		entries      []LogEntry
		expectErr    error
		wantLastIdx  LogIndex
		wantLastTerm Term
	}{
		{"empty append", nil, nil, 0, 0},
		{"auto-index", []LogEntry{{Term: 1, Command: "x"}}, nil, 1, 1},
		{"explicit index", []LogEntry{{Index: 2, Term: 2, Command: "y"}}, nil, 2, 2},
		{"mismatched index", []LogEntry{{Index: 99, Term: 2, Command: "bad"}}, ErrInvalidIndex, 0, 0},
	}

	log := NewMemoryLog()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := log.Append(tc.entries...)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if log.LastIndex() != tc.wantLastIdx || log.LastTerm() != tc.wantLastTerm {
				t.Fatalf("got (%d, %d); want (%d, %d)", log.LastIndex(), log.LastTerm(), tc.wantLastIdx, tc.wantLastTerm)
			}
		})
	}
}

func TestLog_Append_Atomicity(t *testing.T) {
	log := NewMemoryLog()
	_ = log.Append(LogEntry{Term: 1, Command: "c1"})

	batch := []LogEntry{
		{Index: 2, Term: 1, Command: "c2"},
		{Index: 5, Term: 1, Command: "c5"},
	}
	err := log.Append(batch...)
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("expected ErrInvalidIndex, got %v", err)
	}

	if log.LastIndex() != 1 {
		t.Fatalf("expected LastIndex to remain 1 after failed batch; got %d", log.LastIndex())
	}
}

func TestLog_TermAt_TableDriven(t *testing.T) {
	log := NewMemoryLog()
	_ = log.Append(
		LogEntry{Term: 1, Command: "a"},
		LogEntry{Term: 1, Command: "b"},
		LogEntry{Term: 3, Command: "c"},
	)

	tests := []struct {
		name      string
		index     LogIndex
		wantTerm  Term
		expectErr error
	}{
		{"sentinel at 0", 0, 0, nil},
		{"first entry at 1", 1, 1, nil},
		{"second entry at 2", 2, 1, nil},
		{"third entry at 3", 3, 3, nil},
		{"out of bounds at 4", 4, 0, ErrIndexOutOfBounds},
		{"far out of bounds at 50", 50, 0, ErrIndexOutOfBounds},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			term, err := log.TermAt(tc.index)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil || term != tc.wantTerm {
				t.Fatalf("got term=%d, err=%v; want term=%d, nil", term, err, tc.wantTerm)
			}
		})
	}
}

func TestLog_TruncateFrom_TableDriven(t *testing.T) {
	setupLog := func() *MemoryLog {
		l := NewMemoryLog()
		_ = l.Append(
			LogEntry{Term: 1, Command: "1"},
			LogEntry{Term: 1, Command: "2"},
			LogEntry{Term: 2, Command: "3"},
			LogEntry{Term: 3, Command: "4"},
		)
		return l
	}

	tests := []struct {
		name         string
		truncIdx     LogIndex
		expectErr    error
		wantLastIdx  LogIndex
		wantLastTerm Term
	}{
		{"sentinel disallowed", 0, ErrCannotTruncateSentinel, 0, 0},
		{"truncate beyond lastIndex+1", 6, ErrIndexOutOfBounds, 0, 0},
		{"truncate at lastIndex+1 is noop", 5, nil, 4, 3},
		{"truncate last element", 4, nil, 3, 2},
		{"truncate from middle", 3, nil, 2, 1},
		{"truncate all down to sentinel", 1, nil, 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := setupLog()
			err := l.TruncateFrom(tc.truncIdx)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if l.LastIndex() != tc.wantLastIdx || l.LastTerm() != tc.wantLastTerm {
				t.Fatalf("got (%d, %d); want (%d, %d)", l.LastIndex(), l.LastTerm(), tc.wantLastIdx, tc.wantLastTerm)
			}
		})
	}
}

func TestLog_Slice_TableDriven(t *testing.T) {
	l := NewMemoryLog()
	_ = l.Append(
		LogEntry{Term: 1, Command: "c1"},
		LogEntry{Term: 2, Command: "c2"},
		LogEntry{Term: 2, Command: "c3"},
	)

	tests := []struct {
		name      string
		low       LogIndex
		high      LogIndex
		expectErr error
		wantLen   int
		wantTerms []Term
	}{
		{"invalid range low > high", 3, 2, ErrInvalidIndex, 0, nil},
		{"high exceeds lastIndex+1", 1, 5, ErrIndexOutOfBounds, 0, nil},
		{"empty slice low == high", 2, 2, nil, 0, []Term{}},
		{"sentinel only", 0, 1, nil, 1, []Term{0}},
		{"all entries excluding sentinel", 1, 4, nil, 3, []Term{1, 2, 2}},
		{"sub-slice middle", 2, 4, nil, 2, []Term{2, 2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries, err := l.Slice(tc.low, tc.high)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(entries) != tc.wantLen {
				t.Fatalf("got len %d, want %d", len(entries), tc.wantLen)
			}
			gotTerms := make([]Term, 0, len(entries))
			for _, e := range entries {
				gotTerms = append(gotTerms, e.Term)
			}
			if tc.wantTerms != nil && !reflect.DeepEqual(gotTerms, tc.wantTerms) {
				t.Fatalf("got terms %v, want %v", gotTerms, tc.wantTerms)
			}
		})
	}
}

func TestLog_Slice_Immutability(t *testing.T) {
	l := NewMemoryLog()
	_ = l.Append(LogEntry{Term: 1, Command: "original"})

	slice, err := l.Slice(1, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	slice[0].Term = 99
	slice[0].Command = "mutated"

	term, _ := l.TermAt(1)
	if term != 1 {
		t.Fatalf("expected internal log term to remain 1, got %d", term)
	}
}

func TestLog_Concurrent(t *testing.T) {
	l := NewMemoryLog()
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = l.Append(LogEntry{Term: Term(worker + 1), Command: j})
				_ = l.LastIndex()
				_ = l.LastTerm()
				_, _ = l.TermAt(1)
				_, _ = l.Slice(0, 1)
			}
		}(i)
	}
	wg.Wait()

	if l.LastIndex() != 1000 {
		t.Fatalf("expected LastIndex=1000 after concurrent appends, got %d", l.LastIndex())
	}
}
