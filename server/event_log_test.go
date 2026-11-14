package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raft/raft"
)

func TestEventLog_Basics(t *testing.T) {
	el := NewEventLog(3)
	if el.Cap() != 3 {
		t.Fatalf("expected cap 3, got %d", el.Cap())
	}
	if el.Len() != 0 {
		t.Fatalf("expected len 0, got %d", el.Len())
	}
	if entries := el.Entries(); len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}

	// 1. Append first event
	ev1 := el.Append(EventElectionStarted, 1, "election 1", nil)
	if ev1.Index != 1 {
		t.Errorf("expected index 1, got %d", ev1.Index)
	}
	if el.Len() != 1 {
		t.Errorf("expected len 1, got %d", el.Len())
	}

	// 2. Append second event
	ev2 := el.Append(EventVoteGranted, 1, "granted vote to 2", map[string]interface{}{"candidate": 2})
	if ev2.Index != 2 {
		t.Errorf("expected index 2, got %d", ev2.Index)
	}
	if el.Len() != 2 {
		t.Errorf("expected len 2, got %d", el.Len())
	}

	entries := el.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Index != 1 || entries[1].Index != 2 {
		t.Errorf("unexpected entry indexes: [%d, %d]", entries[0].Index, entries[1].Index)
	}

	// 3. Append third event (buffer now full)
	ev3 := el.Append(EventStateTransition, 1, "transitioned to leader", nil)
	if ev3.Index != 3 {
		t.Errorf("expected index 3, got %d", ev3.Index)
	}
	if el.Len() != 3 {
		t.Errorf("expected len 3, got %d", el.Len())
	}

	entries = el.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i, ev := range entries {
		if ev.Index != uint64(i+1) {
			t.Errorf("expected index %d, got %d", i+1, ev.Index)
		}
	}
}

func TestEventLog_RingBufferWrapAround(t *testing.T) {
	capacity := 4
	el := NewEventLog(capacity)

	// Append 10 items (more than capacity 4)
	for i := 1; i <= 10; i++ {
		el.Append(EventStateTransition, raft.Term(i), fmt.Sprintf("event-%d", i), nil)
		time.Sleep(1 * time.Millisecond)
	}

	if el.Len() != capacity {
		t.Fatalf("expected len %d, got %d", capacity, el.Len())
	}

	entries := el.Entries()
	if len(entries) != capacity {
		t.Fatalf("expected %d entries, got %d", capacity, len(entries))
	}

	// Chronological order should be events 7, 8, 9, 10
	expectedIndexes := []uint64{7, 8, 9, 10}
	for i, ev := range entries {
		if ev.Index != expectedIndexes[i] {
			t.Errorf("at index %d: expected event index %d, got %d", i, expectedIndexes[i], ev.Index)
		}
		if ev.Message != fmt.Sprintf("event-%d", expectedIndexes[i]) {
			t.Errorf("at index %d: expected message event-%d, got %s", i, expectedIndexes[i], ev.Message)
		}
	}

	// Test Recent(2) -> [9, 10]
	recent2 := el.Recent(2)
	if len(recent2) != 2 {
		t.Fatalf("expected 2 recent entries, got %d", len(recent2))
	}
	if recent2[0].Index != 9 || recent2[1].Index != 10 {
		t.Errorf("unexpected Recent(2) indexes: [%d, %d]", recent2[0].Index, recent2[1].Index)
	}

	// Test Recent(10) -> all 4
	recentAll := el.Recent(10)
	if len(recentAll) != 4 {
		t.Fatalf("expected 4 entries from Recent(10), got %d", len(recentAll))
	}

	// Test Recent(0) -> empty
	recentNone := el.Recent(0)
	if len(recentNone) != 0 {
		t.Fatalf("expected 0 entries from Recent(0), got %d", len(recentNone))
	}
}

func TestEventLog_Clear(t *testing.T) {
	el := NewEventLog(5)
	for i := 1; i <= 3; i++ {
		el.Append(EventStateTransition, raft.Term(i), fmt.Sprintf("event-%d", i), nil)
	}
	if el.Len() != 3 {
		t.Fatalf("expected len 3, got %d", el.Len())
	}

	el.Clear()
	if el.Len() != 0 {
		t.Fatalf("expected len 0 after clear, got %d", el.Len())
	}
	if entries := el.Entries(); len(entries) != 0 {
		t.Fatalf("expected 0 entries after clear, got %d", len(entries))
	}

	// Subsequent appends should continue with monotonic index
	ev := el.Append(EventHeartbeatSent, 4, "post-clear", nil)
	if ev.Index != 4 {
		t.Errorf("expected sequence index 4 after clear, got %d", ev.Index)
	}
}

func TestEventLog_Concurrency(t *testing.T) {
	el := NewEventLog(50)
	var wg sync.WaitGroup

	// Concurrently append from multiple goroutines
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				el.Append(EventStateTransition, raft.Term(gid), fmt.Sprintf("g%d-e%d", gid, i), nil)
				_ = el.Entries()
				_ = el.Recent(10)
				_ = el.Len()
			}
		}(g)
	}

	wg.Wait()

	if el.Len() != 50 {
		t.Fatalf("expected len 50, got %d", el.Len())
	}
	entries := el.Entries()
	if len(entries) != 50 {
		t.Fatalf("expected 50 entries, got %d", len(entries))
	}

	// Verify entries are strictly monotonic in Index
	for i := 1; i < len(entries); i++ {
		if entries[i].Index <= entries[i-1].Index {
			t.Fatalf("expected strictly increasing indexes, got %d then %d", entries[i-1].Index, entries[i].Index)
		}
	}
}

