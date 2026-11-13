package store_test

import (
	"sync"
	"testing"

	"raft/raft"
	"raft/store"
)

func TestMemoryStorage_InitialEmpty(t *testing.T) {
	s := store.NewMemoryStorage()
	if s == nil {
		t.Fatal("expected non-nil MemoryStorage")
	}

	state, err := s.Load()
	if err != nil {
		t.Fatalf("expected nil error on empty load, got %v", err)
	}
	if state != nil {
		t.Fatalf("expected nil state on empty load, got %+v", state)
	}

	snap, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("expected nil error on empty snapshot load, got %v", err)
	}
	if snap != nil {
		t.Fatalf("expected nil snapshot on empty load, got %v", snap)
	}
}

func TestMemoryStorage_PersistAndLoad(t *testing.T) {
	s := store.NewMemoryStorage()

	orig := &raft.PersistentState{
		CurrentTerm: 42,
		VotedFor:    7,
		Log: []raft.LogEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
			{Index: 2, Term: 2, Command: []byte("cmd2")},
			{Index: 3, Term: 42, Command: []byte("cmd3")},
		},
	}

	if err := s.Persist(orig); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	loaded, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil state from Load")
	}

	if loaded.CurrentTerm != orig.CurrentTerm {
		t.Errorf("CurrentTerm mismatch: got %d, want %d", loaded.CurrentTerm, orig.CurrentTerm)
	}
	if loaded.VotedFor != orig.VotedFor {
		t.Errorf("VotedFor mismatch: got %d, want %d", loaded.VotedFor, orig.VotedFor)
	}
	if len(loaded.Log) != len(orig.Log) {
		t.Fatalf("Log length mismatch: got %d, want %d", len(loaded.Log), len(orig.Log))
	}
	for i := range orig.Log {
		if loaded.Log[i].Index != orig.Log[i].Index || loaded.Log[i].Term != orig.Log[i].Term {
			t.Errorf("Log[%d] mismatch: got %+v, want %+v", i, loaded.Log[i], orig.Log[i])
		}
	}
}

func TestMemoryStorage_DeepCopyIsolation(t *testing.T) {
	s := store.NewMemoryStorage()

	orig := &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    2,
		Log: []raft.LogEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
		},
	}

	if err := s.Persist(orig); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Mutate the original struct after persisting
	orig.CurrentTerm = 99
	orig.VotedFor = 99
	orig.Log[0].Term = 99
	orig.Log = append(orig.Log, raft.LogEntry{Index: 2, Term: 99})

	// Loaded state should remain unaffected
	loaded, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.CurrentTerm != 5 {
		t.Errorf("expected stored CurrentTerm to be 5, got %d", loaded.CurrentTerm)
	}
	if loaded.VotedFor != 2 {
		t.Errorf("expected stored VotedFor to be 2, got %d", loaded.VotedFor)
	}
	if len(loaded.Log) != 1 || loaded.Log[0].Term != 1 {
		t.Errorf("expected stored log untouched, got %+v", loaded.Log)
	}

	// Mutating the returned copy from Load should not affect subsequent loads
	loaded.Log = append(loaded.Log, raft.LogEntry{Index: 2, Term: 100})
	loaded.CurrentTerm = 100

	reloaded, err := s.Load()
	if err != nil {
		t.Fatalf("second Load failed: %v", err)
	}
	if reloaded.CurrentTerm != 5 {
		t.Errorf("expected reloaded CurrentTerm to be 5, got %d", reloaded.CurrentTerm)
	}
	if len(reloaded.Log) != 1 {
		t.Errorf("expected reloaded log length 1, got %d", len(reloaded.Log))
	}
}

func TestMemoryStorage_PersistNilClears(t *testing.T) {
	s := store.NewMemoryStorage()

	orig := &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    1,
		Log:         []raft.LogEntry{{Index: 1, Term: 1}},
	}
	if err := s.Persist(orig); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if err := s.Persist(nil); err != nil {
		t.Fatalf("Persist(nil) failed: %v", err)
	}

	loaded, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded != nil {
		t.Fatalf("expected nil state after persisting nil, got %+v", loaded)
	}
}

func TestMemoryStorage_Snapshot(t *testing.T) {
	s := store.NewMemoryStorage()

	snapData := []byte("state-machine-snapshot-v1")
	if err := s.SaveSnapshot(snapData); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Mutate original byte slice
	snapData[0] = 'X'

	loaded, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}
	if string(loaded) != "state-machine-snapshot-v1" {
		t.Errorf("snapshot data mismatch: got %q, want %q", string(loaded), "state-machine-snapshot-v1")
	}

	// Mutating loaded slice does not corrupt store
	loaded[0] = 'Y'
	reloaded, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("second LoadSnapshot failed: %v", err)
	}
	if string(reloaded) != "state-machine-snapshot-v1" {
		t.Errorf("reloaded snapshot data corrupted: got %q", string(reloaded))
	}

	// Clearing snapshot
	if err := s.SaveSnapshot(nil); err != nil {
		t.Fatalf("SaveSnapshot(nil) failed: %v", err)
	}
	cleared, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot after clear failed: %v", err)
	}
	if cleared != nil {
		t.Fatalf("expected nil snapshot, got %v", cleared)
	}
}

func TestMemoryStorage_Close(t *testing.T) {
	s := store.NewMemoryStorage()
	if err := s.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestNewNoOpStorage_ConformsToInterfaces(t *testing.T) {
	var s store.Storage = store.NewNoOpStorage()
	var p store.Persister = s
	if p == nil {
		t.Fatal("expected non-nil persister")
	}
}

func TestMemoryStorage_ConcurrentAccess(t *testing.T) {
	s := store.NewMemoryStorage()

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				term := raft.Term(workerID*iterations + j)
				_ = s.Persist(&raft.PersistentState{
					CurrentTerm: term,
					VotedFor:    raft.NodeID(workerID),
					Log: []raft.LogEntry{
						{Index: raft.LogIndex(j), Term: term},
					},
				})

				state, err := s.Load()
				if err != nil {
					t.Errorf("concurrent Load failed: %v", err)
				}
				if state == nil {
					t.Errorf("concurrent Load returned nil state unexpectedly")
				}

				_ = s.SaveSnapshot([]byte("snapshot"))
				_, _ = s.LoadSnapshot()
			}
		}(i)
	}

	wg.Wait()
}

