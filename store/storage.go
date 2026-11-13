package store

import (
	"errors"
	"fmt"
	"sync"

	"raft/raft"
)

var (
	// ErrNotFound is returned when no persistent state has been saved yet.
	ErrNotFound = errors.New("no persistent state found")
)

// Storage defines the interface for persisting and restoring Raft consensus state
// (currentTerm, votedFor, and log entries as per Raft paper §5.2).
// This in-memory no-op implementation serves as the placeholder to be replaced with
// a durable disk-backed / WAL storage engine in Month 4.
type Storage interface {
	// Persist saves the given Raft persistent state.
	Persist(state *raft.PersistentState) error

	// Load restores the Raft persistent state. Returns nil if no state has been persisted.
	Load() (*raft.PersistentState, error)

	// SaveSnapshot persists a state machine snapshot (stubbed for future Month 4 snapshotting).
	SaveSnapshot(snapshot []byte) error

	// LoadSnapshot retrieves the latest state machine snapshot.
	LoadSnapshot() ([]byte, error)

	// Close closes the storage engine and releases any underlying resources.
	Close() error
}

// Persister is an alias for Storage to match alternative Raft terminology.
type Persister = Storage

// MemoryStorage is a thread-safe, in-memory implementation of Storage.
// It acts as a no-op persistence layer in development, holding state in RAM
// until durable disk storage replaces it in Month 4.
type MemoryStorage struct {
	mu       sync.RWMutex
	state    *raft.PersistentState
	snapshot []byte
}

// NewMemoryStorage creates a new in-memory Storage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// NewNoOpStorage creates a new in-memory no-op Storage instance.
func NewNoOpStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// Persist saves a deep copy of the persistent consensus state in memory.
func (m *MemoryStorage) Persist(state *raft.PersistentState) error {
	if m == nil {
		return fmt.Errorf("storage is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if state == nil {
		m.state = nil
		return nil
	}

	logCopy := make([]raft.LogEntry, len(state.Log))
	copy(logCopy, state.Log)

	m.state = &raft.PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         logCopy,
	}
	return nil
}

// Load returns a deep copy of the in-memory persistent consensus state.
// Returns (nil, nil) if no state has been persisted yet.
func (m *MemoryStorage) Load() (*raft.PersistentState, error) {
	if m == nil {
		return nil, fmt.Errorf("storage is nil")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state == nil {
		return nil, nil
	}

	logCopy := make([]raft.LogEntry, len(m.state.Log))
	copy(logCopy, m.state.Log)

	return &raft.PersistentState{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         logCopy,
	}, nil
}

// SaveSnapshot saves a snapshot in memory.
func (m *MemoryStorage) SaveSnapshot(snapshot []byte) error {
	if m == nil {
		return fmt.Errorf("storage is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if snapshot == nil {
		m.snapshot = nil
		return nil
	}
	snapCopy := make([]byte, len(snapshot))
	copy(snapCopy, snapshot)
	m.snapshot = snapCopy
	return nil
}

// LoadSnapshot returns a copy of the stored in-memory snapshot.
func (m *MemoryStorage) LoadSnapshot() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("storage is nil")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.snapshot == nil {
		return nil, nil
	}
	snapCopy := make([]byte, len(m.snapshot))
	copy(snapCopy, m.snapshot)
	return snapCopy, nil
}

// Close is a no-op for in-memory storage.
func (m *MemoryStorage) Close() error {
	return nil
}

