package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrIndexOutOfBounds is returned when a requested log index exceeds LastIndex() + 1.
	ErrIndexOutOfBounds = errors.New("log index out of bounds")

	// ErrInvalidIndex is returned when an invalid log index or slicing range is provided.
	ErrInvalidIndex = errors.New("invalid log index")

	// ErrCannotTruncateSentinel is returned when attempting to truncate the sentinel entry at index 0.
	ErrCannotTruncateSentinel = errors.New("cannot truncate sentinel entry at index 0")
)

// Log represents a consensus log maintaining replicated log entries with 1-based indexing.
// Index 0 is reserved for a sentinel entry with Term 0.
// Keeping Log behind an interface allows swapping between in-memory and disk-backed logs.
type Log interface {
	// Append appends one or more entries to the log.
	// If an entry's Index is 0, it is automatically assigned the next contiguous index.
	// Returns ErrInvalidIndex if an explicit non-contiguous entry index is provided.
	Append(entries ...LogEntry) error

	// TermAt returns the term of the log entry at the given index.
	// Returns ErrIndexOutOfBounds if index > LastIndex().
	// TermAt(0) returns (0, nil) corresponding to the sentinel entry.
	TermAt(index LogIndex) (Term, error)

	// LastIndex returns the index of the last entry in the log.
	// Returns 0 if the log contains only the sentinel entry.
	LastIndex() LogIndex

	// LastTerm returns the term of the last entry in the log.
	// Returns 0 if the log contains only the sentinel entry.
	LastTerm() Term

	// TruncateFrom discards entries starting from index to the end of the log.
	// Truncating at index 0 is disallowed and returns ErrCannotTruncateSentinel.
	// Truncating at LastIndex() + 1 is a valid no-op.
	// Returns ErrIndexOutOfBounds if index > LastIndex() + 1.
	TruncateFrom(index LogIndex) error

	// Slice returns a copy of log entries in the half-open interval [low, high).
	// Returns ErrInvalidIndex if low > high.
	// Returns ErrIndexOutOfBounds if high > LastIndex() + 1.
	Slice(low, high LogIndex) ([]LogEntry, error)
}

// MemoryLog is an in-memory thread-safe implementation of the Log interface.
type MemoryLog struct {
	mu      sync.RWMutex
	entries []LogEntry
}

var _ Log = (*MemoryLog)(nil)

// NewMemoryLog constructs an initialized in-memory log with a sentinel entry at index 0.
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		entries: []LogEntry{
			{Term: 0, Index: 0, Command: nil},
		},
	}
}

// Append appends one or more entries to the log.
func (m *MemoryLog) Append(entries ...LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	lastIdx := LogIndex(len(m.entries) - 1)
	toAppend := make([]LogEntry, len(entries))
	for i, entry := range entries {
		expectedIdx := lastIdx + 1 + LogIndex(i)
		if entry.Index != 0 && entry.Index != expectedIdx {
			return fmt.Errorf("%w: entry index %d does not match expected %d", ErrInvalidIndex, entry.Index, expectedIdx)
		}
		entry.Index = expectedIdx
		toAppend[i] = entry
	}

	m.entries = append(m.entries, toAppend...)
	return nil
}

// TermAt returns the term of the log entry at index.
func (m *MemoryLog) TermAt(index LogIndex) (Term, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if index >= LogIndex(len(m.entries)) {
		return 0, fmt.Errorf("%w: index %d exceeds last index %d", ErrIndexOutOfBounds, index, len(m.entries)-1)
	}
	return m.entries[index].Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (m *MemoryLog) LastIndex() LogIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.entries[len(m.entries)-1].Index
}

// LastTerm returns the term of the last entry in the log.
func (m *MemoryLog) LastTerm() Term {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.entries[len(m.entries)-1].Term
}

// TruncateFrom discards entries starting from index to the end of the log.
func (m *MemoryLog) TruncateFrom(index LogIndex) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if index == 0 {
		return ErrCannotTruncateSentinel
	}
	lastIdx := LogIndex(len(m.entries) - 1)
	if index > lastIdx+1 {
		return fmt.Errorf("%w: index %d exceeds last index + 1 (%d)", ErrIndexOutOfBounds, index, lastIdx+1)
	}
	if index == lastIdx+1 {
		return nil
	}

	for i := index; i < LogIndex(len(m.entries)); i++ {
		m.entries[i] = LogEntry{}
	}
	m.entries = m.entries[:index]
	return nil
}

// Slice returns a copy of log entries in [low, high).
func (m *MemoryLog) Slice(low, high LogIndex) ([]LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if low > high {
		return nil, fmt.Errorf("%w: low (%d) > high (%d)", ErrInvalidIndex, low, high)
	}
	lastIdx := LogIndex(len(m.entries) - 1)
	if high > lastIdx+1 {
		return nil, fmt.Errorf("%w: high (%d) exceeds last index + 1 (%d)", ErrIndexOutOfBounds, high, lastIdx+1)
	}

	size := high - low
	if size == 0 {
		return []LogEntry{}, nil
	}
	res := make([]LogEntry, size)
	copy(res, m.entries[low:high])
	return res, nil
}

