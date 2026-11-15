package server

import (
	"sync"
	"time"

	"raft/raft"
)

// EventType categorizes Raft consensus and server lifecycle events.
type EventType string

const (
	EventStateTransition   EventType = "state_transition"
	EventElectionStarted   EventType = "election_started"
	EventVoteGranted       EventType = "vote_granted"
	EventVoteDenied        EventType = "vote_denied"
	EventHeartbeatSent     EventType = "heartbeat_sent"
	EventHeartbeatReceived EventType = "heartbeat_received"
	EventStepDown          EventType = "step_down"
	EventQuorumLoss        EventType = "quorum_loss"
	EventServerStarted     EventType = "server_started"
	EventServerStopped     EventType = "server_stopped"
)

// Event represents an immutable timestamped event recorded in the node's ring buffer.
type Event struct {
	Index     uint64                 `json:"index"`
	Timestamp time.Time              `json:"timestamp"`
	Type      EventType              `json:"type"`
	Term      raft.Term              `json:"term"`
	Message   string                 `json:"message"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// DefaultEventLogCapacity is the default maximum capacity of the in-memory ring buffer.
const DefaultEventLogCapacity = 128

// EventLog is a thread-safe, fixed-size circular buffer of consensus events
// designed for post-mortem debugging and real-time observability.
type EventLog struct {
	mu       sync.RWMutex
	capacity int
	buffer   []Event
	head     int    // Index where next append will write
	count    int    // Number of valid elements currently in buffer
	nextIdx  uint64 // Monotonic sequence number starting at 1
}

// NewEventLog creates a new event ring buffer with the given capacity.
// If capacity <= 0, DefaultEventLogCapacity is used.
func NewEventLog(capacity int) *EventLog {
	if capacity <= 0 {
		capacity = DefaultEventLogCapacity
	}
	return &EventLog{
		capacity: capacity,
		buffer:   make([]Event, capacity),
		nextIdx:  1,
	}
}

// Append records a new event into the circular buffer, overwriting the oldest event
// if the buffer is at full capacity.
func (el *EventLog) Append(eventType EventType, term raft.Term, message string, details map[string]interface{}) Event {
	el.mu.Lock()
	defer el.mu.Unlock()

	ev := Event{
		Index:     el.nextIdx,
		Timestamp: time.Now(),
		Type:      eventType,
		Term:      term,
		Message:   message,
		Details:   details,
	}
	el.nextIdx++

	el.buffer[el.head] = ev
	el.head = (el.head + 1) % el.capacity
	if el.count < el.capacity {
		el.count++
	}

	return ev
}

// Entries returns all events currently in the buffer in chronological order (oldest to newest).
func (el *EventLog) Entries() []Event {
	el.mu.RLock()
	defer el.mu.RUnlock()

	if el.count == 0 {
		return []Event{}
	}

	res := make([]Event, el.count)
	if el.count < el.capacity {
		copy(res, el.buffer[:el.count])
		return res
	}

	// Buffer is full; the oldest element is at el.head.
	tailCount := el.capacity - el.head
	copy(res[:tailCount], el.buffer[el.head:])
	copy(res[tailCount:], el.buffer[:el.head])
	return res
}

// Recent returns at most n recent events in chronological order (oldest to newest).
func (el *EventLog) Recent(n int) []Event {
	all := el.Entries()
	if n <= 0 || len(all) == 0 {
		return []Event{}
	}
	if n >= len(all) {
		return all
	}
	return all[len(all)-n:]
}

// Len returns the current number of events stored in the buffer.
func (el *EventLog) Len() int {
	el.mu.RLock()
	defer el.mu.RUnlock()
	return el.count
}

// Cap returns the maximum capacity of the buffer.
func (el *EventLog) Cap() int {
	return el.capacity
}

// Clear resets the buffer to empty while maintaining its capacity.
func (el *EventLog) Clear() {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.buffer = make([]Event, el.capacity)
	el.head = 0
	el.count = 0
}
