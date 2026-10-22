package raft

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Default election timeout range recommended by the Raft paper (Section 5.2): 150-300ms.
const (
	DefaultMinElectionTimeout = 150 * time.Millisecond
	DefaultMaxElectionTimeout = 300 * time.Millisecond
)

// RandomElectionTimeout generates a pseudo-random duration uniformly distributed
// in the range [min, max].
// If min <= 0 or max <= 0, the default range [150ms, 300ms] is used.
// If min >= max, min is returned.
func RandomElectionTimeout(min, max time.Duration) time.Duration {
	if min <= 0 || max <= 0 {
		min = DefaultMinElectionTimeout
		max = DefaultMaxElectionTimeout
	}
	if min >= max {
		return min
	}
	delta := max - min
	return min + time.Duration(rand.Int64N(int64(delta)+1))
}

// ElectionTimer manages an election timeout backed by time.Timer.
// It provides safe resetting with randomized durations in [min, max]
// to prevent split votes during leader election.
type ElectionTimer struct {
	mu         sync.Mutex
	timer      *time.Timer
	minTimeout time.Duration
	maxTimeout time.Duration
	duration   time.Duration
	stopped    bool
}

// NewElectionTimer creates a new ElectionTimer initialized with a randomized
// timeout within [min, max] using time.NewTimer.
// If min <= 0 or max <= 0, the default range [150ms, 300ms] is applied.
func NewElectionTimer(min, max time.Duration) *ElectionTimer {
	if min <= 0 || max <= 0 {
		min = DefaultMinElectionTimeout
		max = DefaultMaxElectionTimeout
	}
	if min > max {
		min, max = max, min
	}

	d := RandomElectionTimeout(min, max)
	return &ElectionTimer{
		timer:      time.NewTimer(d),
		minTimeout: min,
		maxTimeout: max,
		duration:   d,
	}
}

// C returns the underlying time.Timer channel that receives the current time
// when the election timeout expires.
func (et *ElectionTimer) C() <-chan time.Time {
	et.mu.Lock()
	defer et.mu.Unlock()
	if et.timer == nil {
		return nil
	}
	return et.timer.C
}

// Reset resets the election timer with a newly selected randomized duration
// within [minTimeout, maxTimeout].
// It safely stops and drains the timer if it had already expired.
// Returns the newly selected timeout duration.
func (et *ElectionTimer) Reset() time.Duration {
	et.mu.Lock()
	defer et.mu.Unlock()

	if et.timer == nil {
		d := RandomElectionTimeout(et.minTimeout, et.maxTimeout)
		et.duration = d
		et.timer = time.NewTimer(d)
		et.stopped = false
		return d
	}

	// Safely stop and drain the timer channel if already fired
	if !et.timer.Stop() {
		select {
		case <-et.timer.C:
		default:
		}
	}

	d := RandomElectionTimeout(et.minTimeout, et.maxTimeout)
	et.duration = d
	et.timer.Reset(d)
	et.stopped = false
	return d
}

// ResetWithTimeout resets the election timer to a specific duration,
// instead of generating a random one. Useful for testing.
func (et *ElectionTimer) ResetWithTimeout(d time.Duration) time.Duration {
	et.mu.Lock()
	defer et.mu.Unlock()

	if et.timer == nil {
		et.duration = d
		et.timer = time.NewTimer(d)
		et.stopped = false
		return d
	}

	if !et.timer.Stop() {
		select {
		case <-et.timer.C:
		default:
		}
	}

	et.duration = d
	et.timer.Reset(d)
	et.stopped = false
	return d
}

// Stop stops the election timer, preventing it from firing.
// Returns true if the timer was active and stopped before expiring.
func (et *ElectionTimer) Stop() bool {
	et.mu.Lock()
	defer et.mu.Unlock()

	if et.timer == nil {
		return false
	}
	stopped := et.timer.Stop()
	et.stopped = true
	return stopped
}

// Duration returns the duration of the current timeout interval.
func (et *ElectionTimer) Duration() time.Duration {
	et.mu.Lock()
	defer et.mu.Unlock()
	return et.duration
}

// Range returns the configured [min, max] timeout range.
func (et *ElectionTimer) Range() (time.Duration, time.Duration) {
	et.mu.Lock()
	defer et.mu.Unlock()
	return et.minTimeout, et.maxTimeout
}

// SetRange updates the election timeout range. Subsequent calls to Reset
// will draw durations from this new interval.
func (et *ElectionTimer) SetRange(min, max time.Duration) {
	et.mu.Lock()
	defer et.mu.Unlock()
	if min <= 0 || max <= 0 {
		min = DefaultMinElectionTimeout
		max = DefaultMaxElectionTimeout
	}
	if min > max {
		min, max = max, min
	}
	et.minTimeout = min
	et.maxTimeout = max
}
