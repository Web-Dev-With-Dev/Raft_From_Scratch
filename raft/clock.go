package raft

import "time"

// Clock abstracts time-related operations (Now, NewTimer, NewTicker, After)
// to decouple consensus logic from the physical system clock.
// This enables deterministic, instant time-advancement testing and simulation.
type Clock interface {
	// Now returns the current clock time.
	Now() time.Time

	// NewTimer creates a new Timer that will send the current time on its channel
	// after at least duration d.
	NewTimer(d time.Duration) Timer

	// NewTicker returns a new Ticker containing a channel that will send the
	// current time with a period specified by duration d.
	NewTicker(d time.Duration) Ticker

	// After waits for the duration to elapse and then sends the current time
	// on the returned channel.
	After(d time.Duration) <-chan time.Time
}

// Timer represents a single event. When the Timer expires, the current time
// will be sent on C().
type Timer interface {
	// C returns the channel on which the timer fires.
	C() <-chan time.Time

	// Reset changes the timer to expire after duration d.
	// It returns true if the timer had been active, false if the timer had
	// expired or been stopped.
	Reset(d time.Duration) bool

	// Stop prevents the Timer from firing.
	// It returns true if the call stops the timer, false if the timer has already
	// expired or been stopped.
	Stop() bool
}

// Ticker delivers "ticks" of a clock at regular intervals.
type Ticker interface {
	// C returns the channel on which ticks are delivered.
	C() <-chan time.Time

	// Reset stops a ticker and resets its period to the specified duration.
	Reset(d time.Duration)

	// Stop turns off a ticker. After Stop, no more ticks will be sent.
	Stop()
}

// RealClock implements Clock using Go's standard library time package.
type RealClock struct{}

// NewRealClock returns a new RealClock backed by the standard library time package.
func NewRealClock() *RealClock {
	return &RealClock{}
}

// Now returns the current local time via time.Now().
func (c *RealClock) Now() time.Time {
	return time.Now()
}

// After calls time.After(d).
func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// NewTimer creates a real Timer wrapping time.NewTimer(d).
func (c *RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

// NewTicker creates a real Ticker wrapping time.NewTicker(d).
func (c *RealClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

type realTimer struct {
	t *time.Timer
}

func (rt *realTimer) C() <-chan time.Time {
	return rt.t.C
}

func (rt *realTimer) Reset(d time.Duration) bool {
	return rt.t.Reset(d)
}

func (rt *realTimer) Stop() bool {
	return rt.t.Stop()
}

type realTicker struct {
	t *time.Ticker
}

func (rt *realTicker) C() <-chan time.Time {
	return rt.t.C
}

func (rt *realTicker) Reset(d time.Duration) {
	rt.t.Reset(d)
}

func (rt *realTicker) Stop() {
	rt.t.Stop()
}

