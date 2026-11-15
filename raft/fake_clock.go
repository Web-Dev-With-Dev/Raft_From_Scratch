package raft

import (
	"sync"
	"time"
)

// FakeClock implements Clock with deterministic, manually controlled time advancement.
// It fires timers and tickers in strict chronological order and supports timers that
// schedule or reset timers while firing without deadlocks.
type FakeClock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	seq     uint64
	timers  map[uint64]*fakeTimer
	tickers map[uint64]*fakeTicker
}

// NewFakeClock returns a FakeClock initialized to UTC epoch (2026-01-01 00:00:00 UTC).
func NewFakeClock() *FakeClock {
	return NewFakeClockAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

// NewFakeClockAt returns a FakeClock initialized to the specified time.
func NewFakeClockAt(t time.Time) *FakeClock {
	fc := &FakeClock{
		now:     t,
		timers:  make(map[uint64]*fakeTimer),
		tickers: make(map[uint64]*fakeTicker),
	}
	fc.cond = sync.NewCond(&fc.mu)
	return fc
}

// Now returns the current simulated clock time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set explicitly sets the simulated time. If the target is after Now(),
// Advance is called to fire intervening events. Otherwise, time is set directly.
func (c *FakeClock) Set(t time.Time) time.Time {
	c.mu.Lock()
	cur := c.now
	c.mu.Unlock()

	if t.After(cur) {
		return c.Advance(t.Sub(cur))
	}
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
	return t
}

// NewTimer creates a new Timer that fires on its channel after duration d.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.seq++
	ft := &fakeTimer{
		id:       c.seq,
		clk:      c,
		fireAt:   c.now.Add(d),
		duration: d,
		ch:       make(chan time.Time, 1),
		active:   true,
	}
	c.timers[ft.id] = ft
	c.cond.Broadcast()
	return ft
}

// AfterFunc creates a new Timer that calls f in response to duration d elapsing.
func (c *FakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.seq++
	ft := &fakeTimer{
		id:       c.seq,
		clk:      c,
		fireAt:   c.now.Add(d),
		duration: d,
		fn:       f,
		active:   true,
	}
	c.timers[ft.id] = ft
	c.cond.Broadcast()
	return ft
}

// After returns a channel that receives the simulated time after duration d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	return c.NewTimer(d).C()
}

// NewTicker returns a new Ticker delivering simulated ticks every period d.
func (c *FakeClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.seq++
	ftk := &fakeTicker{
		id:     c.seq,
		clk:    c,
		fireAt: c.now.Add(d),
		period: d,
		ch:     make(chan time.Time, 64),
		active: true,
	}
	c.tickers[ftk.id] = ftk
	c.cond.Broadcast()
	return ftk
}

// Waiters returns the number of active timers and tickers currently registered.
func (c *FakeClock) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers) + len(c.tickers)
}

// BlockUntil blocks until at least n timers/tickers are registered and active.
func (c *FakeClock) BlockUntil(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.timers)+len(c.tickers) < n {
		c.cond.Wait()
	}
}

// Advance advances the clock by duration d, firing all timers and tickers in chronological order.
// Any timers scheduled or reset by callbacks while firing are interleaved properly.
func (c *FakeClock) Advance(d time.Duration) time.Time {
	if d < 0 {
		return c.Now()
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.now.Add(d)
	for {
		fireAt, timer, ticker := c.findEarliestLocked(target)
		if timer == nil && ticker == nil {
			break
		}
		c.now = fireAt
		if timer != nil {
			c.fireTimerLocked(timer, fireAt)
		} else if ticker != nil {
			c.fireTickerLocked(ticker, fireAt)
		}
	}
	c.now = target
	return c.now
}

// findEarliestLocked searches for the active timer or ticker with the lowest fireAt <= target.
func (c *FakeClock) findEarliestLocked(target time.Time) (time.Time, *fakeTimer, *fakeTicker) {
	var (
		earliestTime time.Time
		earliestID   uint64
		earliestT    *fakeTimer
		earliestTk   *fakeTicker
		found        bool
	)

	for _, t := range c.timers {
		if !t.fireAt.After(target) {
			if !found || t.fireAt.Before(earliestTime) || (t.fireAt.Equal(earliestTime) && t.id < earliestID) {
				earliestTime, earliestID, earliestT, earliestTk, found = t.fireAt, t.id, t, nil, true
			}
		}
	}

	for _, tk := range c.tickers {
		if !tk.fireAt.After(target) {
			if !found || tk.fireAt.Before(earliestTime) || (tk.fireAt.Equal(earliestTime) && tk.id < earliestID) {
				earliestTime, earliestID, earliestT, earliestTk, found = tk.fireAt, tk.id, nil, tk, true
			}
		}
	}

	return earliestTime, earliestT, earliestTk
}

// fireTimerLocked removes the timer and dispatches its callback/channel outside of the lock.
func (c *FakeClock) fireTimerLocked(t *fakeTimer, fireAt time.Time) {
	delete(c.timers, t.id)
	t.active = false
	fn := t.fn
	ch := t.ch

	c.mu.Unlock()
	if fn != nil {
		fn()
	}
	if ch != nil {
		select {
		case ch <- fireAt:
		default:
		}
	}
	c.mu.Lock()
}

// fireTickerLocked updates the ticker period and delivers a tick outside of the lock.
func (c *FakeClock) fireTickerLocked(tk *fakeTicker, fireAt time.Time) {
	tk.fireAt = tk.fireAt.Add(tk.period)
	ch := tk.ch

	c.mu.Unlock()
	if ch != nil {
		select {
		case ch <- fireAt:
		default:
		}
	}
	c.mu.Lock()
}

type fakeTimer struct {
	id       uint64
	clk      *FakeClock
	fireAt   time.Time
	duration time.Duration
	ch       chan time.Time
	fn       func()
	active   bool
}

func (ft *fakeTimer) C() <-chan time.Time {
	return ft.ch
}

func (ft *fakeTimer) Stop() bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()

	if !ft.active {
		return false
	}
	ft.active = false
	delete(ft.clk.timers, ft.id)
	return true
}

func (ft *fakeTimer) Reset(d time.Duration) bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()

	wasActive := ft.active
	ft.active = true
	ft.duration = d
	ft.fireAt = ft.clk.now.Add(d)

	// Remove old key and register under new sequential ID
	delete(ft.clk.timers, ft.id)
	ft.clk.seq++
	ft.id = ft.clk.seq
	ft.clk.timers[ft.id] = ft

	drainChan(ft.ch)
	ft.clk.cond.Broadcast()
	return wasActive
}

type fakeTicker struct {
	id     uint64
	clk    *FakeClock
	fireAt time.Time
	period time.Duration
	ch     chan time.Time
	active bool
}

func (ftk *fakeTicker) C() <-chan time.Time {
	return ftk.ch
}

func (ftk *fakeTicker) Stop() {
	ftk.clk.mu.Lock()
	defer ftk.clk.mu.Unlock()

	ftk.active = false
	delete(ftk.clk.tickers, ftk.id)
}

func (ftk *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("non-positive interval for Ticker.Reset")
	}
	ftk.clk.mu.Lock()
	defer ftk.clk.mu.Unlock()

	ftk.period = d
	ftk.fireAt = ftk.clk.now.Add(d)
	ftk.active = true

	delete(ftk.clk.tickers, ftk.id)
	ftk.clk.seq++
	ftk.id = ftk.clk.seq
	ftk.clk.tickers[ftk.id] = ftk

	drainTickerChan(ftk.ch)
	ftk.clk.cond.Broadcast()
}

func drainChan(ch chan time.Time) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
	}
}

func drainTickerChan(ch chan time.Time) {
	if ch == nil {
		return
	}
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

