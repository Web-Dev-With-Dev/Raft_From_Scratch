package raft

import (
	"sync"
	"testing"
	"time"
)

// mockClock implements Clock for deterministic testing.
type mockClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*mockTimer
	tickers []*mockTicker
}

func newMockClock(initial time.Time) *mockClock {
	if initial.IsZero() {
		initial = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	}
	return &mockClock{
		now: initial,
	}
}

func (m *mockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *mockClock) After(d time.Duration) <-chan time.Time {
	t := m.NewTimer(d)
	return t.C()
}

func (m *mockClock) NewTimer(d time.Duration) Timer {
	m.mu.Lock()
	defer m.mu.Unlock()
	mt := &mockTimer{
		clk:      m,
		fireAt:   m.now.Add(d),
		duration: d,
		ch:       make(chan time.Time, 1),
		active:   true,
	}
	m.timers = append(m.timers, mt)
	return mt
}

func (m *mockClock) NewTicker(d time.Duration) Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()
	mt := &mockTicker{
		clk:      m,
		period:   d,
		nextTick: m.now.Add(d),
		ch:       make(chan time.Time, 16),
		active:   true,
	}
	m.tickers = append(m.tickers, mt)
	return mt
}

// Advance moves mock clock forward by d and fires all eligible timers and tickers.
func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)

	for _, t := range m.timers {
		t.mu.Lock()
		if t.active && !m.now.Before(t.fireAt) {
			t.active = false
			select {
			case t.ch <- m.now:
			default:
			}
		}
		t.mu.Unlock()
	}

	for _, tick := range m.tickers {
		tick.mu.Lock()
		if tick.active {
			for !m.now.Before(tick.nextTick) {
				select {
				case tick.ch <- tick.nextTick:
				default:
				}
				tick.nextTick = tick.nextTick.Add(tick.period)
			}
		}
		tick.mu.Unlock()
	}
}

type mockTimer struct {
	mu       sync.Mutex
	clk      *mockClock
	fireAt   time.Time
	duration time.Duration
	ch       chan time.Time
	active   bool
}

func (mt *mockTimer) C() <-chan time.Time {
	return mt.ch
}

func (mt *mockTimer) Reset(d time.Duration) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	wasActive := mt.active
	mt.clk.mu.Lock()
	mt.fireAt = mt.clk.now.Add(d)
	mt.clk.mu.Unlock()
	mt.duration = d
	mt.active = true
	return wasActive
}

func (mt *mockTimer) Stop() bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	wasActive := mt.active
	mt.active = false
	return wasActive
}

type mockTicker struct {
	mu       sync.Mutex
	clk      *mockClock
	period   time.Duration
	nextTick time.Time
	ch       chan time.Time
	active   bool
}

func (mt *mockTicker) C() <-chan time.Time {
	return mt.ch
}

func (mt *mockTicker) Reset(d time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.period = d
	mt.clk.mu.Lock()
	mt.nextTick = mt.clk.now.Add(d)
	mt.clk.mu.Unlock()
	mt.active = true
}

func (mt *mockTicker) Stop() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.active = false
}

func TestRealClock_Basic(t *testing.T) {
	clk := NewRealClock()

	now := clk.Now()
	if now.IsZero() {
		t.Fatalf("expected non-zero Now()")
	}

	timer := clk.NewTimer(10 * time.Millisecond)
	select {
	case fireTime := <-timer.C():
		if fireTime.Before(now) {
			t.Fatalf("timer fired before now")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("real timer timed out waiting to fire")
	}

	timer.Reset(10 * time.Millisecond)
	if !timer.Stop() {
		// Stop returns false if already expired or stopped
	}

	ticker := clk.NewTicker(10 * time.Millisecond)
	select {
	case <-ticker.C():
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("real ticker timed out waiting for tick")
	}
	ticker.Stop()

	select {
	case <-clk.After(5 * time.Millisecond):
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("real After timed out")
	}
}

func TestElectionTimer_WithMockClock(t *testing.T) {
	startTime := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	mockClk := newMockClock(startTime)

	et := NewElectionTimerWithClock(mockClk, 150*time.Millisecond, 300*time.Millisecond)
	if et.Clock() != mockClk {
		t.Fatalf("expected et.Clock() to be mockClk")
	}
	if !et.LastReset().Equal(startTime) {
		t.Fatalf("expected last reset %v, got %v", startTime, et.LastReset())
	}

	// Advance 50ms: timer should NOT fire yet
	mockClk.Advance(50 * time.Millisecond)
	select {
	case <-et.C():
		t.Fatalf("timer fired prematurely at 50ms")
	default:
	}

	// Advance past maximum timeout (another 300ms): timer MUST fire
	mockClk.Advance(300 * time.Millisecond)
	select {
	case fireTime := <-et.C():
		if fireTime.Before(startTime.Add(150 * time.Millisecond)) {
			t.Fatalf("timer fired earlier than min timeout: %v", fireTime)
		}
	default:
		t.Fatalf("expected election timer to fire after advancing mock clock")
	}

	// Reset with fixed timeout of 200ms
	resetTime := mockClk.Now()
	et.ResetWithTimeout(200 * time.Millisecond)
	if !et.LastReset().Equal(resetTime) {
		t.Fatalf("expected last reset %v, got %v", resetTime, et.LastReset())
	}

	mockClk.Advance(100 * time.Millisecond)
	select {
	case <-et.C():
		t.Fatalf("timer fired prematurely at 100ms after reset")
	default:
	}

	mockClk.Advance(101 * time.Millisecond)
	select {
	case <-et.C():
		// Successfully fired on mock time advancement!
	default:
		t.Fatalf("timer failed to fire after 201ms on mock clock")
	}

	// Test Stop
	et.ResetWithTimeout(100 * time.Millisecond)
	if !et.Stop() {
		t.Fatalf("expected et.Stop() to return true for active timer")
	}
	mockClk.Advance(200 * time.Millisecond)
	select {
	case <-et.C():
		t.Fatalf("stopped timer fired after advance")
	default:
	}
}

func TestHeartbeatTicker_WithMockClock(t *testing.T) {
	mockClk := newMockClock(time.Now())

	ticker := mockClk.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// Initial: no tick
	select {
	case <-ticker.C():
		t.Fatalf("unexpected tick before advance")
	default:
	}

	// Advance 50ms: exactly one tick
	mockClk.Advance(50 * time.Millisecond)
	select {
	case <-ticker.C():
		// Received tick!
	default:
		t.Fatalf("expected tick after advancing 50ms")
	}

	// Advance 100ms: two ticks
	mockClk.Advance(100 * time.Millisecond)
	count := 0
	for i := 0; i < 2; i++ {
		select {
		case <-ticker.C():
			count++
		default:
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 ticks after advancing 100ms, got %d", count)
	}

	ticker.Stop()
	mockClk.Advance(200 * time.Millisecond)
	select {
	case <-ticker.C():
		t.Fatalf("stopped ticker delivered tick")
	default:
	}
}
