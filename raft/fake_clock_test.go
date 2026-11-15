package raft

import (
	"sync"
	"testing"
	"time"
)

// TestFakeClock_Ordering verifies timers fire in chronological order regardless of registration order.
func TestFakeClock_Ordering(t *testing.T) {
	clk := NewFakeClock()
	var order []string
	var mu sync.Mutex

	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, name)
	}

	clk.AfterFunc(50*time.Millisecond, func() { record("50ms") })
	clk.AfterFunc(10*time.Millisecond, func() { record("10ms") })
	clk.AfterFunc(30*time.Millisecond, func() { record("30ms") })
	clk.AfterFunc(20*time.Millisecond, func() { record("20ms") })
	clk.AfterFunc(20*time.Millisecond, func() { record("20ms-dup") })

	clk.Advance(60 * time.Millisecond)

	expected := []string{"10ms", "20ms", "20ms-dup", "30ms", "50ms"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d events, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("at index %d: expected %s, got %s", i, v, order[i])
		}
	}
}

// TestFakeClock_ChannelTimerOrdering verifies channel-based timers fire at the correct time.
func TestFakeClock_ChannelTimerOrdering(t *testing.T) {
	clk := NewFakeClock()
	t1 := clk.NewTimer(10 * time.Millisecond)
	t2 := clk.NewTimer(25 * time.Millisecond)

	clk.Advance(15 * time.Millisecond)
	select {
	case <-t1.C():
	default:
		t.Fatalf("expected t1 to have fired at 15ms")
	}

	select {
	case <-t2.C():
		t.Fatalf("t2 fired prematurely at 15ms")
	default:
	}

	clk.Advance(15 * time.Millisecond)
	select {
	case <-t2.C():
	default:
		t.Fatalf("expected t2 to have fired after advancing to 30ms")
	}
}

// TestFakeClock_ScheduleWhileFiring verifies nested timers scheduled within callbacks fire in order.
func TestFakeClock_ScheduleWhileFiring(t *testing.T) {
	clk := NewFakeClock()
	var events []string
	var mu sync.Mutex

	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, name)
	}

	// T1 at 10ms schedules T1_nested at +5ms (15ms).
	clk.AfterFunc(10*time.Millisecond, func() {
		record("T1-10ms")
		clk.AfterFunc(5*time.Millisecond, func() {
			record("T1_nested-15ms")
			clk.AfterFunc(2*time.Millisecond, func() {
				record("T1_deep-17ms")
			})
		})
	})

	// T2 pre-scheduled at 20ms.
	clk.AfterFunc(20*time.Millisecond, func() {
		record("T2-20ms")
	})

	clk.Advance(25 * time.Millisecond)

	expected := []string{"T1-10ms", "T1_nested-15ms", "T1_deep-17ms", "T2-20ms"}
	if len(events) != len(expected) {
		t.Fatalf("expected %d events, got %d: %v", len(expected), len(events), events)
	}
	for i, v := range expected {
		if events[i] != v {
			t.Errorf("at index %d: expected %s, got %s", i, v, events[i])
		}
	}
}

// TestFakeClock_GoroutineSchedulesNewTimer verifies concurrent goroutines can schedule new timers upon receiving a tick.
func TestFakeClock_GoroutineSchedulesNewTimer(t *testing.T) {
	clk := NewFakeClock()
	t1 := clk.NewTimer(10 * time.Millisecond)
	t2 := clk.NewTimer(25 * time.Millisecond)
	var t3 Timer
	t3Fired := make(chan time.Time, 1)

	go func() {
		<-t1.C()
		t3 = clk.NewTimer(5 * time.Millisecond)
		tVal := <-t3.C()
		t3Fired <- tVal
	}()

	// Advance to 10ms to trigger t1
	clk.Advance(10 * time.Millisecond)

	// Wait until t3 is registered (total active waiters: t2 and t3 = 2)
	clk.BlockUntil(2)

	// Advance 7ms (reaches 17ms, which is past t3's 15ms fireAt, but before t2's 25ms)
	clk.Advance(7 * time.Millisecond)

	select {
	case fireTime := <-t3Fired:
		expected := clk.Now().Add(-2 * time.Millisecond) // t3 fired at 15ms, now is 17ms
		if !fireTime.Equal(expected) {
			t.Fatalf("expected t3 to fire at %v, got %v", expected, fireTime)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for t3 to fire")
	}

	select {
	case <-t2.C():
		t.Fatalf("t2 fired prematurely at 17ms")
	default:
	}
}

// TestFakeClock_CallbackResetsSelf verifies that a timer can safely reset itself during its callback.
func TestFakeClock_CallbackResetsSelf(t *testing.T) {
	clk := NewFakeClock()
	count := 0
	var tm Timer

	tm = clk.AfterFunc(10*time.Millisecond, func() {
		count++
		if count < 3 {
			tm.Reset(10 * time.Millisecond)
		}
	})

	clk.Advance(35 * time.Millisecond)
	if count != 3 {
		t.Fatalf("expected callback to fire 3 times via self-reset, got %d", count)
	}
}

// TestFakeClock_CallbackStopsOther verifies a timer can cancel a pending timer while firing.
func TestFakeClock_CallbackStopsOther(t *testing.T) {
	clk := NewFakeClock()
	var t2 Timer
	t2Fired := false

	clk.AfterFunc(10*time.Millisecond, func() {
		t2.Stop()
	})
	t2 = clk.AfterFunc(20*time.Millisecond, func() {
		t2Fired = true
	})

	clk.Advance(30 * time.Millisecond)
	if t2Fired {
		t.Fatalf("t2 should have been stopped before firing")
	}
}

// TestFakeClock_Stop verifies Timer.Stop behavior for pending and expired timers.
func TestFakeClock_Stop(t *testing.T) {
	clk := NewFakeClock()
	tm := clk.NewTimer(20 * time.Millisecond)

	if !tm.Stop() {
		t.Fatalf("expected Stop to return true for active timer")
	}
	if tm.Stop() {
		t.Fatalf("expected second Stop to return false")
	}

	clk.Advance(30 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatalf("stopped timer fired on channel")
	default:
	}

	// Stop after expiry should return false
	tm2 := clk.NewTimer(10 * time.Millisecond)
	clk.Advance(15 * time.Millisecond)
	if tm2.Stop() {
		t.Fatalf("expected Stop to return false on already expired timer")
	}
}

// TestFakeClock_Reset verifies Timer.Reset behavior before and after expiration.
func TestFakeClock_Reset(t *testing.T) {
	clk := NewFakeClock()
	tm := clk.NewTimer(20 * time.Millisecond)

	clk.Advance(10 * time.Millisecond)
	// Active reset returns true
	if !tm.Reset(30 * time.Millisecond) {
		t.Fatalf("expected Reset to return true for active timer")
	}

	// Advance 20ms (total 30ms from start, 20ms into 30ms reset): should not fire yet
	clk.Advance(20 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatalf("timer fired prematurely after reset")
	default:
	}

	// Advance another 15ms (total 45ms, past 40ms fireAt): should fire
	clk.Advance(15 * time.Millisecond)
	select {
	case <-tm.C():
	default:
		t.Fatalf("expected timer to fire after reset duration elapsed")
	}

	// Reset after expiry returns false
	if tm.Reset(10 * time.Millisecond) {
		t.Fatalf("expected Reset to return false after timer expired")
	}
	clk.Advance(15 * time.Millisecond)
	select {
	case <-tm.C():
	default:
		t.Fatalf("expected timer to fire again after post-expiry reset")
	}
}

// TestFakeClock_TickerBehavior verifies ticks arrive at regular intervals and can be stopped.
func TestFakeClock_TickerBehavior(t *testing.T) {
	clk := NewFakeClock()
	ticker := clk.NewTicker(10 * time.Millisecond)

	// Advance 35ms: should deliver 3 ticks (10ms, 20ms, 30ms)
	clk.Advance(35 * time.Millisecond)
	ticks := 0
	for i := 0; i < 3; i++ {
		select {
		case <-ticker.C():
			ticks++
		default:
			t.Fatalf("expected tick %d, channel empty", i+1)
		}
	}
	if ticks != 3 {
		t.Fatalf("expected 3 ticks, got %d", ticks)
	}

	// Verify no 4th tick yet
	select {
	case <-ticker.C():
		t.Fatalf("unexpected 4th tick at 35ms")
	default:
	}

	// Stop ticker
	ticker.Stop()
	clk.Advance(50 * time.Millisecond)
	select {
	case <-ticker.C():
		t.Fatalf("stopped ticker delivered tick")
	default:
	}
}

// TestFakeClock_TickerReset verifies Ticker.Reset modifies tick rate.
func TestFakeClock_TickerReset(t *testing.T) {
	clk := NewFakeClock()
	ticker := clk.NewTicker(10 * time.Millisecond)

	clk.Advance(15 * time.Millisecond)
	<-ticker.C()

	// Reset to 25ms period
	ticker.Reset(25 * time.Millisecond)

	// Advance 20ms: no tick yet (needs 25ms)
	clk.Advance(20 * time.Millisecond)
	select {
	case <-ticker.C():
		t.Fatalf("ticker fired before reset period elapsed")
	default:
	}

	// Advance another 10ms (30ms total since reset): tick fires
	clk.Advance(10 * time.Millisecond)
	select {
	case <-ticker.C():
	default:
		t.Fatalf("expected tick after reset period elapsed")
	}
}

// TestFakeClock_InterleavedTimersAndTickers asserts correct event timestamps between tickers and timers.
func TestFakeClock_InterleavedTimersAndTickers(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClockAt(start)

	ticker := clk.NewTicker(10 * time.Millisecond)
	timer15 := clk.NewTimer(15 * time.Millisecond)
	timer25 := clk.NewTimer(25 * time.Millisecond)

	clk.Advance(32 * time.Millisecond)

	// Collect ticker ticks
	var tickTimes []time.Time
	for i := 0; i < 3; i++ {
		select {
		case tick := <-ticker.C():
			tickTimes = append(tickTimes, tick)
		default:
			t.Fatalf("expected tick %d", i+1)
		}
	}

	// Verify exact ticker fire timestamps
	expectedTicks := []time.Time{
		start.Add(10 * time.Millisecond),
		start.Add(20 * time.Millisecond),
		start.Add(30 * time.Millisecond),
	}
	for i, exp := range expectedTicks {
		if !tickTimes[i].Equal(exp) {
			t.Errorf("tick %d: expected %v, got %v", i+1, exp, tickTimes[i])
		}
	}

	// Verify timers fired at their respective timestamps
	select {
	case t1 := <-timer15.C():
		if !t1.Equal(start.Add(15 * time.Millisecond)) {
			t.Fatalf("timer15 fired with wrong time: %v", t1)
		}
	default:
		t.Fatalf("timer15 did not fire")
	}

	select {
	case t2 := <-timer25.C():
		if !t2.Equal(start.Add(25 * time.Millisecond)) {
			t.Fatalf("timer25 fired with wrong time: %v", t2)
		}
	default:
		t.Fatalf("timer25 did not fire")
	}
}

// TestFakeClock_BlockUntil verifies BlockUntil unblocks when target number of waiters register.
func TestFakeClock_BlockUntil(t *testing.T) {
	clk := NewFakeClock()
	done := make(chan struct{})

	go func() {
		clk.BlockUntil(2)
		close(done)
	}()

	clk.NewTimer(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatalf("BlockUntil unblocked prematurely with only 1 waiter")
	case <-time.After(20 * time.Millisecond):
	}

	clk.NewTicker(20 * time.Millisecond)
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("BlockUntil timed out waiting for 2 waiters")
	}

	if clk.Waiters() != 2 {
		t.Fatalf("expected 2 waiters, got %d", clk.Waiters())
	}
}

// TestFakeClock_Set verifies Set time mechanics.
func TestFakeClock_Set(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClockAt(start)
	fired := false

	clk.AfterFunc(10*time.Millisecond, func() {
		fired = true
	})

	target := start.Add(20 * time.Millisecond)
	clk.Set(target)

	if !fired {
		t.Fatalf("expected timer to fire when Set advances past timeout")
	}
	if !clk.Now().Equal(target) {
		t.Fatalf("expected clock time %v, got %v", target, clk.Now())
	}
}
