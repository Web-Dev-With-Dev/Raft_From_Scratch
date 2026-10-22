package raft

import (
	"sync"
	"testing"
	"time"
)

func TestRandomElectionTimeout_Range(t *testing.T) {
	min := 150 * time.Millisecond
	max := 300 * time.Millisecond

	seenMin := max
	seenMax := min

	for i := 0; i < 1000; i++ {
		timeout := RandomElectionTimeout(min, max)
		if timeout < min || timeout > max {
			t.Fatalf("iteration %d: timeout %v outside of expected range [%v, %v]", i, timeout, min, max)
		}
		if timeout < seenMin {
			seenMin = timeout
		}
		if timeout > seenMax {
			seenMax = timeout
		}
	}

	// Verify variance across 1000 trials
	if seenMin == seenMax {
		t.Fatalf("expected randomized timeouts to vary, but all were %v", seenMin)
	}

	// Test default fallback when zero/negative bounds are provided
	defaultTimeout := RandomElectionTimeout(0, 0)
	if defaultTimeout < DefaultMinElectionTimeout || defaultTimeout > DefaultMaxElectionTimeout {
		t.Fatalf("expected default timeout in [%v, %v], got %v", DefaultMinElectionTimeout, DefaultMaxElectionTimeout, defaultTimeout)
	}

	// Test edge cases: min >= max
	equalTimeout := RandomElectionTimeout(200*time.Millisecond, 200*time.Millisecond)
	if equalTimeout != 200*time.Millisecond {
		t.Fatalf("expected equal bounds to return min (200ms), got %v", equalTimeout)
	}

	invertedTimeout := RandomElectionTimeout(300*time.Millisecond, 150*time.Millisecond)
	if invertedTimeout != 300*time.Millisecond {
		t.Fatalf("expected inverted bounds to return min (300ms), got %v", invertedTimeout)
	}
}

func TestNewElectionTimer(t *testing.T) {
	min := 150 * time.Millisecond
	max := 300 * time.Millisecond

	et := NewElectionTimer(min, max)
	defer et.Stop()

	if et == nil {
		t.Fatal("expected non-nil ElectionTimer")
	}

	if et.C() == nil {
		t.Fatal("expected non-nil timer channel")
	}

	dur := et.Duration()
	if dur < min || dur > max {
		t.Fatalf("initial duration %v outside range [%v, %v]", dur, min, max)
	}

	rMin, rMax := et.Range()
	if rMin != min || rMax != max {
		t.Fatalf("expected range [%v, %v], got [%v, %v]", min, max, rMin, rMax)
	}
}

func TestElectionTimer_Reset(t *testing.T) {
	min := 150 * time.Millisecond
	max := 300 * time.Millisecond

	et := NewElectionTimer(min, max)
	defer et.Stop()

	seenDifferent := false
	firstDur := et.Duration()

	for i := 0; i < 50; i++ {
		newDur := et.Reset()
		if newDur < min || newDur > max {
			t.Fatalf("iteration %d: reset duration %v outside range [%v, %v]", i, newDur, min, max)
		}
		if et.Duration() != newDur {
			t.Fatalf("et.Duration() %v does not match Reset() return %v", et.Duration(), newDur)
		}
		if newDur != firstDur {
			seenDifferent = true
		}
	}

	if !seenDifferent {
		t.Fatalf("expected Reset() to generate varying durations over 50 iterations")
	}
}

func TestElectionTimer_Fires(t *testing.T) {
	// Use small duration for fast test
	min := 20 * time.Millisecond
	max := 40 * time.Millisecond

	et := NewElectionTimer(min, max)
	defer et.Stop()

	start := time.Now()
	select {
	case fireTime := <-et.C():
		elapsed := time.Since(start)
		if elapsed < 15*time.Millisecond {
			t.Fatalf("timer fired too fast: %v", elapsed)
		}
		if fireTime.IsZero() {
			t.Fatal("expected non-zero fireTime")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("election timer failed to fire within 1 second")
	}
}

func TestElectionTimer_ResetBeforeExpiry(t *testing.T) {
	min := 60 * time.Millisecond
	max := 80 * time.Millisecond

	et := NewElectionTimer(min, max)
	defer et.Stop()

	// Sleep for part of the duration
	time.Sleep(30 * time.Millisecond)

	// Reset timer
	start := time.Now()
	et.Reset()

	// Ensure timer does not fire immediately from previous schedule
	select {
	case <-et.C():
		elapsed := time.Since(start)
		if elapsed < 50*time.Millisecond {
			t.Fatalf("timer fired prematurely after reset: elapsed %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timer failed to fire after reset within 1 second")
	}
}

func TestElectionTimer_ResetAfterExpiry(t *testing.T) {
	min := 10 * time.Millisecond
	max := 20 * time.Millisecond

	et := NewElectionTimer(min, max)
	defer et.Stop()

	// Wait for it to fire
	select {
	case <-et.C():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timer failed to fire")
	}

	// Reset after already expired
	et.ResetWithTimeout(30 * time.Millisecond)

	start := time.Now()
	select {
	case <-et.C():
		elapsed := time.Since(start)
		if elapsed < 20*time.Millisecond {
			t.Fatalf("fired too early on second cycle: %v", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timer failed to fire after post-expiry reset")
	}
}

func TestElectionTimer_Stop(t *testing.T) {
	min := 50 * time.Millisecond
	max := 100 * time.Millisecond

	et := NewElectionTimer(min, max)
	stopped := et.Stop()
	if !stopped {
		t.Fatal("expected Stop() to return true for active timer")
	}

	select {
	case <-et.C():
		t.Fatal("stopped election timer should not fire")
	case <-time.After(120 * time.Millisecond):
		// Success: timer did not fire
	}
}

func TestElectionTimer_SetRange(t *testing.T) {
	et := NewElectionTimer(150*time.Millisecond, 300*time.Millisecond)
	defer et.Stop()

	et.SetRange(20*time.Millisecond, 40*time.Millisecond)
	rMin, rMax := et.Range()
	if rMin != 20*time.Millisecond || rMax != 40*time.Millisecond {
		t.Fatalf("expected updated range [20ms, 40ms], got [%v, %v]", rMin, rMax)
	}

	dur := et.Reset()
	if dur < 20*time.Millisecond || dur > 40*time.Millisecond {
		t.Fatalf("expected duration in new range [20ms, 40ms], got %v", dur)
	}
}

func TestElectionTimer_ConcurrentSafety(t *testing.T) {
	et := NewElectionTimer(150*time.Millisecond, 300*time.Millisecond)
	defer et.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if idx%3 == 0 {
					et.Reset()
				} else if idx%3 == 1 {
					_ = et.Duration()
				} else {
					_, _ = et.Range()
				}
			}
		}(i)
	}

	wg.Wait()
}
