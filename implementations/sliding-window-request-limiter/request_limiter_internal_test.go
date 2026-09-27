package slidingwindowrequestlimiter

import (
	"sync"
	"testing"
	"time"

	"request-limiter/internal/limitertest"
)

func newBare(limit int, interval time.Duration) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{limit: limit, interval: interval, currentTime: time.Now()}
}

func TestAllowAdmitsUpToTheLimit(t *testing.T) {
	l := newBare(3, 200*time.Millisecond)

	allowed := 0
	for i := 0; i < 10; i++ {
		if l.Allow() {
			allowed++
		}
	}

	limitertest.EqualInt(t, allowed, 3, "allowed against a limit of 3")
	limitertest.EqualInt(t, l.currCount, 3, "current window count")
}

func TestAllowDeniesWhileTheCurrentWindowIsFull(t *testing.T) {
	l := newBare(2, 200*time.Millisecond)

	for i := 0; i < 2; i++ {
		if !l.Allow() {
			t.Fatalf("request %d of 2 was denied", i)
		}
	}

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, 60*time.Millisecond)
	if l.Allow() {
		t.Error("request was allowed while both slots were still inside the window")
	}
}

// Once the window rolls over, the previous count decays linearly with elapsed
// time: it is still fully in effect right after the roll, and has halved half
// way through the new window.
func TestAllowDecaysThePreviousWindowLinearly(t *testing.T) {
	const (
		limit    = 2
		interval = 200 * time.Millisecond
	)

	l := newBare(limit, interval)
	if !l.Allow() || !l.Allow() {
		t.Fatal("the first two requests should have been allowed")
	}

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, interval+30*time.Millisecond)
	if l.Allow() {
		t.Error("request was allowed immediately after the roll, want the previous count to still be full")
	}
	if l.prevCount != limit {
		t.Errorf("prevCount = %d after the roll, want %d", l.prevCount, limit)
	}
	if l.currCount != 0 {
		t.Errorf("currCount = %d after the roll, want 0", l.currCount)
	}

	clock.WaitUntil(t, interval*2)
	if !l.Allow() {
		t.Error("request was denied after the previous count decayed, want it to be allowed")
	}
}

// A single rollover used to carry the current count forward unconditionally, so
// a limiter that had been idle for several intervals still charged for traffic
// that was long gone. Now an idle gap of more than one window drops the stale
// count instead of moving it to prevCount.
func TestAllowDropsStaleCountsAfterALongIdlePeriod(t *testing.T) {
	const (
		limit    = 2
		interval = 60 * time.Millisecond
	)

	l := newBare(limit, interval)
	if !l.Allow() || !l.Allow() {
		t.Fatal("the first two requests should have been allowed")
	}

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, interval*5)
	if !l.Allow() {
		t.Fatal("request was denied right after a long idle period")
	}

	if l.prevCount != 0 {
		t.Errorf("prevCount = %d after %d idle intervals, want 0", l.prevCount, 5)
	}
	if l.currCount != 1 {
		t.Errorf("currCount = %d, want 1", l.currCount)
	}
}

func TestAllowWithZeroLimitDeniesEverything(t *testing.T) {
	l := newBare(0, 200*time.Millisecond)

	for i := 0; i < 5; i++ {
		if l.Allow() {
			t.Fatalf("request %d was allowed with a zero limit", i)
		}
	}
}

func TestConcurrentBurstAdmitsExactlyTheLimit(t *testing.T) {
	l := newBare(4, 200*time.Millisecond)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, 4, "allowed under 64 concurrent requests")
}

// The accounting fields are only reachable under the mutex, so this checks the
// invariant they must keep while windows roll over under concurrent traffic.
func TestAllowKeepsWindowCountsConsistentUnderLoad(t *testing.T) {
	const (
		limit    = 4
		interval = 20 * time.Millisecond
	)

	l := newBare(limit, interval)

	var wg sync.WaitGroup
	worst := 0

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				_ = l.Allow()

				l.mutex.Lock()
				if total := l.prevCount + l.currCount; total > worst {
					worst = total
				}
				l.mutex.Unlock()
			}
		}()
	}
	wg.Wait()

	limitertest.BetweenInt(t, worst, 0, limit*2, "largest observed combined window count")
}
