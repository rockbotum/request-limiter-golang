package fixedwindowrequestlimiter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"request-limiter/internal/limitertest"
)

// The counter is only reachable through the public API, but its growth is
// observable indirectly. Allow now checks the limit before incrementing, so a
// saturated window parks the counter exactly on the limit instead of overshooting
// it and freezing there.
func TestAllowStopsCounterAtTheLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := NewFixedWindowLimiter(ctx, 4, time.Hour)
	defer cancel()

	allowed, denied := 0, 0
	for i := 0; i < 1000; i++ {
		if l.Allow() {
			allowed++
		} else {
			denied++
		}
	}

	limitertest.EqualInt(t, allowed, 4, "allowed in a saturated window")
	limitertest.EqualInt(t, denied, 996, "denied in a saturated window")

	if got := atomic.LoadInt32(&l.count); got != 4 {
		t.Errorf("count = %d after 1000 requests, want 4: the counter must not overshoot the limit", got)
	}
}

func TestAllowKeepsCountingUpToTheLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := NewFixedWindowLimiter(ctx, 3, time.Hour)
	defer cancel()

	for i := 1; i <= 3; i++ {
		if !l.Allow() {
			t.Fatalf("request %d of 3 was denied", i)
		}
	}
	if l.Allow() {
		t.Error("request 4 of 3 was allowed")
	}
}

func TestTickerResetsCounterToZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := NewFixedWindowLimiter(ctx, 2, 50*time.Millisecond)
	defer cancel()

	for i := 0; i < 2; i++ {
		if !l.Allow() {
			t.Fatalf("request %d of 2 was denied", i)
		}
	}
	if l.Allow() {
		t.Fatal("request 3 of 2 was allowed")
	}

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, 100*time.Millisecond)

	if got := atomic.LoadInt32(&l.count); got != 0 {
		t.Errorf("count = %d after the ticker fired, want 0", got)
	}
	if !l.Allow() {
		t.Error("request was denied after the window rolled over")
	}
}
