package limitertest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingLimiter admits exactly quota requests per window and resets on a
// ticker, mirroring the shape of a fixed window limiter.
type countingLimiter struct {
	quota  int
	count  atomic.Int64
	reset  chan struct{}
	mu     sync.Mutex
	closed bool
}

func newCountingLimiter(ctx context.Context, quota int, window time.Duration) *countingLimiter {
	l := &countingLimiter{quota: quota, reset: make(chan struct{})}
	go func() {
		t := time.NewTicker(window)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				l.mu.Lock()
				l.closed = true
				l.mu.Unlock()
				return
			case <-t.C:
				l.count.Store(0)
			case <-l.reset:
				return
			}
		}
	}()
	return l
}

func (l *countingLimiter) Allow() bool {
	for {
		n := l.count.Load()
		if int(n) >= l.quota {
			return false
		}
		if l.count.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (l *countingLimiter) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func TestReplayHonoursEventOffsets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newCountingLimiter(ctx, 2, time.Hour)

	clock := NewVirtualClock(0.5)
	res := Replay(t, clock, l, []Event{
		Burst(0, 5),
		Burst(40*time.Millisecond, 5),
	})

	EqualInt(t, res.At(0).Allowed, 2, "first burst allowed")
	EqualInt(t, res.At(0).Denied, 3, "first burst denied")
	EqualInt(t, res.At(40*time.Millisecond).Allowed, 0, "second burst allowed")
	EqualInt(t, res.Allowed, 2, "total allowed")
	EqualInt(t, res.Denied, 8, "total denied")

	if gap := res.Events[1].At - res.Events[0].At; gap != 40*time.Millisecond {
		t.Errorf("events were not replayed in offset order: %s", gap)
	}
}

func TestReplayReordersUnsortedEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newCountingLimiter(ctx, 10, time.Hour)

	res := Replay(t, NewVirtualClock(0.5), l, []Event{
		Burst(60*time.Millisecond, 1),
		Burst(0, 1),
		Burst(30*time.Millisecond, 1),
	})

	for i := 1; i < len(res.Events); i++ {
		if res.Events[i].At < res.Events[i-1].At {
			t.Fatalf("events out of order: %v", res.Events)
		}
	}
	EqualInt(t, res.Allowed, 3, "total allowed")
}

func TestResultWindowAggregates(t *testing.T) {
	res := Result{Events: []EventResult{
		{Event: Event{At: 0, Burst: 4}, Allowed: 2, Denied: 2},
		{Event: Event{At: 50 * time.Millisecond, Burst: 4}, Allowed: 1, Denied: 3},
		{Event: Event{At: 100 * time.Millisecond, Burst: 4}, Allowed: 3, Denied: 1},
	}}

	w := res.Window(0, 50*time.Millisecond)
	EqualInt(t, w.Allowed, 1, "window (0,50ms] allowed")
	EqualInt(t, w.Denied, 3, "window (0,50ms] denied")
	EqualInt(t, len(w.Events), 1, "window (0,50ms] events")

	all := res.Window(0, 100*time.Millisecond)
	EqualInt(t, all.Allowed, 4, "window (0,100ms] allowed")
	EqualInt(t, all.Denied, 4, "window (0,100ms] denied")
	EqualInt(t, len(all.Events), 2, "window (0,100ms] events")

	if got := res.At(100 * time.Millisecond).Allowed; got != 3 {
		t.Errorf("At(100ms).Allowed = %d, want 3", got)
	}
}

func TestSteadyGeneratesEvenlySpacedEvents(t *testing.T) {
	events := Steady(10*time.Millisecond, 20*time.Millisecond, 3, 4)
	if len(events) != 4 {
		t.Fatalf("Steady produced %d events, want 4", len(events))
	}
	for i, e := range events {
		wantAt := 10*time.Millisecond + time.Duration(i)*20*time.Millisecond
		if e.At != wantAt {
			t.Errorf("event %d at %s, want %s", i, e.At, wantAt)
		}
		if e.Burst != 3 {
			t.Errorf("event %d burst = %d, want 3", i, e.Burst)
		}
	}
}

func TestVirtualClockScaling(t *testing.T) {
	clock := NewVirtualClock(0.5)

	if got := clock.Virtual(100 * time.Millisecond); got != 200*time.Millisecond {
		t.Errorf("Virtual(100ms) = %s, want 200ms", got)
	}
	if got := clock.Instant(200 * time.Millisecond).Sub(clock.Start()); got > 101*time.Millisecond {
		t.Errorf("Instant(200ms) mapped to %s, want ~100ms", got)
	}

	clock.WaitUntil(t, 200*time.Millisecond)
	if elapsed := time.Since(clock.Start()); elapsed < 95*time.Millisecond {
		t.Errorf("WaitUntil(200ms) returned after %s, want >= ~100ms", elapsed)
	}
}

func TestNewVirtualClockClampsNonPositiveScale(t *testing.T) {
	clock := NewVirtualClock(0)
	if got := clock.Virtual(time.Second); got != time.Second {
		t.Errorf("Virtual(1s) = %s, want 1s for a clamped scale", got)
	}
}

func TestConcurrentBurstCountsEveryDecision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newCountingLimiter(ctx, 10, time.Hour)

	allowed := ConcurrentBurst(t, l, 8, 20)
	BetweenInt(t, allowed, 10, 10, "concurrently allowed with quota 10")
}

func TestReplayRunsOnArriveHooksInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newCountingLimiter(ctx, 2, 40*time.Millisecond)

	var hooks int
	res := Replay(t, NewVirtualClock(0.5), l, []Event{
		{At: 0, Burst: 1},
		{At: 60 * time.Millisecond, OnArrive: func() { hooks++ }},
		{At: 100 * time.Millisecond, Burst: 1, OnArrive: func() { hooks++ }},
	})

	EqualInt(t, hooks, 2, "hooks invoked")
	EqualInt(t, res.At(100*time.Millisecond).Allowed, 1, "allowed after the window rolled over")
}

func TestConstructWithinReturnsValue(t *testing.T) {
	got, err := ConstructWithin(time.Second, func() int { return 42 })
	if err != nil {
		t.Fatalf("ConstructWithin: %v", err)
	}
	EqualInt(t, got, 42, "constructed value")
}

func TestConstructWithinDetectsBlockingConstructor(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	_, err := ConstructWithin(20*time.Millisecond, func() int {
		<-release
		return 0
	})
	if !errors.Is(err, ErrConstructorBlocked) {
		t.Errorf("err = %v, want ErrConstructorBlocked", err)
	}
}

func TestPanicsWithinDetectsPanic(t *testing.T) {
	panicked, err := PanicsWithin(time.Second, func() { panic("boom") })
	if err != nil {
		t.Fatalf("PanicsWithin: %v", err)
	}
	if !panicked {
		t.Error("PanicsWithin did not observe the panic")
	}
}

func TestPanicsWithinDetectsNormalReturn(t *testing.T) {
	panicked, err := PanicsWithin(time.Second, func() {})
	if err != nil {
		t.Fatalf("PanicsWithin: %v", err)
	}
	if panicked {
		t.Error("PanicsWithin reported a panic for a constructor that returned normally")
	}
}

func TestPanicsWithinDetectsBlockingConstructor(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	panicked, err := PanicsWithin(20*time.Millisecond, func() { <-release })
	if !errors.Is(err, ErrConstructorBlocked) {
		t.Errorf("err = %v, want ErrConstructorBlocked", err)
	}
	if panicked {
		t.Error("blocked constructor must not be reported as panicking")
	}
}

func TestRecoverWithinReturnsPanicValue(t *testing.T) {
	recovered, panicked, err := RecoverWithin(time.Second, func() { panic("boom") })
	if err != nil {
		t.Fatalf("RecoverWithin: %v", err)
	}
	if !panicked {
		t.Fatal("RecoverWithin did not report a panic")
	}
	EqualInt(t, len([]byte(fmt.Sprint(recovered))), 4, "length of the recovered message")
}

func TestExpectPanicContainingPasses(t *testing.T) {
	ExpectPanicContaining(t, time.Second, "limit", func() { panic("limit must be positive, got 0") })
}

func TestExpectPanicContainingFailsOnWrongMessage(t *testing.T) {
	fake := &testing.T{}
	ExpectPanicContaining(fake, time.Second, "interval", func() { panic("limit must be positive") })
	if !fake.Failed() {
		t.Error("panic about the wrong parameter should have failed the test")
	}
}

func TestExpectPanicContainingFailsWithoutPanic(t *testing.T) {
	fake := &testing.T{}
	ExpectPanicContaining(fake, time.Second, "limit", func() {})
	if !fake.Failed() {
		t.Error("missing panic should have failed the test")
	}
}

func TestConstructAndConstructPanicsThinWrappers(t *testing.T) {
	Construct(t, time.Second, func() string { return "ok" })
	ConstructPanics(t, time.Second, func() { panic("boom") })
}

func TestSettleGoroutinesWaitsForCancellation(t *testing.T) {
	baseline := Goroutines()

	ctx, cancel := context.WithCancel(context.Background())
	l := newCountingLimiter(ctx, 1, 5*time.Millisecond)
	_ = l
	time.Sleep(20 * time.Millisecond)

	cancel()
	if n := SettleGoroutines(baseline, 2*time.Second); n > baseline {
		t.Errorf("goroutines settled at %d, want <= baseline %d", n, baseline)
	}
	if !l.isClosed() {
		t.Error("background goroutine did not observe context cancellation")
	}
}

func TestBetweenIntReportsOutOfRange(t *testing.T) {
	fake := &testing.T{}
	BetweenInt(fake, 7, 1, 3, "value")
	if !fake.Failed() {
		t.Error("out of range value should have failed the test")
	}
}

func TestEqualIntReportsMismatch(t *testing.T) {
	fake := &testing.T{}
	EqualInt(fake, 1, 2, "value")
	if !fake.Failed() {
		t.Error("mismatched value should have failed the test")
	}
}
