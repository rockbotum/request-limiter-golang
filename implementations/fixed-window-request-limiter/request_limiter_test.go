package fixedwindowrequestlimiter_test

import (
	"context"
	"testing"
	"time"

	fixedwindow "request-limiter/implementations/fixed-window-request-limiter"
	"request-limiter/internal/limitertest"
)

const (
	testLimit    = 4
	testInterval = 300 * time.Millisecond
)

func newLimiter(t *testing.T, limit int, interval time.Duration) (context.CancelFunc, *fixedwindow.FixedWindowLimiter) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	l := limitertest.Construct(t, time.Second, func() *fixedwindow.FixedWindowLimiter {
		return fixedwindow.NewFixedWindowLimiter(ctx, limit, interval)
	})
	return cancel, l
}

func TestFixedWindowAllowsExactlyLimitPerWindow(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit*5),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed in first window")
	limitertest.EqualInt(t, res.Denied, testLimit*4, "denied in first window")
}

func TestFixedWindowResetsCounterOnTicker(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit*2),
		limitertest.Burst(testInterval+50*time.Millisecond, testLimit*2),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "allowed in first window")
	limitertest.EqualInt(t, res.At(testInterval+50*time.Millisecond).Allowed, testLimit, "allowed in second window")
	limitertest.EqualInt(t, res.Denied, testLimit*2, "denied across both windows")
}

func TestFixedWindowAlignsWindowsToConstructionTime(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(250*time.Millisecond, testLimit),
		limitertest.Burst(320*time.Millisecond, testLimit),
	})

	limitertest.EqualInt(t, res.At(250*time.Millisecond).Allowed, testLimit, "allowed just before the boundary")
	limitertest.EqualInt(t, res.At(320*time.Millisecond).Allowed, testLimit, "allowed just after the boundary")
	limitertest.EqualInt(t, res.Window(240*time.Millisecond, 320*time.Millisecond).Allowed, testLimit*2,
		"allowed across a single boundary")
}

func TestFixedWindowDrainsCounterAfterARejectedBurst(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval-20*time.Millisecond)

	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit*3),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed before the boundary")
	limitertest.EqualInt(t, res.Denied, testLimit*2, "denied before the boundary")
}

func TestFixedWindowConcurrentBurstAdmitsExactlyLimit(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, testLimit, "allowed under 64 concurrent requests")
}

func TestNewFixedWindowLimiterRejectsZeroLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = fixedwindow.NewFixedWindowLimiter(ctx, 0, testInterval)
	})
}

func TestNewFixedWindowLimiterRejectsZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "interval", func() {
		_ = fixedwindow.NewFixedWindowLimiter(ctx, testLimit, 0)
	})
}

func TestFixedWindowStopsResettingAfterContextCancel(t *testing.T) {
	cancel, l := newLimiter(t, testLimit, testInterval)

	first := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
	})
	limitertest.EqualInt(t, first.Allowed, testLimit, "allowed before cancellation")

	cancel()

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*2)
	after := limitertest.Replay(t, clock, l, []limitertest.Event{limitertest.Burst(0, 1)})
	limitertest.EqualInt(t, after.Allowed, 0, "allowed after the ticker stopped")
}

func TestFixedWindowReleasesGoroutineOnCancel(t *testing.T) {
	baseline := limitertest.Goroutines()

	ctx, cancel := context.WithCancel(context.Background())
	_ = limitertest.Construct(t, time.Second, func() *fixedwindow.FixedWindowLimiter {
		return fixedwindow.NewFixedWindowLimiter(ctx, testLimit, testInterval)
	})

	if n := limitertest.SettleGoroutines(baseline+1, time.Second); n > baseline+1 {
		t.Errorf("goroutines = %d while running, want <= %d", n, baseline+1)
	}
	cancel()
	if n := limitertest.SettleGoroutines(baseline, 2*time.Second); n > baseline {
		t.Errorf("goroutines = %d after cancel, want <= %d", n, baseline)
	}
}
