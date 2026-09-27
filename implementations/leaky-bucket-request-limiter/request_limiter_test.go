package leakybucketrequestlimiter_test

import (
	"context"
	"testing"
	"time"

	leakybucket "request-limiter/implementations/leaky-bucket-request-limiter"
	"request-limiter/internal/limitertest"
)

const (
	testLimit    = 4
	testInterval = 200 * time.Millisecond
)

// replenish is how often the background goroutine returns one token.
const replenish = testInterval / testLimit

func newLimiter(t *testing.T, limit int, interval time.Duration) (context.CancelFunc, *leakybucket.LeakyBucketLimiter) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	l := limitertest.Construct(t, time.Second, func() *leakybucket.LeakyBucketLimiter {
		return leakybucket.NewLeakyBucketLimiter(ctx, limit, interval)
	})
	return cancel, l
}

func TestNewLeakyBucketLimiterStartsWithFullCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit*3),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed in the initial burst")
	limitertest.EqualInt(t, res.Denied, testLimit*2, "denied in the initial burst")
}

func TestLeakyBucketRejectsBurstAboveReplenishmentRate(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(replenish+10*time.Millisecond, testLimit),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "allowed while draining the bucket")
	limitertest.BetweenInt(t, res.At(replenish+10*time.Millisecond).Allowed, 1, 2,
		"allowed after a single replenish tick")
}

func TestLeakyBucketAdmitsOneRequestPerReplenishmentTick(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	events := limitertest.Steady(0, replenish, 1, 8)
	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, events)

	limitertest.BetweenInt(t, res.Allowed, 6, 8, "allowed while offering 1 request per replenish tick")
}

func TestLeakyBucketRefillsFullyAfterOneInterval(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(testInterval+50*time.Millisecond, testLimit*3),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "allowed in the initial burst")
	limitertest.EqualInt(t, res.At(testInterval+50*time.Millisecond).Allowed, testLimit,
		"allowed after a full interval")
}

func TestLeakyBucketDoesNotAccumulateBeyondCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*4)

	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(testInterval*4, testLimit*10),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed after a long idle period")
}

func TestLeakyBucketConcurrentBurstAdmitsExactlyCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, testLimit, "allowed under 64 concurrent requests")
}

func TestLeakyBucketStopsReplenishingAfterContextCancel(t *testing.T) {
	cancel, l := newLimiter(t, testLimit, testInterval)

	drained := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
	})
	limitertest.EqualInt(t, drained.Allowed, testLimit, "allowed before cancellation")

	cancel()

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*2)
	after := limitertest.Replay(t, clock, l, []limitertest.Event{limitertest.Burst(0, testLimit)})
	limitertest.EqualInt(t, after.Allowed, 0, "allowed after replenishment stopped")
}

func TestLeakyBucketReleasesGoroutineOnCancel(t *testing.T) {
	baseline := limitertest.Goroutines()

	ctx, cancel := context.WithCancel(context.Background())
	_ = limitertest.Construct(t, time.Second, func() *leakybucket.LeakyBucketLimiter {
		return leakybucket.NewLeakyBucketLimiter(ctx, testLimit, testInterval)
	})

	if n := limitertest.SettleGoroutines(baseline+1, time.Second); n > baseline+1 {
		t.Errorf("goroutines = %d while running, want <= %d", n, baseline+1)
	}
	cancel()
	if n := limitertest.SettleGoroutines(baseline, 2*time.Second); n > baseline {
		t.Errorf("goroutines = %d after cancel, want <= %d", n, baseline)
	}
}

func TestNewLeakyBucketLimiterRejectsZeroLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = leakybucket.NewLeakyBucketLimiter(ctx, 0, testInterval)
	})
}

func TestNewLeakyBucketLimiterRejectsZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "interval", func() {
		_ = leakybucket.NewLeakyBucketLimiter(ctx, testLimit, 0)
	})
}
