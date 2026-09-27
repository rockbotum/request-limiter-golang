package tokenbucketrequestlimiter_test

import (
	"context"
	"testing"
	"time"

	tokenbucket "request-limiter/implementations/token-bucket-request-limiter"
	"request-limiter/internal/limitertest"
)

const (
	testLimit    = 4
	testInterval = 200 * time.Millisecond
)

func newLimiter(t *testing.T, limit int, interval time.Duration) (context.CancelFunc, *tokenbucket.TokenBucketLimiter) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	l := limitertest.Construct(t, time.Second, func() *tokenbucket.TokenBucketLimiter {
		return tokenbucket.NewTokenBucketLimiter(ctx, limit, interval)
	})
	return cancel, l
}

func TestNewTokenBucketLimiterFillsBucketToCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit*5),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed in initial burst")
	limitertest.EqualInt(t, res.Denied, testLimit*4, "denied in initial burst")
}

func TestTokenBucketRefillsOneTokenPerReplenishmentInterval(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(60*time.Millisecond, 2),
		limitertest.Burst(110*time.Millisecond, 2),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "initial burst allowed")
	limitertest.BetweenInt(t, res.At(60*time.Millisecond).Allowed, 1, 1, "allowed after 60ms")
	limitertest.BetweenInt(t, res.At(110*time.Millisecond).Allowed, 1, 2, "allowed after 110ms")
}

func TestTokenBucketRefillsFullyAfterOneInterval(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(testInterval+50*time.Millisecond, testLimit*5),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "initial burst allowed")
	limitertest.EqualInt(t, res.At(testInterval+50*time.Millisecond).Allowed, testLimit, "allowed after a full interval")
}

func TestTokenBucketDoesNotAccumulateBeyondCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*4)

	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(testInterval*4, testLimit*10),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed after long idle period")
}

func TestTokenBucketAllowsSustainedRate(t *testing.T) {
	const periods = 2

	every := testInterval / testLimit
	_, l := newLimiter(t, testLimit, testInterval)

	events := limitertest.Steady(0, every, 1, periods*testLimit)
	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, events)

	limitertest.EqualInt(t, res.Allowed, len(events), "allowed at exactly the refill rate")
}

func TestTokenBucketDeniesBurstAboveRefillRate(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	events := limitertest.Steady(0, 10*time.Millisecond, 4, 10)
	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, events)

	if res.Allowed == testLimit*len(events) {
		t.Errorf("limiter admitted the whole %d request burst over %s, expected throttling",
			testLimit*len(events), res.Elapsed)
	}
	limitertest.BetweenInt(t, res.Allowed, testLimit, testLimit*4, "allowed over a 100ms window")
}

func TestTokenBucketConcurrentBurstAdmitsExactlyCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, testLimit, "allowed under 64 concurrent requests")
}

func TestTokenBucketStopsRefillingAfterContextCancel(t *testing.T) {
	cancel, l := newLimiter(t, 2, testInterval)

	if res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, 2),
	}); res.Allowed != 2 {
		t.Fatalf("failed to drain the bucket: allowed %d, want 2", res.Allowed)
	}

	cancel()

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*2)
	after := limitertest.Replay(t, clock, l, []limitertest.Event{limitertest.Burst(0, 2)})
	limitertest.EqualInt(t, after.Allowed, 0, "allowed after cancellation")
}

func TestTokenBucketReleasesGoroutineOnCancel(t *testing.T) {
	baseline := limitertest.Goroutines()

	ctx, cancel := context.WithCancel(context.Background())
	_ = limitertest.Construct(t, time.Second, func() *tokenbucket.TokenBucketLimiter {
		return tokenbucket.NewTokenBucketLimiter(ctx, testLimit, testInterval)
	})

	if n := limitertest.SettleGoroutines(baseline+1, time.Second); n > baseline+1 {
		t.Errorf("goroutines = %d after cancel, want <= %d", n, baseline+1)
	}
	cancel()
	if n := limitertest.SettleGoroutines(baseline, 2*time.Second); n > baseline {
		t.Errorf("goroutines = %d after cancel, want <= %d", n, baseline)
	}
}

func TestNewTokenBucketLimiterRejectsZeroLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = tokenbucket.NewTokenBucketLimiter(ctx, 0, testInterval)
	})
}

func TestNewTokenBucketLimiterRejectsNegativeLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = tokenbucket.NewTokenBucketLimiter(ctx, -1, testInterval)
	})
}

func TestNewTokenBucketLimiterRejectsZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "interval", func() {
		_ = tokenbucket.NewTokenBucketLimiter(ctx, testLimit, 0)
	})
}

// The guards now run in the constructor, so the panic reaches the caller
// synchronously instead of arriving from a background goroutine after the
// constructor has already returned.
func TestNewTokenBucketLimiterValidatesBeforeSpawningGoroutines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicked, err := limitertest.PanicsWithin(time.Second, func() {
		_ = tokenbucket.NewTokenBucketLimiter(ctx, 0, testInterval)
	})
	if err != nil {
		t.Fatalf("PanicsWithin: %v", err)
	}
	if !panicked {
		t.Fatal("constructor did not panic, so it must have returned before spawning a goroutine")
	}
}
