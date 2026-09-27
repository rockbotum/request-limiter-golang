package slidinglogrequestlimiter_test

import (
	"context"
	"testing"
	"time"

	slidinglog "request-limiter/implementations/sliding-log-request-limiter"
	"request-limiter/internal/limitertest"
)

const (
	testLimit    = 4
	testInterval = 300 * time.Millisecond
)

func newLimiter(t *testing.T, limit int, interval time.Duration) *slidinglog.SlidingLogLimiter {
	t.Helper()

	return limitertest.Construct(t, time.Second, func() *slidinglog.SlidingLogLimiter {
		return slidinglog.NewSlidingLogLimiter(context.Background(), limit, interval)
	})
}

func TestSlidingLogAllowsUpToLimit(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit*3),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed in a single burst")
	limitertest.EqualInt(t, res.Denied, testLimit*2, "denied in a single burst")
}

// A sliding window frees slots one by one as individual requests age out. The
// old implementation dropped the whole log on a tick, which released every slot
// at once and made the limiter behave like a fixed window.
func TestSlidingLogFreesSlotsGradually(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(testInterval/2, testLimit),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "allowed at the start of the window")
	limitertest.EqualInt(t, res.At(testInterval/2).Allowed, 0,
		"allowed halfway through, when no request has aged out yet")
}

func TestSlidingLogFreesSlotsOneByOne(t *testing.T) {
	const quarter = testInterval / 4

	l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, 1),
		limitertest.Burst(quarter, 1),
		limitertest.Burst(2*quarter, 1),
		limitertest.Burst(3*quarter, 1),
		limitertest.Burst(testInterval+quarter, 1),
	})

	limitertest.EqualInt(t, res.Allowed, 5, "allowed in total")
	limitertest.EqualInt(t, res.At(testInterval+quarter).Allowed, 1,
		"allowed once the oldest request aged out of the window")
	limitertest.EqualInt(t, res.Denied, 0, "denied in total")
}

func TestSlidingLogFreesEverythingAfterOneInterval(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit*2),
		limitertest.Burst(testInterval+50*time.Millisecond, testLimit*2),
	})

	limitertest.EqualInt(t, res.At(0).Allowed, testLimit, "allowed in the first window")
	limitertest.EqualInt(t, res.At(testInterval+50*time.Millisecond).Allowed, testLimit,
		"allowed once every request aged out")
	limitertest.EqualInt(t, res.Denied, testLimit*2, "denied in total")
}

func TestSlidingLogDoesNotAccumulateBeyondLimit(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*4)

	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(testInterval*4, testLimit*10),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed after a long idle period")
}

func TestSlidingLogConcurrentBurstAdmitsExactlyLimit(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, testLimit, "allowed under 64 concurrent requests")
}

// The old implementation reset l.logs from a background goroutine without
// taking the mutex. Allow is now the only writer, so concurrent traffic across
// a tick must stay race free and still respect the limit.
func TestSlidingLogConcurrentTrafficStaysWithinLimit(t *testing.T) {
	l := newLimiter(t, testLimit, 50*time.Millisecond)

	const (
		rounds    = 6
		workers   = 8
		perWorker = 2
		gap       = 40 * time.Millisecond
	)

	clock := limitertest.NewVirtualClock(1)
	allowed := 0
	for i := 0; i < rounds; i++ {
		clock.WaitUntil(t, time.Duration(i)*gap)
		allowed += limitertest.ConcurrentBurst(t, l, workers, perWorker)
	}

	limitertest.BetweenInt(t, allowed, testLimit, rounds*workers*perWorker,
		"allowed under sustained concurrent traffic")
}

// The log is maintained entirely inside Allow, so the constructor has no
// background work and must not leave a goroutine behind. This replaces the old
// goroutine-release test: with no goroutine there is nothing to release.
func TestNewSlidingLogLimiterStartsNoBackgroundGoroutine(t *testing.T) {
	baseline := limitertest.Goroutines()

	for i := 0; i < 16; i++ {
		_ = slidinglog.NewSlidingLogLimiter(context.Background(), testLimit, testInterval)
	}

	time.Sleep(50 * time.Millisecond)
	if n := limitertest.Goroutines(); n > baseline {
		t.Errorf("goroutines = %d after building 16 limiters, want <= %d", n, baseline)
	}
}

func TestNewSlidingLogLimiterRejectsZeroLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = slidinglog.NewSlidingLogLimiter(ctx, 0, testInterval)
	})
}

func TestNewSlidingLogLimiterRejectsZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "interval", func() {
		_ = slidinglog.NewSlidingLogLimiter(ctx, testLimit, 0)
	})
}
