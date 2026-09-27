package slidingwindowrequestlimiter_test

import (
	"context"
	"testing"
	"time"

	slidingwindow "request-limiter/implementations/sliding-window-request-limiter"
	"request-limiter/internal/limitertest"
)

const (
	testLimit    = 4
	testInterval = 200 * time.Millisecond
)

func newLimiter(t *testing.T, limit int, interval time.Duration) *slidingwindow.SlidingWindowLimiter {
	t.Helper()

	// Construct fails the test if the constructor does not return promptly, which
	// is the regression that used to make this implementation unusable.
	return limitertest.Construct(t, time.Second, func() *slidingwindow.SlidingWindowLimiter {
		return slidingwindow.NewSlidingWindowLimiter(context.Background(), limit, interval)
	})
}

// The constructor used to call Start on the calling goroutine. Since Start never
// returns, the constructor never returned and the limiter could not be used at
// all. The harness would report that as a blocked constructor; now it must
// return promptly and hand back a working limiter.
func TestNewSlidingWindowLimiterReturnsImmediately(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	if l == nil {
		t.Fatal("constructor returned no limiter")
	}
	if !l.Allow() {
		t.Error("a freshly built limiter denied the first request")
	}
}

func TestNewSlidingWindowLimiterIsUsableStraightAway(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit*3),
	})

	limitertest.EqualInt(t, res.Allowed, testLimit, "allowed in the initial burst")
	limitertest.EqualInt(t, res.Denied, testLimit*2, "denied in the initial burst")
}

func TestSlidingWindowConcurrentBurstAdmitsExactlyLimit(t *testing.T) {
	l := newLimiter(t, testLimit, testInterval)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, testLimit, "allowed under 64 concurrent requests")
}

// The window state is derived from the clock inside Allow, so the constructor
// has no background work and must not leave a goroutine behind. This replaces
// the old goroutine-release test: with no goroutine there is nothing to release.
func TestNewSlidingWindowLimiterStartsNoBackgroundGoroutine(t *testing.T) {
	baseline := limitertest.Goroutines()

	for i := 0; i < 16; i++ {
		_ = slidingwindow.NewSlidingWindowLimiter(context.Background(), testLimit, testInterval)
	}

	time.Sleep(50 * time.Millisecond)
	if n := limitertest.Goroutines(); n > baseline {
		t.Errorf("goroutines = %d after building 16 limiters, want <= %d", n, baseline)
	}
}

func TestNewSlidingWindowLimiterRejectsZeroLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = slidingwindow.NewSlidingWindowLimiter(ctx, 0, testInterval)
	})
}

func TestNewSlidingWindowLimiterRejectsZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "interval", func() {
		_ = slidingwindow.NewSlidingWindowLimiter(ctx, testLimit, 0)
	})
}
