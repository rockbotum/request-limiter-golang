package leakybucketrequestlimiter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	leakybucket "request-limiter/implementations/leaky-bucket-request-limiter"
	"request-limiter/internal/limitertest"
)

const (
	testLimit    = 4
	testInterval = 200 * time.Millisecond
)

// leak is how often the background goroutine releases one queued request.
const leak = testInterval / testLimit

func newLimiter(t *testing.T, limit int, interval time.Duration) (context.CancelFunc, *leakybucket.LeakyBucketLimiter) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	l := limitertest.Construct(t, time.Second, func() *leakybucket.LeakyBucketLimiter {
		return leakybucket.NewLeakyBucketLimiter(ctx, limit, interval)
	})
	return cancel, l
}

func atLeast(t *testing.T, got, want time.Duration, what string) {
	t.Helper()
	if got < want {
		t.Errorf("%s = %s, want at least %s", what, got, want)
	}
}

func atMost(t *testing.T, got, want time.Duration, what string) {
	t.Helper()
	if got > want {
		t.Errorf("%s = %s, want at most %s", what, got, want)
	}
}

// The leak ticker runs on its own phase, so a request queued just before a tick
// can be released almost immediately. Timing assertions therefore compare
// against a slightly shortened bound: short enough to catch a token bucket,
// which would hand the request out at once, and long enough not to depend on
// where the ticker happens to be.
func atLeastTick(t *testing.T, got time.Duration, ticks int, what string) {
	t.Helper()
	atLeast(t, got, leak*time.Duration(ticks)*9/10, what)
}

func waitForQueueLen(t *testing.T, l *leakybucket.LeakyBucketLimiter, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if l.Len() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue length = %d, want %d", l.Len(), want)
}

// burstAdmitsCapacity checks that a burst of requests is admitted according to
// the size of the bucket.
//
// A burst is fired from the test goroutine in a tight loop, so a leak tick can
// land in the middle of it and free one more slot; that is correct behaviour,
// not a capacity violation. Hence the slack of one. The capacity itself is
// pinned down exactly by TestLeakyBucketQueueDrainsOneRequestPerTick, which
// reads the queue depth and is immune to where the ticker happens to be.
func burstAdmitsCapacity(t *testing.T, allowed, limit int, what string) {
	t.Helper()

	if allowed < limit {
		t.Errorf("%s = %d, want the %d slots of the bucket to be usable", what, allowed, limit)
	}
	if allowed > limit+1 {
		t.Errorf("%s = %d, want about %d, since the bucket holds %d requests", what, allowed, limit, limit)
	}
}

// --- Allow: admission into a bucket of `limit` slots ---

func TestLeakyBucketAdmitsUpToCapacityInOneBurst(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit*3),
	})

	burstAdmitsCapacity(t, res.Allowed, testLimit, "allowed in a burst three times the capacity")
	limitertest.BetweenInt(t, res.Denied, testLimit*2-1, testLimit*2, "denied in a burst three times the capacity")
}

func TestLeakyBucketFreesOneSlotPerLeakTick(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(leak+10*time.Millisecond, testLimit),
	})

	burstAdmitsCapacity(t, res.At(0).Allowed, testLimit, "allowed while the bucket was empty")
	limitertest.BetweenInt(t, res.At(leak+10*time.Millisecond).Allowed, 1, 2,
		"allowed after a single leak tick")
}

func TestLeakyBucketAdmitsOneRequestPerLeakTick(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	events := limitertest.Steady(0, leak, 1, 8)
	res := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, events)

	limitertest.BetweenInt(t, res.Allowed, 6, 8, "allowed while offering 1 request per leak tick")
}

func TestLeakyBucketDrainsFullyWithinOneInterval(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
		limitertest.Burst(testInterval+50*time.Millisecond, testLimit*3),
	})

	burstAdmitsCapacity(t, res.At(0).Allowed, testLimit, "allowed in the initial burst")
	burstAdmitsCapacity(t, res.At(testInterval+50*time.Millisecond).Allowed, testLimit,
		"allowed once the bucket has drained")
}

func TestLeakyBucketHoldsNoMoreThanCapacityWhileIdle(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*4)

	res := limitertest.Replay(t, clock, l, []limitertest.Event{
		limitertest.Burst(testInterval*4, testLimit*10),
	})

	// Ticks that fired while the bucket was empty handed out nothing, so a long
	// idle period buys no extra capacity.
	burstAdmitsCapacity(t, res.Allowed, testLimit, "allowed after a long idle period")
}

func TestLeakyBucketConcurrentBurstAdmitsExactlyCapacity(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	allowed := limitertest.ConcurrentBurst(t, l, 16, 4)
	limitertest.EqualInt(t, allowed, testLimit, "allowed under 64 concurrent requests")
}

func TestLeakyBucketStopsLeakingAfterContextCancel(t *testing.T) {
	cancel, l := newLimiter(t, testLimit, testInterval)

	drained := limitertest.Replay(t, limitertest.NewVirtualClock(1), l, []limitertest.Event{
		limitertest.Burst(0, testLimit),
	})
	burstAdmitsCapacity(t, drained.Allowed, testLimit, "allowed before cancellation")

	cancel()

	clock := limitertest.NewVirtualClock(1)
	clock.WaitUntil(t, testInterval*2)
	after := limitertest.Replay(t, clock, l, []limitertest.Event{limitertest.Burst(0, testLimit)})
	limitertest.EqualInt(t, after.Allowed, 0, "allowed after the leak stopped")
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

// --- Wait: the part that makes this a leaky bucket and not a token bucket ---

func TestLeakyBucketWaitHoldsARequestUntilItsTurn(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	// Four requests arrive at once and must not be released together: the leak
	// hands them out one per tick, so the last one waits three more ticks.
	starts := make(chan struct{})
	releases := make(chan time.Duration, testLimit)

	var wg sync.WaitGroup
	wg.Add(testLimit)
	for i := 0; i < testLimit; i++ {
		go func() {
			defer wg.Done()
			<-starts
			began := time.Now()
			if err := l.Wait(context.Background()); err != nil {
				t.Errorf("Wait returned %v", err)
				return
			}
			releases <- time.Since(began)
		}()
	}

	// The callers are already parked on the barrier, so closing it queues all
	// four of them within a tick of each other.
	close(starts)
	wg.Wait()
	close(releases)

	var times []time.Duration
	for d := range releases {
		times = append(times, d)
	}

	quickest, slowest := times[0], times[0]
	for _, d := range times[1:] {
		if d < quickest {
			quickest = d
		}
		if d > slowest {
			slowest = d
		}
	}

	atLeastTick(t, quickest, 1, "release of the first queued request")
	atLeastTick(t, slowest-quickest, 2, "spread between the first and the last release")
}

func TestLeakyBucketWaitDoesNotCreditIdleTime(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	// A token bucket banks the ticks that fire while nobody is waiting and hands
	// all of these out at once after a long idle period. A leaky bucket has no
	// bank: each request waits for a tick of its own, so four of them span four
	// ticks however long the bucket has been sitting there empty.
	time.Sleep(testInterval * 5)

	began := time.Now()
	for i := 0; i < 4; i++ {
		if err := l.Wait(context.Background()); err != nil {
			t.Fatalf("Wait %d returned %v", i, err)
		}
	}
	elapsed := time.Since(began)

	atLeastTick(t, elapsed, 3, "four sequential waits after a long idle period")
	atMost(t, elapsed, 6*leak, "four sequential waits after a long idle period")
}

func TestLeakyBucketWaitPacesSequentialCallers(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	began := time.Now()
	for i := 0; i < 3; i++ {
		if err := l.Wait(context.Background()); err != nil {
			t.Fatalf("Wait %d returned %v", i, err)
		}
	}
	elapsed := time.Since(began)

	// Three requests, three ticks. A token bucket would have served all three
	// from the initial fill.
	atLeastTick(t, elapsed, 2, "three sequential waits")
	atMost(t, elapsed, 4*leak, "three sequential waits")
}

func TestLeakyBucketQueueDrainsOneRequestPerTick(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	for i := 0; i < testLimit; i++ {
		if !l.Allow() {
			t.Fatalf("Allow %d returned false while the bucket had room", i)
		}
	}
	limitertest.EqualInt(t, l.Len(), testLimit, "queued immediately after filling the bucket")

	time.Sleep(leak / 2)
	limitertest.EqualInt(t, l.Len(), testLimit, "queued before the first leak tick")

	// Three ticks later at most three requests can have left, and the fourth
	// must still be waiting, which is the point: the bucket is not simply
	// emptying itself the moment it has room.
	time.Sleep(3 * leak)
	limitertest.BetweenInt(t, l.Len(), 0, 2, "queued after three leak ticks")

	time.Sleep(4 * leak)
	limitertest.EqualInt(t, l.Len(), 0, "queued once the bucket has drained")
}

func TestLeakyBucketWaitReturnsImmediatelyOnCancelledContext(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	began := time.Now()
	err := l.Wait(ctx)
	elapsed := time.Since(began)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Wait error = %v, want %v", err, context.Canceled)
	}
	atMost(t, elapsed, leak, "wait on an already cancelled context")
}

func TestLeakyBucketWaitTimesOutWhenTheBucketStaysFull(t *testing.T) {
	// A leak period of a second keeps the ticker far away from the deadline
	// below, so the only thing that can unblock this Wait is the deadline.
	_, l := newLimiter(t, testLimit, 4*time.Second)

	for i := 0; i < testLimit; i++ {
		if !l.Allow() {
			t.Fatalf("Allow %d returned false while the bucket had room", i)
		}
	}

	// Wait does not turn a caller away on a full bucket, it parks it, so the
	// only way out here is the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	began := time.Now()
	err := l.Wait(ctx)
	elapsed := time.Since(began)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait error = %v, want %v", err, context.DeadlineExceeded)
	}
	atMost(t, elapsed, 500*time.Millisecond, "wait on a bucket that stays full")
}

func TestLeakyBucketWaitKeepsItsSlotWhenTheContextExpires(t *testing.T) {
	// A leak period of a second keeps the ticker far away from the deadline, so
	// this request is queued and only the deadline can release it.
	_, l := newLimiter(t, testLimit, 4*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	began := time.Now()
	err := l.Wait(ctx)
	elapsed := time.Since(began)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait error = %v, want %v", err, context.DeadlineExceeded)
	}
	atMost(t, elapsed, 500*time.Millisecond, "wait on a queue that is not leaking soon")

	// The request gave up, but its entry is still in line: handing the slot back
	// early would let one more request into the bucket than the capacity allows.
	limitertest.EqualInt(t, l.Len(), 1, "queued after a wait that timed out")
}

func TestLeakyBucketWaitQueuesBehindARequestThatIsNotCollected(t *testing.T) {
	_, l := newLimiter(t, testLimit, testInterval)

	// The bucket is full of requests admitted through Allow, which nobody ever
	// collects. Wait must still get its turn, after the ones ahead of it.
	for i := 0; i < testLimit; i++ {
		if !l.Allow() {
			t.Fatalf("Allow %d returned false while the bucket had room", i)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*testInterval)
	defer cancel()

	began := time.Now()
	err := l.Wait(ctx)

	if err != nil {
		t.Fatalf("Wait returned %v, want it to be served after the queued requests", err)
	}
	elapsed := time.Since(began)

	// Four requests are already queued and this one makes five, so it is the
	// fifth tick that releases it.
	atLeastTick(t, elapsed, 4, "wait behind four uncollected requests")
}

func TestLeakyBucketReleasesWaitingRequestsInArrivalOrder(t *testing.T) {
	// Three slots, so a caller can be observed joining the queue rather than
	// parking on a full one: the queue length then shows the arrival order.
	_, l := newLimiter(t, 3, testInterval)

	// One request is admitted through Allow, then two callers wait. Goroutine
	// names do not establish arrival order, so each caller is only started once
	// the queue confirms that its predecessor is in line.
	if !l.Allow() {
		t.Fatal("Allow returned false while the bucket had room")
	}

	var mutex sync.Mutex
	var order []string
	done := make(chan struct{}, 2)

	wait := func(name string) {
		if err := l.Wait(context.Background()); err != nil {
			t.Errorf("Wait %s returned %v", name, err)
			done <- struct{}{}
			return
		}
		mutex.Lock()
		order = append(order, name)
		mutex.Unlock()
		done <- struct{}{}
	}

	go wait("first")
	waitForQueueLen(t, l, 2)
	go wait("second")
	waitForQueueLen(t, l, 3)

	<-done
	<-done

	if len(order) != 2 {
		t.Fatalf("released %d requests, want 2", len(order))
	}
	if order[0] != "first" {
		t.Errorf("release order = %v, want the first caller released before the second", order)
	}
}

func TestLeakyBucketConcurrentWaitersAreAllServed(t *testing.T) {
	const callers = 8

	_, l := newLimiter(t, testLimit, testInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			errs <- l.Wait(ctx)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Wait returned %v, want every caller served", err)
		}
	}
	limitertest.EqualInt(t, l.Len(), 0, "queued once every caller has been served")
}

// --- Argument validation ---

func TestNewLeakyBucketLimiterRejectsZeroLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = leakybucket.NewLeakyBucketLimiter(ctx, 0, testInterval)
	})
}

func TestNewLeakyBucketLimiterRejectsNegativeLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "limit", func() {
		_ = leakybucket.NewLeakyBucketLimiter(ctx, -1, testInterval)
	})
}

func TestNewLeakyBucketLimiterRejectsZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limitertest.ExpectPanicContaining(t, time.Second, "interval", func() {
		_ = leakybucket.NewLeakyBucketLimiter(ctx, testLimit, 0)
	})
}

func TestNewLeakyBucketLimiterRejectsIntervalThatRoundsTheLeakPeriodToZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// interval/limit rounds down to zero here, which would panic the ticker
	// inside the background goroutine once the constructor had already returned.
	limitertest.ExpectPanicContaining(t, time.Second, "leak period", func() {
		_ = leakybucket.NewLeakyBucketLimiter(ctx, 2, time.Nanosecond)
	})
}
