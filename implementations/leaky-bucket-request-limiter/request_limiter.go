package leakybucketrequestlimiter

import (
	"context"
	"fmt"
	"time"
)

// LeakyBucketLimiter is a leaky bucket: accepted requests are queued and then
// released one per leak period, so a burst on the input turns into an evenly
// paced stream on the output.
//
// That is the whole point of the algorithm, and it is what separates it from the
// token bucket next door. A token bucket hands out a permit immediately and
// banks unused permits while the bucket is idle, so an idle client can later
// fire off a full burst. A leaky bucket has no bank: every queued request waits
// for its own tick, whether the bucket has been busy or idle.
type LeakyBucketLimiter struct {
	// queue holds one entry per accepted request. A buffered channel enforces
	// the capacity limit and the order of service at the same time: a send that
	// does not fit in the buffer is rejected, and a receive takes the request
	// that has been waiting longest.
	queue chan chan struct{}

	// leakEvery is the interval between two released requests.
	leakEvery time.Duration
}

func NewLeakyBucketLimiter(ctx context.Context, limit int, interval time.Duration) *LeakyBucketLimiter {
	// The constructor takes (ctx, limit, interval), like every other limiter in
	// this repository.
	if limit <= 0 {
		panic(fmt.Sprintf("leaky bucket: limit must be positive, got %d", limit))
	}

	if interval <= 0 {
		panic(fmt.Sprintf("leaky bucket: interval must be positive, got %s", interval))
	}

	// The leak period is interval/limit, because limit requests are released per
	// interval. A leak period that rounds down to zero would make the ticker
	// panic inside the background goroutine, after this constructor has already
	// returned, so it is caught here instead.
	leakEvery := time.Duration(interval.Nanoseconds() / int64(limit))
	if leakEvery <= 0 {
		panic(fmt.Sprintf("leaky bucket: interval %s is too small for limit %d: the leak period rounds down to zero", interval, limit))
	}

	limiter := &LeakyBucketLimiter{
		queue:     make(chan chan struct{}, limit),
		leakEvery: leakEvery,
	}

	go limiter.Start(ctx, leakEvery)
	return limiter
}

// Start releases one queued request per interval until the context is done.
//
// A request is released only on a tick, never as soon as there is room, and a
// tick that finds an empty bucket is dropped rather than saved. Together those
// two rules are what make the output rate constant: without them the bucket
// would either drain instantly (no smoothing at all) or hand out free passes
// for time spent idle (which is a token bucket).
func (l *LeakyBucketLimiter) Start(ctx context.Context, interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			select {
			case ready := <-l.queue:
				// Closing hands the permit to the waiting request. The request
				// side is a close rather than a send, so a Wait that gave up on
				// its context leaves nothing to receive and nothing blocks here.
				close(ready)
			default:
				// Nobody is waiting: the tick is wasted, as it must be.
			}
		}
	}
}

// Allow reports whether the request fits in the bucket right now, without
// waiting for its turn.
//
// A true result means the request is queued: it occupies one of the limit slots
// and will be released by a tick, no earlier. Allow itself does not pace the
// caller, so use Wait when the delay is what matters.
func (l *LeakyBucketLimiter) Allow() bool {
	select {
	case l.queue <- make(chan struct{}):
		return true
	default:
		return false
	}
}

// Wait queues the request and blocks until the bucket releases it, the context
// is done, or the bucket is full.
//
// Unlike Allow it does not reject on a full bucket: the request waits for a slot
// to free up. Use it when the caller has to be slowed down rather than turned
// away.
//
// Cancelling the context after the request is queued leaves the entry in place
// until a tick releases it, so the slot is not returned early.
func (l *LeakyBucketLimiter) Wait(ctx context.Context) error {
	ready := make(chan struct{})

	select {
	case l.queue <- ready:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Len returns the number of requests currently queued. It is the observable
// side of the leak: it drops by one per leak period while the bucket is not
// empty, and stays at zero while it is idle.
func (l *LeakyBucketLimiter) Len() int {
	return len(l.queue)
}
