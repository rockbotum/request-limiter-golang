package leakybucketrequestlimiter

import (
	"context"
	"fmt"
	"time"
)

type LeakyBucketLimiter struct {
	queue    chan chan struct{}
	interval time.Duration
}

func NewLeakyBucketLimiter(ctx context.Context, limit int, interval time.Duration) *LeakyBucketLimiter {
	if limit <= 0 {
		panic(fmt.Sprintf("leaky bucket: limit must be positive, got %d", limit))
	}
	if interval <= 0 {
		panic(fmt.Sprintf("leaky bucket: interval must be positive, got %s", interval))
	}

	leakEvery := time.Duration(interval.Nanoseconds() / int64(limit))
	if leakEvery <= 0 {
		panic(fmt.Sprintf("leaky bucket: interval %s is too small for limit %d: the leak period rounds down to zero", interval, limit))
	}

	limiter := &LeakyBucketLimiter{
		queue:    make(chan chan struct{}, limit),
		interval: leakEvery,
	}

	go limiter.Start(ctx, leakEvery)
	return limiter
}

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
				close(ready)
			default:
			}
		}
	}
}

func (l *LeakyBucketLimiter) Allow() bool {
	select {
	case l.queue <- make(chan struct{}):
		return true
	default:
		return false
	}
}

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

func (l *LeakyBucketLimiter) Len() int {
	return len(l.queue)
}
