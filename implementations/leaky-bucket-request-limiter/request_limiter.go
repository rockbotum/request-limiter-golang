package leakybucketrequestlimiter

import (
	"context"
	"fmt"
	"time"
)

type LeakyBucketLimiter struct {
	leakyBucketCh chan struct{}
}

func NewLeakyBucketLimiter(ctx context.Context, limit int, interval time.Duration) *LeakyBucketLimiter {
	if limit <= 0 {
		panic(fmt.Sprintf("leaky bucket: limit must be positive, got %d", limit))
	}
	if interval <= 0 {
		panic(fmt.Sprintf("leaky bucket: interval must be positive, got %s", interval))
	}

	limiter := &LeakyBucketLimiter{
		leakyBucketCh: make(chan struct{}, limit),
	}

	for i := 0; i < limit; i++ {
		limiter.leakyBucketCh <- struct{}{}
	}

	replenishmentInterval := interval.Nanoseconds() / int64(limit)
	go limiter.Start(ctx, time.Duration(replenishmentInterval))
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
			case l.leakyBucketCh <- struct{}{}:
			default:
			}
		}
	}
}

func (l *LeakyBucketLimiter) Allow() bool {
	select {
	case <-l.leakyBucketCh:
		return true
	default:
		return false
	}
}
