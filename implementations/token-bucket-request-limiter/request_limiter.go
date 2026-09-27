package tokenbucketrequestlimiter

import (
	"context"
	"fmt"
	"time"
)

type TokenBucketLimiter struct {
	tokenBucketCh chan struct{}
}

func NewTokenBucketLimiter(ctx context.Context, limit int, interval time.Duration) *TokenBucketLimiter {
	if limit <= 0 {
		panic(fmt.Sprintf("token bucket: limit must be positive, got %d", limit))
	}
	if interval <= 0 {
		panic(fmt.Sprintf("token bucket: interval must be positive, got %s", interval))
	}

	limiter := &TokenBucketLimiter{
		tokenBucketCh: make(chan struct{}, limit),
	}

	for i := 0; i < limit; i++ {
		limiter.tokenBucketCh <- struct{}{}
	}

	replenishmentInterval := interval.Nanoseconds() / int64(limit)
	go limiter.Start(ctx, time.Duration(replenishmentInterval))
	return limiter
}

func (l *TokenBucketLimiter) Start(ctx context.Context, interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			select {
			case l.tokenBucketCh <- struct{}{}:
			default:
			}
		}
	}
}

func (l *TokenBucketLimiter) Allow() bool {
	select {
	case <-l.tokenBucketCh:
		return true
	default:
		return false
	}
}
