package fixedwindowrequestlimiter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type FixedWindowLimiter struct {
	count int32
	limit int
}

func NewFixedWindowLimiter(ctx context.Context, limit int, interval time.Duration) *FixedWindowLimiter {
	if interval <= 0 {
		panic(fmt.Sprintf("fixed window: interval must be positive, got %s", interval))
	}
	if limit <= 0 {
		panic(fmt.Sprintf("fixed window: limit must be positive, got %d", limit))
	}
	limiter := &FixedWindowLimiter{
		count: 0,
		limit: limit,
	}

	go limiter.startPeriodicCountRefresh(ctx, interval)
	return limiter
}

func (l *FixedWindowLimiter) startPeriodicCountRefresh(ctx context.Context, interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			atomic.StoreInt32(&l.count, 0)
		}
	}
}

func (l *FixedWindowLimiter) Allow() bool {
	for {
		count := atomic.LoadInt32(&l.count)
		if count >= int32(l.limit) {
			return false
		}
		if atomic.CompareAndSwapInt32(&l.count, count, count+1) {
			return true
		}
	}
}
