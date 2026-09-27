package slidinglogrequestlimiter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type SlidingLogLimiter struct {
	limit    int
	interval time.Duration
	logs     []time.Time
	mutex    sync.Mutex
}

func NewSlidingLogLimiter(ctx context.Context, limit int, interval time.Duration) *SlidingLogLimiter {
	if interval <= 0 {
		panic(fmt.Sprintf("sliding log: interval must be positive, got %s", interval))
	}
	if limit <= 0 {
		panic(fmt.Sprintf("sliding log: limit must be positive, got %d", limit))
	}
	limiter := &SlidingLogLimiter{
		limit:    limit,
		interval: interval,
	}
	return limiter
}

func (l *SlidingLogLimiter) Allow() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()
	cutoff := now.Add(-l.interval)

	kept := l.logs[:0]
	for _, at := range l.logs {
		if !at.Before(cutoff) {
			kept = append(kept, at)
		}
	}
	l.logs = kept

	if len(l.logs) >= l.limit {
		return false
	}

	l.logs = append(l.logs, now)
	return true
}
