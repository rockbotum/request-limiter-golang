package slidingwindowrequestlimiter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type SlidingWindowLimiter struct {
	limit    int
	interval time.Duration
	mutex    sync.Mutex

	currentTime time.Time
	prevCount   int
	currCount   int
}

func NewSlidingWindowLimiter(ctx context.Context, limit int, interval time.Duration) *SlidingWindowLimiter {
	if limit <= 0 {
		panic(fmt.Sprintf("sliding window: limit must be positive, got %d", limit))
	}
	if interval <= 0 {
		panic(fmt.Sprintf("sliding window: interval must be positive, got %s", interval))
	}

	limiter := &SlidingWindowLimiter{
		limit:       limit,
		interval:    interval,
		currentTime: time.Now(),
	}

	return limiter
}

func (l *SlidingWindowLimiter) Start(ctx context.Context) {
	<-ctx.Done()
}

func (l *SlidingWindowLimiter) Allow() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	now := time.Now()

	newPeriodTime := l.currentTime.Add(l.interval)
	if now.After(newPeriodTime) {
		if now.After(newPeriodTime.Add(l.interval)) {
			l.prevCount = 0
		} else {
			l.prevCount = l.currCount
		}
		l.currCount = 0
		l.currentTime = now
	}

	interval := float64(l.interval)
	elapsed := now.Sub(l.currentTime).Seconds()
	count := float64(l.prevCount)*(interval-elapsed)/interval + float64(l.currCount)
	if count >= float64(l.limit) {
		return false
	}

	l.currCount++
	return true
}
