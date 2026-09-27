package slidinglogrequestlimiter

import (
	"context"
	"sync"
	"testing"
	"time"

	"request-limiter/internal/limitertest"
)

// Allow used to append to the log before deciding, so every rejected request
// was remembered as well and the log grew for as long as traffic kept arriving.
func TestAllowDoesNotRecordRejectedRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		limit    = 4
		attempts = 500
	)

	l := NewSlidingLogLimiter(ctx, limit, time.Hour)
	defer cancel()

	allowed := 0
	for i := 0; i < attempts; i++ {
		if l.Allow() {
			allowed++
		}
	}

	limitertest.EqualInt(t, allowed, limit, "allowed in a saturated window")
	limitertest.EqualInt(t, len(l.logs), limit, "entries in the log")
}

// Nothing clears the log from the outside any more, so a saturated limiter must
// keep denying until its own entries age out of the window one by one.
func TestAllowKeepsEntriesInsideTheWindow(t *testing.T) {
	const (
		limit    = 4
		interval = 400 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := NewSlidingLogLimiter(ctx, limit, interval)
	defer cancel()

	for i := 0; i < limit; i++ {
		if !l.Allow() {
			t.Fatalf("request %d of %d was denied", i, limit)
		}
	}
	if l.Allow() {
		t.Fatal("request past the limit was allowed")
	}

	time.Sleep(interval / 2)
	if l.Allow() {
		t.Error("request was allowed while every entry was still inside the window")
	}
	if got := len(l.logs); got != limit {
		t.Errorf("len(logs) = %d, want %d: no entry may be dropped early", got, limit)
	}

	// Отсечение ленивое и происходит только внутри Allow, поэтому устаревшие
	// записи лежат в логе до следующего запроса, а не сами собой исчезают.
	time.Sleep(interval)
	if !l.Allow() {
		t.Error("request was denied after every entry aged out")
	}
	if got := len(l.logs); got != 1 {
		t.Errorf("len(logs) = %d, want 1: aged out entries must be dropped and the new one recorded", got)
	}
}

// All mutations go through the mutex now, so the log must never exceed the
// limit no matter how many goroutines call Allow at once. Run with -race to
// confirm the log is free of data races too.
func TestAllowIsSafeUnderConcurrentUse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		limit    = 4
		interval = 20 * time.Millisecond
	)

	l := NewSlidingLogLimiter(ctx, limit, interval)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	maxLog := 0

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				_ = l.Allow()

				mu.Lock()
				if len(l.logs) > maxLog {
					maxLog = len(l.logs)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	limitertest.EqualInt(t, maxLog, limit, "largest observed log size")
}
