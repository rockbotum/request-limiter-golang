// Package limitertest provides a shared test harness for the request limiter
// implementations.
//
// The harness emulates traffic as a virtual timeline: a scenario is described
// as a set of bursts placed at virtual offsets from the start of the test, and
// the harness replays those offsets against wall-clock time. This makes it
// possible to express scenarios that depend on window boundaries (for example
// "burst right before a fixed window rolls over, burst right after") without
// hand-tuned sleeps scattered across every test file.
package limitertest

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Limiter is the minimal surface every implementation exposes.
type Limiter interface {
	Allow() bool
}

// VirtualClock maps virtual offsets onto wall-clock instants. The zero value is
// not usable; build one with NewVirtualClock.
//
// A clock may be shared between goroutines. It records the furthest virtual
// instant any caller has waited for, so a goroutine that is scheduled out of
// order relative to its peers is reported instead of silently skewing results.
type VirtualClock struct {
	start time.Time
	scale float64

	mu      sync.Mutex
	maxSeen time.Duration
}

// NewVirtualClock returns a clock that started now. scale converts virtual
// duration units into real ones: 1 means the scenario runs in real time, 0.5
// means it runs twice as fast.
func NewVirtualClock(scale float64) *VirtualClock {
	if scale <= 0 {
		scale = 1
	}
	return &VirtualClock{start: time.Now(), scale: scale}
}

// Start returns the wall-clock instant the virtual timeline begins at.
func (c *VirtualClock) Start() time.Time { return c.start }

// Instant converts a virtual offset into the wall-clock instant it maps to.
func (c *VirtualClock) Instant(virtual time.Duration) time.Time {
	return c.start.Add(time.Duration(float64(virtual) * c.scale))
}

// Virtual converts a real duration back into virtual units.
func (c *VirtualClock) Virtual(real time.Duration) time.Duration {
	return time.Duration(float64(real) / c.scale)
}

// WaitUntil blocks until the wall-clock instant matching virtual is reached.
func (c *VirtualClock) WaitUntil(t testing.TB, virtual time.Duration) time.Time {
	t.Helper()

	c.mu.Lock()
	if virtual > c.maxSeen {
		c.maxSeen = virtual
	}
	c.mu.Unlock()

	deadline := c.Instant(virtual)
	if d := time.Until(deadline); d > 0 {
		time.Sleep(d)
	}
	return deadline
}

// Event is a burst of requests placed at a virtual offset. OnArrive, when set,
// runs once the offset is reached and just before the burst is fired, which
// lets a scenario change the world mid-run (cancel a context, for example)
// without splitting it into several replays with drifting clocks.
type Event struct {
	At       time.Duration
	Burst    int
	OnArrive func()
}

// Burst returns an event placing n requests at the given virtual offset.
func Burst(at time.Duration, n int) Event {
	return Event{At: at, Burst: n}
}

// Steady returns count events of n requests each, every `every` virtual
// units starting at from.
func Steady(from, every time.Duration, n, count int) []Event {
	events := make([]Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, Burst(from+time.Duration(i)*every, n))
	}
	return events
}

// EventResult is the outcome of replaying a single Event.
type EventResult struct {
	Event
	Allowed int
	Denied  int
}

// Result aggregates the outcome of a replayed scenario.
type Result struct {
	Events  []EventResult
	Allowed int
	Denied  int
	Elapsed time.Duration
}

// Window returns the aggregate over all bursts inside the half-open virtual
// interval (from, to].
func (r Result) Window(from, to time.Duration) Result {
	out := Result{}
	for _, e := range r.Events {
		if e.At <= from || e.At > to {
			continue
		}
		out.Allowed += e.Allowed
		out.Denied += e.Denied
		out.Events = append(out.Events, e)
	}
	return out
}

// At returns the result of the burst placed at the given virtual offset.
func (r Result) At(at time.Duration) EventResult {
	for _, e := range r.Events {
		if e.At == at {
			return e
		}
	}
	return EventResult{Event: Event{At: at}}
}

// Replay fires every event in order, waiting for the virtual instant each one
// is placed at. Events are replayed on the calling goroutine, so within a
// single burst the requests are strictly ordered; use ConcurrentBurst to
// exercise contention.
func Replay(t testing.TB, clock *VirtualClock, l Limiter, events []Event) Result {
	t.Helper()

	ordered := append([]Event(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At < ordered[j].At })

	began := time.Now()
	res := Result{}
	for _, e := range ordered {
		clock.WaitUntil(t, e.At)
		if e.OnArrive != nil {
			e.OnArrive()
		}
		r := EventResult{Event: e}
		for i := 0; i < e.Burst; i++ {
			if l.Allow() {
				r.Allowed++
			} else {
				r.Denied++
			}
		}
		res.Events = append(res.Events, r)
		res.Allowed += r.Allowed
		res.Denied += r.Denied
	}
	res.Elapsed = time.Since(began)
	return res
}

// ConcurrentBurst fires n*workers requests from n goroutines released by a
// common start barrier, and reports how many were allowed.
func ConcurrentBurst(t testing.TB, l Limiter, workers, perWorker int) int {
	t.Helper()

	var ready, done sync.WaitGroup
	var allowed atomic.Int64
	ready.Add(workers)
	done.Add(workers)

	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			local := 0
			for j := 0; j < perWorker; j++ {
				if l.Allow() {
					local++
				}
			}
			allowed.Add(int64(local))
		}()
	}

	ready.Wait()
	close(start)
	done.Wait()
	return int(allowed.Load())
}

// ConstructWithin runs ctor in a goroutine and returns an error instead of
// blocking when it does not return within timeout.
func ConstructWithin[T any](timeout time.Duration, ctor func() T) (T, error) {
	type outcome struct {
		value T
	}
	ch := make(chan outcome, 1)
	go func() { ch <- outcome{value: ctor()} }()

	select {
	case got := <-ch:
		return got.value, nil
	case <-time.After(timeout):
		var zero T
		return zero, ErrConstructorBlocked
	}
}

// ErrConstructorBlocked is returned when a limiter constructor fails to return
// within the allotted time, which is what happens when the implementation
// forgets to spawn its background goroutine.
var ErrConstructorBlocked = errors.New("limitertest: constructor did not return within timeout")

// Construct runs a constructor and fails the test if it blocks for longer than
// timeout. Implementations that forget to spawn their background goroutine
// block here instead of deadlocking the whole test binary.
func Construct[T any](t testing.TB, timeout time.Duration, ctor func() T) T {
	t.Helper()

	value, err := ConstructWithin(timeout, ctor)
	if err != nil {
		t.Fatalf("constructor did not return within %s: it blocks the caller", timeout)
	}
	return value
}

// PanicsWithin reports whether ctor panics. Panics are recovered in a
// separate goroutine so a buggy constructor cannot take the test binary down
// with it.
func PanicsWithin(timeout time.Duration, ctor func()) (bool, error) {
	_, panicked, err := RecoverWithin(timeout, ctor)
	return panicked, err
}

// RecoverWithin runs ctor in a goroutine and returns the value it panicked
// with. Implementations validate their arguments by panicking with a message
// that names the offending parameter, so tests can assert on the message rather
// than only on the fact of the panic.
func RecoverWithin(timeout time.Duration, ctor func()) (recovered any, panicked bool, err error) {
	ch := make(chan any, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- r
			}
		}()
		ctor()
		ch <- nil
	}()

	select {
	case r := <-ch:
		return r, r != nil, nil
	case <-time.After(timeout):
		return nil, false, ErrConstructorBlocked
	}
}

// ConstructPanics fails the test unless ctor panics.
func ConstructPanics(t testing.TB, timeout time.Duration, ctor func()) {
	t.Helper()

	panicked, err := PanicsWithin(timeout, ctor)
	switch {
	case err != nil:
		t.Errorf("constructor neither returned nor panicked within %s: %v", timeout, err)
	case !panicked:
		t.Errorf("constructor returned normally, expected a panic")
	}
}

// ExpectPanicContaining fails the test unless ctor panics with a message that
// mentions want, which lets a test pin down that the guard complains about the
// argument the test actually passed.
func ExpectPanicContaining(t testing.TB, timeout time.Duration, want string, ctor func()) {
	t.Helper()

	recovered, panicked, err := RecoverWithin(timeout, ctor)
	switch {
	case err != nil:
		t.Errorf("constructor neither returned nor panicked within %s: %v", timeout, err)
		return
	case !panicked:
		t.Errorf("constructor returned normally, expected a panic mentioning %q", want)
		return
	}

	if msg := fmt.Sprint(recovered); !strings.Contains(msg, want) {
		t.Errorf("panic message = %q, want it to mention %q", msg, want)
	}
}

// Goroutines returns the current number of live goroutines.
func Goroutines() int { return runtime.NumGoroutine() }

// SettleGoroutines waits until the goroutine count drops to want, polling until
// the deadline, and reports the final count. A limiter whose background
// goroutine ignores context cancellation keeps the count high.
func SettleGoroutines(want int, within time.Duration) int {
	deadline := time.Now().Add(within)
	n := Goroutines()
	for time.Now().Before(deadline) {
		if n <= want {
			return n
		}
		time.Sleep(5 * time.Millisecond)
		n = Goroutines()
	}
	return n
}

// EqualInt fails unless got == want.
func EqualInt(t testing.TB, got, want int, what string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d, want %d", what, got, want)
	}
}

// BetweenInt fails unless min <= got <= max.
func BetweenInt(t testing.TB, got, min, max int, what string) {
	t.Helper()
	if got < min || got > max {
		t.Errorf("%s = %d, want within [%d, %d]", what, got, min, max)
	}
}
