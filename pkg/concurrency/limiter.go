package concurrency

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrLimiterTimeout = errors.New("limiter: request timed out waiting for slot")
	ErrLimiterFull    = errors.New("limiter: capacity full")
)

// Limiter restricts the number of concurrent operations using a buffered channel semaphore.
type Limiter struct {
	sem chan struct{}
}

// NewLimiter creates a new concurrency Limiter with the given maximum concurrent slots.
func NewLimiter(maxConcurrent int) *Limiter {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	return &Limiter{
		sem: make(chan struct{}, maxConcurrent),
	}
}

// Acquire acquires a slot. If context is cancelled or times out, it returns the context error.
func (l *Limiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrLimiterTimeout, ctx.Err())
	case l.sem <- struct{}{}:
		return nil
	}
}

// TryAcquire attempts to acquire a slot without blocking. Returns true if acquired.
func (l *Limiter) TryAcquire() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a previously acquired slot.
func (l *Limiter) Release() {
	select {
	case <-l.sem:
	default:
	}
}

// Do executes the callback function while holding a slot.
func (l *Limiter) Do(ctx context.Context, fn func(context.Context) error) error {
	if err := l.Acquire(ctx); err != nil {
		return err
	}
	defer l.Release()

	return fn(ctx)
}
