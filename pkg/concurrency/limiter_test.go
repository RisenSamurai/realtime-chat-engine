package concurrency

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiter_Capacity(t *testing.T) {
	limiter := NewLimiter(3)

	var currentConcurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := limiter.Do(ctx, func(execCtx context.Context) error {
				curr := currentConcurrent.Add(1)
				for {
					oldMax := maxConcurrent.Load()
					if curr <= oldMax || maxConcurrent.CompareAndSwap(oldMax, curr) {
						break
					}
				}
				time.Sleep(30 * time.Millisecond)
				currentConcurrent.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	if maxConcurrent.Load() > 3 {
		t.Fatalf("expected max concurrent <= 3, got %d", maxConcurrent.Load())
	}
}

func TestLimiter_Timeout(t *testing.T) {
	limiter := NewLimiter(1)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()

	if err := limiter.Acquire(ctx1); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Второй acquire с коротким таймаутом должен завершиться с ошибкой
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	err := limiter.Acquire(ctx2)
	if err == nil {
		t.Fatal("expected timeout error on second acquire, got nil")
	}

	limiter.Release()

	// Теперь снова должен браться слот
	if !limiter.TryAcquire() {
		t.Fatal("expected TryAcquire to succeed after Release")
	}
	limiter.Release()
}
