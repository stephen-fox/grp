package grp

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestNewParallelFuncPool(t *testing.T) {
	numWorkers := between(2, 20, t)
	_, err := NewParallelFuncPool(numWorkers)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = NewParallelFuncPool(1)
	if err != nil {
		t.Fatalf("1 worker pool returned non-nil error - got %v", err)
	}

	_, err = NewParallelFuncPool(0)
	if err == nil {
		t.Fatal("0 worker pool should have returned non-nil error")
	}

	_, err = NewParallelFuncPool(-1)
	if err == nil {
		t.Fatal("-1 worker pool should have returned non-nil error")
	}
}

func TestParallelFuncPool_QueueFunction(t *testing.T) {
	numWorkers := between(4, 20, t)
	pool, err := NewParallelFuncPool(numWorkers)
	if err != nil {
		t.Fatal(err.Error())
	}

	releaseBad := &sync.WaitGroup{}
	releaseBad.Add(1)

	ready := &sync.WaitGroup{}
	ready.Add(numWorkers)

	completed := &sync.WaitGroup{}
	completed.Add(numWorkers)

	fn := func() error {
		defer completed.Done()
		ready.Done()
		releaseBad.Wait()
		return nil
	}

	for i := 0; i < numWorkers; i++ {
		pool.QueueFunction(fn)
	}

	ready.Wait()
	releaseBad.Done()

	err = pool.Wait()
	if err != nil {
		t.Fatalf("wait did not produce an error - got %v", err)
	}

	completedDone := make(chan struct{})
	go func() {
		completed.Wait()
		completedDone <- struct{}{}
	}()

	select {
	case <-completedDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("functions did not complete within the alotted time")
	}
}

func TestParallelFuncPool_QueueFunction_DeadOnError(t *testing.T) {
	numWorkers := between(4, 20, t)
	pool, err := NewParallelFuncPool(numWorkers)
	if err != nil {
		t.Fatal(err.Error())
	}

	releaseBad := &sync.WaitGroup{}
	releaseBad.Add(1)

	ready := &sync.WaitGroup{}
	ready.Add(numWorkers)

	completed := &sync.WaitGroup{}
	completed.Add(numWorkers)

	badFn := func() error {
		defer completed.Done()
		ready.Done()
		releaseBad.Wait()
		return fmt.Errorf("foo")
	}

	for i := 0; i < numWorkers; i++ {
		pool.QueueFunction(badFn)
	}

	ready.Wait()
	releaseBad.Done()

	err = pool.Wait()
	if err == nil {
		t.Fatalf("wait did not produce an error")
	}

	select {
	case _, open := <-pool.dead:
		if open {
			t.Fatalf("dead channel should be closed, got %v", open)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("failed to read from 'dead' channel in alotted time")
	}

	completedDone := make(chan struct{})
	go func() {
		completed.Wait()
		completedDone <- struct{}{}
	}()

	select {
	case <-completedDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("functions did not complete within the alotted time")
	}
}

func TestParallelFuncPool_QueueFunction_PoolIsDead(t *testing.T) {
	numWorkers := between(4, 20, t)
	pool, err := NewParallelFuncPool(numWorkers)
	if err != nil {
		t.Fatal(err.Error())
	}

	done := make(chan struct{})
	pool.QueueFunction(func() error {
		defer func() {
			done <- struct{}{}
		}()
		return fmt.Errorf("foo")
	})
	<-done

	shouldNotBeClosed := make(chan struct{})

	for i := 0; i < between(4, 20, t); i++ {
		pool.QueueFunction(func() error {
			close(shouldNotBeClosed)
			return nil
		})
	}

	select {
	case <-shouldNotBeClosed:
		t.Fatal("channel was closed - it should not be closed")
	default:
	}
}

func between(atLeast int, atMost int, t *testing.T) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	iterations := 10
	for i := 0; i < iterations; i++ {
		z := r.Intn(atMost)
		if z > atLeast {
			return z
		}
	}

	t.Fatalf("failed to generate a number between %d and %d after %d iterations",
		atLeast, atMost, iterations)

	return 0
}
