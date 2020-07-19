package grp

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// NewParallelFuncPool creates a new ParallelFuncPool.
func NewParallelFuncPool(numWorkers int) (*ParallelFuncPool, error) {
	if numWorkers < 1 {
		return nil, fmt.Errorf("number of workers cannot be less than 1")
	}

	return &ParallelFuncPool{
		pool:   make(chan struct{}, numWorkers),
		failed: make(chan error, 1),
		dead:   make(chan struct{}),
		wg:     &sync.WaitGroup{},
	}, nil
}

// ParallelFuncPool parallelizes a single task into smaller subtasks by
// queueing functions in goroutines which wait until the number of active
// workers decreases. If a single subtask fails, then Wait() returns
// a non-nil error, and any queued work is cancelled.
type ParallelFuncPool struct {
	pool   chan struct{}
	failed chan error
	d      int32
	dead   chan struct{}
	wg     *sync.WaitGroup
}

// QueueFunction queues a function for execution at some point in the future.
//
// This method is asynchronous.
func (o *ParallelFuncPool) QueueFunction(fn func() error) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		select {
		case o.pool <- struct{}{}:
			if atomic.LoadInt32(&o.d) > 0 {
				return
			}
			err := fn()
			if err != nil {
				select {
				case o.failed <- err:
					atomic.AddInt32(&o.d, 1)
					close(o.dead)
				default:
				}
				return
			}
			select {
			case <-o.pool:
			default:
			}
		case <-o.dead:
			return
		}
	}()
}

// Wait waits until either all of the queued subtasks complete, or a single
// subtask fails.
//
// This should only be called after queuing all possible subtasks.
func (o *ParallelFuncPool) Wait() error {
	o.wg.Wait()
	select {
	case err := <-o.failed:
		return err
	default:
		return nil
	}
}
