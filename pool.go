package grp

import (
	"fmt"
	"sync"
	"sync/atomic"
)

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

// It is possible for a job to run even when the pool is considered dead.
// This is because
type ParallelFuncPool struct {
	pool   chan struct{}
	failed chan error
	d      int32
	dead   chan struct{}
	wg     *sync.WaitGroup
}

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

// Wait waits until the queued jobs complete.
func (o *ParallelFuncPool) Wait() error {
	o.wg.Wait()
	select {
	case err := <-o.failed:
		return err
	default:
		return nil
	}
}
