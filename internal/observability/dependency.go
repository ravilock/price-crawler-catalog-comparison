package observability

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Dependency continuously rechecks a bounded dependency probe.
type Dependency struct {
	Check                  func(context.Context) error
	InitialDelay, MaxDelay time.Duration
	ready                  atomic.Bool
	mu                     sync.RWMutex
	err                    error
}

func (d *Dependency) Ready() bool      { return d.ready.Load() }
func (d *Dependency) LastError() error { d.mu.RLock(); defer d.mu.RUnlock(); return d.err }
func (d *Dependency) Run(ctx context.Context, firstReady chan<- struct{}) {
	initial, maximum := d.InitialDelay, d.MaxDelay
	if initial <= 0 {
		initial = time.Second
	}
	if maximum < initial {
		maximum = initial
	}
	backoff, first := initial, true
	defer d.ready.Store(false)
	for ctx.Err() == nil {
		probe, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := d.Check(probe)
		cancel()
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
		d.ready.Store(err == nil)
		if err == nil {
			if first {
				close(firstReady)
				first = false
			}
			backoff = initial
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if err != nil {
			backoff *= 2
			if backoff > maximum {
				backoff = maximum
			}
		}
	}
}
