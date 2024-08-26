package dblock

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type Lock struct {
	manager *Manager
	name    string

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex

	version uuid.UUID

	isReleased bool
}

func newLock(manager *Manager, name string) *Lock {
	ctx, cancel := context.WithCancel(manager.ctx)
	return &Lock{
		manager: manager,
		name:    name,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Close releases the lock and interrupts the locks heartbeat, if configured.
func (l *Lock) Close() error {
	err := l.manager.Release(l)
	return err
}

// IsReleased indicates whether the lock is either released or lost after
// heartbeat.
func (l *Lock) IsReleased() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isReleased
}

func (l *Lock) Version() uuid.UUID {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.version
}
