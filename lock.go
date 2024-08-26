package dblock

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type Lock struct {
	db *DBLock
	id string

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex

	version uuid.UUID

	isReleased bool
}

func newLock(db *DBLock, id string) *Lock {
	ctx, cancel := context.WithCancel(db.ctx)
	return &Lock{
		db:     db,
		id:     id,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (l *Lock) Close() error {
	err := l.db.Release(l)
	return err
}

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
