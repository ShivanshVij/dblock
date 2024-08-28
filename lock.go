// SPDX-License-Identifier: Apache-2.0

package dblock

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

const (
	notifyStateOpen int32 = iota
	notifyStateClosed
)

type Lock struct {
	db *DBLock
	id string

	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	notifyState atomic.Int32
	notify      chan struct{}

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
		notify: make(chan struct{}),
	}
}

func (l *Lock) ID() string {
	return l.id
}

func (l *Lock) Acquire(failIfLocked ...bool) error {
	return l.db.Acquire(l, failIfLocked...)
}

func (l *Lock) Release() error {
	return l.db.Release(l)
}

func (l *Lock) Notify() <-chan struct{} {
	return l.notify
}

func (l *Lock) closeNotify() {
	if l.notifyState.CompareAndSwap(notifyStateOpen, notifyStateClosed) {
		close(l.notify)
	}
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
