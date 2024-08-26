// SPDX-License-Identifier: Apache-2.0

package dblock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"entgo.io/ent/dialect"
	entSQL "entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/loopholelabs/logging/types"

	"github.com/shivanshvij/dblock/ent"
	"github.com/shivanshvij/dblock/ent/lock"
	"github.com/shivanshvij/dblock/pkg/utils"
)

var (
	ErrInvalidOptions    = errors.New("invalid options")
	ErrOpeningDatabase   = errors.New("failed opening connection to database")
	ErrRunningMigrations = errors.New("failed running database migrations")
	ErrClosingDatabase   = errors.New("failed closing connection to database")

	ErrNotAcquired = errors.New("lock not acquired")
	ErrNotReleased = errors.New("lock not released")

	ErrCreateTransaction = errors.New("cannot create transaction")
	ErrCommitTransaction = errors.New("cannot commit transaction")

	ErrRefreshLease        = errors.New("cannot refresh lease")
	ErrLockAlreadyReleased = errors.New("lock already released")
)

const maxRetries = 1024

var versionIsNull = entSQL.P().Append(func(b *entSQL.Builder) {
	b.WriteString(fmt.Sprintf(`"%s"."%s" IS NULL`, lock.Table, lock.FieldVersion))
})

type DBLock struct {
	logger  types.SubLogger
	options *Options

	sql *ent.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(options *Options) (*DBLock, error) {
	var err error
	if err = options.validate(); err != nil {
		return nil, errors.Join(ErrInvalidOptions, err)
	}

	logger := options.Logger.SubLogger("dblock").With().Str("owner", options.Owner).Logger()
	logger.Debug().Msg("connecting to database")

	var db *sql.DB
	var kind string
	switch options.DBType {
	case Postgres:
		kind = dialect.Postgres
		db, err = sql.Open("pgx", options.DatabaseURL)
	default:
		return nil, ErrInvalidDBType
	}
	if err != nil {
		return nil, errors.Join(ErrOpeningDatabase, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sqlClient := ent.NewClient(ent.Driver(entSQL.OpenDB(kind, db)))
	logger.Debug().Msg("running database migrations")
	err = sqlClient.Schema.Create(ctx)
	if err != nil {
		cancel()
		_ = sqlClient.Close()
		return nil, errors.Join(ErrRunningMigrations, err)
	}
	logger.Debug().Msg("database migrations complete")

	return &DBLock{
		logger:  logger,
		options: options,
		sql:     sqlClient,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (m *DBLock) Lock(id string) *Lock {
	return newLock(m, id)
}

func (m *DBLock) Acquire(l *Lock, failIfLocked ...bool) error {
	var err error
	for {
		if err = l.ctx.Err(); err != nil {
			return ErrNotAcquired
		}
		err = m.try(l.ctx, func() error { return m.tryAcquire(l) })
		switch {
		case len(failIfLocked) > 0 && failIfLocked[0] && errors.Is(err, ErrNotAcquired):
			return err
		case errors.Is(err, ErrNotAcquired):
			utils.Wait(l.ctx, m.options.LeaseDuration)
			continue
		case err != nil:
			return err
		}
		return nil
	}
}

func (m *DBLock) Release(l *Lock) error {
	l.cancel()
	l.wg.Wait()
	err := m.try(m.ctx, func() error { return m.storeRelease(l) })
	return err
}

func (m *DBLock) Stop() error {
	m.cancel()
	m.wg.Wait()
	err := m.sql.Close()
	if err != nil {
		return errors.Join(ErrClosingDatabase, err)
	}
	return nil
}

func (m *DBLock) tryAcquire(l *Lock) error {
	err := m.storeAcquire(l)
	if err != nil {
		return err
	}
	l.wg.Add(1)
	m.wg.Add(1)
	go func() {
		defer l.cancel()
		defer l.wg.Done()
		defer m.wg.Done()
		defer m.logger.Debug().Str("lock", l.id).Msg("lease refresh stopped")
		m.logger.Debug().Str("lock", l.id).Msg("lease refresh started")
		for {
			if err := m.leashRefresh(l); err != nil {
				m.logger.Error().Err(err).Str("lock", l.id).Msg("lease refresh failed")
				return
			} else if err := l.ctx.Err(); err != nil {
				return
			}
			m.logger.Debug().Str("lock", l.id).Msg("lease refreshed")
			utils.Wait(l.ctx, m.options.LeaseRefreshFrequency)
		}
	}()
	return nil
}

func (m *DBLock) storeAcquire(l *Lock) error {
	ctx, cancel := context.WithTimeout(l.ctx, m.options.LeaseDuration)
	defer cancel()

	version := uuid.New()

	tx, err := m.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Join(ErrCreateTransaction, err)
	}

	var versionIsEQ = entSQL.P().Append(func(b *entSQL.Builder) {
		b.WriteString(fmt.Sprintf(`"%s"."%s" = `, lock.Table, lock.FieldVersion))
		b.Arg(l.version)
	})

	err = tx.Lock.
		Create().
		SetID(l.id).
		SetVersion(version).
		SetOwner(m.options.Owner).
		OnConflict(
			entSQL.ConflictColumns(lock.FieldID),
			entSQL.ResolveWithNewValues(),
			entSQL.UpdateWhere(entSQL.Or(versionIsNull, versionIsEQ)),
		).
		UpdateNewValues().
		Exec(ctx)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(ErrNotAcquired, err)
	}

	_l, err := tx.Lock.Query().Where(lock.ID(l.id)).Modify(func(s *entSQL.Selector) { s.ForUpdate() }).Only(ctx)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(ErrNotAcquired, err)
	}

	if _l.Version != version {
		l.version = _l.Version
		_ = tx.Rollback()
		return ErrNotAcquired
	}
	if err = tx.Commit(); err != nil {
		return errors.Join(ErrCommitTransaction, err)
	}
	l.version = version
	return nil
}

func (m *DBLock) storeRelease(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithTimeout(m.ctx, m.options.LeaseDuration)
	defer cancel()
	tx, err := m.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Join(ErrCreateTransaction, err)
	}
	affected, err := tx.Lock.
		Update().
		ClearVersion().
		Where(lock.And(lock.IDEQ(l.id), lock.Version(l.version))).
		Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(ErrNotReleased, err)
	}
	if affected == 0 {
		l.isReleased = true
		_ = tx.Rollback()
		return ErrLockAlreadyReleased
	}
	_, err = tx.Lock.
		Delete().
		Where(lock.And(lock.ID(l.id), lock.VersionIsNil())).
		Exec(ctx)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(ErrNotReleased, err)
	}
	if err = tx.Commit(); err != nil {
		return errors.Join(ErrCommitTransaction, err)
	}
	l.isReleased = true
	l.cancel()
	return nil
}

func (m *DBLock) storeLease(l *Lock) error {
	ctx, cancel := context.WithTimeout(l.ctx, m.options.LeaseDuration)
	defer cancel()
	version := uuid.New()

	tx, err := m.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Join(ErrCreateTransaction, err)
	}

	affected, err := tx.Lock.
		Update().
		SetVersion(version).
		Where(lock.And(lock.ID(l.id)), lock.Version(l.version)).
		Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(ErrRefreshLease, err)
	}
	if affected == 0 {
		m.logger.Warn().Str("lock", l.id).Msg("lease lost")
		_ = tx.Rollback()
		return ErrLockAlreadyReleased
	}
	if err = tx.Commit(); err != nil {
		return errors.Join(ErrCommitTransaction, err)
	}
	l.version = version
	return nil
}

func (m *DBLock) leashRefresh(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isReleased {
		return ErrLockAlreadyReleased
	}
	err := m.try(l.ctx, func() error { return m.storeLease(l) })
	if err != nil {
		l.isReleased = true
		return errors.Join(ErrRefreshLease, err)
	}
	return nil
}

func (m *DBLock) try(ctx context.Context, do func() error) error {
	retryPeriod := m.options.LeaseRefreshFrequency
	var err error
	for i := 0; i < maxRetries; i++ {
		err = do()
		if err == nil || ctx.Err() != nil {
			break
		}
		m.logger.Warn().Err(err).Msg("invalid transaction, retrying")
		utils.Wait(ctx, retryPeriod)
	}
	return err
}
