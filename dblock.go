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
	_ "github.com/mattn/go-sqlite3"

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
	case SQLite:
		kind = dialect.SQLite
		db, err = sql.Open("sqlite3", options.DatabaseURL)
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

func (db *DBLock) Lock(id string) *Lock {
	return newLock(db, id)
}

func (db *DBLock) Acquire(l *Lock, failIfLocked ...bool) error {
	var err error
	for {
		if err = l.ctx.Err(); err != nil {
			return ErrNotAcquired
		}
		err = db.try(l.ctx, func() error { return db.tryAcquire(l) })
		switch {
		case len(failIfLocked) > 0 && failIfLocked[0] && errors.Is(err, ErrNotAcquired):
			return err
		case errors.Is(err, ErrNotAcquired):
			utils.Wait(l.ctx, db.options.LeaseDuration)
			continue
		case err != nil:
			return err
		}
		return nil
	}
}

func (db *DBLock) Release(l *Lock) error {
	l.cancel()
	l.wg.Wait()
	err := db.try(db.ctx, func() error { return db.storeRelease(l) })
	return err
}

func (db *DBLock) Stop() error {
	db.cancel()
	db.wg.Wait()
	err := db.sql.Close()
	if err != nil {
		return errors.Join(ErrClosingDatabase, err)
	}
	return nil
}

func (db *DBLock) tryAcquire(l *Lock) error {
	err := db.storeAcquire(l)
	if err != nil {
		return err
	}
	l.wg.Add(1)
	db.wg.Add(1)
	go db.doLeaseRefresh(l)
	return nil
}

func (db *DBLock) doLeaseRefresh(l *Lock) {
	defer l.cancel()
	defer l.wg.Done()
	defer db.wg.Done()
	defer db.logger.Debug().Str("lock", l.id).Msg("lease refresh stopped")
	db.logger.Debug().Str("lock", l.id).Msg("lease refresh started")
	for {
		if err := db.leaseRefresh(l); err != nil {
			db.logger.Error().Err(err).Str("lock", l.id).Msg("lease refresh failed")
			return
		} else if err := l.ctx.Err(); err != nil {
			return
		}
		db.logger.Debug().Str("lock", l.id).Msg("lease refreshed")
		utils.Wait(l.ctx, db.options.LeaseRefreshFrequency)
	}
}

func (db *DBLock) storeAcquire(l *Lock) error {
	ctx, cancel := context.WithTimeout(l.ctx, db.options.LeaseDuration)
	defer cancel()

	version := uuid.New()

	tx, err := db.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Join(ErrCreateTransaction, err)
	}

	var versionIsNull = entSQL.P().Append(func(b *entSQL.Builder) {
		b.WriteString(fmt.Sprintf(`"%s"."%s" IS NULL`, lock.Table, lock.FieldVersion))
	})

	var versionIsEQ = entSQL.P().Append(func(b *entSQL.Builder) {
		b.WriteString(fmt.Sprintf(`"%s"."%s" = `, lock.Table, lock.FieldVersion))
		b.Arg(l.version)
	})

	err = tx.Lock.
		Create().
		SetID(l.id).
		SetVersion(version).
		SetOwner(db.options.Owner).
		OnConflict(
			entSQL.ConflictColumns(lock.FieldID),
			entSQL.ResolveWithNewValues(),
			entSQL.UpdateWhere(entSQL.Or(versionIsNull, versionIsEQ)),
		).
		Exec(ctx)
	if err != nil {
		_l, refreshErr := db.getLock(ctx, tx, l)
		if refreshErr != nil {
			_ = tx.Rollback()
			return errors.Join(ErrNotAcquired, err, refreshErr)
		}
		l.version = _l.Version
		_ = tx.Rollback()
		return errors.Join(ErrNotAcquired, err)
	}

	var _l *ent.Lock
	_l, err = db.getLock(ctx, tx, l)
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

func (db *DBLock) getLock(ctx context.Context, tx *ent.Tx, l *Lock) (_l *ent.Lock, err error) {
	switch db.options.DBType {
	case Postgres:
		_l, err = tx.Lock.Query().Where(lock.ID(l.id)).Modify(func(s *entSQL.Selector) { s.ForUpdate() }).Only(ctx)
	case SQLite:
		_l, err = tx.Lock.Query().Where(lock.ID(l.id)).Only(ctx)
	default:
		return nil, ErrInvalidDBType
	}
	return
}

func (db *DBLock) storeRelease(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithTimeout(db.ctx, db.options.LeaseDuration)
	defer cancel()
	tx, err := db.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
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

func (db *DBLock) storeLease(l *Lock) error {
	ctx, cancel := context.WithTimeout(l.ctx, db.options.LeaseDuration)
	defer cancel()
	version := uuid.New()

	tx, err := db.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
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
		db.logger.Warn().Str("lock", l.id).Msg("lease lost")
		_ = tx.Rollback()
		return ErrLockAlreadyReleased
	}
	if err = tx.Commit(); err != nil {
		return errors.Join(ErrCommitTransaction, err)
	}
	l.version = version
	return nil
}

func (db *DBLock) leaseRefresh(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isReleased {
		return ErrLockAlreadyReleased
	}
	err := db.try(l.ctx, func() error { return db.storeLease(l) })
	if err != nil {
		l.isReleased = true
		return errors.Join(ErrRefreshLease, err)
	}
	return nil
}

func (db *DBLock) try(ctx context.Context, do func() error) error {
	retryPeriod := db.options.LeaseRefreshFrequency
	var err error
	for i := 0; i < maxRetries; i++ {
		err = do()
		if err == nil || ctx.Err() != nil || errors.Is(err, ErrLockAlreadyReleased) {
			break
		}
		db.logger.Warn().Err(err).Msg("invalid transaction, retrying")
		utils.Wait(ctx, retryPeriod)
	}
	return err
}
