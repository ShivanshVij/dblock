// SPDX-License-Identifier: Apache-2.0

package dblock

import (
	"context"
	"database/sql"
	"errors"
	"strings"
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

	ErrNotAcquired  = errors.New("lock not acquired")
	ErrNotReleased  = errors.New("lock not released")
	ErrGettingOwner = errors.New("error getting owner")
	ErrGettingLock  = errors.New("error getting lock")

	ErrCreateTransaction = errors.New("cannot create transaction")
	ErrCommitTransaction = errors.New("cannot commit transaction")

	ErrSerializationError  = errors.New("serialization error")
	ErrRefreshLease        = errors.New("cannot refresh lease")
	ErrLockAlreadyReleased = errors.New("lock already released")
)

const PGQuery = `
INSERT INTO ` + lock.Table + `("` + lock.FieldID + `", "` + lock.FieldVersion + `", "` + lock.FieldOwner + `") 
VALUES($1, $2, $3) 
ON CONFLICT ("` + lock.FieldID + `") DO 
UPDATE SET "` + lock.FieldVersion + `" = $2, "` + lock.FieldOwner + `" = $3 
WHERE ` + lock.Table + `."` + lock.FieldVersion + `" IS NULL OR ` + lock.Table + `."` + lock.FieldVersion + `" = $4`

const SQLiteQuery = `
INSERT INTO ` + lock.Table + ` ("` + lock.FieldID + `", "` + lock.FieldVersion + `", "` + lock.FieldOwner + `")
VALUES( ?, ?, ? )
ON CONFLICT ("` + lock.FieldID + `") DO 
UPDATE SET "` + lock.FieldVersion + `" = ?, "` + lock.FieldOwner + `" = ? 
WHERE ` + lock.Table + `."` + lock.FieldVersion + `" IS NULL OR ` + lock.Table + `."` + lock.FieldVersion + `" = ?`

type DBLock struct {
	logger  types.Logger
	options *Options

	sql *ent.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(options *Options) (*DBLock, error) {
	var err error
	if err = options.Validate(); err != nil {
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

func (db *DBLock) Owner(id string) (string, error) {
	var _l *ent.Lock
	err := db.try(db.ctx, func() error { return db.tryOwner(&_l, id) })
	if err != nil {
		return "", err
	}
	return _l.Owner, nil
}

func (db *DBLock) Acquire(l *Lock, failIfLocked ...bool) error {
	var err error
	for {
		if err = l.ctx.Err(); err != nil {
			return ErrNotAcquired
		}
		err = db.try(l.ctx, func() error { return db.tryAcquire(l) })
		switch {
		case errors.Is(err, context.Canceled):
			return ErrNotAcquired
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
	l.mu.Lock()
	l.cancel()
	if !l.acquired {
		l.mu.Unlock()
		db.logger.Warn().Str("lock", l.id).Msg("lock already released")
		return ErrLockAlreadyReleased
	}
	l.mu.Unlock()
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

func (db *DBLock) tryOwner(_l **ent.Lock, id string) error {
	var _err error
	*_l, _err = db.getLock(db.ctx, nil, &Lock{id: id})
	if _err != nil {
		return db.serializeError(ErrGettingOwner, _err)
	}
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
	l.mu.Lock()
	defer l.mu.Unlock()

	ctx, cancel := context.WithTimeout(l.ctx, db.options.LeaseDuration)
	defer cancel()

	version := uuid.New()

	tx, err := db.sql.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Join(ErrCreateTransaction, err)
	}

	switch db.options.DBType {
	case Postgres:
		_, err = tx.ExecContext(ctx, PGQuery, l.id, version, db.options.Owner, l.version)
	case SQLite:
		_, err = tx.ExecContext(ctx, SQLiteQuery, l.id, version, db.options.Owner, version, db.options.Owner, l.version)
	default:
		return ErrInvalidDBType
	}
	if err != nil {
		_ = tx.Rollback()
		return db.serializeError(ErrNotAcquired, err)
	}

	_l, err := db.getLock(ctx, tx, l)
	if err != nil {
		_ = tx.Rollback()
		return db.serializeError(ErrNotAcquired, err)
	}

	if _l.Version != version {
		l.version = _l.Version
		_ = tx.Rollback()
		return ErrNotAcquired
	}
	if err = tx.Commit(); err != nil {
		return db.serializeError(ErrCommitTransaction, err)
	}
	l.version = version
	l.acquired = true
	return nil
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
		return db.serializeError(ErrNotReleased, err)
	}
	if affected == 0 {
		l.acquired = false
		l.closeNotify()
		_ = tx.Rollback()
		return ErrLockAlreadyReleased
	}
	_, err = tx.Lock.
		Delete().
		Where(lock.And(lock.ID(l.id), lock.VersionIsNil())).
		Exec(ctx)
	if err != nil {
		_ = tx.Rollback()
		return db.serializeError(ErrNotReleased, err)
	}
	if err = tx.Commit(); err != nil {
		return db.serializeError(ErrCommitTransaction, err)
	}
	l.acquired = false
	l.cancel()
	l.closeNotify()
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
		return db.serializeError(ErrRefreshLease, err)
	}
	if affected == 0 {
		db.logger.Warn().Str("lock", l.id).Msg("lease lost")
		_ = tx.Rollback()
		return ErrLockAlreadyReleased
	}
	if err = tx.Commit(); err != nil {
		return db.serializeError(ErrCommitTransaction, err)
	}
	l.version = version
	return nil
}

func (db *DBLock) leaseRefresh(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.acquired {
		return ErrLockAlreadyReleased
	}
	err := db.try(l.ctx, func() error { return db.storeLease(l) })
	if err != nil {
		l.acquired = false
		l.closeNotify()
		return errors.Join(ErrRefreshLease, err)
	}
	return nil
}

func (db *DBLock) getLock(ctx context.Context, tx *ent.Tx, l *Lock) (_l *ent.Lock, err error) {
	switch db.options.DBType {
	case Postgres:
		if tx == nil {
			_l, err = db.sql.Lock.Query().Where(lock.ID(l.id)).Modify(func(s *entSQL.Selector) { s.ForUpdate() }).Only(ctx)
		} else {
			_l, err = tx.Lock.Query().Where(lock.ID(l.id)).Modify(func(s *entSQL.Selector) { s.ForUpdate() }).Only(ctx)
		}
	case SQLite:
		if tx == nil {
			_l, err = db.sql.Lock.Query().Where(lock.ID(l.id)).Only(ctx)
		} else {
			_l, err = tx.Lock.Query().Where(lock.ID(l.id)).Only(ctx)
		}
	default:
		return nil, errors.Join(ErrGettingLock, ErrInvalidDBType)
	}
	if err != nil {
		err = errors.Join(ErrGettingLock, err)
	}
	return
}

func (db *DBLock) serializeError(expected error, err error) error {
	switch db.options.DBType {
	case Postgres:
		if strings.Contains(err.Error(), "(SQLSTATE 40001)") {
			return errors.Join(expected, ErrSerializationError, err)
		}
	case SQLite:
		if strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database table is locked") {
			return errors.Join(expected, ErrSerializationError, err)
		}
	default:
		return ErrInvalidDBType
	}
	return errors.Join(expected, err)
}

func (db *DBLock) try(ctx context.Context, do func() error) error {
	retryPeriod := db.options.LeaseRefreshFrequency
	var err error
	for {
		err = do()
		if err == nil || ctx.Err() != nil || !errors.Is(err, ErrSerializationError) {
			break
		}
		db.logger.Warn().Err(err).Msg("invalid transaction, retrying")
		utils.Wait(ctx, retryPeriod)
	}
	return err
}
