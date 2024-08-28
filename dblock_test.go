// SPDX-License-Identifier: Apache-2.0

package dblock

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/loopholelabs/logging"
)

const leaseDuration = time.Millisecond * 100

func setupPostgres(tb testing.TB, pgContainer *postgres.PostgresContainer, name ...string) (*DBLock, *postgres.PostgresContainer) {
	ctx := context.Background()

	dbName := tb.Name()
	if len(name) > 0 && len(name[0]) > 0 {
		dbName = name[0]
	}

	var err error
	if pgContainer == nil {
		pgContainer, err = postgres.Run(ctx, "postgres:15.3-alpine",
			postgres.WithDatabase(dbName),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(5*time.Second)),
		)
		require.NoError(tb, err)
		tb.Cleanup(func() {
			err := pgContainer.Terminate(ctx)
			require.NoError(tb, err)
		})
	}

	connStr, err := pgContainer.ConnectionString(context.Background(), "sslmode=disable")
	require.NoError(tb, err)

	db, err := New(&Options{
		Logger:        logging.Test(tb, logging.Zerolog, dbName),
		Owner:         dbName,
		DBType:        Postgres,
		DatabaseURL:   connStr,
		LeaseDuration: leaseDuration,
	})
	require.NoError(tb, err)

	return db, pgContainer
}

func setupSQLite(tb testing.TB, name ...string) *DBLock {
	connStr := fmt.Sprintf("file:%s/%s?cache=shared&_fk=1", tb.TempDir(), tb.Name())

	dbName := tb.Name()
	if len(name) > 0 && len(name[0]) > 0 {
		dbName = name[0]
	}

	db, err := New(&Options{
		Logger:        logging.Test(tb, logging.Zerolog, dbName),
		Owner:         dbName,
		DBType:        SQLite,
		DatabaseURL:   connStr,
		LeaseDuration: leaseDuration,
	})
	require.NoError(tb, err)

	return db
}

func testSingleWriter(db *DBLock) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("lost lease", func(t *testing.T) {
			l := db.Lock(t.Name())
			err := db.Acquire(l)
			require.NoError(t, err)

			err = db.Release(l)
			require.NoError(t, err)
		})
		t.Run("version update", func(t *testing.T) {
			l := db.Lock(t.Name())
			err := db.Acquire(l)
			require.NoError(t, err)

			version := l.Version()

			time.Sleep(db.options.LeaseDuration)

			assert.NotEqual(t, version, l.Version())

			err = db.Release(l)
			require.NoError(t, err)
		})

		t.Run("race", func(t *testing.T) {
			l := db.Lock(t.Name())
			err := db.Acquire(l)
			require.NoError(t, err)

			var wg sync.WaitGroup
			done := make(chan struct{})
			wg.Add(1)
			releaseErr := make(chan error, 1)
			go func() {
				defer wg.Done()
				if err := db.Release(l); err != nil {
					releaseErr <- err
					return
				}
				close(done)
			}()

			select {
			case err := <-releaseErr:
				t.Fatal("unexpected error while releasing lock:", err)
			case <-time.After(5 * time.Second):
				t.Fatal("deadlock between lease refresh and release")
			case <-done:
			}

			wg.Wait()
		})
	}
}

func TestSingleWriter(t *testing.T) {
	pgDB, _ := setupPostgres(t, nil)
	t.Cleanup(func() { require.NoError(t, pgDB.Stop()) })

	t.Run("postgres", testSingleWriter(pgDB))

	//sqliteDB := setupSQLite(t)
	//t.Cleanup(func() { require.NoError(t, sqliteDB.Stop()) })
	//
	//t.Run("sqlite", testSingleWriter(sqliteDB))
}

func testContention(numClients int, DBs []*DBLock) func(t *testing.T) {

	return func(t *testing.T) {
		require.Equal(t, numClients, len(DBs))

		acquired := make(chan struct{})
		acquire := func(client int, db *DBLock) {
			db.logger.Debug().Str("lock", t.Name()).Int("client", client).Msg("client acquiring lock")
			l := db.Lock(t.Name())
			err := db.Acquire(l)
			require.NoError(t, err)
			db.logger.Debug().Str("lock", t.Name()).Int("client", client).Msg("client acquired lock")

			acquired <- struct{}{}

			time.Sleep(db.options.LeaseDuration)

			db.logger.Debug().Str("lock", t.Name()).Int("client", client).Msg("client releasing lock")
			err = l.Release()
			require.NoError(t, err)
		}

		var wg sync.WaitGroup
		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				acquire(client, DBs[i])
			}(i)
		}

		for i := 0; i < numClients; i++ {
			select {
			case <-acquired:
			case <-time.After(leaseDuration * time.Duration(numClients)):
				t.Fatal("timed out waiting for lock acquisition")
			}
		}

		wg.Wait()
	}
}

func TestContention(t *testing.T) {
	const numClients = 3

	var pgContainer *postgres.PostgresContainer
	pgDBs := make([]*DBLock, numClients)
	pgDBs[0], pgContainer = setupPostgres(t, nil, fmt.Sprintf("client-0"))
	t.Cleanup(func() { require.NoError(t, pgDBs[0].Stop()) })
	for i := 1; i < numClients; i++ {
		pgDBs[i], _ = setupPostgres(t, pgContainer, fmt.Sprintf("client-%d", i))
		t.Cleanup(func() { require.NoError(t, pgDBs[i].Stop()) })
	}
	t.Run("postgres", testContention(numClients, pgDBs))

	//sqliteDBs := make([]*DBLock, numClients)
	//sqliteDBs[0] = setupSQLite(t, fmt.Sprintf("client-0"))
	//t.Cleanup(func() { require.NoError(t, sqliteDBs[0].Stop()) })
	//for i := 1; i < numClients; i++ {
	//	sqliteDBs[i] = setupSQLite(t, fmt.Sprintf("client-%d", i))
	//	t.Cleanup(func() { require.NoError(t, sqliteDBs[i].Stop()) })
	//}
	//t.Run("sqlite", testContention(numClients, sqliteDBs))
}

func testFlakyConnection(numClients int, DBs []*DBLock) func(t *testing.T) {
	return func(t *testing.T) {
		require.Equal(t, numClients, len(DBs))

		flakyAcquired := make(chan struct{})
		normalAcquired := make(chan struct{})

		flakyAcquire := func(client int, db *DBLock) {
			l := db.Lock(t.Name())
			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("flaky client acquiring lock")
			err := db.Acquire(l)
			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("flaky client acquired lock")
			require.NoError(t, err)

			flakyAcquired <- struct{}{}

			time.Sleep(db.options.LeaseDuration)

			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("flaky client releasing lock")

			l.cancel()
			l.wg.Wait()

			time.Sleep(db.options.LeaseDuration)

			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("flaky client attempting to retake ownership of lock")
			l.ctx, l.cancel = context.WithCancel(db.ctx)

			l.wg.Add(1)
			db.wg.Add(1)
			go db.doLeaseRefresh(l)

			time.Sleep(db.options.LeaseDuration)

			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("flaky client attempting to release lock")

			err = db.Release(l)
			require.ErrorIs(t, err, ErrLockAlreadyReleased)
		}

		normalAcquire := func(client int, db *DBLock) {
			l := db.Lock(t.Name())
			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("normal client acquiring lock")
			err := db.Acquire(l)
			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("normal client acquired lock")
			require.NoError(t, err)

			normalAcquired <- struct{}{}

			time.Sleep(db.options.LeaseDuration)

			db.logger.Debug().Str("lock", l.ID()).Int("client", client).Msg("normal client releasing lock")
			err = db.Release(l)
			require.NoError(t, err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			flakyAcquire(0, DBs[0])
		}()

		select {
		case <-flakyAcquired:
		case <-time.After(leaseDuration * 2):
			t.Fatal("timed out waiting for flaky lock acquisition")
		}

		for i := 1; i < numClients; i++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				normalAcquire(client, DBs[i])
			}(i)
		}

		for i := 1; i < numClients; i++ {
			select {
			case <-normalAcquired:
			case <-time.After(leaseDuration * 3):
				t.Fatal("timed out waiting for normal lock acquisition")
			}
		}

		wg.Wait()
	}
}

func TestFlakyConnection(t *testing.T) {
	const numClients = 3

	var pgContainer *postgres.PostgresContainer
	pgDBs := make([]*DBLock, numClients)
	pgDBs[0], pgContainer = setupPostgres(t, nil, fmt.Sprintf("client-0"))
	t.Cleanup(func() { require.NoError(t, pgDBs[0].Stop()) })
	for i := 1; i < numClients; i++ {
		pgDBs[i], _ = setupPostgres(t, pgContainer, fmt.Sprintf("client-%d", i))
		t.Cleanup(func() { require.NoError(t, pgDBs[i].Stop()) })
	}

	t.Run("postgres", testFlakyConnection(numClients, pgDBs))

	//sqliteDBs := make([]*DBLock, numClients)
	//sqliteDBs[0] = setupSQLite(t, fmt.Sprintf("client-0"))
	//t.Cleanup(func() { require.NoError(t, sqliteDBs[0].Stop()) })
	//for i := 1; i < numClients; i++ {
	//	sqliteDBs[i] = setupSQLite(t, fmt.Sprintf("client-%d", i))
	//	t.Cleanup(func() { require.NoError(t, sqliteDBs[i].Stop()) })
	//}
	//
	//t.Run("sqlite", testFlakyConnection(numClients, sqliteDBs))
}

func TestManyClients(t *testing.T) {
	const numClients = 64

	var pgContainer *postgres.PostgresContainer
	pgDBs := make([]*DBLock, numClients)
	pgDBs[0], pgContainer = setupPostgres(t, nil, fmt.Sprintf("client-0"))
	t.Cleanup(func() { require.NoError(t, pgDBs[0].Stop()) })
	for i := 1; i < numClients; i++ {
		pgDBs[i], _ = setupPostgres(t, pgContainer, fmt.Sprintf("client-%d", i))
		t.Cleanup(func() { require.NoError(t, pgDBs[i].Stop()) })
	}
	t.Run("postgres", testContention(numClients, pgDBs))

	//sqliteDBs := make([]*DBLock, numClients)
	//sqliteDBs[0] = setupSQLite(t, fmt.Sprintf("client-0"))
	//t.Cleanup(func() { require.NoError(t, sqliteDBs[0].Stop()) })
	//for i := 1; i < numClients; i++ {
	//	sqliteDBs[i] = setupSQLite(t, fmt.Sprintf("client-%d", i))
	//	t.Cleanup(func() { require.NoError(t, sqliteDBs[i].Stop()) })
	//}
	//t.Run("sqlite", testContention(numClients, sqliteDBs))
}
