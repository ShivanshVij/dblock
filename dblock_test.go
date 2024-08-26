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

func setupPostgres(tb testing.TB, name ...string) *DBLock {
	ctx := context.Background()

	dbName := tb.Name()
	if len(name) > 0 && len(name[0]) > 0 {
		dbName = name[0]
	}

	pgContainer, err := postgres.Run(ctx, "postgres:15.3-alpine",
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

	return db
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
	pgDB := setupPostgres(t)
	t.Cleanup(func() { require.NoError(t, pgDB.Stop()) })

	t.Run("postgres", testSingleWriter(pgDB))

	sqliteDB := setupSQLite(t)
	t.Cleanup(func() { require.NoError(t, sqliteDB.Stop()) })

	t.Run("sqlite", testSingleWriter(sqliteDB))
}

func testContention(db *DBLock, numClients int) func(t *testing.T) {
	return func(t *testing.T) {
		acquired := make(chan struct{})
		acquire := func(client int) {
			l := db.Lock(t.Name())
			t.Logf("client %d acquiring lock", client)
			err := db.Acquire(l)
			t.Logf("client %d acquired lock", client)
			require.NoError(t, err)

			acquired <- struct{}{}

			time.Sleep(db.options.LeaseDuration * 2)

			t.Logf("client %d releasing lock", client)
			err = db.Release(l)
			require.NoError(t, err)
		}

		var wg sync.WaitGroup
		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				acquire(client)
			}(i)
			time.Sleep(leaseDuration)
		}

		for i := 0; i < numClients; i++ {
			select {
			case <-acquired:
			case <-time.After(leaseDuration * 3):
				t.Fatal("timed out waiting for lock acquisition")
			}
		}

		wg.Wait()
	}
}

func TestContention(t *testing.T) {
	const numClients = 3

	pgDB := setupPostgres(t)
	t.Cleanup(func() { require.NoError(t, pgDB.Stop()) })

	t.Run("postgres", testContention(pgDB, numClients))

	sqliteDB := setupSQLite(t)
	t.Cleanup(func() { require.NoError(t, sqliteDB.Stop()) })

	t.Run("sqlite", testContention(sqliteDB, numClients))
}
