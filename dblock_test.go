package dblock

import (
	"context"
	"fmt"
	"github.com/loopholelabs/logging"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const leaseDuration = time.Millisecond * 100

func setupPostgres(tb testing.TB, name ...string) *postgres.PostgresContainer {
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

	return pgContainer
}

func setupDB(tb testing.TB, pgContainer *postgres.PostgresContainer, name ...string) *Manager {
	connStr, err := pgContainer.ConnectionString(context.Background(), "sslmode=disable")
	require.NoError(tb, err)

	dbName := tb.Name()
	if len(name) > 0 && len(name[0]) > 0 {
		dbName = name[0]
	}

	db, err := New(&Options{
		Logger:        logging.Test(tb, logging.Zerolog, dbName),
		Name:          dbName,
		DBType:        Postgres,
		DatabaseURL:   connStr,
		LeaseDuration: leaseDuration,
	})

	require.NoError(tb, err)

	return db
}

func TestSingleWriter(t *testing.T) {
	pg := setupPostgres(t)
	db := setupDB(t, pg)
	t.Cleanup(func() { require.NoError(t, db.Stop()) })

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

func TestContention(t *testing.T) {
	const numClients = 3
	pg := setupPostgres(t)

	acquired := make(chan struct{})
	acquire := func(client int) {
		db := setupDB(t, pg, fmt.Sprintf("%s-%d", t.Name(), client))
		t.Cleanup(func() { require.NoError(t, db.Stop()) })

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
