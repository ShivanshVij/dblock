package dblock

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/loopholelabs/logging"
)

func setupPostgres(tb testing.TB) *Manager {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:15.3-alpine",
		postgres.WithDatabase(tb.Name()),
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

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(tb, err)

	m, err := New(&Options{
		Logger:        logging.Test(tb, logging.Zerolog, tb.Name()),
		Name:          tb.Name(),
		DBType:        Postgres,
		DatabaseURL:   connStr,
		LeaseDuration: time.Millisecond * 100,
	})

	require.NoError(tb, err)

	return m
}

func TestPostgres(t *testing.T) {
	db := setupPostgres(t)
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
