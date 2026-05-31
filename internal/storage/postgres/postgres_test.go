//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

// container holds the per-test-run Postgres testcontainer; it is
// brought up lazily on first use and torn down when the test binary
// exits (testcontainers' reaper handles the actual cleanup).
type container struct {
	dsn     string
	cleanup func()
}

var (
	containerOnce sync.Once
	containerInst *container
	containerErr  error

	// schemaCounter mints unique schema names so concurrent subtests
	// (and parallel runs) don't share state.
	schemaCounter atomic.Int64
)

// startPostgres brings up postgres:17-alpine via testcontainers-go and
// returns a base DSN against the default `postgres` database. The
// container is left running for the lifetime of the test binary; a
// per-process registry of allocated schemas would be overkill — we
// just sweep them in t.Cleanup.
func startPostgres(t *testing.T) *container {
	t.Helper()
	containerOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		pgC, err := tcpostgres.Run(ctx,
			"postgres:17-alpine",
			tcpostgres.WithDatabase("ably"),
			tcpostgres.WithUsername("ably"),
			tcpostgres.WithPassword("ably"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			containerErr = fmt.Errorf("start postgres container: %w", err)
			return
		}
		dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			_ = pgC.Terminate(context.Background())
			containerErr = fmt.Errorf("get connection string: %w", err)
			return
		}
		containerInst = &container{
			dsn: dsn,
			cleanup: func() {
				_ = pgC.Terminate(context.Background())
			},
		}
	})
	if containerErr != nil {
		t.Fatalf("postgres container: %v", containerErr)
	}
	return containerInst
}

// freshSchemaDSN returns a DSN whose connections default to a brand-new
// empty schema, plus a teardown closure that drops the schema. Each
// subtest gets its own schema so the contract suite's "fresh storage"
// contract holds without paying the cost of spinning up a separate
// database per subtest.
func freshSchemaDSN(t *testing.T, baseDSN string) string {
	t.Helper()

	schema := fmt.Sprintf("test_%d", schemaCounter.Add(1))

	// Open a one-shot connection on the base DSN to create the schema.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect to base DSN: %v", err)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		conn, err := pgx.Connect(cctx, baseDSN)
		if err != nil {
			t.Logf("teardown: connect: %v", err)
			return
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(cctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema)); err != nil {
			t.Logf("teardown: drop schema %s: %v", schema, err)
		}
	})

	// Append a search_path option so every connection from the pool
	// resolves unqualified table names against our schema.
	return appendSearchPath(t, baseDSN, schema)
}

// appendSearchPath rewrites a libpq-style DSN to set search_path to
// the given schema. Works for both URL-form ("postgres://...") and
// key=value DSNs.
func appendSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	opt := "-c search_path=" + schema
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse DSN: %v", err)
		}
		q := u.Query()
		q.Set("options", opt)
		u.RawQuery = q.Encode()
		return u.String()
	}
	return dsn + " options='" + opt + "'"
}

func TestPostgresChannelStoreContract(t *testing.T) {
	c := startPostgres(t)
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		dsn := freshSchemaDSN(t, c.dsn)
		s, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestPostgresBootstrapIsIdempotent(t *testing.T) {
	c := startPostgres(t)
	dsn := freshSchemaDSN(t, c.dsn)

	// First Open creates the schema; second Open should observe it
	// already exists and be a no-op.
	s1, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	s2, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open #2 (idempotent bootstrap): %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
}

// TestPostgresMigrateIsConcurrentSafe spawns N goroutines that each
// call postgres.Open on the same fresh schema simultaneously. The
// session-scoped pg_advisory_lock should serialise the migration
// sweep: all Opens succeed; schema_migrations ends up with exactly
// one row per shipped migration (no duplicates from concurrent
// inserts); the messages table is correctly created.
func TestPostgresMigrateIsConcurrentSafe(t *testing.T) {
	c := startPostgres(t)
	dsn := freshSchemaDSN(t, c.dsn)
	ctx := context.Background()

	const workers = 10
	var (
		wg   sync.WaitGroup
		errs = make(chan error, workers)
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
			if err != nil {
				errs <- err
				return
			}
			_ = s.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Open: %v", err)
	}

	// schema_migrations must have exactly one row per shipped
	// migration — no duplicates, no partial application.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for verification: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if !slices.Equal(versions, []string{"0001_initial"}) {
		t.Errorf("schema_migrations rows = %v, want [0001_initial]", versions)
	}

	// The messages table must exist and be queryable.
	if _, err := conn.Exec(ctx, `SELECT 1 FROM messages LIMIT 0`); err != nil {
		t.Errorf("messages table not present: %v", err)
	}
}
