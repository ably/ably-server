// Package pgtest is the shared testcontainer harness for tests that
// need a real PostgreSQL. It carries no build constraint, so the tests
// using it compile in every build; whether they run is decided at run
// time by internal/integration.Enabled (DESIGN.md §18).
//
// Typical use:
//
//	func TestThing(t *testing.T) {
//	    c := pgtest.Start(t)
//	    dsn := c.FreshSchemaDSN(t)
//	    // open postgres.Storage{DSN: dsn} ...
//	}
//
// Start brings up postgres:17-alpine once per test binary
// (sync.Once-guarded); the container is reaped when the binary exits
// via the testcontainers ryuk sidecar. FreshSchemaDSN allocates a
// brand-new schema per subtest and registers a t.Cleanup to drop it,
// so each subtest sees a fresh empty schema without paying for a
// fresh database.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Container is the testcontainer handle returned by Start. It holds
// the base DSN against the container's default database; per-subtest
// isolation is via FreshSchemaDSN.
type Container struct {
	dsn string
}

// BaseDSN returns the DSN against the container's default database
// (no per-subtest schema isolation). Most tests should use
// FreshSchemaDSN instead.
func (c *Container) BaseDSN() string { return c.dsn }

var (
	containerOnce sync.Once
	containerInst *Container
	containerErr  error

	// schemaCounter mints unique schema names so concurrent subtests
	// don't collide.
	schemaCounter atomic.Int64
)

// Start brings up postgres:17-alpine via testcontainers-go (lazily,
// once per test binary) and returns a Container handle. Test
// failures during container startup are reported via t.Fatalf.
func Start(t *testing.T) *Container {
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
		containerInst = &Container{dsn: dsn}
	})
	if containerErr != nil {
		t.Fatalf("postgres container: %v", containerErr)
	}
	return containerInst
}

// FreshSchemaDSN allocates a brand-new schema in the container's
// default database and returns a DSN whose connections default to
// that schema (via the search_path connection option). The schema
// is dropped on t.Cleanup.
func (c *Container) FreshSchemaDSN(t *testing.T) string {
	t.Helper()

	schema := fmt.Sprintf("test_%d", schemaCounter.Add(1))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, c.dsn)
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
		conn, err := pgx.Connect(cctx, c.dsn)
		if err != nil {
			t.Logf("teardown: connect: %v", err)
			return
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(cctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema)); err != nil {
			t.Logf("teardown: drop schema %s: %v", schema, err)
		}
	})

	return appendSearchPath(t, c.dsn, schema)
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
