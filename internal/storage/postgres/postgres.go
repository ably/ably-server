// Package postgres is the database-backed storage backend used by
// ably-server's cluster mode (DESIGN.md §6.3).
//
// State is held in a single `messages` table (one row per Message,
// grouped under a shared channelSerial PK), provisioned at Open() via
// an embedded migration sweep guarded by a session-scoped
// pg_advisory_lock — so N nodes booting simultaneously against an
// empty database serialise on the lock and only one applies the
// pending migrations. Each publish runs inside a transaction that
// takes a per-channel advisory lock (pg_advisory_xact_lock) so
// concurrent writers serialise per channel without blocking writers
// on other channels. Idempotency on client-supplied Message.IDs is
// enforced by a partial UNIQUE index on (channel, id).
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationLockKey is the int8 used with pg_advisory_lock to serialise
// the migration sweep across all processes sharing this database. The
// value is arbitrary — pg_advisory_lock's keyspace is per-database and
// opt-in, so collisions with other applications would only matter if
// someone else's code also picked this exact key on the same DB.
const migrationLockKey int64 = 0x1ab1_e5e7_2e0a_17a3

// Options configures the Postgres backend.
type Options struct {
	// DSN is the libpq-style connection string (e.g.
	// "postgres://user:pw@host:5432/db?sslmode=disable"). Required.
	DSN string

	// Now is the clock used by the serial generator. Nil means
	// time.Now().UnixMilli — overridden by tests for determinism.
	Now func() int64
}

// Storage is the pgx/pgxpool-backed storage.Storage.
type Storage struct {
	pool *pgxpool.Pool
	gen  *serial.Generator

	mu       sync.Mutex
	channels map[string]*channelStore
}

// Open dials Postgres at opts.DSN, applies any pending migrations
// (under a session-scoped advisory lock so concurrent Opens
// serialise), and returns a Storage ready for use. The seriesId is
// freshly generated per process — multi-node deployments rely on
// distinct per-node seriesIds to disambiguate concurrent mints
// (DESIGN.md §8).
func Open(ctx context.Context, opts Options) (*Storage, error) {
	if opts.DSN == "" {
		return nil, errors.New("storage/postgres: Open requires a DSN")
	}
	pool, err := pgxpool.New(ctx, opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: ping: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: migrate: %w", err)
	}
	return &Storage{
		pool:     pool,
		gen:      serial.NewGenerator(serial.NewSeriesID(), opts.Now),
		channels: make(map[string]*channelStore),
	}, nil
}

// migrate applies any pending embedded migrations under a session-
// scoped pg_advisory_lock. Other processes calling Open against the
// same database block on the lock acquire, then observe an
// up-to-date schema_migrations table and apply nothing.
//
// Migrations live in the embedded migrations/ tree and are applied
// in lex order of filename (the convention is "<4-digit>_<name>.sql",
// e.g. 0001_initial.sql). Each migration runs in its own transaction
// alongside the schema_migrations INSERT, so a crash mid-sweep
// leaves the DB consistent (either fully applied or not), and the
// next Open picks up where the previous one left off.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	// Session-scoped lock: held until we explicitly release it (or
	// the conn returns to the pool, since pgxpool resets the
	// session). We unlock explicitly for symmetry / defensiveness.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		// Best-effort release; the conn's session-end would do it too.
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
		  version    TEXT        PRIMARY KEY,
		  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := make(map[string]struct{})
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("scan applied version: %w", err)
		}
		applied[v] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema_migrations: %w", err)
	}

	pending, err := listMigrations()
	if err != nil {
		return err
	}
	for _, m := range pending {
		if _, ok := applied[m.version]; ok {
			continue
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return fmt.Errorf("apply %s: %w", m.version, err)
		}
	}
	return nil
}

type migration struct {
	version string // filename minus ".sql", e.g. "0001_initial"
	sql     string
}

// listMigrations reads the embedded migrations/ tree and returns the
// migrations in lex order (which by convention matches numeric order
// of the leading digit prefix).
func listMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := make([]migration, 0, len(names))
	for _, name := range names {
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}
		out = append(out, migration{
			version: name[:len(name)-len(".sql")],
			sql:     string(body),
		})
	}
	return out, nil
}

// applyMigration runs a single migration's SQL and records the
// schema_migrations row in the same transaction.
func applyMigration(ctx context.Context, conn *pgxpool.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("exec migration sql: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`,
		m.version,
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Channel returns the ChannelStore for name. Successive calls with
// the same name return the same instance.
func (s *Storage) Channel(name string) storage.ChannelStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs
	}
	cs := &channelStore{pool: s.pool, gen: s.gen, name: name}
	s.channels[name] = cs
	return cs
}

// Close releases the connection pool.
func (s *Storage) Close() error {
	s.pool.Close()
	return nil
}

// channelStore is the per-channel facet. Concurrency is controlled by
// the per-channel advisory lock taken inside AppendChannelMessage's
// transaction — that lock serialises writers on the same channel
// across all processes sharing this database.
type channelStore struct {
	pool *pgxpool.Pool
	gen  *serial.Generator
	name string
}

// AppendChannelMessage runs the full publish under a transaction:
// take a per-channel advisory lock, look up any contained Message.IDs
// for prior matches (idempotent return on hit), otherwise mint a
// fresh channelSerial, stamp each Message.Serial, and insert the
// channel_messages + messages rows. Caller-side ordering across
// nodes is not preserved by the network race to acquire the lock —
// what matters is that history ORDER BY channel_serial reflects the
// global lex order of minted serials, which the serial format
// guarantees (§8).
func (cs *channelStore) AppendChannelMessage(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/postgres: AppendChannelMessage with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Per-channel advisory lock — released automatically at commit
	// or rollback. Serialises concurrent writers on this channel
	// across every process sharing this database.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, cs.name); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: advisory lock: %w", err)
	}

	// Idempotency pre-check. Any contained ID that's already indexed
	// makes this whole publish a duplicate; we return the original
	// ChannelMessage at the matching channelSerial.
	ids := nonEmptyIDs(msgs)
	if len(ids) > 0 {
		var existingCS string
		err := tx.QueryRow(ctx,
			`SELECT channel_serial FROM messages
			 WHERE channel = $1 AND id = ANY($2) LIMIT 1`,
			cs.name, ids).Scan(&existingCS)
		switch {
		case err == nil:
			original, lerr := loadChannelMessage(ctx, tx, cs.name, existingCS)
			if lerr != nil {
				return nil, false, lerr
			}
			if cerr := tx.Commit(ctx); cerr != nil {
				return nil, false, fmt.Errorf("storage/postgres: commit: %w", cerr)
			}
			return original, true, nil
		case errors.Is(err, pgx.ErrNoRows):
			// no prior match — fall through to insert
		default:
			return nil, false, fmt.Errorf("storage/postgres: idempotency lookup: %w", err)
		}
	}

	// Fresh publish: mint serial, stamp Message.Serials, persist.
	channelSerial := cs.gen.Mint()
	for i, m := range msgs {
		m.Serial = serial.MessageSerial(channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: msgs}

	for i, m := range msgs {
		payload, err := msgpack.Marshal(m)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: encode message %d: %w", i, err)
		}
		// id is stored as NULL when empty so the partial UNIQUE
		// idempotency index never matches a no-id publish.
		var idArg any
		if m.ID != "" {
			idArg = m.ID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO messages (channel, channel_serial, idx, id, payload)
			 VALUES ($1, $2, $3, $4, $5)`,
			cs.name, channelSerial, i, idArg, payload,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert message %d: %w", i, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: commit: %w", err)
	}
	return cm, false, nil
}

// History runs a forward range scan over messages, ordered by
// (channel_serial, idx). Rows are grouped into ChannelMessages by
// serial; HasMore is set when (Limit+1) distinct channel_serials were
// available, signalled by fetching one extra ChannelMessage's worth
// of rows than requested.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	// We fetch Limit+1 distinct channel_serials to detect HasMore. The
	// CTE picks the window of serials we want; the outer query joins
	// back to messages for the actual rows.
	limit := q.Limit
	overLimit := q.Limit > 0
	rows, err := cs.pool.Query(ctx, `
		WITH window_cms AS (
		  SELECT DISTINCT channel_serial
		  FROM messages
		  WHERE channel = $1
		    AND ($2 = '' OR channel_serial > $2)
		  ORDER BY channel_serial
		  LIMIT CASE WHEN $3 > 0 THEN $3 + 1 ELSE NULL END
		)
		SELECT m.channel_serial, m.idx, m.payload
		FROM window_cms w
		JOIN messages m
		  ON m.channel = $1 AND m.channel_serial = w.channel_serial
		ORDER BY m.channel_serial, m.idx
	`, cs.name, q.AfterChannelSerial, limit)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history query: %w", err)
	}
	defer rows.Close()

	var (
		page    storage.HistoryPage
		current *protocol.ChannelMessage
	)
	for rows.Next() {
		var (
			cs2     string
			idx     int
			payload []byte
		)
		if err := rows.Scan(&cs2, &idx, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan row: %w", err)
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode payload %s:%d: %w", cs2, idx, err)
		}
		if current == nil || current.ChannelSerial != cs2 {
			current = &protocol.ChannelMessage{ChannelSerial: cs2}
			page.ChannelMessages = append(page.ChannelMessages, current)
		}
		current.Messages = append(current.Messages, &m)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history rows: %w", err)
	}

	if overLimit && len(page.ChannelMessages) > limit {
		page.ChannelMessages = page.ChannelMessages[:limit]
		page.HasMore = true
	}
	return page, nil
}

// nonEmptyIDs returns the subset of m.ID values that are non-empty.
// Used for the idempotency pre-check.
func nonEmptyIDs(msgs []*protocol.Message) []string {
	var ids []string
	for _, m := range msgs {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

// loadChannelMessage fetches a previously-persisted ChannelMessage by
// (channel, channelSerial) — used to return the original on an
// idempotent hit.
func loadChannelMessage(ctx context.Context, tx pgx.Tx, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	rows, err := tx.Query(ctx,
		`SELECT idx, payload FROM messages
		 WHERE channel = $1 AND channel_serial = $2
		 ORDER BY idx`,
		channel, channelSerial)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: load original messages: %w", err)
	}
	defer rows.Close()

	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial}
	for rows.Next() {
		var (
			idx     int
			payload []byte
		)
		if err := rows.Scan(&idx, &payload); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan original row: %w", err)
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("storage/postgres: decode original payload %d: %w", idx, err)
		}
		cm.Messages = append(cm.Messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: original rows: %w", err)
	}
	if len(cm.Messages) == 0 {
		return nil, fmt.Errorf("storage/postgres: id index points to missing ChannelMessage %q", channelSerial)
	}
	return cm, nil
}
