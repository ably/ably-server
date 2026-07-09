// Package postgres is the database-backed storage backend used by
// ably-server's cluster mode (DESIGN.md §6.3, §7.2).
//
// State is held in a single `messages` table (one row per Message,
// grouped under a shared channelSerial PK), provisioned at Open() via
// an embedded migration sweep guarded by a session-scoped
// pg_advisory_lock — so N nodes booting simultaneously against an
// empty database serialise on the lock and only one applies the
// pending migrations.
//
// The publish path:
//
//   - ChannelStore.Store persists the cm and emits a NOTIFY on
//     channel "ably_channel" inside the same transaction (PG buffers
//     NOTIFYs until commit, so listeners only see it if the publish
//     committed).
//   - A LISTEN goroutine inside Storage, running on a dedicated
//     pgx.Conn, receives every NOTIFY (including the publisher's
//     own), fetches the canonical cm by (channel, channel_serial),
//     looks up the channelStore that was registered for that channel
//     via Channel(name, appender), and calls appender.Append(cm).
//
// There is no self-dedup at the broker level: the publish path does
// not call the appender directly; the appender is the sole writer to
// the live linked list, always via the LISTEN round-trip. NOTIFYs for
// channels that no one on this node has opened (no Channel(name,…)
// call yet) are silently dropped — local subscribers materialise the
// channel via ATTACH, which calls core.Manager.GetChannel(name) and
// in turn registers an appender here.
//
// Concurrent writers serialise per channel via a per-channel
// pg_advisory_xact_lock inside Store's transaction, so the row
// stream remains ordered by channelSerial without cross-channel
// contention.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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

// notifyChannelName is the LISTEN channel used by the publish/subscribe
// broker — distinct concept from an Ably channel.
const notifyChannelName = "ably_channel"

// notifyPayload is the JSON-encoded NOTIFY body. Keeping it JSON
// avoids ambiguity in the face of Ably channel names that contain
// arbitrary characters (including ':' and '@').
type notifyPayload struct {
	Channel string `json:"channel"`
	Serial  string `json:"serial"`
}

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
	pool   *pgxpool.Pool
	series string // per-process seriesId, embedded in every minted channelSerial

	mu       sync.Mutex
	channels map[string]*channelStore

	listenConn   *pgx.Conn
	listenCancel context.CancelFunc
	listenDone   chan struct{}
}

// Open dials Postgres at opts.DSN, applies any pending migrations
// (under a session-scoped advisory lock so concurrent Opens
// serialise), opens a dedicated LISTEN connection for the cluster
// pub/sub broker, and returns a Storage ready for use. The seriesId
// is freshly generated per process — multi-node deployments rely on
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

	// Dedicated LISTEN connection. pgxpool doesn't expose the long-
	// lived single-conn semantics LISTEN needs, so we acquire a
	// separate raw conn for the broker goroutine.
	listenConn, err := pgx.Connect(ctx, opts.DSN)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: dial LISTEN conn: %w", err)
	}
	if _, err := listenConn.Exec(ctx, `LISTEN `+pgx.Identifier{notifyChannelName}.Sanitize()); err != nil {
		_ = listenConn.Close(context.Background())
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: LISTEN: %w", err)
	}

	s := &Storage{
		pool:       pool,
		series:     serial.NewSeriesID(),
		channels:   make(map[string]*channelStore),
		listenConn: listenConn,
		listenDone: make(chan struct{}),
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	s.listenCancel = cancel
	go s.listenLoop(loopCtx)
	return s, nil
}

// Channel returns the ChannelStore for name, binding it to appender on
// first access. Subsequent calls with the same name return the same
// instance and ignore the new appender. The internal LISTEN goroutine
// looks up channelStores in this map by name to dispatch notifications.
//
// On first creation the channels row is upserted via ensure_channel
// (creating it with a fresh seed serial if absent), and the resulting
// channelSerial — the watermark — is handed to appender.Initialize
// before this call returns.
func (s *Storage) Channel(ctx context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	s.mu.Lock()
	if cs, ok := s.channels[name]; ok {
		s.mu.Unlock()
		return cs, nil
	}
	cs := &channelStore{pool: s.pool, series: s.series, name: name, appender: appender}
	s.channels[name] = cs
	s.mu.Unlock()

	if appender == nil {
		return cs, nil
	}
	var current, initial string
	if err := s.pool.QueryRow(ctx, `SELECT current_serial, initial_serial FROM ensure_channel($1, $2)`, name, s.series).Scan(&current, &initial); err != nil {
		// Unwind: the channelStore exists in the map but was never
		// Initialize'd. Remove it so a retry can re-attempt.
		s.mu.Lock()
		delete(s.channels, name)
		s.mu.Unlock()
		return nil, fmt.Errorf("storage/postgres: ensure_channel: %w", err)
	}
	appender.Initialize(current, initial)
	return cs, nil
}

// Close stops the LISTEN goroutine, closes the LISTEN conn, and
// releases the pool.
func (s *Storage) Close() error {
	if s.listenCancel != nil {
		s.listenCancel()
		<-s.listenDone
	}
	if s.listenConn != nil {
		_ = s.listenConn.Close(context.Background())
	}
	s.pool.Close()
	return nil
}

// Ping reports whether the Postgres pool is reachable. It satisfies
// storage.Pinger, backing the /readyz check in cluster mode
// (DESIGN.md §2.2).
func (s *Storage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// listenLoop dispatches NOTIFY events to the registered channelStore
// for each channel. A NOTIFY for an unregistered channel is dropped:
// local attachments materialise the channelStore on demand via
// Storage.Channel, so events that arrive before any local interest
// are intentionally lost (the canonical cm is still in storage and
// will be picked up by a subsequent ATTACH+resume via History).
func (s *Storage) listenLoop(ctx context.Context) {
	defer close(s.listenDone)

	for {
		n, err := s.listenConn.WaitForNotification(ctx)
		if err != nil {
			// Context cancellation is the expected shutdown path.
			// Other errors are terminal for this conn — there's no
			// reconnect strategy yet (TASK-22 follow-up).
			return
		}

		var p notifyPayload
		if err := json.Unmarshal([]byte(n.Payload), &p); err != nil {
			continue // malformed; nothing actionable
		}

		s.mu.Lock()
		cs, ok := s.channels[p.Channel]
		s.mu.Unlock()
		if !ok || cs.appender == nil {
			continue
		}

		cm, err := s.loadChannelMessage(ctx, p.Channel, p.Serial)
		if err != nil {
			continue // best-effort; nothing we can do without the cm
		}
		cs.appender.Append(cm)
	}
}

// loadChannelMessage fetches the canonical ChannelMessage at
// (channel, channelSerial) via the pool. Used by the LISTEN loop
// after each NOTIFY.
func (s *Storage) loadChannelMessage(ctx context.Context, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	rows, err := s.pool.Query(ctx, sqlLoadCM, channel, channelSerial)
	return decodeChannelMessageRows(rows, err, channel, channelSerial)
}

const sqlLoadCM = `
SELECT idx, kind, payload FROM channel_messages
WHERE channel = $1 AND channel_serial = $2
ORDER BY idx
`

// decodeChannelMessageRows materialises a ChannelMessage from a rows
// result of (idx, kind, payload). All rows of a cm share a kind; a
// presence cm decodes into Presence, a message cm into Messages. Closes
// rows on exit.
func decodeChannelMessageRows(rows pgx.Rows, queryErr error, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	if queryErr != nil {
		return nil, fmt.Errorf("storage/postgres: load %s:%s: %w", channel, channelSerial, queryErr)
	}
	defer rows.Close()

	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial}
	for rows.Next() {
		var (
			idx     int
			kind    string
			payload []byte
		)
		if err := rows.Scan(&idx, &kind, &payload); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan %s:%s: %w", channel, channelSerial, err)
		}
		if kind == string(storage.KindPresence) {
			var p protocol.PresenceMessage
			if err := msgpack.Unmarshal(payload, &p); err != nil {
				return nil, fmt.Errorf("storage/postgres: decode presence payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
			}
			cm.Presence = append(cm.Presence, &p)
			continue
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("storage/postgres: decode payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
		}
		cm.Messages = append(cm.Messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: rows %s:%s: %w", channel, channelSerial, err)
	}
	if len(cm.Messages) == 0 && len(cm.Presence) == 0 {
		return nil, fmt.Errorf("storage/postgres: ChannelMessage not found: %s:%s", channel, channelSerial)
	}
	return cm, nil
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
			version: strings.TrimSuffix(name, ".sql"),
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

// channelStore is the per-channel facet. Concurrency is controlled by
// the channels-row UPDATE inside advance_channel_serial: every Store
// call serialises against other writers to the same channel via that
// row's lock.
type channelStore struct {
	pool     *pgxpool.Pool
	series   string
	name     string
	appender storage.Appender
}

// Store persists one publish atomically: look up any contained
// Message.IDs for prior matches (idempotent return on hit), otherwise
// advance the channel's serial via advance_channel_serial (which
// takes a row-level lock on the channels row and is the per-channel
// write serialiser), stamp each Message.Serial, insert one row per
// Message, and emit a NOTIFY on the broker channel. The cm is
// delivered to the channel's appender asynchronously by the LISTEN
// goroutine after the NOTIFY round-trips through the database.
func (cs *channelStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/postgres: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotency pre-check. Any contained ID that's already indexed
	// makes this whole publish a duplicate; we return the original
	// ChannelMessage at the matching channelSerial. We do this before
	// advancing the channels row so a duplicate publish does not
	// burn a serial.
	ids := nonEmptyIDs(msgs)
	if len(ids) > 0 {
		var existingCS string
		err := tx.QueryRow(ctx,
			`SELECT channel_serial FROM channel_messages
			 WHERE channel = $1 AND id = ANY($2) LIMIT 1`,
			cs.name, ids).Scan(&existingCS)
		switch {
		case err == nil:
			original, lerr := loadChannelMessageTx(ctx, tx, cs.name, existingCS)
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

	// Fresh publish: advance the channels-row serial (cluster-wide
	// monotonic via the row lock), stamp Message.Serials, persist.
	var channelSerial string
	if err := tx.QueryRow(ctx,
		`SELECT advance_channel_serial($1, $2)`,
		cs.name, cs.series,
	).Scan(&channelSerial); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	for i, m := range msgs {
		m.Serial = serial.MessageSerial(channelSerial, i)
		m.Action = protocol.MessageCreate
		storage.StampCreateVersion(m)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: msgs}

	for i, m := range msgs {
		payload, err := msgpack.Marshal(m)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: encode message %d: %w", i, err)
		}
		// id is stored as NULL when empty so the partial UNIQUE
		// idempotency index never matches a no-id publish. message_serial
		// is the message identity (its own serial for a create) — the
		// versions index and the projection key off it (DESIGN.md §13.4).
		var idArg any
		if m.ID != "" {
			idArg = m.ID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
			 VALUES ($1, $2, $3, $4, 'message', $5, $6)`,
			cs.name, channelSerial, i, idArg, payload, m.Serial,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert message %d: %w", i, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO messages (channel, message_serial, payload, deleted)
			 VALUES ($1, $2, $3, FALSE)`,
			cs.name, m.Serial, payload,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert projection %d: %w", i, err)
		}
	}

	// NOTIFY inside the tx: PG buffers the payload until commit, so
	// listeners only see it if the publish actually lands. The
	// LISTEN goroutine on every node (including this one) routes
	// the cm to the channel's appender (DESIGN.md §7.2).
	body, err := json.Marshal(notifyPayload{Channel: cs.name, Serial: channelSerial})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_notify($1, $2)`,
		notifyChannelName, string(body),
	); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: commit: %w", err)
	}
	return cm, false, nil
}

// Mutate applies an update/delete/append to an existing message
// (DESIGN.md §13.2) in one transaction: resolve the target's current
// latest version from the projection (ErrTargetNotFound if absent),
// apply the shallow-mixin merge, mint a fresh version serial, insert the
// merged version row on channel_messages (message_serial = identity),
// upsert the projection (deleted = TRUE for a delete), and NOTIFY. The
// cm reaches every node's appender via the LISTEN round-trip, like Store.
func (cs *channelStore) Mutate(ctx context.Context, mut *protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.Action.IsMutation() {
		return nil, false, errors.New("storage/postgres: Mutate requires a mutation action")
	}
	if mut.Serial == "" {
		return nil, false, errors.New("storage/postgres: Mutate requires a target serial")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotency pre-check — shared id namespace with creates.
	if mut.ID != "" {
		var existingCS string
		switch err := tx.QueryRow(ctx,
			`SELECT channel_serial FROM channel_messages WHERE channel = $1 AND id = $2 LIMIT 1`,
			cs.name, mut.ID).Scan(&existingCS); {
		case err == nil:
			original, lerr := loadChannelMessageTx(ctx, tx, cs.name, existingCS)
			if lerr != nil {
				return nil, false, lerr
			}
			if cerr := tx.Commit(ctx); cerr != nil {
				return nil, false, fmt.Errorf("storage/postgres: commit: %w", cerr)
			}
			return original, true, nil
		case errors.Is(err, pgx.ErrNoRows):
			// fall through
		default:
			return nil, false, fmt.Errorf("storage/postgres: idempotency lookup: %w", err)
		}
	}

	// Resolve the target's current latest version from the projection.
	var curPayload []byte
	switch err := tx.QueryRow(ctx,
		`SELECT payload FROM messages WHERE channel = $1 AND message_serial = $2`,
		cs.name, mut.Serial).Scan(&curPayload); {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, storage.ErrTargetNotFound
	case err != nil:
		return nil, false, fmt.Errorf("storage/postgres: load target version: %w", err)
	}
	var current protocol.Message
	if err := msgpack.Unmarshal(curPayload, &current); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: decode target version: %w", err)
	}

	var channelSerial string
	if err := tx.QueryRow(ctx,
		`SELECT advance_channel_serial($1, $2)`, cs.name, cs.series,
	).Scan(&channelSerial); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	version := storage.MergeVersion(&current, mut, serial.MessageSerial(channelSerial, 0))
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: []*protocol.Message{version}}

	payload, err := msgpack.Marshal(version)
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: encode version: %w", err)
	}
	var idArg any
	if mut.ID != "" {
		idArg = mut.ID
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
		 VALUES ($1, $2, 0, $3, 'message', $4, $5)`,
		cs.name, channelSerial, idArg, payload, mut.Serial,
	); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: insert version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE messages SET payload = $3, deleted = $4 WHERE channel = $1 AND message_serial = $2`,
		cs.name, mut.Serial, payload, version.Action == protocol.MessageDelete,
	); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: update projection: %w", err)
	}

	body, err := json.Marshal(notifyPayload{Channel: cs.name, Serial: channelSerial})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: commit: %w", err)
	}
	return cm, false, nil
}

// LatestVersion returns the projection entry for serial, or
// ErrTargetNotFound (DESIGN.md §13.4).
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var payload []byte
	switch err := cs.pool.QueryRow(ctx,
		`SELECT payload FROM messages WHERE channel = $1 AND message_serial = $2`,
		cs.name, serial).Scan(&payload); {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, storage.ErrTargetNotFound
	case err != nil:
		return nil, fmt.Errorf("storage/postgres: latest version: %w", err)
	}
	var m protocol.Message
	if err := msgpack.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("storage/postgres: decode latest version: %w", err)
	}
	return &m, nil
}

// Versions returns every version of serial ordered by version (the
// versions index), paginated at version granularity (DESIGN.md §13.4).
// The cursor is a version serial decomposed to (channel_serial, idx);
// Limit+1 detects HasMore. ErrTargetNotFound if the message has no rows.
func (cs *channelStore) Versions(ctx context.Context, serial2 string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	forwards := q.Direction == storage.DirectionForwards
	order, cursorOp := "ASC", ">"
	if !forwards {
		order, cursorOp = "DESC", "<"
	}
	var (
		cursorCS  string
		cursorIdx int
	)
	if q.Cursor != "" {
		var err error
		cursorCS, cursorIdx, err = serial.ParseMessageSerial(q.Cursor)
		if err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: parse versions cursor: %w", err)
		}
	}

	query := fmt.Sprintf(`
		SELECT channel_serial, payload FROM channel_messages
		WHERE channel = $1 AND message_serial = $2
		  AND ($3 = '' OR (channel_serial, idx) %s ($3, $4))
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, serial2, cursorCS, cursorIdx, q.Limit)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: versions query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			cs2     string
			payload []byte
		)
		if err := rows.Scan(&cs2, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan version: %w", err)
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode version %s: %w", cs2, err)
		}
		page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
			ChannelSerial: cs2,
			Messages:      []*protocol.Message{&m},
		})
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: versions rows: %w", err)
	}
	if len(page.ChannelMessages) == 0 {
		return storage.HistoryPage{}, storage.ErrTargetNotFound
	}
	if q.Limit > 0 && len(page.ChannelMessages) > q.Limit {
		page.ChannelMessages = page.ChannelMessages[:q.Limit]
		page.HasMore = true
	}
	return page, nil
}

// StorePresence persists a presence publish on channel_messages (kind =
// presence) and folds it into the presence projection table, all in one
// transaction, then emits a NOTIFY. The cm reaches the channel's
// appender via the LISTEN round-trip exactly like a message publish
// (DESIGN.md §12.2, §12.5); the membership table is authoritative across
// nodes the moment the tx commits.
func (cs *channelStore) StorePresence(ctx context.Context, presence []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/postgres: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotency pre-check — shared id namespace with messages.
	if ids := nonEmptyPresenceIDs(presence); len(ids) > 0 {
		var existingCS string
		err := tx.QueryRow(ctx,
			`SELECT channel_serial FROM channel_messages
			 WHERE channel = $1 AND id = ANY($2) LIMIT 1`,
			cs.name, ids).Scan(&existingCS)
		switch {
		case err == nil:
			original, lerr := loadChannelMessageTx(ctx, tx, cs.name, existingCS)
			if lerr != nil {
				return nil, false, lerr
			}
			if cerr := tx.Commit(ctx); cerr != nil {
				return nil, false, fmt.Errorf("storage/postgres: commit: %w", cerr)
			}
			return original, true, nil
		case errors.Is(err, pgx.ErrNoRows):
			// no prior match — fall through
		default:
			return nil, false, fmt.Errorf("storage/postgres: idempotency lookup: %w", err)
		}
	}

	var channelSerial string
	if err := tx.QueryRow(ctx,
		`SELECT advance_channel_serial($1, $2)`, cs.name, cs.series,
	).Scan(&channelSerial); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	for i, p := range presence {
		p.Serial = serial.MessageSerial(channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Presence: presence}

	for i, p := range presence {
		payload, err := msgpack.Marshal(p)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: encode presence %d: %w", i, err)
		}
		var idArg any
		if p.ID != "" {
			idArg = p.ID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload)
			 VALUES ($1, $2, $3, $4, 'presence', $5)`,
			cs.name, channelSerial, i, idArg, payload,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert presence %d: %w", i, err)
		}

		// Fold into the membership projection in the same tx.
		switch p.Action {
		case protocol.PresenceLeave, protocol.PresenceAbsent:
			if _, err := tx.Exec(ctx,
				`DELETE FROM presence WHERE channel = $1 AND connection_id = $2 AND client_id = $3`,
				cs.name, p.ConnectionID, p.ClientID,
			); err != nil {
				return nil, false, fmt.Errorf("storage/postgres: presence leave: %w", err)
			}
		default: // Enter, Update, Present
			if _, err := tx.Exec(ctx,
				`INSERT INTO presence (channel, connection_id, client_id, channel_serial, payload)
				 VALUES ($1, $2, $3, $4, $5)
				 ON CONFLICT (channel, connection_id, client_id)
				 DO UPDATE SET channel_serial = EXCLUDED.channel_serial, payload = EXCLUDED.payload`,
				cs.name, p.ConnectionID, p.ClientID, channelSerial, payload,
			); err != nil {
				return nil, false, fmt.Errorf("storage/postgres: presence upsert: %w", err)
			}
		}
	}

	body, err := json.Marshal(notifyPayload{Channel: cs.name, Serial: channelSerial})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: commit: %w", err)
	}
	return cm, false, nil
}

// Members returns the channel's presence projection plus the channel's
// current watermark serial as the as-of point.
func (cs *channelStore) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	rows, err := cs.pool.Query(ctx,
		`SELECT payload FROM presence WHERE channel = $1 ORDER BY channel_serial, client_id`, cs.name)
	if err != nil {
		return nil, "", fmt.Errorf("storage/postgres: members query: %w", err)
	}
	defer rows.Close()

	var out []*protocol.PresenceMessage
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, "", fmt.Errorf("storage/postgres: scan member: %w", err)
		}
		var p protocol.PresenceMessage
		if err := msgpack.Unmarshal(payload, &p); err != nil {
			return nil, "", fmt.Errorf("storage/postgres: decode member: %w", err)
		}
		out = append(out, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("storage/postgres: members rows: %w", err)
	}

	var asOf string
	if err := cs.pool.QueryRow(ctx,
		`SELECT channel_serial FROM channels WHERE name = $1`, cs.name,
	).Scan(&asOf); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, "", nil
		}
		return nil, "", fmt.Errorf("storage/postgres: members watermark: %w", err)
	}
	return out, asOf, nil
}

// History runs a direction-aware range scan over channel_messages at item
// granularity. Time bounds (q.Start / q.End) are applied as predicates
// on channel_serial — the serial's leading timestamp prefix makes the
// lex compare correct (DESIGN.md §8 + internal/serial.TimestampBounds),
// so no separate timestamp column or index is needed.
//
// q.Cursor is a Message.Serial (`<channelSerial>:<idx>`), decomposed
// into a (channel_serial, idx) tuple and applied as a strict (>/<)
// predicate over the lex-ordered pair. q.Limit caps Messages (not
// ChannelMessages); we fetch Limit+1 rows to detect HasMore. Rows are
// grouped into ChannelMessages in Go — a multi-message batch may be
// split across pages, so the head/tail ChannelMessage in a page can
// be partial.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	if q.Collapse && q.Kind.Normalize() == storage.KindMessage {
		return cs.collapsedHistory(ctx, q)
	}

	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	forwards := q.Direction == storage.DirectionForwards

	var (
		cursorChannelSerial string
		cursorIdx           int
	)
	if q.Cursor != "" {
		var err error
		cursorChannelSerial, cursorIdx, err = serial.ParseMessageSerial(q.Cursor)
		if err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: parse cursor: %w", err)
		}
	}

	limit := q.Limit
	overLimit := q.Limit > 0
	order := "ASC"
	cursorOp := ">"
	if !forwards {
		order = "DESC"
		cursorOp = "<"
	}

	// The cursor predicate is the lex compare over the (channel_serial,
	// idx) tuple. $4 holds the cursor's channelSerial and $5 its idx;
	// empty $4 means "no cursor". $7 is the optional inclusive
	// channelSerial upper bound (EndChannelSerial).
	wantKind := q.Kind.Normalize()

	query := fmt.Sprintf(`
		SELECT channel_serial, idx, payload
		FROM channel_messages
		WHERE channel = $1
		  AND kind = $8
		  AND ($2 = '' OR channel_serial >= $2)
		  AND ($3 = '' OR channel_serial <  $3)
		  AND ($4 = '' OR (channel_serial, idx) %s ($4, $5))
		  AND ($7 = '' OR channel_serial <= $7)
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $6 > 0 THEN $6 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query,
		cs.name, timeLower, timeUpper,
		cursorChannelSerial, cursorIdx,
		limit,
		q.EndChannelSerial,
		string(wantKind),
	)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			cs2     string
			idx     int
			payload []byte
		)
		if err := rows.Scan(&cs2, &idx, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan row: %w", err)
		}
		if wantKind == storage.KindPresence {
			var p protocol.PresenceMessage
			if err := msgpack.Unmarshal(payload, &p); err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode presence payload %s:%d: %w", cs2, idx, err)
			}
			appendPresence(&page, cs2, &p)
			continue
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode payload %s:%d: %w", cs2, idx, err)
		}
		appendMessage(&page, cs2, &m)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history rows: %w", err)
	}

	if overLimit && itemCount(page) > limit {
		trimToLimit(&page, limit)
		page.HasMore = true
	}
	return page, nil
}

// collapsedHistory returns the latest version of each message positioned
// at its create serial (DESIGN.md §13.4), backing the default REST
// message history. It scans the latest-version projection ordered by
// message_serial (== create serial, so create-timeline order), applies
// the time bounds / cursor / limit against that identity, and regroups
// rows under their create channelSerial (DESC within a batch for
// backwards, matching the raw scan).
func (cs *channelStore) collapsedHistory(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	forwards := q.Direction == storage.DirectionForwards
	order, cursorOp := "ASC", ">"
	if !forwards {
		order, cursorOp = "DESC", "<"
	}

	query := fmt.Sprintf(`
		SELECT message_serial, payload FROM messages
		WHERE channel = $1
		  AND ($2 = '' OR message_serial >= $2)
		  AND ($3 = '' OR message_serial <  $3)
		  AND ($4 = '' OR message_serial %s $4)
		ORDER BY message_serial %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, timeLower, timeUpper, q.Cursor, q.Limit)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: collapsed history query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			identity string
			payload  []byte
		)
		if err := rows.Scan(&identity, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan collapsed row: %w", err)
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode projection %s: %w", identity, err)
		}
		appendMessage(&page, storage.CreateChannelSerial(identity), &m)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: collapsed history rows: %w", err)
	}

	if q.Limit > 0 && itemCount(page) > q.Limit {
		trimToLimit(&page, q.Limit)
		page.HasMore = true
	}
	return page, nil
}

// appendMessage tacks m onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. Used by the
// row-scanning loop above where consecutive rows from the same batch
// arrive contiguously.
func appendMessage(page *storage.HistoryPage, channelSerial string, m *protocol.Message) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Messages = append(page.ChannelMessages[n-1].Messages, m)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Messages:      []*protocol.Message{m},
	})
}

// appendPresence tacks p onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. The presence
// analogue of appendMessage.
func appendPresence(page *storage.HistoryPage, channelSerial string, p *protocol.PresenceMessage) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Presence = append(page.ChannelMessages[n-1].Presence, p)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Presence:      []*protocol.PresenceMessage{p},
	})
}

// cmLen is the item count of a cm — Messages or Presence, whichever the
// (single-kind) cm carries.
func cmLen(cm *protocol.ChannelMessage) int {
	return len(cm.Messages) + len(cm.Presence)
}

// itemCount totals the items (Messages or Presence) across all
// ChannelMessages in page.
func itemCount(page storage.HistoryPage) int {
	n := 0
	for _, cm := range page.ChannelMessages {
		n += cmLen(cm)
	}
	return n
}

// trimToLimit drops items past limit, in order, possibly leaving the
// last surviving ChannelMessage partial. Trims whichever slice the cm
// carries (a page is single-kind).
func trimToLimit(page *storage.HistoryPage, limit int) {
	left := limit
	for i, cm := range page.ChannelMessages {
		if left == 0 {
			page.ChannelMessages = page.ChannelMessages[:i]
			return
		}
		if left >= cmLen(cm) {
			left -= cmLen(cm)
			continue
		}
		if len(cm.Presence) > 0 {
			cm.Presence = cm.Presence[:left]
		} else {
			cm.Messages = cm.Messages[:left]
		}
		page.ChannelMessages = page.ChannelMessages[:i+1]
		return
	}
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

// nonEmptyPresenceIDs is the presence analogue of nonEmptyIDs.
func nonEmptyPresenceIDs(presence []*protocol.PresenceMessage) []string {
	var ids []string
	for _, p := range presence {
		if p.ID != "" {
			ids = append(ids, p.ID)
		}
	}
	return ids
}

// loadChannelMessageTx fetches a previously-persisted ChannelMessage
// inside the caller's transaction — used by the idempotency pre-check
// in Store to return the original cm on a hit.
func loadChannelMessageTx(ctx context.Context, tx pgx.Tx, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	rows, err := tx.Query(ctx, sqlLoadCM, channel, channelSerial)
	return decodeChannelMessageRows(rows, err, channel, channelSerial)
}
