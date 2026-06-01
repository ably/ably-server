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
	pool *pgxpool.Pool
	gen  *serial.Generator

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
		gen:        serial.NewGenerator(serial.NewSeriesID(), opts.Now),
		channels:   make(map[string]*channelStore),
		listenConn: listenConn,
		listenDone: make(chan struct{}),
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	s.listenCancel = cancel
	go s.listenLoop(loopCtx)
	return s, nil
}

// Channel returns the ChannelStore for name, binding it to appender
// on first access. Subsequent calls with the same name return the
// same instance and ignore the new appender. The internal LISTEN
// goroutine looks up channelStores in this map by name to dispatch
// notifications.
func (s *Storage) Channel(name string, appender storage.Appender) storage.ChannelStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs
	}
	cs := &channelStore{pool: s.pool, gen: s.gen, name: name, appender: appender}
	s.channels[name] = cs
	return cs
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
SELECT idx, payload FROM messages
WHERE channel = $1 AND channel_serial = $2
ORDER BY idx
`

// decodeChannelMessageRows materialises a ChannelMessage from a rows
// result of (idx, payload). Closes rows on exit.
func decodeChannelMessageRows(rows pgx.Rows, queryErr error, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	if queryErr != nil {
		return nil, fmt.Errorf("storage/postgres: load %s:%s: %w", channel, channelSerial, queryErr)
	}
	defer rows.Close()

	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial}
	for rows.Next() {
		var (
			idx     int
			payload []byte
		)
		if err := rows.Scan(&idx, &payload); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan %s:%s: %w", channel, channelSerial, err)
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
	if len(cm.Messages) == 0 {
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
// the per-channel advisory lock taken inside Store's transaction —
// that lock serialises writers on the same channel across every
// process sharing this database.
type channelStore struct {
	pool     *pgxpool.Pool
	gen      *serial.Generator
	name     string
	appender storage.Appender
}

// Store persists one publish atomically: take a per-channel advisory
// lock, look up any contained Message.IDs for prior matches
// (idempotent return on hit), otherwise mint a fresh channelSerial,
// stamp each Message.Serial, insert one row per Message, and emit a
// NOTIFY on the broker channel. The cm is delivered to the channel's
// appender asynchronously by the LISTEN goroutine after the NOTIFY
// round-trips through the database.
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

// History runs a direction-aware range scan over messages at Message
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
	// empty $4 means "no cursor".
	query := fmt.Sprintf(`
		SELECT channel_serial, idx, payload
		FROM messages
		WHERE channel = $1
		  AND ($2 = '' OR channel_serial >= $2)
		  AND ($3 = '' OR channel_serial <  $3)
		  AND ($4 = '' OR (channel_serial, idx) %s ($4, $5))
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $6 > 0 THEN $6 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query,
		cs.name, timeLower, timeUpper,
		cursorChannelSerial, cursorIdx,
		limit,
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
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode payload %s:%d: %w", cs2, idx, err)
		}
		appendMessage(&page, cs2, &m)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history rows: %w", err)
	}

	if overLimit && messageCount(page) > limit {
		trimToLimit(&page, limit)
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

// messageCount totals the Messages across all ChannelMessages in page.
func messageCount(page storage.HistoryPage) int {
	n := 0
	for _, cm := range page.ChannelMessages {
		n += len(cm.Messages)
	}
	return n
}

// trimToLimit drops Messages past limit, in order, possibly leaving
// the last surviving ChannelMessage partial.
func trimToLimit(page *storage.HistoryPage, limit int) {
	left := limit
	for i, cm := range page.ChannelMessages {
		if left == 0 {
			page.ChannelMessages = page.ChannelMessages[:i]
			return
		}
		if left >= len(cm.Messages) {
			left -= len(cm.Messages)
			continue
		}
		cm.Messages = cm.Messages[:left]
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

// loadChannelMessageTx fetches a previously-persisted ChannelMessage
// inside the caller's transaction — used by the idempotency pre-check
// in Store to return the original cm on a hit.
func loadChannelMessageTx(ctx context.Context, tx pgx.Tx, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	rows, err := tx.Query(ctx, sqlLoadCM, channel, channelSerial)
	return decodeChannelMessageRows(rows, err, channel, channelSerial)
}
