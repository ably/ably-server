// Package postgres is the database-backed storage backend used by
// ably-server's cluster mode (DESIGN.md §6.3).
//
// State is held in a single `messages` table (one row per Message,
// grouped under a shared channelSerial PK), applied at Open() via the
// embedded schema.sql. Each publish runs inside a transaction that
// takes a per-channel advisory lock (pg_advisory_xact_lock) so
// concurrent writers serialise per channel without blocking writers
// on other channels. Idempotency on client-supplied Message.IDs is
// enforced by a partial UNIQUE index on (channel, id).
//
// Concurrent-safe schema application across N nodes booting against an
// empty database is TASK-23 (advisory-lock around the whole bootstrap
// + a schema_migrations tracker); for now Open() relies on the
// CREATE-IF-NOT-EXISTS DDL.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

//go:embed schema.sql
var schemaSQL string

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

// Open dials Postgres at opts.DSN, applies the embedded schema, and
// returns a Storage ready for use. The seriesId is freshly generated
// per process — multi-node deployments rely on distinct per-node
// seriesIds to disambiguate concurrent mints (DESIGN.md §8).
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
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: apply schema: %w", err)
	}
	return &Storage{
		pool:     pool,
		gen:      serial.NewGenerator(serial.NewSeriesID(), opts.Now),
		channels: make(map[string]*channelStore),
	}, nil
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
			cs.name, channelSerial, i, idArg, payload); err != nil {
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
