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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/server-protocol/go/wire"
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

// listenReconnectBaseDelay / listenReconnectMaxDelay bound the capped
// exponential backoff the LISTEN goroutine applies between re-dial
// attempts after its connection drops (DESIGN.md §7.2). They are
// package vars, not consts, so integration tests can shrink them; in
// production they are effectively constant.
var (
	listenReconnectBaseDelay = 200 * time.Millisecond
	listenReconnectMaxDelay  = 5 * time.Second
)

// Presence liveness lease + crashed-node reaper timings (DESIGN.md
// §12.5). presenceLeaseWindow is how long a member's lease is valid
// without a refresh; presenceLeaseBumpInterval is the cadence at which
// a live node bumps the lease for all of its own rows (comfortably
// shorter than the window so a live node's members never expire); and
// presenceReaperInterval is how often each node sweeps for lapsed
// leases. Making these operator-configurable is a follow-up — they are
// effectively constant in production, package vars only so integration
// tests can shrink them.
var (
	presenceLeaseWindow       = 30 * time.Second
	presenceLeaseBumpInterval = 10 * time.Second
	presenceReaperInterval    = 5 * time.Second
)

// Occupancy shares presence's lease timings (DESIGN.md §16.2): the same
// bump loop cadence keeps a live node's contribution from lapsing, and the
// same reaper sweep drops a dead node's. They are the same problem — state
// owned by a node that may vanish without tearing it down — so they are
// deliberately not given separate knobs to drift apart.

// fixtureNodeID is the sentinel owner recorded on static fixture presence
// rows (DESIGN.md §9, §12.5). It is not a real node id, so no live node's
// lease-bump loop (WHERE node_id = $node) ever touches these rows; paired
// with an 'infinity' lease they are never reaped.
const fixtureNodeID = "__fixtures__"

// notifyPayload is the JSON-encoded NOTIFY body. Keeping it JSON
// avoids ambiguity in the face of Ably channel names that contain
// arbitrary characters (including ':' and '@').
type notifyPayload struct {
	Channel string `json:"channel"`
	Serial  string `json:"serial"`

	// Occupancy marks a notification that the channel's occupancy moved
	// rather than that a cm was minted (DESIGN.md §16.3). The two share one
	// LISTEN channel because they share a fan-out — every node holding the
	// channel wants both — and separating them would mean a second dedicated
	// connection and a second reconnect path for a strictly rarer event.
	//
	// An occupancy notification carries no serial: occupancy is not on the
	// log and has no position in it. The receiving node re-reads the
	// aggregate rather than being told it, because by the time the
	// notification lands several other nodes may have moved too.
	Occupancy bool `json:"occupancy,omitempty"`
}

// Options configures the Postgres backend.
type Options struct {
	// DSN is the libpq-style connection string (e.g.
	// "postgres://user:pw@host:5432/db?sslmode=disable"). Required.
	DSN string

	// Now is the clock used by the serial generator. Nil means
	// time.Now().UnixMilli — overridden by tests for determinism.
	Now func() int64

	// Logger receives operational events — currently the LISTEN
	// broker's reconnect/reconcile lifecycle (DESIGN.md §7.2) and the
	// presence reaper (§12.5). Nil means logging.Default().
	Logger *logging.Logger
}

// Storage is the pgx/pgxpool-backed storage.Storage.
type Storage struct {
	pool   *pgxpool.Pool
	dsn    string // retained so the LISTEN goroutine can re-dial on drop
	series string // seeds the seriesId of channels this node materialises first
	node   string // per-process node id, owning presence rows for the liveness lease (§12.5)
	logger *logging.Logger

	mu       sync.Mutex
	channels map[string]*channelStore

	initialListenConn *pgx.Conn // first LISTEN conn, dialed by Open; owned by listenLoop thereafter
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	closeOnce         sync.Once
}

// Open dials Postgres at opts.DSN, applies any pending migrations
// (under a session-scoped advisory lock so concurrent Opens
// serialise), opens a dedicated LISTEN connection for the cluster
// pub/sub broker, and returns a Storage ready for use.
//
// The seriesId generated here seeds the channels this node is the
// first to materialise; a channel keeps the series it was seeded with
// for as long as its row lives, whichever node publishes to it
// afterwards (DESIGN.md §8).
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
	// separate raw conn for the broker goroutine. Dialing it here lets
	// Open fail fast on a bad DSN; the goroutine re-dials fresh conns
	// itself when this one drops.
	listenConn, err := dialAndListen(ctx, opts.DSN)
	if err != nil {
		pool.Close()
		return nil, err
	}

	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	s := &Storage{
		pool:              pool,
		dsn:               opts.DSN,
		series:            serial.NewSeriesID(),
		node:              serial.NewSeriesID(),
		logger:            logger,
		channels:          make(map[string]*channelStore),
		initialListenConn: listenConn,
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(3)
	go s.listenLoop(loopCtx)
	go s.presenceLeaseBumpLoop(loopCtx)
	go s.presenceReaperLoop(loopCtx)
	return s, nil
}

// dialAndListen opens a fresh raw pgx.Conn and issues the broker LISTEN
// on it. Used both for the initial conn in Open and for every
// post-drop re-dial in the LISTEN goroutine.
func dialAndListen(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: dial LISTEN conn: %w", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN `+pgx.Identifier{notifyChannelName}.Sanitize()); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("storage/postgres: LISTEN: %w", err)
	}
	return conn, nil
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
	cs := &channelStore{pool: s.pool, series: s.series, node: s.node, name: name, appender: appender}
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

// Close stops the background goroutines (LISTEN broker and, in cluster
// presence, the lease-bump and reaper loops) and releases the pool. The
// LISTEN goroutine owns closing its own conn, so Close only cancels and
// waits.
func (s *Storage) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
			s.wg.Wait()
		}
		s.pool.Close()
	})
	return nil
}

// Ping reports whether the Postgres pool is reachable. It satisfies
// storage.Pinger, backing the /readyz check in cluster mode
// (DESIGN.md §2.2).
func (s *Storage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// listenLoop dispatches NOTIFY events to the registered channelStore
// for each channel, surviving dropped LISTEN connections (DESIGN.md
// §7.2). It runs consume() on the current conn until a
// WaitForNotification error; unless that error is Close() cancelling
// the context, it re-dials a fresh conn with capped-exponential
// backoff, re-LISTENs, reconciles each channel's missed cms from
// history, and resumes. It exits only when the storage is Close()d.
//
// A NOTIFY for an unregistered channel is dropped: local attachments
// materialise the channelStore on demand via Storage.Channel, so
// events that arrive before any local interest are intentionally lost
// (the canonical cm is still in storage and picked up by a subsequent
// ATTACH+resume via History).
func (s *Storage) listenLoop(ctx context.Context) {
	defer s.wg.Done()

	conn := s.initialListenConn
	for {
		err := s.consume(ctx, conn)
		_ = conn.Close(context.Background())
		if ctx.Err() != nil {
			return // Close(): expected shutdown
		}
		s.logger.Warn("storage/postgres: LISTEN connection lost; reconnecting", "err", err)

		conn = s.redial(ctx)
		if conn == nil {
			return // ctx cancelled during backoff
		}
		// Re-LISTEN is already done by redial; reconcile before resuming
		// so any cm minted during the gap is replayed exactly once
		// (deliver() dedups against a subsequent buffered NOTIFY).
		s.reconcile(ctx)
		s.logger.Info("storage/postgres: LISTEN reconnected and reconciled")
	}
}

// consume runs the steady-state WaitForNotification dispatch on conn,
// returning the error that ended it (a dropped conn, or ctx
// cancellation on Close).
func (s *Storage) consume(ctx context.Context, conn *pgx.Conn) error {
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
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

		if p.Occupancy {
			// Nothing to load: the signal says the aggregate is worth
			// re-reading, and whoever reports occupancy does the reading.
			cs.appender.OccupancyChanged()
			continue
		}

		cm, err := s.loadChannelMessage(ctx, p.Channel, p.Serial)
		if err != nil {
			continue // best-effort; nothing we can do without the cm
		}
		cs.deliver(cm)
	}
}

// redial re-establishes the LISTEN connection with capped exponential
// backoff, retrying until it succeeds or ctx is cancelled (Close). A
// nil return means ctx was cancelled.
func (s *Storage) redial(ctx context.Context) *pgx.Conn {
	delay := listenReconnectBaseDelay
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		conn, err := dialAndListen(ctx, s.dsn)
		if err == nil {
			return conn
		}
		if ctx.Err() != nil {
			return nil
		}
		s.logger.Warn("storage/postgres: LISTEN re-dial failed; backing off", "err", err, "delay", delay)
		if delay *= 2; delay > listenReconnectMaxDelay {
			delay = listenReconnectMaxDelay
		}
	}
}

// reconcile replays, per registered channel, every cm minted past the
// channel's last-delivered serial — the cms whose NOTIFY was lost while
// the LISTEN conn was down (DESIGN.md §7.2). Each is delivered through
// cs.deliver, whose high-water mark makes replay idempotent against the
// normal NOTIFY path.
func (s *Storage) reconcile(ctx context.Context) {
	s.mu.Lock()
	stores := make([]*channelStore, 0, len(s.channels))
	for _, cs := range s.channels {
		if cs.appender != nil {
			stores = append(stores, cs)
		}
	}
	s.mu.Unlock()

	for _, cs := range stores {
		if err := cs.reconcileFromHistory(ctx); err != nil {
			s.logger.Warn("storage/postgres: reconcile failed", "channel", cs.name, "err", err)
		}
	}
}

// reapExpiredOccupancy deletes the occupancy contributions of nodes whose
// lease has lapsed and tells every node still holding those channels that the
// aggregate moved (DESIGN.md §16.2).
//
// It is simpler than the presence reap in one way and stricter in another. No
// event is synthesised — occupancy has no departure to publish, only a number
// that is now smaller — but the notification is not optional: nothing else
// would ever prompt a re-read, so a reaped contribution that went unannounced
// would leave every node reporting a dead node's connections until something
// unrelated moved the channel.
//
// The DELETE ... RETURNING takes a row lock per row, so when several nodes
// reap concurrently exactly one node's statement yields any given row — and
// therefore notifies for it once.
func (s *Storage) reapExpiredOccupancy(ctx context.Context) {
	rows, err := s.pool.Query(ctx,
		`DELETE FROM channel_occupancy WHERE expires_at < now()
		 RETURNING channel, node_id`)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: occupancy reap query failed", "err", err)
		}
		return
	}

	// One notification per channel however many nodes lapsed on it: the
	// notification says only that the aggregate is worth re-reading, and one
	// re-read covers every contribution that went.
	reaped := make(map[string]int)
	for rows.Next() {
		var channel, node string
		if err := rows.Scan(&channel, &node); err != nil {
			rows.Close()
			s.logger.Warn("storage/postgres: occupancy reap scan failed", "err", err)
			return
		}
		reaped[channel]++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: occupancy reap rows failed", "err", err)
		}
		return
	}

	for channel, nodes := range reaped {
		body, err := json.Marshal(notifyPayload{Channel: channel, Occupancy: true})
		if err != nil {
			continue
		}
		if _, err := s.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("storage/postgres: occupancy reap notify failed", "channel", channel, "err", err)
			}
			continue
		}
		s.logger.Debug("storage/postgres: reaped lapsed occupancy", "channel", channel, "nodes", nodes)
	}
}

// presenceLeaseBumpLoop refreshes the liveness lease for every presence
// row this node owns, in one UPDATE on the bump cadence (DESIGN.md
// §12.5). A live node thus keeps its members' expires_at ahead of now,
// so only a crashed node's rows ever lapse and become reapable.
func (s *Storage) presenceLeaseBumpLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(presenceLeaseBumpInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.pool.Exec(ctx,
				`UPDATE presence SET expires_at = now() + make_interval(secs => $2) WHERE node_id = $1`,
				s.node, presenceLeaseWindow.Seconds(),
			); err != nil && ctx.Err() == nil {
				s.logger.Warn("storage/postgres: presence lease bump failed", "err", err)
			}
			// This node's occupancy contributions hold the same lease
			// (DESIGN.md §16.2), bumped on the same tick so the two cannot
			// drift into disagreeing about whether this node is alive.
			if _, err := s.pool.Exec(ctx,
				`UPDATE channel_occupancy SET expires_at = now() + make_interval(secs => $2) WHERE node_id = $1`,
				s.node, presenceLeaseWindow.Seconds(),
			); err != nil && ctx.Err() == nil {
				s.logger.Warn("storage/postgres: occupancy lease bump failed", "err", err)
			}
		}
	}
}

// presenceReaperLoop periodically sweeps presence rows whose lease has
// lapsed, and the occupancy contributions of the nodes that left them
// (DESIGN.md §12.5, §16.2). See reapExpiredPresence and
// reapExpiredOccupancy for the mechanics.
func (s *Storage) presenceReaperLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(presenceReaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reapExpiredPresence(ctx)
			s.reapExpiredOccupancy(ctx)
		}
	}
}

// reapExpiredPresence deletes every presence row past its lease and
// synthesises a LEAVE for each through the normal publish path, so
// subscribers on every node observe the departure (DESIGN.md §12.5).
// The DELETE ... RETURNING takes a row lock per row, so when several
// nodes reap concurrently exactly one node's statement yields (and thus
// emits the LEAVE for) any given row. Rows are drained before the LEAVE
// publishes because StorePresence acquires its own pooled conn.
func (s *Storage) reapExpiredPresence(ctx context.Context) {
	rows, err := s.pool.Query(ctx,
		`DELETE FROM presence WHERE expires_at < now()
		 RETURNING channel, connection_id, client_id`)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence reap query failed", "err", err)
		}
		return
	}

	type orphan struct{ channel, connID, clientID string }
	var orphans []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.channel, &o.connID, &o.clientID); err != nil {
			rows.Close()
			s.logger.Warn("storage/postgres: presence reap scan failed", "err", err)
			return
		}
		orphans = append(orphans, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence reap rows failed", "err", err)
		}
		return
	}

	for _, o := range orphans {
		// Publish through a transient channelStore rather than the
		// registered one: the registered store (if any) must keep its
		// appender binding, and the transient one still mints a serial,
		// inserts the LEAVE cm and NOTIFYs, reaching every node's
		// appender via the LISTEN round-trip. Match the teardown-LEAVE
		// shape (§12.5): action + connectionId + clientId, no id (a
		// fresh publish, must not collide with the ENTER's idempotency
		// key).
		cs := &channelStore{pool: s.pool, series: s.series, node: s.node, name: o.channel}
		leave := &wire.PresenceMessage{
			Action:       wire.PresenceMessage_LEAVE,
			ClientId:     &o.clientID,
			ConnectionId: o.connID,
		}
		if _, _, err := cs.StorePresence(ctx, []*wire.PresenceMessage{leave}); err != nil && ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence reap LEAVE failed", "channel", o.channel, "err", err)
		} else if err == nil {
			s.logger.Debug("storage/postgres: reaped orphaned presence member", "channel", o.channel, "clientId", o.clientID, "connectionId", o.connID)
		}
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
// presence cm decodes into Presence, a message cm into Messages, an
// annotation cm into Annotations.
// (DESIGN.md §14.2). Closes rows on exit.
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
			var p *wire.PresenceMessage
			{
				var err error
				p, err = storage.DecodePresence(payload)
				if err != nil {
					return nil, fmt.Errorf("storage/postgres: decode presence payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
				}
			}
			cm.Presence = append(cm.Presence, p)
			continue
		}
		if kind == string(storage.KindState) {
			var sm *wire.StateMessage
			{
				var err error
				sm, err = storage.DecodeState(payload)
				if err != nil {
					return nil, fmt.Errorf("storage/postgres: decode state payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
				}
			}
			cm.State = append(cm.State, sm)
			continue
		}
		if kind == string(storage.KindAnnotation) {
			var a *wire.Annotation
			{
				var err error
				a, err = storage.DecodeAnnotation(payload)
				if err != nil {
					return nil, fmt.Errorf("storage/postgres: decode annotation payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
				}
			}
			cm.Annotations = append(cm.Annotations, a)
			continue
		}
		var m *wire.Message
		{
			var err error
			m, err = storage.DecodeMessage(payload)
			if err != nil {
				return nil, fmt.Errorf("storage/postgres: decode payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
			}
		}
		cm.Messages = append(cm.Messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: rows %s:%s: %w", channel, channelSerial, err)
	}
	if len(cm.Messages) == 0 && len(cm.Presence) == 0 && len(cm.Annotations) == 0 && len(cm.State) == 0 {
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
	node     string
	name     string
	appender storage.Appender

	// hwmMu guards lastSeen, the highest channel_serial delivered to
	// appender. It is the per-channel de-dup high-water mark that makes
	// post-reconnect history replay (reconcileFromHistory) idempotent
	// against the normal NOTIFY dispatch (DESIGN.md §7.2). Only the
	// LISTEN goroutine writes it, via deliver.
	hwmMu    sync.Mutex
	lastSeen string
}

// deliver hands cm to the appender exactly once and in order, advancing
// the per-channel high-water mark. A cm whose serial is not strictly
// greater than the last delivered serial is dropped — the case where a
// reconnect's history replay and a subsequently-buffered NOTIFY both
// carry it. All appends (steady-state NOTIFY dispatch and reconcile)
// funnel through here, from the single LISTEN goroutine.
func (cs *channelStore) deliver(cm *protocol.ChannelMessage) {
	cs.hwmMu.Lock()
	if cm.ChannelSerial <= cs.lastSeen {
		cs.hwmMu.Unlock()
		return
	}
	cs.lastSeen = cm.ChannelSerial
	cs.hwmMu.Unlock()
	cs.appender.Append(cm)
}

// reconcileFromHistory replays every cm minted after the channel's
// last-delivered serial — both message and presence kinds, merged in
// channelSerial order — through deliver (DESIGN.md §7.2). Called after
// a LISTEN reconnect to recover cms whose NOTIFY was lost in the gap.
func (cs *channelStore) reconcileFromHistory(ctx context.Context) error {
	cs.hwmMu.Lock()
	after := cs.lastSeen
	cs.hwmMu.Unlock()

	messages, err := cs.History(ctx, storage.HistoryQuery{
		Kind:               storage.KindMessage,
		Direction:          storage.DirectionForwards,
		AfterChannelSerial: after,
	})
	if err != nil {
		return fmt.Errorf("reconcile messages: %w", err)
	}
	presence, err := cs.History(ctx, storage.HistoryQuery{
		Kind:               storage.KindPresence,
		Direction:          storage.DirectionForwards,
		AfterChannelSerial: after,
	})
	if err != nil {
		return fmt.Errorf("reconcile presence: %w", err)
	}

	for _, cm := range mergeByChannelSerial(messages.ChannelMessages, presence.ChannelMessages) {
		cs.deliver(cm)
	}
	return nil
}

// mergeByChannelSerial merges two channelSerial-ascending cm slices
// (the message and presence streams share one channelSerial namespace
// but never collide on a serial) into a single ascending slice.
func mergeByChannelSerial(a, b []*protocol.ChannelMessage) []*protocol.ChannelMessage {
	out := make([]*protocol.ChannelMessage, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].ChannelSerial <= b[j].ChannelSerial {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// Store persists one publish atomically: look up any contained
// Message.IDs for prior matches (idempotent return on hit), otherwise
// advance the channel's serial via advance_channel_serial (which
// takes a row-level lock on the channels row and is the per-channel
// write serialiser), stamp each Message.Serial, insert one row per
// Message, and emit a NOTIFY on the broker channel. The cm is
// delivered to the channel's appender asynchronously by the LISTEN
// goroutine after the NOTIFY round-trips through the database.
func (cs *channelStore) Store(ctx context.Context, msgs []*wire.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/postgres: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Resolve the batch id and stamp each Message.ID = "<batchID>:<idx>"
	// (DESIGN.md §8) before the tx, so the partial UNIQUE id index keys on
	// the batch-derived ids.
	batchID, err := storage.StampMessageIDs(msgs)
	if err != nil {
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
		m.Action = wire.MessageAction_MESSAGE_CREATE
		storage.StampCreateVersion(m)
	}
	cm := &protocol.ChannelMessage{ID: batchID, ChannelSerial: channelSerial, Messages: msgs}

	for i, m := range msgs {
		payload, err := storage.EncodeMessage(m)
		if err != nil {
			return nil, false, err
		}
		// id is stored as NULL when empty so the partial UNIQUE
		// idempotency index never matches a no-id publish. message_serial
		// is the message identity (its own serial for a create) — the
		// versions index and the projection key off it (DESIGN.md §13.4).
		var idArg any
		if m.GetId() != "" {
			idArg = m.GetId()
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

// StoreSummary logs the summaries of freshly-annotated messages so they reach
// subscribers on every node via the LISTEN round-trip, without recording them
// as versions (see storage.ChannelStore). The log rows carry no message_serial,
// which is what keeps them off the version chain: the versions read selects by
// it, so a row without one is not a version of anything.
func (cs *channelStore) StoreSummary(ctx context.Context, summaries []*wire.Message) (*protocol.ChannelMessage, error) {
	if len(summaries) == 0 {
		return nil, errors.New("storage/postgres: StoreSummary with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tx, err := cs.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var channelSerial string
	if err := tx.QueryRow(ctx,
		`SELECT advance_channel_serial($1, $2)`, cs.name, cs.series,
	).Scan(&channelSerial); err != nil {
		return nil, fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}

	for i, m := range summaries {
		payload, err := storage.EncodeMessage(m)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, kind, payload)
			 VALUES ($1, $2, $3, 'message', $4)`,
			cs.name, channelSerial, i, payload,
		); err != nil {
			return nil, fmt.Errorf("storage/postgres: insert summary %d: %w", i, err)
		}
	}

	body, err := json.Marshal(notifyPayload{Channel: cs.name, Serial: channelSerial})
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
		return nil, fmt.Errorf("storage/postgres: notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("storage/postgres: commit: %w", err)
	}
	return &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: summaries}, nil
}

// Mutate applies an update/delete/append to an existing message
// (DESIGN.md §13.2) in one transaction: resolve the target's current
// latest version from the projection (ErrTargetNotFound if absent),
// apply the shallow-mixin merge, mint a fresh version serial, insert the
// merged version row on channel_messages (message_serial = identity),
// upsert the projection (deleted = TRUE for a delete), and NOTIFY. The
// cm reaches every node's appender via the LISTEN round-trip, like Store.
func (cs *channelStore) Mutate(ctx context.Context, mut *wire.Message, merge storage.MergeFunc) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.IsMutation() {
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
	if mut.GetId() != "" {
		var existingCS string
		switch err := tx.QueryRow(ctx,
			`SELECT channel_serial FROM channel_messages WHERE channel = $1 AND id = $2 LIMIT 1`,
			cs.name, mut.GetId()).Scan(&existingCS); {
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
	var current *wire.Message
	{
		var err error
		current, err = storage.DecodeMessage(curPayload)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: decode target version: %w", err)
		}
	}

	var channelSerial string
	if err := tx.QueryRow(ctx,
		`SELECT advance_channel_serial($1, $2)`, cs.name, cs.series,
	).Scan(&channelSerial); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	version, err := merge(current, mut, serial.MessageSerial(channelSerial, 0))
	if err != nil {
		return nil, false, err
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: []*wire.Message{version}}

	payload, err := storage.EncodeMessage(version)
	if err != nil {
		return nil, false, err
	}
	var idArg any
	if mut.GetId() != "" {
		idArg = mut.GetId()
	}
	// is_append marks the row as carrying an append, which is not a version of
	// the message (DESIGN.md §13.3): the row stays on the log for live and
	// resume fan-out, and the versions read leaves it out. The other backends
	// keep a version index and simply do not add to it; this one derives
	// versions from the log, so it needs the log to say which rows are versions.
	//
	// It is read off the merged version rather than off the edit, because the
	// merge is what turns an append into the update that carries it, and it
	// does that in place — so by here the edit no longer says it was an append.
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial, is_append)
		 VALUES ($1, $2, 0, $3, 'message', $4, $5, $6)`,
		cs.name, channelSerial, idArg, payload, mut.Serial, version.HasAppend(),
	); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: insert version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE messages SET payload = $3, deleted = $4 WHERE channel = $1 AND message_serial = $2`,
		cs.name, mut.Serial, payload, version.Action == wire.MessageAction_MESSAGE_DELETE,
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
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*wire.Message, error) {
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
	var m *wire.Message
	{
		var err error
		m, err = storage.DecodeMessage(payload)
		if err != nil {
			return nil, fmt.Errorf("storage/postgres: decode latest version: %w", err)
		}
	}
	return m, nil
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

	// An append is not a version of the message, so the log rows that carry
	// one are excluded (DESIGN.md §13.3, §13.4): the log keeps them for live
	// and resume fan-out, but a streamed append shows on the version chain as
	// the message's current content rather than as an entry per increment.
	// Pagination applies over what is left, so a page is whole versions.
	query := fmt.Sprintf(`
		SELECT channel_serial, payload FROM channel_messages
		WHERE channel = $1 AND message_serial = $2 AND kind = 'message'
		  AND is_append = FALSE
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
		var m *wire.Message
		{
			var err error
			m, err = storage.DecodeMessage(payload)
			if err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode version %s: %w", cs2, err)
			}
		}
		page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
			ChannelSerial: cs2,
			Messages:      []*wire.Message{m},
		})
		page.LastSerial = m.VersionOrSerial()
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
func (cs *channelStore) StorePresence(ctx context.Context, presence []*wire.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/postgres: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	static := storage.IsStaticPresence(ctx)

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
		storage.StampPresenceMember(p, channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Presence: presence}

	for i, p := range presence {
		payload, err := storage.EncodePresence(p)
		if err != nil {
			return nil, false, err
		}
		var idArg any
		if p.GetId() != "" {
			idArg = p.GetId()
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
		case wire.PresenceMessage_LEAVE, wire.PresenceMessage_ABSENT:
			if _, err := tx.Exec(ctx,
				`DELETE FROM presence WHERE channel = $1 AND connection_id = $2 AND client_id = $3`,
				cs.name, p.ConnectionId, p.GetClientId(),
			); err != nil {
				return nil, false, fmt.Errorf("storage/postgres: presence leave: %w", err)
			}
		default: // Enter, Update, Present
			if static {
				// Static fixture member (DESIGN.md §9, §12.5): a sentinel
				// owner and an 'infinity' lease so no lease-bump loop claims
				// it and the reaper (WHERE expires_at < now()) never deletes
				// it. It belongs to no connection, so nothing ever
				// synthesises a LEAVE for it.
				if _, err := tx.Exec(ctx,
					`INSERT INTO presence (channel, connection_id, client_id, channel_serial, payload, node_id, expires_at)
					 VALUES ($1, $2, $3, $4, $5, $6, 'infinity')
					 ON CONFLICT (channel, connection_id, client_id)
					 DO UPDATE SET channel_serial = EXCLUDED.channel_serial,
					               payload = EXCLUDED.payload,
					               node_id = EXCLUDED.node_id,
					               expires_at = EXCLUDED.expires_at`,
					cs.name, p.ConnectionId, p.GetClientId(), channelSerial, payload, fixtureNodeID,
				); err != nil {
					return nil, false, fmt.Errorf("storage/postgres: presence fixture upsert: %w", err)
				}
				break
			}
			// Stamp the owning node and a fresh lease (§12.5): this
			// node's bump loop keeps expires_at ahead while it lives; if
			// it crashes, the reaper on another node deletes the row once
			// the lease lapses and emits a synthetic LEAVE.
			if _, err := tx.Exec(ctx,
				`INSERT INTO presence (channel, connection_id, client_id, channel_serial, payload, node_id, expires_at)
				 VALUES ($1, $2, $3, $4, $5, $6, now() + make_interval(secs => $7))
				 ON CONFLICT (channel, connection_id, client_id)
				 DO UPDATE SET channel_serial = EXCLUDED.channel_serial,
				               payload = EXCLUDED.payload,
				               node_id = EXCLUDED.node_id,
				               expires_at = EXCLUDED.expires_at`,
				cs.name, p.ConnectionId, p.GetClientId(), channelSerial, payload, cs.node, presenceLeaseWindow.Seconds(),
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

// StoreAnnotation persists an annotation publish on channel_messages
// (kind = annotation, message_serial = the TARGET message serial so the
// channel_messages serial index serves annotations-for-message scans,
// DESIGN.md §14.1, §14.4), then emits a NOTIFY so the cm reaches every
// node's appender via the LISTEN round-trip exactly like a message
// publish. Every target must resolve in the messages projection
// (ErrTargetNotFound otherwise, like a mutation). The returned cm is the
// annotation summary-fold seam.
func (cs *channelStore) StoreAnnotation(ctx context.Context, annotations []*wire.Annotation, fold storage.FoldFunc) (*protocol.ChannelMessage, bool, error) {
	if len(annotations) == 0 {
		return nil, false, errors.New("storage/postgres: StoreAnnotation with no annotations")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotency pre-check — shared id namespace with messages/presence.
	if ids := nonEmptyAnnotationIDs(annotations); len(ids) > 0 {
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

	// Target existence: every annotation must reference a message in the
	// projection (DESIGN.md §14.1). Checked before advancing the serial so
	// a bad target does not burn one.
	for _, a := range annotations {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM messages WHERE channel = $1 AND message_serial = $2)`,
			cs.name, a.MessageSerial,
		).Scan(&exists); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: annotation target lookup: %w", err)
		}
		if !exists {
			return nil, false, storage.ErrTargetNotFound
		}
	}

	var channelSerial string
	if err := tx.QueryRow(ctx,
		`SELECT advance_channel_serial($1, $2)`, cs.name, cs.series,
	).Scan(&channelSerial); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	for i, a := range annotations {
		a.Serial = storage.AnnotationSerial(channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Annotations: annotations}

	for i, a := range annotations {
		payload, err := storage.EncodeAnnotation(a)
		if err != nil {
			return nil, false, err
		}
		// Fold the annotation into its target's summary projection,
		// atomically within this tx (DESIGN.md §14.2), so that every later
		// read of the message it annotates carries the folded summary — on
		// this node and on every other.
		if err := cs.foldSummaryTx(ctx, tx, a, fold); err != nil {
			return nil, false, err
		}
		var idArg any
		if a.GetId() != "" {
			idArg = a.GetId()
		}
		// message_serial holds the TARGET message serial so the existing
		// channel_messages_serial_idx serves the annotations-for-message
		// scan (DESIGN.md §14.4).
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
			 VALUES ($1, $2, $3, $4, 'annotation', $5, $6)`,
			cs.name, channelSerial, i, idArg, payload, a.MessageSerial,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert annotation %d: %w", i, err)
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

// foldSummaryTx folds one annotation into its target message's summary on
// the messages projection (DESIGN.md §14.2). It runs inside the
// StoreAnnotation tx with the target already validated to exist: it reads the
// projection payload, folds, and writes the merged Message back, so message
// reads carry the current summary — including on a node that never witnessed
// the annotation.
func (cs *channelStore) foldSummaryTx(ctx context.Context, tx pgx.Tx, a *wire.Annotation, fold storage.FoldFunc) error {
	var payload []byte
	if err := tx.QueryRow(ctx,
		`SELECT payload FROM messages WHERE channel = $1 AND message_serial = $2`,
		cs.name, a.MessageSerial,
	).Scan(&payload); err != nil {
		return fmt.Errorf("storage/postgres: load projection for summary fold: %w", err)
	}
	var m *wire.Message
	{
		var err error
		m, err = storage.DecodeMessage(payload)
		if err != nil {
			return fmt.Errorf("storage/postgres: decode projection for summary fold: %w", err)
		}
	}
	fold(m, a)
	merged, err := storage.EncodeMessage(m)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE messages SET payload = $3 WHERE channel = $1 AND message_serial = $2`,
		cs.name, a.MessageSerial, merged,
	); err != nil {
		return fmt.Errorf("storage/postgres: update projection after summary fold: %w", err)
	}
	return nil
}

// Annotations returns the annotations attached to messageSerial in stream
// order, paginated at annotation-serial granularity (DESIGN.md §14.4). The
// scan filters channel_messages by kind = 'annotation' and message_serial
// = the target, served by channel_messages_serial_idx. An unknown target
// yields an empty page.
func (cs *channelStore) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	forwards := q.Direction == storage.DirectionForwards
	order, cursorOp := "ASC", ">"
	if !forwards {
		order, cursorOp = "DESC", "<"
	}

	var (
		cursorChannelSerial string
		cursorIdx           int
	)
	if q.Cursor != "" {
		var err error
		cursorChannelSerial, cursorIdx, err = serial.ParseMessageSerial(q.Cursor)
		if err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: parse annotation cursor: %w", err)
		}
	}

	query := fmt.Sprintf(`
		SELECT channel_serial, idx, payload
		FROM channel_messages
		WHERE channel = $1
		  AND kind = 'annotation'
		  AND message_serial = $2
		  AND ($3 = '' OR (channel_serial, idx) %s ($3, $4))
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, messageSerial, cursorChannelSerial, cursorIdx, q.Limit)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: annotations query: %w", err)
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
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan annotation row: %w", err)
		}
		var a *wire.Annotation
		{
			var err error
			a, err = storage.DecodeAnnotation(payload)
			if err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode annotation payload %s:%d: %w", cs2, idx, err)
			}
		}
		appendAnnotation(&page, cs2, a)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: annotation rows: %w", err)
	}

	if q.Limit > 0 && itemCount(page) > q.Limit {
		trimToLimit(&page, q.Limit)
		page.HasMore = true
	}
	return page, nil
}

// Members returns the channel's presence projection plus the channel's
// current watermark serial as the as-of point.
// The page is cut in SQL rather than read whole and sliced: a channel with
// many thousands of members is exactly the case paging exists for, and reading
// the set into memory to return a hundred of it would defeat the point.
//
// The cursor is the (channel_serial, client_id) pair the ordering is on,
// which is this backend's own — a cursor from another means nothing here.
func (cs *channelStore) Members(ctx context.Context, q storage.MembersQuery) (storage.MembersPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.MembersPage{}, err
	}

	query := `SELECT payload, channel_serial, client_id FROM presence WHERE channel = $1`
	args := []any{cs.name}
	if q.After != "" {
		serial, clientID, ok := strings.Cut(q.After, "\x00")
		if !ok {
			return storage.MembersPage{}, fmt.Errorf("storage/postgres: members cursor %q is not one of ours", q.After)
		}
		query += ` AND (channel_serial, client_id) > ($2, $3)`
		args = append(args, serial, clientID)
	}
	query += ` ORDER BY channel_serial, client_id`
	if q.Limit > 0 {
		// One more than asked for, so that a full page can be told from the
		// last one without a second query counting what is left.
		query += fmt.Sprintf(` LIMIT %d`, q.Limit+1)
	}

	rows, err := cs.pool.Query(ctx, query, args...)
	if err != nil {
		return storage.MembersPage{}, fmt.Errorf("storage/postgres: members query: %w", err)
	}
	defer rows.Close()

	var out []*wire.PresenceMessage
	var cursors []string
	for rows.Next() {
		var payload []byte
		var serial, clientID string
		if err := rows.Scan(&payload, &serial, &clientID); err != nil {
			return storage.MembersPage{}, fmt.Errorf("storage/postgres: scan member: %w", err)
		}
		var p *wire.PresenceMessage
		{
			var err error
			p, err = storage.DecodePresence(payload)
			if err != nil {
				return storage.MembersPage{}, fmt.Errorf("storage/postgres: decode member: %w", err)
			}
		}
		out = append(out, p)
		cursors = append(cursors, serial+"\x00"+clientID)
	}
	if err := rows.Err(); err != nil {
		return storage.MembersPage{}, fmt.Errorf("storage/postgres: members rows: %w", err)
	}

	var next string
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
		next = cursors[q.Limit-1]
	}

	page := storage.MembersPage{Members: out, NextCursor: next}

	if err := cs.pool.QueryRow(ctx,
		`SELECT channel_serial FROM channels WHERE name = $1`, cs.name,
	).Scan(&page.AsOfSerial); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			page.AsOfSerial = ""
			return page, nil
		}
		return storage.MembersPage{}, fmt.Errorf("storage/postgres: members watermark: %w", err)
	}
	return page, nil
}

// StoreState persists a LiveObjects publish onto channel_messages and applies
// its operations to the state_objects projection, in one transaction
// (DESIGN.md §15.2, §15.3) — so a node reading the set never sees it disagree
// with the log it is the fold of.
//
// Two nodes publishing to the same channel cannot interleave their applies:
// advance_channel_serial takes the channels row's lock and holds it to commit,
// and it runs before the objects are loaded. That is what makes the load,
// apply and write-back atomic across the cluster without locking the objects
// themselves — which could not be locked anyway, since an operation may name
// an object that does not exist yet.
func (cs *channelStore) StoreState(ctx context.Context, state []*wire.StateMessage, apply storage.ApplyFunc) (*protocol.ChannelMessage, bool, error) {
	if len(state) == 0 {
		return nil, false, errors.New("storage/postgres: StoreState with no messages")
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
	if ids := nonEmptyStateIDs(state); len(ids) > 0 {
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
	for i, sm := range state {
		storage.StampStateMessage(sm, channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, State: state}

	for i, sm := range state {
		payload, err := storage.EncodeState(sm)
		if err != nil {
			return nil, false, err
		}
		var idArg any
		if sm.GetId() != "" {
			idArg = sm.GetId()
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload)
			 VALUES ($1, $2, $3, $4, 'state', $5)`,
			cs.name, channelSerial, i, idArg, payload,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert state %d: %w", i, err)
		}
	}

	named, err := loadStateObjectsTx(ctx, tx, cs.name, storage.StateObjectIDs(state))
	if err != nil {
		return nil, false, err
	}
	changed, err := apply(named, state)
	if err != nil {
		return nil, false, err
	}
	for _, obj := range changed {
		payload, err := storage.EncodeStateObject(obj)
		if err != nil {
			return nil, false, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO state_objects (channel, object_id, channel_serial, payload)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (channel, object_id)
			 DO UPDATE SET channel_serial = EXCLUDED.channel_serial,
			               payload = EXCLUDED.payload`,
			cs.name, obj.GetObjectId(), channelSerial, payload,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: state object upsert: %w", err)
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

// loadStateObjectsTx reads the named objects, in the order named, skipping the
// ones that do not exist yet — an operation may be the one that creates its
// object, and the apply is what decides that.
func loadStateObjectsTx(ctx context.Context, tx pgx.Tx, channel string, ids []string) ([]*wire.StateObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx,
		`SELECT object_id, payload FROM state_objects WHERE channel = $1 AND object_id = ANY($2)`,
		channel, ids)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: load state objects: %w", err)
	}
	defer rows.Close()

	byID := make(map[string]*wire.StateObject, len(ids))
	for rows.Next() {
		var (
			id      string
			payload []byte
		)
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan state object: %w", err)
		}
		obj, err := storage.DecodeStateObject(payload)
		if err != nil {
			return nil, fmt.Errorf("storage/postgres: decode state object %s/%s: %w", channel, id, err)
		}
		byID[id] = obj
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: state object rows: %w", err)
	}

	out := make([]*wire.StateObject, 0, len(byID))
	for _, id := range ids {
		if obj, ok := byID[id]; ok {
			out = append(out, obj)
		}
	}
	return out, nil
}

// Objects pages the materialised object set out of the state_objects
// projection, ordered by object id — which is both the table's key order and
// the order a state sync walks the set in, so the page needs no sort and its
// cursor is the last object id on it.
func (cs *channelStore) Objects(ctx context.Context, q storage.ObjectsQuery) (storage.ObjectsPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ObjectsPage{}, err
	}

	query := `SELECT payload FROM state_objects WHERE channel = $1 AND ($2 = '' OR object_id > $2) ORDER BY object_id`
	if q.Limit > 0 {
		// One more than asked for, so that a full page can be told from the
		// last one without a second query counting what is left.
		query += fmt.Sprintf(` LIMIT %d`, q.Limit+1)
	}

	rows, err := cs.pool.Query(ctx, query, cs.name, q.After)
	if err != nil {
		return storage.ObjectsPage{}, fmt.Errorf("storage/postgres: objects query: %w", err)
	}
	defer rows.Close()

	var out []*wire.StateObject
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return storage.ObjectsPage{}, fmt.Errorf("storage/postgres: scan object: %w", err)
		}
		obj, err := storage.DecodeStateObject(payload)
		if err != nil {
			return storage.ObjectsPage{}, fmt.Errorf("storage/postgres: decode object: %w", err)
		}
		out = append(out, obj)
	}
	if err := rows.Err(); err != nil {
		return storage.ObjectsPage{}, fmt.Errorf("storage/postgres: objects rows: %w", err)
	}

	var next string
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
		next = out[q.Limit-1].GetObjectId()
	}

	page := storage.ObjectsPage{Objects: out, NextCursor: next}

	if err := cs.pool.QueryRow(ctx,
		`SELECT channel_serial FROM channels WHERE name = $1`, cs.name,
	).Scan(&page.AsOfSerial); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			page.AsOfSerial = ""
			return page, nil
		}
		return storage.ObjectsPage{}, fmt.Errorf("storage/postgres: objects watermark: %w", err)
	}
	return page, nil
}

// StoreOccupancy upserts this node's contribution and notifies every node
// holding the channel that the aggregate moved (DESIGN.md §16.2, §16.3).
//
// A contribution of nothing deletes the row rather than storing zeros, so a
// channel nobody is attached to on any node has no rows at all and its
// aggregate is the zero value by construction — there is nothing to
// distinguish "unoccupied" from "never occupied", and nothing wants to.
//
// The NOTIFY goes inside the transaction, as a publish's does, so a listener
// only sees it if the write committed. The notifying node hears its own
// notification back and reports from that, exactly as a publisher sees its own
// publish via the LISTEN round-trip (§7.2): one delivery path, no
// self-vs-foreign split.
func (cs *channelStore) StoreOccupancy(ctx context.Context, counts *wire.ChannelOccupancy) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("storage/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if storage.OccupancyIsEmpty(counts) {
		if _, err := tx.Exec(ctx,
			`DELETE FROM channel_occupancy WHERE channel = $1 AND node_id = $2`,
			cs.name, cs.node,
		); err != nil {
			return fmt.Errorf("storage/postgres: occupancy delete: %w", err)
		}
	} else if _, err := tx.Exec(ctx,
		`INSERT INTO channel_occupancy (
		   channel, node_id, channel_mode, connections, publishers, subscribers,
		   presence_connections, presence_subscribers, object_subscribers,
		   object_publishers, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now() + make_interval(secs => $11))
		 ON CONFLICT (channel, node_id)
		 DO UPDATE SET channel_mode = EXCLUDED.channel_mode,
		               connections = EXCLUDED.connections,
		               publishers = EXCLUDED.publishers,
		               subscribers = EXCLUDED.subscribers,
		               presence_connections = EXCLUDED.presence_connections,
		               presence_subscribers = EXCLUDED.presence_subscribers,
		               object_subscribers = EXCLUDED.object_subscribers,
		               object_publishers = EXCLUDED.object_publishers,
		               expires_at = EXCLUDED.expires_at`,
		cs.name, cs.node, counts.GetChannelMode(), counts.GetConnections(),
		counts.GetPublishers(), counts.GetSubscribers(), counts.GetPresenceConnections(),
		counts.GetPresenceSubscribers(), counts.GetObjectSubscribers(),
		counts.GetObjectPublishers(), presenceLeaseWindow.Seconds(),
	); err != nil {
		return fmt.Errorf("storage/postgres: occupancy upsert: %w", err)
	}

	body, err := json.Marshal(notifyPayload{Channel: cs.name, Occupancy: true})
	if err != nil {
		return fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
		return fmt.Errorf("storage/postgres: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("storage/postgres: commit: %w", err)
	}
	return nil
}

// Occupancy sums every node's contribution in SQL — counts added, modes
// bit_or'd — and reads presenceMembers off the presence table, which is
// already global (DESIGN.md §16.2).
//
// Rows past their lease are excluded rather than waited on: a dead node's
// contribution is wrong the moment it dies, and the reaper deleting it is a
// tidy-up rather than the thing that makes the aggregate correct.
func (cs *channelStore) Occupancy(ctx context.Context) (*wire.ChannelOccupancy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	occ := &wire.ChannelOccupancy{}
	if err := cs.pool.QueryRow(ctx,
		`SELECT COALESCE(bit_or(channel_mode), 0),
		        COALESCE(SUM(connections), 0),
		        COALESCE(SUM(publishers), 0),
		        COALESCE(SUM(subscribers), 0),
		        COALESCE(SUM(presence_connections), 0),
		        COALESCE(SUM(presence_subscribers), 0),
		        COALESCE(SUM(object_subscribers), 0),
		        COALESCE(SUM(object_publishers), 0)
		 FROM channel_occupancy
		 WHERE channel = $1 AND expires_at >= now()`,
		cs.name,
	).Scan(
		&occ.ChannelMode, &occ.Connections, &occ.Publishers, &occ.Subscribers,
		&occ.PresenceConnections, &occ.PresenceSubscribers,
		&occ.ObjectSubscribers, &occ.ObjectPublishers,
	); err != nil {
		return nil, fmt.Errorf("storage/postgres: occupancy aggregate: %w", err)
	}

	if err := cs.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM presence WHERE channel = $1`, cs.name,
	).Scan(&occ.PresenceMembers); err != nil {
		return nil, fmt.Errorf("storage/postgres: occupancy presence members: %w", err)
	}
	return occ, nil
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
		  AND ($9 = '' OR channel_serial > $9)
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $6 > 0 THEN $6 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query,
		cs.name, timeLower, timeUpper,
		cursorChannelSerial, cursorIdx,
		limit,
		q.EndChannelSerial,
		string(wantKind),
		q.AfterChannelSerial,
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
			var p *wire.PresenceMessage
			{
				var err error
				p, err = storage.DecodePresence(payload)
				if err != nil {
					return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode presence payload %s:%d: %w", cs2, idx, err)
				}
			}
			appendPresence(&page, cs2, p)
			continue
		}
		if wantKind == storage.KindState {
			var sm *wire.StateMessage
			{
				var err error
				sm, err = storage.DecodeState(payload)
				if err != nil {
					return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode state payload %s:%d: %w", cs2, idx, err)
				}
			}
			appendState(&page, cs2, sm)
			continue
		}
		if wantKind == storage.KindAnnotation {
			var a *wire.Annotation
			{
				var err error
				a, err = storage.DecodeAnnotation(payload)
				if err != nil {
					return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode annotation payload %s:%d: %w", cs2, idx, err)
				}
			}
			appendAnnotation(&page, cs2, a)
			continue
		}
		var m *wire.Message
		{
			var err error
			m, err = storage.DecodeMessage(payload)
			if err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode payload %s:%d: %w", cs2, idx, err)
			}
		}
		appendMessage(&page, cs2, m)
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
// backwards, matching the raw scan). q.EndChannelSerial (the
// fromSerial/untilAttached bound) caps rows to their CREATE
// channelSerial <= the bound: split_part(message_serial, ':', 1)
// strips the ":idx" suffix (message_serial never has more than one
// ':' — serial.ParseMessageSerial's channelSerial half never contains
// one) to get the pure channelSerial for the compare.
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
		  AND ($6 = '' OR split_part(message_serial, ':', 1) <= $6)
		ORDER BY message_serial %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, timeLower, timeUpper, q.Cursor, q.Limit, q.EndChannelSerial)
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
		var m *wire.Message
		{
			var err error
			m, err = storage.DecodeMessage(payload)
			if err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode projection %s: %w", identity, err)
			}
		}
		appendMessage(&page, storage.CreateChannelSerial(identity), m)
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
func appendMessage(page *storage.HistoryPage, channelSerial string, m *wire.Message) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Messages = append(page.ChannelMessages[n-1].Messages, m)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Messages:      []*wire.Message{m},
	})
}

// appendPresence tacks p onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. The presence
// analogue of appendMessage.
func appendPresence(page *storage.HistoryPage, channelSerial string, p *wire.PresenceMessage) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Presence = append(page.ChannelMessages[n-1].Presence, p)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Presence:      []*wire.PresenceMessage{p},
	})
}

// appendAnnotation tacks a onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. The annotation
// analogue of appendMessage.
func appendAnnotation(page *storage.HistoryPage, channelSerial string, a *wire.Annotation) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Annotations = append(page.ChannelMessages[n-1].Annotations, a)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Annotations:   []*wire.Annotation{a},
	})
}

// appendState is the state analogue of appendPresence.
func appendState(page *storage.HistoryPage, channelSerial string, sm *wire.StateMessage) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].State = append(page.ChannelMessages[n-1].State, sm)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		State:         []*wire.StateMessage{sm},
	})
}

// cmLen is the item count of a cm — Messages, Presence, Annotations or
// State, whichever the (single-kind) cm carries.
func cmLen(cm *protocol.ChannelMessage) int {
	return len(cm.Messages) + len(cm.Presence) + len(cm.Annotations) + len(cm.State)
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
		switch {
		case len(cm.Presence) > 0:
			cm.Presence = cm.Presence[:left]
		case len(cm.Annotations) > 0:
			cm.Annotations = cm.Annotations[:left]
		case len(cm.State) > 0:
			cm.State = cm.State[:left]
		default:
			cm.Messages = cm.Messages[:left]
		}
		page.ChannelMessages = page.ChannelMessages[:i+1]
		return
	}
}

// nonEmptyIDs returns the subset of m.GetId() values that are non-empty.
// Used for the idempotency pre-check.
func nonEmptyIDs(msgs []*wire.Message) []string {
	var ids []string
	for _, m := range msgs {
		if m.GetId() != "" {
			ids = append(ids, m.GetId())
		}
	}
	return ids
}

// nonEmptyPresenceIDs is the presence analogue of nonEmptyIDs.
func nonEmptyPresenceIDs(presence []*wire.PresenceMessage) []string {
	var ids []string
	for _, p := range presence {
		if p.GetId() != "" {
			ids = append(ids, p.GetId())
		}
	}
	return ids
}

// nonEmptyStateIDs is the state analogue of nonEmptyIDs.
func nonEmptyStateIDs(state []*wire.StateMessage) []string {
	var ids []string
	for _, sm := range state {
		if sm.GetId() != "" {
			ids = append(ids, sm.GetId())
		}
	}
	return ids
}

// nonEmptyAnnotationIDs is the annotation analogue of nonEmptyIDs.
func nonEmptyAnnotationIDs(annotations []*wire.Annotation) []string {
	var ids []string
	for _, a := range annotations {
		if a.GetId() != "" {
			ids = append(ids, a.GetId())
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
