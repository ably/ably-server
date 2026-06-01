// Package storage defines the persistence boundary for channel data.
// Implementations live in sub-packages (memory, bbolt, postgres) and
// are selected by the server's --mode flag.
//
// Each persisted publish reaches the in-process channel via an
// Appender callback: callers (core.Manager) pair a core.Channel with
// its ChannelStore by calling Storage.Channel(name, channelAsAppender).
// Memory and bbolt fire the appender synchronously after committing
// the publish; postgres fires it from a LISTEN goroutine after
// receiving the NOTIFY emitted inside the publish transaction. The
// publish path itself only calls ChannelStore.Store — the link onto
// the live linked list always arrives via the appender (DESIGN.md §7).
//
// Serial assignment is owned by the backend because the generator's
// monotonicity state must be persisted alongside the data it secures
// (see DESIGN.md §6, §8).
package storage

import (
	"context"

	"github.com/ably/ably-server/internal/protocol"
)

// Appender receives a ChannelMessage that has just landed on its
// channel (either via a local Store call on memory/bbolt, or via a
// Postgres NOTIFY round-trip in cluster mode). In core, *Channel
// implements this — Appender.Append links the cm onto the live
// linked list so attached streams observe it.
type Appender interface {
	Append(cm *protocol.ChannelMessage)
}

// Storage is the per-process persistence root. It hands out
// per-channel stores and owns any shared resources (e.g. a bolt DB
// handle or a pgxpool).
type Storage interface {
	// Channel returns the ChannelStore for the given channel name,
	// associating it with appender. Successive calls with the same
	// name return the same instance and ignore the new appender (the
	// channelStore→appender binding is fixed at first call). appender
	// may be nil for storage-only use cases (e.g. the contract test
	// suite); a nil appender means committed cms are not delivered
	// anywhere.
	Channel(name string, appender Appender) ChannelStore

	// Close releases any resources held by the storage backend. After
	// Close, behaviour of ChannelStores previously handed out is
	// undefined.
	Close() error
}

// ChannelStore is the per-channel persistence facet. All methods are
// safe for concurrent use.
type ChannelStore interface {
	// Store persists a publish atomically:
	//
	//   - If any contained Message.ID is non-empty AND has already
	//     been seen on this channel within the retention window, the
	//     publish is treated as a duplicate: the originally-persisted
	//     ChannelMessage is returned with idempotent=true and nothing
	//     is written. The appender is NOT invoked (the original was
	//     delivered when first persisted).
	//   - Otherwise a fresh channelSerial is minted, each msgs[i].Serial
	//     is stamped to "<channelSerial>:<idx>", the ChannelMessage is
	//     persisted, IDs (if any) are indexed, and the appender is
	//     delivered the cm — synchronously after commit for in-process
	//     backends, asynchronously via the LISTEN goroutine for the
	//     Postgres backend.
	//
	// On idempotent return, callers should use the returned
	// ChannelMessage (the original) rather than the messages they
	// passed in.
	Store(ctx context.Context, msgs []*protocol.Message) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// History returns ChannelMessages in publish order. An empty
	// AfterChannelSerial means "from the oldest retained
	// ChannelMessage"; otherwise results start strictly after the
	// given channelSerial.
	History(ctx context.Context, q HistoryQuery) (HistoryPage, error)
}

// Direction selects the history scan order.
//
// The zero value is DirectionBackwards to match Ably's REST default
// (newest first), so an unset HistoryQuery yields the SDK-expected
// ordering.
type Direction uint8

const (
	// DirectionBackwards iterates newest publish first. The Messages
	// slice within each returned ChannelMessage is also reversed
	// (highest idx first), so a flatten yields fully-reversed order.
	DirectionBackwards Direction = iota

	// DirectionForwards iterates oldest publish first. The Messages
	// slice within each returned ChannelMessage is in natural idx
	// order.
	DirectionForwards
)

// HistoryQuery bounds a history read.
type HistoryQuery struct {
	// Direction selects scan order. Zero value is DirectionBackwards
	// (Ably default).
	Direction Direction

	// Start, End are inclusive bounds on the publish timestamp encoded
	// in each ChannelMessage's serial (DESIGN.md §8). Units are
	// milliseconds since the Unix epoch. Zero means "no bound on that
	// side".
	Start int64
	End   int64

	// Cursor is a Message.Serial (`<channelSerial>:<idx>`) used for
	// pagination:
	//   - DirectionForwards:  results are strictly after this serial
	//   - DirectionBackwards: results are strictly before this serial
	// Empty means "no cursor". The serial is compared lexicographically
	// (the format makes lex compare match logical order), so a cursor
	// may land mid-batch — the page may begin or end with a partial
	// ChannelMessage carrying only the surviving subset of Messages.
	Cursor string

	// Limit caps the number of MESSAGES (not ChannelMessages) returned,
	// matching Ably's REST `limit` semantics. Zero or negative means no
	// limit. When the limit cuts a multi-message batch, the trailing
	// ChannelMessage in the page is partial; HasMore is true.
	Limit int
}

// HistoryPage is one page of history results, ordered per the query's
// Direction.
//
// For DirectionBackwards the returned ChannelMessages are newest-first
// and each entry's Messages slice has been reversed (highest idx
// first); for DirectionForwards both orderings are natural. The
// Messages slice is always a fresh slice — backends MUST NOT mutate
// the persisted ChannelMessage when reversing or when emitting
// partial-batch pages.
//
// When the query's Cursor lands mid-batch, the first ChannelMessage in
// the page may contain only the Messages that survive the cursor (a
// proper subset of the persisted batch). When Limit cuts mid-batch,
// the last ChannelMessage in the page is similarly partial.
type HistoryPage struct {
	ChannelMessages []*protocol.ChannelMessage

	// HasMore is true if the query was Limit-bounded and at least one
	// further Message exists past the last entry returned (in the
	// requested direction). Counted at Message granularity to match
	// the Limit semantics.
	HasMore bool
}
