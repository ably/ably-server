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

// HistoryQuery bounds a forward history read.
type HistoryQuery struct {
	// AfterChannelSerial — empty means "from the start of retained
	// history". Otherwise, results begin strictly after this serial.
	AfterChannelSerial string

	// Limit caps the number of ChannelMessages returned. Zero or
	// negative means no limit.
	Limit int
}

// HistoryPage is one page of forward history results.
type HistoryPage struct {
	ChannelMessages []*protocol.ChannelMessage

	// HasMore is true if the query was Limit-bounded and at least one
	// further ChannelMessage exists past the last entry returned.
	HasMore bool
}
