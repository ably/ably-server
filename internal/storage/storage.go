// Package storage defines the persistence boundary for channel data.
// Implementations live in sub-packages (memory, bbolt, ...) and are
// selected by the server's --mode flag.
//
// The interface deliberately keeps concerns narrow: append a
// ChannelMessage with atomic idempotency, and read forward history.
// Serial assignment is owned by the backend because the generator's
// monotonicity state must be persisted alongside the data it secures
// (see DESIGN.md §6, §8).
package storage

import (
	"context"

	"github.com/ably/ably-server/internal/protocol"
)

// Storage is the per-process persistence root. It hands out per-channel
// stores and owns any shared resources (e.g. a bolt DB handle).
type Storage interface {
	// Channel returns the ChannelStore for the given channel name.
	// Successive calls with the same name return the same instance.
	Channel(name string) ChannelStore

	// Close releases any resources held by the storage backend. After
	// Close, behaviour of ChannelStores previously handed out is
	// undefined.
	Close() error
}

// ChannelStore is the per-channel persistence facet. All methods are
// safe for concurrent use.
type ChannelStore interface {
	// AppendChannelMessage persists a publish atomically:
	//
	//   - If any contained Message.ID is non-empty AND has already been
	//     seen on this channel within the retention window, the publish
	//     is treated as a duplicate: the originally-persisted
	//     ChannelMessage is returned with idempotent=true and nothing
	//     is written.
	//   - Otherwise a fresh channelSerial is minted, each msgs[i].Serial
	//     is stamped to "<channelSerial>:<idx>", the ChannelMessage is
	//     persisted, and the IDs (if any) are indexed.
	//
	// On idempotent return, callers should use the returned
	// ChannelMessage (the original) rather than the messages they
	// passed in.
	AppendChannelMessage(ctx context.Context, msgs []*protocol.Message) (cm *protocol.ChannelMessage, idempotent bool, err error)

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
