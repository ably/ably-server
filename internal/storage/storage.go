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

// Appender is the bridge between the storage backend and the in-process
// channel state. The backend calls Initialize exactly once, before any
// Append, to hand the channel two channelSerials — the current cursor
// (used as the attach point for fresh attaches) and the channel's
// immutable initial serial (used as the attach point for rewinds that
// reach back past every persisted cm). After Initialize, Append
// delivers each persisted ChannelMessage (synchronously for
// memory/bbolt, asynchronously via the Postgres LISTEN goroutine in
// cluster mode).
//
// In core, *Channel implements Appender — Initialize seeds the channel
// sentinel's serial, records the initial value, and unblocks Attach;
// Append links the cm onto the live linked list so attached streams
// observe it.
type Appender interface {
	// Initialize is called once by the storage backend with two
	// channelSerials. current is the channel's current cursor at
	// materialisation time (== latest persisted cm's serial, or the
	// freshly-minted seed for an empty channel). initial is the
	// channel's immutable seed serial, guaranteed to sort strictly
	// less than every cm ever persisted on this channel. Until
	// Initialize returns, the channel is "not ready" and Attach blocks.
	Initialize(current, initial string)

	// Append delivers one persisted ChannelMessage to the live linked
	// list. Backends guarantee monotonicity: every Append's serial is
	// strictly greater than every prior Append's serial and strictly
	// greater than the Initialize current/initial serials.
	Append(cm *protocol.ChannelMessage)
}

// Storage is the per-process persistence root. It hands out
// per-channel stores and owns any shared resources (e.g. a bolt DB
// handle or a pgxpool).
type Storage interface {
	// Channel returns the ChannelStore for the given channel name,
	// associating it with appender. The backend calls
	// appender.Initialize(initialSerial) synchronously before returning,
	// so callers can safely treat the channel as ready for Attach.
	// Successive calls with the same name return the same instance and
	// ignore the new appender (the channelStore→appender binding is
	// fixed at first call). appender may be nil for storage-only use
	// cases (e.g. the contract test suite); a nil appender means
	// committed cms are not delivered anywhere and Initialize is not
	// called.
	Channel(ctx context.Context, name string, appender Appender) (ChannelStore, error)

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

	// StorePresence is the presence analogue of Store (DESIGN.md §12.2,
	// §12.5). It mints a channelSerial, stamps each PresenceMessage.Serial
	// to "<channelSerial>:<idx>", persists the presence ChannelMessage on
	// the same stream (kind = presence), and — atomically with the
	// persist — folds the operations into the channel's membership set
	// keyed by "<connectionId>:<clientId>": ENTER/UPDATE/PRESENT upsert a
	// member, LEAVE/ABSENT remove it. The appender then receives the
	// presence cm exactly as for a message publish.
	//
	// Idempotency works like Store: a contained PresenceMessage.ID already
	// seen on this channel returns the original cm with idempotent=true
	// and folds nothing.
	StorePresence(ctx context.Context, presence []*protocol.PresenceMessage) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// Members returns the channel's current presence set plus the
	// channelSerial the set is current as-of (DESIGN.md §12.4). The
	// as-of serial is the channel's current watermark; it is empty only
	// when the channel has no persisted cms at all. Backs presence sync
	// and the REST presence endpoint.
	Members(ctx context.Context) (members []*protocol.PresenceMessage, asOfSerial string, err error)

	// History returns ChannelMessages in publish order. An empty
	// AfterChannelSerial means "from the oldest retained
	// ChannelMessage"; otherwise results start strictly after the
	// given channelSerial. q.Kind selects the stream (messages or
	// presence); the zero value reads messages.
	History(ctx context.Context, q HistoryQuery) (HistoryPage, error)
}

// Kind distinguishes the two cm streams that share a channel's ordered
// log and channelSerial namespace (DESIGN.md §12.1). The zero value is
// KindMessage so an unset HistoryQuery reads message history.
type Kind string

const (
	KindMessage  Kind = "message"
	KindPresence Kind = "presence"
)

// Normalize maps the zero value to KindMessage.
func (k Kind) Normalize() Kind {
	if k == "" {
		return KindMessage
	}
	return k
}

// MemberKey returns the presence-set key for a member: the pair
// (connectionId, clientId) that identifies one member, so the same
// clientId over two connections is two distinct members (DESIGN.md §12.1).
func MemberKey(connectionID, clientID string) string {
	return connectionID + ":" + clientID
}

// HistItem is one item within a ChannelMessage during a kind-aware
// history scan: either a Message or a PresenceMessage. Serial is the
// item's Message.serial (the pagination cursor unit); Append links the
// item onto a destination ChannelMessage being assembled for the page.
type HistItem struct {
	Serial string
	Append func(dst *protocol.ChannelMessage)
}

// CMItems returns a ChannelMessage's items for the requested kind, in
// natural (idx) order. A cm of the other kind yields no items, so a
// kind-filtered scan transparently skips it. Backends share this so
// message and presence history walk identical pagination/limit logic.
func CMItems(cm *protocol.ChannelMessage, kind Kind) []HistItem {
	if kind.Normalize() == KindPresence {
		out := make([]HistItem, len(cm.Presence))
		for i, pm := range cm.Presence {
			out[i] = HistItem{Serial: pm.Serial, Append: func(dst *protocol.ChannelMessage) {
				dst.Presence = append(dst.Presence, pm)
			}}
		}
		return out
	}
	out := make([]HistItem, len(cm.Messages))
	for i, m := range cm.Messages {
		out[i] = HistItem{Serial: m.Serial, Append: func(dst *protocol.ChannelMessage) {
			dst.Messages = append(dst.Messages, m)
		}}
	}
	return out
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
	// Kind selects which stream to read — messages or presence. The
	// zero value reads messages (DESIGN.md §12.1).
	Kind Kind

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

	// EndChannelSerial, if non-empty, additionally caps results to
	// ChannelMessages with channel_serial <= this value (inclusive).
	// Distinct from Cursor: Cursor is an exclusive pagination boundary
	// applied direction-specifically, EndChannelSerial is an inclusive
	// channel-serial-level upper bound applied in either direction.
	//
	// Used by resume to bound the gap fetch at the attach-time anchor:
	// concurrent publishes that have landed in storage but not yet on
	// the calling Channel's live list are excluded from the scan and
	// will arrive via Stream.Next instead.
	EndChannelSerial string

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
