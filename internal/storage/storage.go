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
	stderrors "errors"
	"strconv"
	"strings"

	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"

	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/wire"
)

// ErrTargetNotFound is returned by Mutate, LatestVersion and Versions
// when the target message identity has never been published on the
// channel, or has aged out of retention (DESIGN.md §13.2). Callers map
// it to a 4xx (REST) or a NACK/ERROR (WS).
var ErrTargetNotFound = stderrors.New("storage: target message not found")

// ErrInvalidMessageID is returned by StampMessageIDs (and therefore by
// Store) when a client supplies message ids that do not conform to the
// required "<batchID>:<idx>" batch shape (DESIGN.md §8). Callers map it
// to a 400 (REST) or a NACK (WS).
var ErrInvalidMessageID = stderrors.New("storage: client-supplied message ids do not match the required <batchID>:<idx> format")

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
// The bridge carries two things: the channel's cm stream, and the fact
// that its aggregate occupancy has moved. Occupancy is not a cm — it is
// not logged, ordered or replayed — but it is in-process channel state
// that storage is the first to learn about, on this node or another, so
// it reaches the channel the same way.
//
// In core, *Channel implements Appender — Initialize seeds the channel
// sentinel's serial, records the initial value, and unblocks Attach;
// Append links the cm onto the live linked list so attached streams
// observe it; OccupancyChanged wakes whoever is reporting occupancy.
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

	// OccupancyChanged reports that the channel's aggregate occupancy may
	// have moved — because this node stored a contribution, or because
	// another node's reached storage (DESIGN.md §16.3). It says only that
	// the aggregate is worth re-reading, not what it now is: a backend that
	// learned of the change from a notification does not have the aggregate
	// to hand, and one that does would be handing over a value the caller
	// must re-read anyway once several nodes contribute.
	//
	// It may be called when nothing actually changed. Backends do not
	// diff contributions, so a node re-storing what it already stored still
	// signals; a caller that reports on every signal reports the same
	// numbers twice rather than missing a change.
	OccupancyChanged()
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

// Pinger is implemented by backends with an external dependency worth
// confirming reachable before serving traffic (currently only
// postgres.Storage, for the cluster-mode readiness check — see
// DESIGN.md §2.2 / §11). Backends without one, such as memory and
// bbolt, don't implement it; callers treat that as "always ready".
type Pinger interface {
	// Ping reports whether the backend's dependency is reachable. It
	// should be cheap and side-effect-free — callers may invoke it on
	// every readiness probe.
	Ping(ctx context.Context) error
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
	Store(ctx context.Context, msgs []*wire.Message) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// StoreSummary publishes the summaries of messages whose annotations have
	// just been folded (DESIGN.md §14.2). Each summary carries the identity of
	// the message it is about and that message's current summary, with
	// action = summary.
	//
	// It mints a channelSerial and logs the cm, so the summary reaches every
	// subscriber and is replayed to one resuming across it, exactly as a
	// publish is. What it does not do is touch the latest-version projection or
	// the version chain: a summary is not a version of the message, and the
	// message's own summary is already on the projection — the fold put it
	// there, in the same transaction as the annotation that caused it.
	StoreSummary(ctx context.Context, summaries []*wire.Message) (cm *protocol.ChannelMessage, err error)

	// Mutate persists an update/delete/append to an existing message
	// (DESIGN.md §13.2). mut carries the mutation: mut.Action is the
	// operation (update/delete/append), mut.Serial is the target message
	// identity, mut.ClientID is the operating client (resolved by the
	// caller), the supplied Data/Name/Encoding are the fields to mix in,
	// and mut.Version (if set) carries an optional operator description /
	// metadata.
	//
	// The backend:
	//   - returns ErrTargetNotFound if the target's identity has no
	//     current version (never published, or aged out);
	//   - applies shallow-mixin merge against the target's current latest
	//     version (only supplied fields replace; append concatenates data)
	//     and mints a fresh version;
	//   - persists the resulting MERGED Message as a new cm on the same
	//     stream (kind = message, message identity recorded), so live and
	//     resume subscribers always carry a complete message, never a diff;
	//   - upserts the latest-version projection (a delete marks the row
	//     deleted) and the serial→versions index in the same transaction
	//     as the log insert;
	//   - delivers the cm to the appender exactly as Store does.
	//
	// Idempotency works like Store: a mut.ID already seen on the channel
	// returns the original cm with idempotent=true and mutates nothing.
	Mutate(ctx context.Context, mut *wire.Message, merge MergeFunc) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// LatestVersion returns the current latest version of the message
	// identified by serial — the materialised projection entry, a fully
	// merged Message (DESIGN.md §13.4). A soft-deleted message is
	// returned as its tombstone version (Action = delete). Returns
	// ErrTargetNotFound if no such message exists. Backs
	// GET .../messages/{serial}.
	LatestVersion(ctx context.Context, serial string) (*wire.Message, error)

	// Versions returns every version of the message identified by serial
	// (create + each update/delete) ordered by version, paginated via the
	// shared HistoryQuery shape (DESIGN.md §13.4). q.Cursor, when set, is
	// a version's Message.Serial-style cursor compared at version
	// granularity; q.Limit caps versions. Returns ErrTargetNotFound if
	// the message has no versions. Backs GET .../messages/{serial}/versions.
	Versions(ctx context.Context, serial string, q HistoryQuery) (HistoryPage, error)

	// StoreAnnotation persists an annotation publish on the same stream as
	// messages and presence, as a third cm kind (kind = annotation,
	// DESIGN.md §14.1). It mints a channelSerial, stamps each
	// Annotation.Serial to "<channelSerial>:<idx>", validates that every
	// annotation's MessageSerial resolves in the latest-version projection
	// (ErrTargetNotFound otherwise, exactly as for a mutation), persists the
	// annotation cm on the log with the TARGET message serial recorded so
	// the serial index serves annotations-for-message scans, and delivers
	// the cm to the appender exactly as Store does.
	//
	// Idempotency works like Store: a contained Annotation.ID already seen
	// on this channel returns the original cm with idempotent=true.
	//
	// The returned cm is the persisted annotation cm — the seam the summary
	// fold slots into at store time (DESIGN.md §14.2).
	StoreAnnotation(ctx context.Context, annotations []*wire.Annotation, fold FoldFunc) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// Annotations returns the annotations attached to the message identified
	// by messageSerial, in stream order, paginated via the shared
	// HistoryQuery shape (DESIGN.md §14.4). q.Cursor, when set, is an
	// Annotation.Serial compared direction-specifically; q.Limit caps the
	// annotations returned. An unknown target yields an empty page (no
	// error) — mirroring Ably, which lists rather than 404s. Backs
	// GET .../messages/{serial}/annotations.
	Annotations(ctx context.Context, messageSerial string, q HistoryQuery) (HistoryPage, error)

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
	StorePresence(ctx context.Context, presence []*wire.PresenceMessage) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// Members returns the channel's current presence set plus the
	// channelSerial the set is current as-of (DESIGN.md §12.4). The
	// as-of serial is the channel's current watermark; it is empty only
	// when the channel has no persisted cms at all. Backs presence sync
	// and the REST presence endpoint.
	Members(ctx context.Context, q MembersQuery) (MembersPage, error)

	// StoreState is the LiveObjects analogue of StorePresence (DESIGN.md
	// §15.2, §15.3). It mints a channelSerial, stamps each
	// StateMessage.Serial to the timeserial "<channelSerial>:<idx>",
	// persists the state ChannelMessage on the same stream (kind = state),
	// and — atomically with the persist — applies the operations to the
	// channel's materialised object set.
	//
	// The apply is the caller's (core.ApplyOperations): the backend loads
	// the objects the publish names, hands them to apply along with the
	// stamped messages, and writes back whichever objects come out changed.
	// What an operation does to an object is the protocol's, so it runs at
	// this seam — with the objects loaded and the write not yet made, inside
	// whatever the backend uses to make the pair atomic (see ApplyFunc).
	//
	// The appender then receives the state cm exactly as for a message
	// publish. Idempotency works like Store: a contained StateMessage.Id
	// already seen on this channel returns the original cm with
	// idempotent=true and applies nothing.
	StoreState(ctx context.Context, state []*wire.StateMessage, apply ApplyFunc) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// Objects returns one page of the channel's materialised LiveObjects set
	// — the fold of its state stream — ordered by object id, plus the
	// channelSerial the set is current as-of (DESIGN.md §15.3). The as-of
	// serial is the channel's current watermark; it is empty only when the
	// channel has no persisted cms at all. Backs the state sync a client is
	// served on attach.
	Objects(ctx context.Context, q ObjectsQuery) (ObjectsPage, error)

	// StoreOccupancy records what this node currently contributes to the
	// channel's occupancy (DESIGN.md §16.2): how many holders it is serving
	// and in which modes.
	//
	// counts is the whole of the node's contribution, not a delta, so a
	// store that is lost costs nothing that the next one does not repair —
	// which is what lets a caller roll several changes into one write. A
	// contribution of nothing removes the node from the aggregate rather
	// than recording zeros.
	//
	// The backend then calls Appender.OccupancyChanged, on this node and
	// on every other node holding the channel, so the new aggregate is
	// read and reported.
	StoreOccupancy(ctx context.Context, counts *wire.ChannelOccupancy) error

	// Occupancy returns the channel's occupancy across every node
	// contributing to it (DESIGN.md §16.2): the per-node contributions
	// summed, with the channelMode the union of theirs.
	//
	// PresenceMembers is not a per-node contribution and is not summed: it
	// is the size of the channel's membership set, which storage already
	// holds and which is global to begin with (§12.5).
	Occupancy(ctx context.Context) (*wire.ChannelOccupancy, error)

	// History returns ChannelMessages in publish order. An empty
	// AfterChannelSerial means "from the oldest retained
	// ChannelMessage"; otherwise results start strictly after the
	// given channelSerial. q.Kind selects the stream (messages or
	// presence); the zero value reads messages.
	History(ctx context.Context, q HistoryQuery) (HistoryPage, error)
}

// Kind distinguishes the cm streams that share a channel's ordered log
// and channelSerial namespace (DESIGN.md §12.1). The zero value is
// KindMessage so an unset HistoryQuery reads message history.
type Kind string

const (
	KindMessage    Kind = "message"
	KindPresence   Kind = "presence"
	KindAnnotation Kind = "annotation"
	KindState      Kind = "state"
)

// Normalize maps the zero value to KindMessage.
func (k Kind) Normalize() Kind {
	if k == "" {
		return KindMessage
	}
	return k
}

// MembersQuery bounds one page of a channel's presence set.
type MembersQuery struct {
	// After is a cursor from a previous page, and the page returned starts
	// strictly after it. Empty starts at the first member.
	//
	// It is opaque and belongs to the backend that issued it: the order a set
	// is read in is a backend's own, so a cursor from one means nothing to
	// another.
	After string

	// Limit is the most members the page may hold. Zero means no limit, which
	// is for a caller that wants the whole set and knows it is small.
	Limit int
}

// MembersPage is one page of a presence set.
type MembersPage struct {
	Members []*wire.PresenceMessage

	// AsOfSerial is the channel's watermark when the set was read, empty only
	// when the channel has no persisted cms at all.
	AsOfSerial string

	// NextCursor is where the page after this one starts, empty when this page
	// is the last.
	NextCursor string
}

// PageMembers cuts a page out of a set already sorted by MemberKey, and
// returns it with the cursor the page after it starts at. It is for a backend
// holding the whole set in memory anyway; one that can bound the read itself
// should do that instead of reading everything and calling this.
func PageMembers(members []*wire.PresenceMessage, q MembersQuery) ([]*wire.PresenceMessage, string) {
	if q.After != "" {
		for len(members) > 0 && MemberKey(members[0].ConnectionId, members[0].GetClientId()) <= q.After {
			members = members[1:]
		}
	}
	if q.Limit <= 0 || len(members) <= q.Limit {
		return members, ""
	}

	page := members[:q.Limit]
	last := page[len(page)-1]
	return page, MemberKey(last.ConnectionId, last.GetClientId())
}

// MemberKey returns the presence-set key for a member: the pair
// (connectionId, clientId) that identifies one member, so the same
// clientId over two connections is two distinct members (DESIGN.md §12.1).
func MemberKey(connectionID, clientID string) string {
	return connectionID + ":" + clientID
}

// ObjectsQuery bounds one page of a channel's materialised LiveObjects set
// (DESIGN.md §15.3).
type ObjectsQuery struct {
	// After is a cursor from a previous page, and the page returned starts
	// strictly after it. Empty starts at the first object.
	//
	// Unlike a presence cursor it is not opaque: the set is ordered by object
	// id and the cursor is an object id, because that is the order the
	// protocol's own paging walks the set in (liveslice.Page) and a client
	// resuming a sync presents the id it stopped at.
	After string

	// Limit is the most objects the page may hold. Zero means no limit, which
	// is for a caller that wants the whole set and knows it is small.
	Limit int
}

// ObjectsPage is one page of a materialised LiveObjects set.
type ObjectsPage struct {
	// Objects are ordered by object id.
	Objects []*wire.StateObject

	// AsOfSerial is the channel's watermark when the set was read, empty only
	// when the channel has no persisted cms at all.
	AsOfSerial string

	// NextCursor is the object id the page after this one starts at, empty
	// when this page is the last.
	NextCursor string
}

// PageObjects cuts a page out of a set already sorted by object id, and
// returns it with the cursor the page after it starts at. It is the objects
// counterpart of PageMembers, and is for the same kind of backend: one holding
// the whole set in memory anyway.
func PageObjects(objects []*wire.StateObject, q ObjectsQuery) ([]*wire.StateObject, string) {
	if q.After != "" {
		for len(objects) > 0 && objects[0].GetObjectId() <= q.After {
			objects = objects[1:]
		}
	}
	if q.Limit <= 0 || len(objects) <= q.Limit {
		return objects, ""
	}

	page := objects[:q.Limit]
	return page, page[len(page)-1].GetObjectId()
}

// StateObjectIDs is every object a state publish names: the objects a backend
// must load before applying it, and — because an operation only ever changes
// the object it names — the only ones the apply can change.
//
// Order is the publish's, deduplicated, so a backend loading them in this
// order loads each once.
func StateObjectIDs(state []*wire.StateMessage) []string {
	ids := make([]string, 0, len(state))
	seen := make(map[string]bool, len(state))
	for _, sm := range state {
		id := sm.GetOperation().GetObjectId()
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

// StampStateMessage stamps the position of the idx'th state message in the
// publish minted at channelSerial: the serial the operation is ordered by, the
// site it originated at, and the time the serial encodes.
//
// The serial is what decides whether an operation is applied — an object keeps
// the latest serial it has seen from each site and ignores anything not after
// it (liveobject.shouldApply) — so it is the server's to assign, not the
// client's.
func StampStateMessage(sm *wire.StateMessage, channelSerial string, idx int) {
	sm.Serial = StateSerial(channelSerial, idx)
	sm.SiteCode = sm.Serial.SiteCode()
	sm.Timestamp = sm.Serial.Time
}

// StateSerial is the position of the idx'th state message in the publish
// minted at channelSerial, as a state message carries it: a timeserial rather
// than a string, the same shape an annotation's is.
func StateSerial(channelSerial string, idx int) *wire.Timeserial {
	return wire.MustTimeserialFromString(serial.MessageSerial(channelSerial, idx))
}

// SumOccupancy adds one node's contribution into an aggregate: the counts add
// and the channelMode unions, because a mode is occupied if any node has a
// holder of it.
//
// It does not touch PresenceMembers, which is not a per-node contribution —
// the membership set is global, so summing it across nodes would multiply it
// by the number of nodes holding the channel.
func SumOccupancy(into, from *wire.ChannelOccupancy) {
	if from == nil {
		return
	}
	into.ChannelMode |= from.ChannelMode
	into.Connections += from.Connections
	into.Publishers += from.Publishers
	into.Subscribers += from.Subscribers
	into.PresenceConnections += from.PresenceConnections
	into.PresenceSubscribers += from.PresenceSubscribers
	into.ObjectSubscribers += from.ObjectSubscribers
	into.ObjectPublishers += from.ObjectPublishers
}

// CopyOccupancy is one node's contribution as a backend records it: the
// counts and the mode, and nothing else.
//
// PresenceMembers and GloballyInSync are dropped rather than copied, so that
// an in-memory backend stores exactly the columns the Postgres one does. A
// node does not contribute a share of the membership set — the set is global
// — and GloballyInSync is not a claim this server makes.
func CopyOccupancy(counts *wire.ChannelOccupancy) *wire.ChannelOccupancy {
	out := &wire.ChannelOccupancy{}
	SumOccupancy(out, counts)
	return out
}

// OccupancyIsEmpty reports a contribution of nothing: a node serving no
// holders of the channel at all, which is removed from the aggregate rather
// than stored as a row of zeros.
//
// It is the counts that decide, not the mode: a node with holders but no
// modes worth naming still occupies the channel, and one with a mode left set
// but no holders is a bug this must not preserve.
func OccupancyIsEmpty(counts *wire.ChannelOccupancy) bool {
	return counts == nil || counts.Connections <= 0
}

// staticPresenceKey marks a StorePresence call as seeding static fixture
// members (DESIGN.md §9, §12.5): members that belong to no connection
// and must never lapse. Threaded via the context so the interface stays
// unchanged; only backends with a liveness reaper (postgres) need act on
// it — memory and bbolt hold the set in memory and never reap.
type staticPresenceKey struct{}

// WithStaticPresence marks ctx so that presence stored under it is
// treated as a static fixture: exempt from the cluster liveness reaper
// (a non-expiring lease). Used by the --fixtures seed path.
func WithStaticPresence(ctx context.Context) context.Context {
	return context.WithValue(ctx, staticPresenceKey{}, true)
}

// IsStaticPresence reports whether ctx was marked by WithStaticPresence.
func IsStaticPresence(ctx context.Context) bool {
	v, _ := ctx.Value(staticPresenceKey{}).(bool)
	return v
}

// StampMessageIDs resolves the ChannelMessage batch id for a create
// publish and stamps the contained Message.Ids (DESIGN.md §8). It is
// called by every backend's Store before minting the channelSerial, so
// the batch id — the idempotency key indexed by storage — is derived
// identically regardless of surface (REST or WS) or backend.
//
// If no contained message carries an id, a fresh 8-char base64 batch id
// is generated and each Message.Id is stamped "<batchID>:<idx>" (idx
// unpadded, matching Ably's wire shape, e.g. "TojWzTkLiH:0"). If any
// message carries an ID, the publish is client-idempotent: the batch id
// is derived from the messages and, for a multi-message batch, each
// Message.ID must equal "<batchID>:<idx>" — a mismatch returns
// ErrInvalidMessageID and nothing is stamped. A single-message publish
// accepts any client id (the batch id is that id with a trailing ":0"
// trimmed), matching Ably. Returns the batch id to stamp onto
// ChannelMessage.ID.
func StampMessageIDs(msgs []*wire.Message) (string, error) {
	base, hasID, err := messageBaseID(msgs)
	if err != nil {
		return "", err
	}
	if !hasID {
		base = id.NewMessageBaseID()
		for i, m := range msgs {
			m.Id = new(base + ":" + strconv.Itoa(i))
		}
	}
	return base, nil
}

// messageBaseID extracts the batch id shared by a publish's message ids,
// validating the "<batchID>:<idx>" shape for a multi-message batch. It
// reports hasID=false (and an empty base) when no message carries an id,
// signalling the caller to generate one. Mirrors Ably's getMessageBaseID.
func messageBaseID(msgs []*wire.Message) (base string, hasID bool, err error) {
	if len(msgs) == 1 {
		// A single-message publish carries no multi-message index
		// requirement: any id is accepted, and the batch id is that id
		// with a trailing ":0" trimmed if present.
		id0 := msgs[0].GetId()
		return strings.TrimSuffix(id0, ":0"), id0 != "", nil
	}

	for _, m := range msgs {
		if m.GetId() != "" {
			hasID = true
		} else if hasID {
			// Some messages carry an id and others do not — all must if any do.
			return "", false, ErrInvalidMessageID
		}
	}
	if !hasID {
		return "", false, nil
	}

	base, ok := strings.CutSuffix(msgs[0].GetId(), ":0")
	if !ok {
		return "", false, ErrInvalidMessageID
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].GetId() != base+":"+strconv.Itoa(i) {
			return "", false, ErrInvalidMessageID
		}
	}
	return base, true, nil
}

// StampCreateVersion stamps a freshly-published create message's Version
// (DESIGN.md §13.1): version.serial == the message's own serial, with a
// server-authoritative timestamp derived from that serial and the
// creator clientId. Called by every backend's Store after the serial is
// assigned, so creates carry the same version shape as mutations. The
// message's own Action stays MessageCreate (the zero value).
//
// It also stamps the top-level Message.Timestamp with the create time, which
// every later version carries forward (§8, §13.2) — matching the reference's
// buildUpdateMessage, whose top-level Timestamp is the ORIGINAL message's
// timestamp (the operation time lives in version.Timestamp). Message.Timestamp
// is omitempty, so an unstamped (zero) value is dropped on the wire; the SDK
// Tree reads the top-level timestamp as the message's create time on every
// delivery and drives its retention clock from it.
func StampCreateVersion(m *wire.Message) {
	ts, _ := serial.Timestamp(m.Serial)
	m.Timestamp = uint64(ts)
	m.Version = &wire.Message_Version{
		Serial:    m.Serial,
		Timestamp: uint64(ts),
		ClientId:  m.ClientId,
	}
}

// StampPresenceMember stamps a freshly-published presence message with a
// server-authoritative Timestamp, derived from the channelSerial the publish
// was minted at. Every backend's StorePresence calls it so presence frames
// carry a timestamp on the wire.
//
// A presence message has no serial of its own to stamp. Its position in the
// stream is `<channelSerial>:<idx>`, which is where it is — not something it
// carries, and not something a client is ever told; a reader that needs it
// derives it from where it found the message (PresenceSerial).
//
// It deliberately does NOT touch PresenceMessage.ID. A genuine (non-
// synthesized) presence op is stamped an id of the form
// "<connectionId>:<msgSerial>:<index>" at realtime publish
// (realtime.presenceID), so SDKs order it by (msgSerial, index) via the
// id path (RTP2b2). Server-fabricated
// events (fixture-seeded members, teardown/detach LEAVEs) stay id-less on
// purpose: they have no real connection/msgSerial, so they are genuinely
// synthesized and the SDK falls back to timestamp comparison for them
// (RTP2b1). The Timestamp stamped here is what that fallback needs — an
// unstamped (zero) timestamp would make a synthesized leave compare as
// not-newer than its own enter, so the SDK would never remove the member
// (DESIGN.md §12.1).
func StampPresenceMember(p *wire.PresenceMessage, channelSerial string, idx int) {
	ts, _ := serial.Timestamp(PresenceSerial(channelSerial, idx))
	p.Timestamp = uint64(ts)
}

// PresenceSerial is the position of the idx'th presence message in the publish
// minted at channelSerial — the `<channelSerial>:<idx>` a message-stream item
// would carry on itself. It is the pagination unit for a presence scan, so a
// backend derives it from where the message sits rather than reading it off
// the message.
func PresenceSerial(channelSerial string, idx int) string {
	return serial.MessageSerial(channelSerial, idx)
}

// AnnotationSerial is the position of the idx'th annotation in the publish
// minted at channelSerial, as an annotation carries it: a timeserial rather
// than a string, because that is the shape the wire gives it.
func AnnotationSerial(channelSerial string, idx int) *wire.Timeserial {
	return wire.MustTimeserialFromString(serial.MessageSerial(channelSerial, idx))
}

// MergeFunc turns the message being edited and the edit into the version to
// store, by mutating the edit in place and returning it — so a backend must
// read anything it needs from the edit before calling it, or read it off the
// returned version instead. FoldFunc folds one annotation into the message it
// annotates, reporting whether the summary changed.
//
// Both are supplied by the caller rather than called from here, because what
// an edit does to a message and what an annotation does to a summary are the
// protocol's and not this server's. What is this server's is that they run
// with the target loaded and the write not yet made — so a backend calls them
// inside whatever it uses to make that pair atomic, and a concurrent edit
// cannot slip between the two.
//
// ApplyFunc is the same arrangement for LiveObjects: it folds a state publish
// into the objects it names, and is given them as the store holds them — those
// that exist, in StateObjectIDs order — together with the publish's stamped
// messages. It returns the objects that came out changed, which the backend
// writes back; an object the publish names but does not change is not
// returned, and an object it names that did not exist is returned as the new
// one the operation created.
//
// It must not mutate the objects it is given: a backend may be handing it the
// very values a concurrent read is serving.
type (
	MergeFunc func(current, mut *wire.Message, versionSerial string) (*wire.Message, error)
	FoldFunc  func(msg *wire.Message, annotation *wire.Annotation) bool
	ApplyFunc func(objects []*wire.StateObject, state []*wire.StateMessage) ([]*wire.StateObject, error)
)

// ProtocolError carries a failure the shared code decided and described — an
// append onto data it cannot be appended to, an edit that would exceed the
// message size — out through a storage call, which reports plain errors.
// Callers unwrap it with AsProtocolError to tell the client what the protocol
// said, rather than restating it.
type ProtocolError struct{ Info *errors.ErrorInfo }

func (e *ProtocolError) Error() string { return e.Info.String() }

// AsProtocolError reports whether err carries a protocol failure, and what it
// was.
func AsProtocolError(err error) (*errors.ErrorInfo, bool) {
	var pe *ProtocolError
	if stderrors.As(err, &pe) {
		return pe.Info, true
	}
	return nil, false
}

// PaginateVersions slices an ascending-by-version list of a single
// message's versions into a HistoryPage per the query's Direction /
// Cursor / Limit (DESIGN.md §13.4). The cursor is a version serial,
// excluded strictly in the scan direction; each version becomes its own
// single-message ChannelMessage positioned at the version's own
// channelSerial. Shared by the in-memory and bbolt backends, which hold
// the versions list directly; Postgres paginates in SQL.
func PaginateVersions(all []*wire.Message, q HistoryQuery) HistoryPage {
	forwards := q.Direction == DirectionForwards
	cursor := q.Cursor
	limit := q.Limit

	var page HistoryPage
	count := 0
	emit := func(m *wire.Message) bool {
		vs := m.VersionOrSerial()
		if cursor != "" {
			if forwards && vs <= cursor {
				return true
			}
			if !forwards && vs >= cursor {
				return true
			}
		}
		if limit > 0 && count >= limit {
			page.HasMore = true
			return false
		}
		page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
			ChannelSerial: CreateChannelSerial(vs),
			Messages:      []*wire.Message{m},
		})
		page.LastSerial = vs
		count++
		return true
	}

	if forwards {
		for _, m := range all {
			if !emit(m) {
				break
			}
		}
	} else {
		for i := len(all) - 1; i >= 0; i-- {
			if !emit(all[i]) {
				break
			}
		}
	}
	return page
}

// PaginateAnnotations slices a stream-ordered (ascending-serial) list of
// one message's annotations into a HistoryPage per the query's Direction /
// Cursor / Limit (DESIGN.md §14.4). The cursor is an Annotation.Serial,
// excluded strictly in the scan direction; each annotation becomes its own
// single-annotation ChannelMessage positioned at its own channelSerial.
// Shared by the in-memory and bbolt backends, which hold the annotation
// list directly; Postgres paginates in SQL.
func PaginateAnnotations(all []*wire.Annotation, q HistoryQuery) HistoryPage {
	forwards := q.Direction == DirectionForwards
	cursor := q.Cursor
	limit := q.Limit

	var page HistoryPage
	count := 0
	emit := func(a *wire.Annotation) bool {
		serial := a.Serial.ToTimeserialString()
		if cursor != "" {
			if forwards && serial <= cursor {
				return true
			}
			if !forwards && serial >= cursor {
				return true
			}
		}
		if limit > 0 && count >= limit {
			page.HasMore = true
			return false
		}
		page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
			ChannelSerial: CreateChannelSerial(serial),
			Annotations:   []*wire.Annotation{a},
		})
		page.LastSerial = serial
		count++
		return true
	}

	if forwards {
		for _, a := range all {
			if !emit(a) {
				break
			}
		}
	} else {
		for i := len(all) - 1; i >= 0; i-- {
			if !emit(all[i]) {
				break
			}
		}
	}
	return page
}

// CreateChannelSerial returns the channelSerial of the publish that
// created the message with the given identity serial — the position a
// collapsed history entry occupies (DESIGN.md §13.4). The identity is
// `<channelSerial>:<idx>`, so this strips the trailing idx.
func CreateChannelSerial(identity string) string {
	cs, _, err := serial.ParseMessageSerial(identity)
	if err != nil {
		return identity
	}
	return cs
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
	switch kind.Normalize() {
	case KindPresence:
		out := make([]HistItem, len(cm.Presence))
		for i, pm := range cm.Presence {
			out[i] = HistItem{Serial: PresenceSerial(cm.ChannelSerial, i), Append: func(dst *protocol.ChannelMessage) {
				dst.Presence = append(dst.Presence, pm)
			}}
		}
		return out
	case KindAnnotation:
		out := make([]HistItem, len(cm.Annotations))
		for i, an := range cm.Annotations {
			out[i] = HistItem{Serial: an.Serial.ToTimeserialString(), Append: func(dst *protocol.ChannelMessage) {
				dst.Annotations = append(dst.Annotations, an)
			}}
		}
		return out
	case KindState:
		out := make([]HistItem, len(cm.State))
		for i, sm := range cm.State {
			out[i] = HistItem{Serial: sm.GetSerial().ToTimeserialString(), Append: func(dst *protocol.ChannelMessage) {
				dst.State = append(dst.State, sm)
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

	// AfterChannelSerial, if non-empty, restricts results to
	// ChannelMessages with channel_serial strictly greater than this
	// value. Distinct from Cursor: Cursor is a Message.Serial applied
	// direction-specifically as an exclusive pagination boundary,
	// AfterChannelSerial is an exclusive channel-serial-level lower
	// bound applied in either direction.
	//
	// Used by the cluster broker's post-reconnect reconcile (DESIGN.md
	// §7.2) to replay every cm minted past a channel's last-delivered
	// serial, at channelSerial (not item) granularity.
	AfterChannelSerial string

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
	//
	// Also set (from the REST fromSerial/from_serial query param) for a
	// GET .../messages untilAttached history read: ably-js
	// sends the channel's attachSerial so a client resuming into the
	// live stream at that point can page backwards through exactly the
	// history that predates it, without duplicating messages the stream
	// will already deliver live. For the default collapsed message view
	// (q.Collapse), the bound applies to each message's CREATE
	// channelSerial (storage.CreateChannelSerial), matching where a
	// collapsed entry sits in the timeline, not to any later edit's
	// channelSerial.
	EndChannelSerial string

	// Limit caps the number of MESSAGES (not ChannelMessages) returned,
	// matching Ably's REST `limit` semantics. Zero or negative means no
	// limit. When the limit cuts a multi-message batch, the trailing
	// ChannelMessage in the page is partial; HasMore is true.
	Limit int

	// Collapse selects the message-history view (DESIGN.md §13.4),
	// ignored for KindPresence. The zero value (false) returns the raw
	// version cms in stream order — every create and every edit — as
	// live and resume delivery require. When true, history collapses to
	// the latest version of each message positioned at its create serial:
	// an edited message keeps its place in the timeline but shows current
	// content, and a deleted message shows as a tombstone. The default
	// REST GET .../messages sets this; resume/rewind replay never does.
	Collapse bool
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

	// LastSerial is the stream position of the last item on the page —
	// the cursor the next page carries on after. The scan records it
	// because only the scan knows it: a page may begin or end with a
	// partial batch, so an item's place in the page is not its place in
	// the publish it came from, and a presence item does not carry its
	// position on itself at all.
	LastSerial string
}
