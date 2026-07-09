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
	"errors"
	"strconv"
	"strings"

	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
)

// ErrTargetNotFound is returned by Mutate, LatestVersion and Versions
// when the target message identity has never been published on the
// channel, or has aged out of retention (DESIGN.md §13.2). Callers map
// it to a 4xx (REST) or a NACK/ERROR (WS).
var ErrTargetNotFound = errors.New("storage: target message not found")

// ErrIncompatibleAppend is returned by Mutate (via MergeVersion) when an
// append's data cannot be concatenated onto the target's current data
// because their types are incompatible — appends are defined only for
// string-onto-string and binary-onto-binary (DESIGN.md §13.3). Callers
// map it to a 400 (REST) or a NACK (WS).
var ErrIncompatibleAppend = errors.New("storage: append data type is incompatible with the target's current data")

// ErrInvalidMessageID is returned by StampMessageIDs (and therefore by
// Store) when a client supplies message ids that do not conform to the
// required "<batchID>:<idx>" batch shape (DESIGN.md §8). Callers map it
// to a 400 (REST) or a NACK (WS).
var ErrInvalidMessageID = errors.New("storage: client-supplied message ids do not match the required <batchID>:<idx> format")

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
	Store(ctx context.Context, msgs []*protocol.Message) (cm *protocol.ChannelMessage, idempotent bool, err error)

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
	Mutate(ctx context.Context, mut *protocol.Message) (cm *protocol.ChannelMessage, idempotent bool, err error)

	// LatestVersion returns the current latest version of the message
	// identified by serial — the materialised projection entry, a fully
	// merged Message (DESIGN.md §13.4). A soft-deleted message is
	// returned as its tombstone version (Action = delete). Returns
	// ErrTargetNotFound if no such message exists. Backs
	// GET .../messages/{serial}.
	LatestVersion(ctx context.Context, serial string) (*protocol.Message, error)

	// Versions returns every version of the message identified by serial
	// (create + each update/delete) ordered by version, paginated via the
	// shared HistoryQuery shape (DESIGN.md §13.4). q.Cursor, when set, is
	// a version's Message.Serial-style cursor compared at version
	// granularity; q.Limit caps versions. Returns ErrTargetNotFound if
	// the message has no versions. Backs GET .../messages/{serial}/versions.
	Versions(ctx context.Context, serial string, q HistoryQuery) (HistoryPage, error)

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

// StampMessageIDs resolves the ChannelMessage batch id for a create
// publish and stamps the contained Message.IDs (DESIGN.md §8). It is
// called by every backend's Store before minting the channelSerial, so
// the batch id — the idempotency key indexed by storage — is derived
// identically regardless of surface (REST or WS) or backend.
//
// If no contained message carries an ID, a fresh 8-char base64 batch id
// is generated and each Message.ID is stamped "<batchID>:<idx>" (idx
// unpadded, matching Ably's wire shape, e.g. "TojWzTkLiH:0"). If any
// message carries an ID, the publish is client-idempotent: the batch id
// is derived from the messages and, for a multi-message batch, each
// Message.ID must equal "<batchID>:<idx>" — a mismatch returns
// ErrInvalidMessageID and nothing is stamped. A single-message publish
// accepts any client id (the batch id is that id with a trailing ":0"
// trimmed), matching Ably. Returns the batch id to stamp onto
// ChannelMessage.ID.
func StampMessageIDs(msgs []*protocol.Message) (string, error) {
	base, hasID, err := messageBaseID(msgs)
	if err != nil {
		return "", err
	}
	if !hasID {
		base = id.NewMessageBaseID()
		for i, m := range msgs {
			m.ID = base + ":" + strconv.Itoa(i)
		}
	}
	return base, nil
}

// messageBaseID extracts the batch id shared by a publish's message ids,
// validating the "<batchID>:<idx>" shape for a multi-message batch. It
// reports hasID=false (and an empty base) when no message carries an id,
// signalling the caller to generate one. Mirrors Ably's getMessageBaseID.
func messageBaseID(msgs []*protocol.Message) (base string, hasID bool, err error) {
	if len(msgs) == 1 {
		// A single-message publish carries no multi-message index
		// requirement: any id is accepted, and the batch id is that id
		// with a trailing ":0" trimmed if present.
		id0 := msgs[0].ID
		return strings.TrimSuffix(id0, ":0"), id0 != "", nil
	}

	for _, m := range msgs {
		if m.ID != "" {
			hasID = true
		} else if hasID {
			// Some messages carry an id and others do not — all must if any do.
			return "", false, ErrInvalidMessageID
		}
	}
	if !hasID {
		return "", false, nil
	}

	base, ok := strings.CutSuffix(msgs[0].ID, ":0")
	if !ok {
		return "", false, ErrInvalidMessageID
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].ID != base+":"+strconv.Itoa(i) {
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
func StampCreateVersion(m *protocol.Message) {
	ts, _ := serial.Timestamp(m.Serial)
	m.Version = &protocol.MessageVersion{
		Serial:    m.Serial,
		Timestamp: ts,
		ClientID:  m.ClientID,
	}
}

// MergeVersion produces the new merged version of a message for a
// mutation (DESIGN.md §13.2, §13.3). current is the target's current
// latest version (a complete Message); mut is the inbound mutation
// carrying the action, the operating clientId and the supplied fields;
// versionSerial is the `<channelSerial>:<idx>` minted for this mutation
// publish. The result is a complete Message that repeats current's
// stable identity (Serial) and creator (ClientID), applies shallow-mixin
// for update/append (only supplied fields replace), tombstones for
// delete, and carries a fresh Version stamped with the operator and
// serial.
//
// An append is stored and fanned out as a full action=update whose Data
// is the rolled-up aggregate, carrying the incremental delta in
// Alt[DeltaAppend] (action=append, just the new data) so the delivery
// path can hand a caught-up subscriber the delta rather than the full
// version (DESIGN.md §13.3). An append whose data cannot concatenate onto
// the current data (incompatible types) returns ErrIncompatibleAppend.
func MergeVersion(current, mut *protocol.Message, versionSerial string) (*protocol.Message, error) {
	v := *current // carry every field forward, then mix in the supplied ones
	v.Serial = current.Serial
	v.ConnectionID = current.ConnectionID
	v.Alt = nil // any prior append delta does not carry forward

	ts, _ := serial.Timestamp(versionSerial)
	ver := &protocol.MessageVersion{
		Serial:    versionSerial,
		Timestamp: ts,
		ClientID:  mut.ClientID,
	}
	if mut.Version != nil {
		ver.Description = mut.Version.Description
		ver.Metadata = mut.Version.Metadata
	}

	switch mut.Action {
	case protocol.MessageDelete:
		// Tombstone: drop the payload but keep identity + creator.
		v.Action = protocol.MessageDelete
		v.Data = nil
		v.Name = ""
		v.Encoding = ""
	case protocol.MessageAppend:
		concatenated, err := concatData(current.Data, mut.Data)
		if err != nil {
			return nil, err
		}
		// The delta the delivery path hands a caught-up subscriber: the
		// incremental append alone, sharing this version so newest-wins
		// convergence treats the delta and the full aggregate as one.
		delta := &protocol.Message{
			Serial:       current.Serial,
			Action:       protocol.MessageAppend,
			ClientID:     current.ClientID,
			ConnectionID: current.ConnectionID,
			Data:         mut.Data,
			Encoding:     mut.Encoding,
			Version:      ver,
		}
		v.Action = protocol.MessageUpdate
		v.Data = concatenated
		if mut.Encoding != "" {
			v.Encoding = mut.Encoding
		}
		if mut.Name != "" {
			v.Name = mut.Name
			delta.Name = mut.Name
		}
		v.Alt = map[string]*protocol.Message{protocol.DeltaAppend: delta}
	default: // update
		v.Action = protocol.MessageUpdate
		if mut.Data != nil {
			v.Data = mut.Data
			v.Encoding = mut.Encoding
		}
		if mut.Name != "" {
			v.Name = mut.Name
		}
	}

	v.Version = ver
	return &v, nil
}

// concatData concatenates an append's data onto the current value. It is
// defined only for string-onto-string and binary-onto-binary; any other
// combination of types is rejected with ErrIncompatibleAppend (DESIGN.md
// §13.3). A nil current is seeded by the append outright, and a nil
// addition leaves the current value unchanged.
func concatData(current, add any) (any, error) {
	if add == nil {
		return current, nil
	}
	if current == nil {
		return add, nil
	}
	switch c := current.(type) {
	case string:
		if a, ok := add.(string); ok {
			return c + a, nil
		}
	case []byte:
		if a, ok := add.([]byte); ok {
			return append(append([]byte{}, c...), a...), nil
		}
	}
	return nil, ErrIncompatibleAppend
}

// CollapseAppendVersions reduces an ascending-by-version list of a single
// message's versions so appends do not appear as individual entries
// (DESIGN.md §13.3, §13.4): each maximal run of append aggregates
// collapses to the run's last (most-aggregated) version, while creates,
// updates and deletes are kept verbatim. The append-only log still
// carries every append cm for live and resume fan-out — this only shapes
// the version-history read-path, so GET .../messages/{serial}/versions
// reflects the aggregate, never each delta. The input is not mutated.
func CollapseAppendVersions(all []*protocol.Message) []*protocol.Message {
	out := make([]*protocol.Message, 0, len(all))
	for i, m := range all {
		// Keep an append aggregate only when it is the last of its run —
		// the next version is not itself an append (or there is none).
		if m.HasAppendDelta() && i+1 < len(all) && all[i+1].HasAppendDelta() {
			continue
		}
		out = append(out, m)
	}
	return out
}

// VersionSerial returns the serial that identifies a single version of a
// message — Version.Serial when present (every server-stamped message),
// falling back to the stable identity Serial. It is the pagination unit
// for a version-history scan (DESIGN.md §13.4).
func VersionSerial(m *protocol.Message) string {
	if m.Version != nil && m.Version.Serial != "" {
		return m.Version.Serial
	}
	return m.Serial
}

// PaginateVersions slices an ascending-by-version list of a single
// message's versions into a HistoryPage per the query's Direction /
// Cursor / Limit (DESIGN.md §13.4). The cursor is a version serial,
// excluded strictly in the scan direction; each version becomes its own
// single-message ChannelMessage positioned at the version's own
// channelSerial. Shared by the in-memory and bbolt backends, which hold
// the versions list directly; Postgres paginates in SQL.
func PaginateVersions(all []*protocol.Message, q HistoryQuery) HistoryPage {
	forwards := q.Direction == DirectionForwards
	cursor := q.Cursor
	limit := q.Limit

	var page HistoryPage
	count := 0
	emit := func(m *protocol.Message) bool {
		vs := VersionSerial(m)
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
			Messages:      []*protocol.Message{m},
		})
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
}
