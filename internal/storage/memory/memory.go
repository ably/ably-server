// Package memory is an in-process storage backend for ably-server.
// All state is held in maps protected by a per-channel mutex; nothing
// is persisted, so a process restart starts each channel fresh.
//
// Used by the `memory` deployment mode and by tests that need a
// storage.Storage without touching disk.
package memory

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/server-protocol/go/wire"
)

// Options configures a Storage. Zero values pick sensible defaults.
type Options struct {
	// SeriesID is the series identifier embedded in every minted
	// channelSerial. Empty means generate one at New time.
	//
	// One series for the whole process is enough here: a series must
	// stay fixed for as long as a channel's log lives (DESIGN.md §8),
	// and nothing in this backend outlives the process that holds it.
	SeriesID string

	// Now is the clock used by every channel's serial generator.
	// Nil means time.Now().UnixMilli — overridden by tests for
	// determinism.
	Now func() int64
}

// Storage is an in-memory storage.Storage. The zero value is not
// usable; construct via New.
type Storage struct {
	// gen is shared across every channel in this Storage — channelSerial
	// counters are per-(timestamp, seriesId), which is process-wide
	// (DESIGN.md §8). Per-channel generators would let two channels
	// minting in the same ms produce identical serials.
	gen *serial.Generator

	mu       sync.Mutex
	channels map[string]*channelStore
}

// New returns a Storage configured by opts.
func New(opts Options) *Storage {
	if opts.SeriesID == "" {
		opts.SeriesID = serial.NewSeriesID()
	}
	return &Storage{
		gen:      serial.NewGenerator(opts.SeriesID, opts.Now),
		channels: make(map[string]*channelStore),
	}
}

// Channel returns the ChannelStore for name, creating it on first
// access and binding it to appender. Subsequent calls with the same
// name return the same instance and ignore the new appender.
//
// On first creation the channel mints an initial channelSerial from
// the shared generator and hands it to the appender via Initialize
// before returning — current and initial are the same value on first
// materialisation (no publishes yet exist), and both sort strictly
// less than every cm subsequently persisted on this channel.
func (s *Storage) Channel(_ context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs, nil
	}
	cs := newChannelStore(s.gen, appender)
	s.channels[name] = cs
	if appender != nil {
		seed := s.gen.Mint()
		appender.Initialize(seed, seed)
	}
	return cs, nil
}

// Close is a no-op for the memory backend.
func (s *Storage) Close() error {
	return nil
}

// channelStore holds the per-channel state: an ordered list of
// channelSerials, a map for O(1) lookup, an idempotency index, and
// the Appender that will receive freshly-stored ChannelMessages.
// The channel's initial watermark is handed to the Appender via
// Initialize at construction time; storage does not retain it.
type channelStore struct {
	gen      *serial.Generator
	appender storage.Appender

	mu      sync.Mutex
	order   []string // append-only, sorted (serials are monotonic): both kinds
	byCS    map[string]*protocol.ChannelMessage
	byID    map[string]string                // Message.id / PresenceMessage.id -> channelSerial
	members map[string]*wire.PresenceMessage // "<connId>:<clientId>" -> latest member (DESIGN.md §12.5)

	// Mutable-message derived structures (DESIGN.md §13.4), maintained
	// under mu alongside the log. latest is the materialised projection:
	// message identity serial -> latest merged version. versions is the
	// serial→versions index: identity serial -> every version in version
	// (publish) order.
	latest   map[string]*wire.Message
	versions map[string][]*wire.Message

	// annotations indexes a target message identity serial to its
	// annotations in stream (publish) order — the annotations-for-message
	// scan (DESIGN.md §14.4), the memory analogue of the postgres
	// channel_messages serial index. The annotation cms also live on the
	// shared log (order/byCS) so they flow to the appender and are
	// kind-skipped by message/presence history.
	annotations map[string][]*wire.Annotation

	// objects is the materialised LiveObjects set (DESIGN.md §15.3): object
	// id -> the object as the channel's state stream has left it. It is the
	// objects analogue of members, maintained under mu alongside the log so a
	// concurrent Objects observes a set consistent with it.
	objects map[string]*wire.StateObject

	// occupancy is this process's contribution to the channel's occupancy
	// (DESIGN.md §16.2), nil when it serves no holders of the channel. This
	// backend is one process, so the contribution is also the aggregate.
	//
	// It is held in memory and not persisted for the same reason the
	// membership set is not: occupancy is what is attached right now, and
	// nothing is attached to a process that has just started.
	occupancy *wire.ChannelOccupancy
}

func newChannelStore(gen *serial.Generator, appender storage.Appender) *channelStore {
	return &channelStore{
		gen:         gen,
		appender:    appender,
		byCS:        make(map[string]*protocol.ChannelMessage),
		byID:        make(map[string]string),
		members:     make(map[string]*wire.PresenceMessage),
		latest:      make(map[string]*wire.Message),
		versions:    make(map[string][]*wire.Message),
		annotations: make(map[string][]*wire.Annotation),
		objects:     make(map[string]*wire.StateObject),
	}
}

// Store implements storage.ChannelStore. The whole operation
// (idempotency check + mint + insert + appender delivery) is guarded
// by a single mutex acquire, so concurrent publishes with the same id
// are serialised: one wins, the rest see the duplicate and return
// the original. The appender is fired synchronously after the insert
// for fresh publishes only; idempotent returns do not re-fire the
// appender (the original was already delivered).
func (cs *channelStore) Store(ctx context.Context, msgs []*wire.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/memory: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Resolve the batch id and stamp each Message.ID = "<batchID>:<idx>"
	// (DESIGN.md §8) before minting, so the idempotency index keys on the
	// batch-derived ids.
	batchID, err := storage.StampMessageIDs(msgs)
	if err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Idempotency: any contained ID that was already published on
	// this channel makes the whole publish a duplicate.
	for _, m := range msgs {
		if m.GetId() == "" {
			continue
		}
		if existingCS, ok := cs.byID[m.GetId()]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	channelSerial := cs.gen.Mint()
	for i, m := range msgs {
		m.Serial = serial.MessageSerial(channelSerial, i)
		m.Action = wire.MessageAction_MESSAGE_CREATE
		storage.StampCreateVersion(m)
	}
	cm := &protocol.ChannelMessage{
		ID:            batchID,
		ChannelSerial: channelSerial,
		Messages:      msgs,
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	for _, m := range msgs {
		if m.GetId() != "" {
			cs.byID[m.GetId()] = channelSerial
		}
		// Register the create as the first version + projection entry,
		// so mutations can resolve their target and collapsed history /
		// single-message reads find it (DESIGN.md §13.4).
		cs.latest[m.Serial] = m
		cs.versions[m.Serial] = []*wire.Message{m}
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// StoreSummary logs the summaries of freshly-annotated messages so they reach
// subscribers, without recording them as versions (see storage.ChannelStore).
func (cs *channelStore) StoreSummary(ctx context.Context, summaries []*wire.Message) (*protocol.ChannelMessage, error) {
	if len(summaries) == 0 {
		return nil, errors.New("storage/memory: StoreSummary with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	channelSerial := cs.gen.Mint()
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: summaries}
	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, nil
}

// Mutate applies an update/delete/append to an existing message under the
// single channel mutex, mirroring Store's idempotency + appender
// discipline (DESIGN.md §13.2). It validates the target exists, merges,
// mints a fresh version cm carrying the complete merged Message, and
// updates the latest projection + versions index atomically.
func (cs *channelStore) Mutate(ctx context.Context, mut *wire.Message, merge storage.MergeFunc) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.IsMutation() {
		return nil, false, errors.New("storage/memory: Mutate requires a mutation action")
	}
	if mut.Serial == "" {
		return nil, false, errors.New("storage/memory: Mutate requires a target serial")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if mut.GetId() != "" {
		if existingCS, ok := cs.byID[mut.GetId()]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	current, ok := cs.latest[mut.Serial]
	if !ok {
		return nil, false, storage.ErrTargetNotFound
	}

	channelSerial := cs.gen.Mint()
	version, err := merge(current, mut, serial.MessageSerial(channelSerial, 0))
	if err != nil {
		return nil, false, err
	}
	cm := &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Messages:      []*wire.Message{version},
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	if mut.GetId() != "" {
		cs.byID[mut.GetId()] = channelSerial
	}
	cs.latest[mut.Serial] = version
	// An append is not a version of the message, so it does not join the
	// chain (DESIGN.md §13.3): it changes what the message currently says,
	// which the projection above records, and it stays on the log for live
	// and resume fan-out.
	if !version.HasAppend() {
		cs.versions[mut.Serial] = append(cs.versions[mut.Serial], version)
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// LatestVersion returns the projection entry for serial, or
// ErrTargetNotFound (DESIGN.md §13.4).
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*wire.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	m, ok := cs.latest[serial]
	if !ok {
		return nil, storage.ErrTargetNotFound
	}
	return m, nil
}

// Versions returns every version of serial ordered by version, paginated
// at version granularity via q.Cursor (a version serial) / q.Limit /
// q.Direction (DESIGN.md §13.4).
func (cs *channelStore) Versions(ctx context.Context, serial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	all, ok := cs.versions[serial]
	if !ok {
		return storage.HistoryPage{}, storage.ErrTargetNotFound
	}
	return storage.PaginateVersions(all, q), nil
}

// StoreAnnotation persists an annotation publish on the same stream as
// messages and presence (DESIGN.md §14.1), validating that every
// annotation's target message resolves in the latest-version projection
// (ErrTargetNotFound otherwise, like a mutation) before minting. The
// annotation cm lands on the shared log and is indexed by its target
// serial so annotations-for-message reads are O(target). Idempotency
// shares the byID index with messages/presence. The returned cm is the
// annotation summary-fold seam.
func (cs *channelStore) StoreAnnotation(ctx context.Context, annotations []*wire.Annotation, fold storage.FoldFunc) (*protocol.ChannelMessage, bool, error) {
	if len(annotations) == 0 {
		return nil, false, errors.New("storage/memory: StoreAnnotation with no annotations")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, a := range annotations {
		if a.GetId() == "" {
			continue
		}
		if existingCS, ok := cs.byID[a.GetId()]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	// Target existence: every annotation must reference a message that
	// resolves in the latest-version projection (DESIGN.md §14.1). Checked
	// before minting so a bad target does not burn a serial.
	for _, a := range annotations {
		if _, ok := cs.latest[a.MessageSerial]; !ok {
			return nil, false, storage.ErrTargetNotFound
		}
	}

	channelSerial := cs.gen.Mint()
	for i, a := range annotations {
		a.Serial = storage.AnnotationSerial(channelSerial, i)
		// Fold the annotation into its target's summary projection and stamp
		// the post-fold snapshot onto the annotation for delivery (DESIGN.md
		// §14.2). Both happen under the channel mutex, atomically with the
		// log write, exactly like the presence membership fold.
		cs.foldSummary(a, fold)
	}
	cm := &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Annotations:   annotations,
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	for _, a := range annotations {
		if a.GetId() != "" {
			cs.byID[a.GetId()] = channelSerial
		}
		cs.annotations[a.MessageSerial] = append(cs.annotations[a.MessageSerial], a)
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// foldSummary folds one annotation into its target message's summary on the
// latest-version projection (DESIGN.md §14.2). It runs under cs.mu with the
// target already validated to exist. The projection Message is replaced by a
// clone carrying the new summary, so a reader holding the prior pointer is
// unaffected.
func (cs *channelStore) foldSummary(a *wire.Annotation, fold storage.FoldFunc) {
	cur := cs.latest[a.MessageSerial]
	if cur == nil {
		return
	}
	updated := cur.Clone()
	fold(updated, a)
	cs.latest[a.MessageSerial] = updated
}

// Annotations returns the annotations attached to messageSerial in stream
// order, paginated at annotation-serial granularity (DESIGN.md §14.4). An
// unknown target yields an empty page.
func (cs *channelStore) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return storage.PaginateAnnotations(cs.annotations[messageSerial], q), nil
}

// StorePresence persists a presence publish on the same stream as
// messages and folds it into the membership set, all under the single
// channel mutex (DESIGN.md §12.2, §12.5). Idempotency shares the byID
// index with messages.
func (cs *channelStore) StorePresence(ctx context.Context, presence []*wire.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/memory: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, p := range presence {
		if p.GetId() == "" {
			continue
		}
		if existingCS, ok := cs.byID[p.GetId()]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	channelSerial := cs.gen.Mint()
	for i, p := range presence {
		storage.StampPresenceMember(p, channelSerial, i)
	}
	cm := &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Presence:      presence,
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	for _, p := range presence {
		if p.GetId() != "" {
			cs.byID[p.GetId()] = channelSerial
		}
		key := storage.MemberKey(p.ConnectionId, p.GetClientId())
		switch p.Action {
		case wire.PresenceMessage_LEAVE, wire.PresenceMessage_ABSENT:
			delete(cs.members, key)
		default: // Enter, Update, Present
			cs.members[key] = p
		}
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// Members returns the current membership set (sorted by Serial for a
// stable order) and the channel's current watermark as the as-of serial.
func (cs *channelStore) Members(ctx context.Context, q storage.MembersQuery) (storage.MembersPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.MembersPage{}, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	out := make([]*wire.PresenceMessage, 0, len(cs.members))
	for _, p := range cs.members {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		return storage.MemberKey(out[i].ConnectionId, out[i].GetClientId()) <
			storage.MemberKey(out[j].ConnectionId, out[j].GetClientId())
	})

	var asOf string
	if n := len(cs.order); n > 0 {
		asOf = cs.order[n-1]
	}

	out, next := storage.PageMembers(out, q)
	return storage.MembersPage{Members: out, AsOfSerial: asOf, NextCursor: next}, nil
}

// StoreState persists a LiveObjects publish on the same stream as messages and
// applies its operations to the materialised object set, all under the single
// channel mutex (DESIGN.md §15.2, §15.3). Idempotency shares the byID index
// with messages and presence.
//
// The apply runs while the mutex is held, so the objects it is given cannot
// have moved under it by the time its result is written back — the same
// atomicity Mutate gets from holding the lock across load-merge-store.
func (cs *channelStore) StoreState(ctx context.Context, state []*wire.StateMessage, apply storage.ApplyFunc) (*protocol.ChannelMessage, bool, error) {
	if len(state) == 0 {
		return nil, false, errors.New("storage/memory: StoreState with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, sm := range state {
		if sm.GetId() == "" {
			continue
		}
		if existingCS, ok := cs.byID[sm.GetId()]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	channelSerial := cs.gen.Mint()
	for i, sm := range state {
		storage.StampStateMessage(sm, channelSerial, i)
	}

	named := make([]*wire.StateObject, 0, len(state))
	for _, id := range storage.StateObjectIDs(state) {
		if obj, ok := cs.objects[id]; ok {
			named = append(named, obj)
		}
	}
	changed, err := apply(named, state)
	if err != nil {
		return nil, false, err
	}

	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, State: state}
	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	for _, sm := range state {
		if sm.GetId() != "" {
			cs.byID[sm.GetId()] = channelSerial
		}
	}
	for _, obj := range changed {
		cs.objects[obj.GetObjectId()] = obj
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// Objects returns the materialised object set (sorted by object id, which is
// the order the protocol's own paging walks it in) and the channel's current
// watermark as the as-of serial.
func (cs *channelStore) Objects(ctx context.Context, q storage.ObjectsQuery) (storage.ObjectsPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ObjectsPage{}, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	out := make([]*wire.StateObject, 0, len(cs.objects))
	for _, obj := range cs.objects {
		out = append(out, obj)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetObjectId() < out[j].GetObjectId()
	})

	var asOf string
	if n := len(cs.order); n > 0 {
		asOf = cs.order[n-1]
	}

	out, next := storage.PageObjects(out, q)
	return storage.ObjectsPage{Objects: out, AsOfSerial: asOf, NextCursor: next}, nil
}

// StoreOccupancy records this process's contribution and signals the appender,
// which is the whole of the broadcast here: one process means the contribution
// is the aggregate, and the only reader is on the other side of that call.
func (cs *channelStore) StoreOccupancy(ctx context.Context, counts *wire.ChannelOccupancy) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cs.mu.Lock()
	if storage.OccupancyIsEmpty(counts) {
		cs.occupancy = nil
	} else {
		cs.occupancy = storage.CopyOccupancy(counts)
	}
	cs.mu.Unlock()

	if cs.appender != nil {
		cs.appender.OccupancyChanged()
	}
	return nil
}

// Occupancy is this process's contribution plus the size of the membership
// set, which is where presenceMembers comes from.
func (cs *channelStore) Occupancy(ctx context.Context) (*wire.ChannelOccupancy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	agg := &wire.ChannelOccupancy{}
	storage.SumOccupancy(agg, cs.occupancy)
	agg.PresenceMembers = int32(len(cs.members))
	return agg, nil
}

// collapsedHistory returns the latest version of each message positioned
// at its create serial (DESIGN.md §13.4), driving the default REST
// message history. Called with cs.mu held. Identities sort by create
// position (identity == createSerial:idx); time bounds and the cursor
// compare against the identity, and a multi-message create batch is
// regrouped under its shared create channelSerial (reversed within the
// batch for backwards, matching the raw scan). q.EndChannelSerial (the
// fromSerial/untilAttached bound) caps entries to their
// CREATE channelSerial <= the bound — an inclusive check against
// storage.CreateChannelSerial(id), not a raw string compare of id
// itself, since id carries a ":idx" suffix the bound doesn't have.
func (cs *channelStore) collapsedHistory(q storage.HistoryQuery) storage.HistoryPage {
	lower, upper := serial.TimestampBounds(q.Start, q.End)
	ids := make([]string, 0, len(cs.latest))
	for id := range cs.latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	forwards := q.Direction == storage.DirectionForwards
	cursor := q.Cursor
	limit := q.Limit

	var page storage.HistoryPage
	count := 0
	emit := func(m *wire.Message) bool {
		if limit > 0 && count >= limit {
			page.HasMore = true
			return false
		}
		ccs := storage.CreateChannelSerial(m.Serial)
		var current *protocol.ChannelMessage
		if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == ccs {
			current = page.ChannelMessages[n-1]
		} else {
			current = &protocol.ChannelMessage{ChannelSerial: ccs}
			page.ChannelMessages = append(page.ChannelMessages, current)
		}
		current.Messages = append(current.Messages, m)
		page.LastSerial = m.Serial
		count++
		return true
	}
	inBounds := func(id string) bool {
		if lower != "" && id < lower {
			return false
		}
		if upper != "" && id >= upper {
			return false
		}
		if q.EndChannelSerial != "" && storage.CreateChannelSerial(id) > q.EndChannelSerial {
			return false
		}
		return true
	}

	if forwards {
		for _, id := range ids {
			if !inBounds(id) || (cursor != "" && id <= cursor) {
				continue
			}
			if !emit(cs.latest[id]) {
				break
			}
		}
		return page
	}
	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		if !inBounds(id) || (cursor != "" && id >= cursor) {
			continue
		}
		if !emit(cs.latest[id]) {
			break
		}
	}
	return page
}

// History implements storage.ChannelStore. The walk over cs.order is
// direction-aware: forwards starts at the lower-bound index and walks
// up; backwards starts at the upper-bound index and walks down. Within
// each batch we iterate Messages in idx order (forwards) or reverse
// idx order (backwards), applying the cursor at Message-serial
// granularity. Time bounds (q.Start / q.End) and the channelSerial-
// extracted cursor are lex compares against the channelSerial column.
//
// Limit and HasMore are counted at Message granularity, matching
// Ably's REST `limit` semantics — a single multi-message batch can be
// split across pages. Emitted ChannelMessages are shallow copies; the
// persisted state is never mutated.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	wantKind := q.Kind.Normalize()
	if q.Collapse && wantKind == storage.KindMessage {
		return cs.collapsedHistory(q), nil
	}
	lower, upper := serial.TimestampBounds(q.Start, q.End)

	lo := 0
	if lower != "" {
		lo = sort.SearchStrings(cs.order, lower)
	}
	if q.AfterChannelSerial != "" {
		// Strict lower bound on channelSerial: the first index whose
		// channelSerial > AfterChannelSerial. Searching for the lex
		// successor (append NUL) skips the equal serial's own rows.
		if after := sort.SearchStrings(cs.order, q.AfterChannelSerial+"\x00"); after > lo {
			lo = after
		}
	}
	hi := len(cs.order)
	if upper != "" {
		hi = sort.SearchStrings(cs.order, upper)
	}
	if q.EndChannelSerial != "" {
		// Tighten hi to the first index whose channelSerial > EndChannelSerial.
		// sort.SearchStrings on the bound directly gives the smallest
		// index with cs.order[i] >= upperKey, so we use the immediate
		// successor in lex space (append a NUL byte) to express "<=".
		hiExclusive := sort.SearchStrings(cs.order, q.EndChannelSerial+"\x00")
		if hiExclusive < hi {
			hi = hiExclusive
		}
	}

	forwards := q.Direction == storage.DirectionForwards
	cursor := q.Cursor
	limit := q.Limit

	var page storage.HistoryPage
	count := 0

	// emit appends one item (a Message or a PresenceMessage, via put)
	// onto the trailing ChannelMessage when its channelSerial matches,
	// or a fresh entry otherwise. Returns false once Limit is reached.
	emit := func(channelSerial, itemSerial string, put func(dst *protocol.ChannelMessage)) bool {
		if limit > 0 && count >= limit {
			page.HasMore = true
			return false
		}
		var current *protocol.ChannelMessage
		if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
			current = page.ChannelMessages[n-1]
		} else {
			current = &protocol.ChannelMessage{ChannelSerial: channelSerial}
			page.ChannelMessages = append(page.ChannelMessages, current)
		}
		put(current)
		page.LastSerial = itemSerial
		count++
		return true
	}

	if forwards {
		for i := lo; i < hi; i++ {
			cm := cs.byCS[cs.order[i]]
			for _, it := range storage.CMItems(cm, wantKind) {
				if cursor != "" && it.Serial <= cursor {
					continue
				}
				if !emit(cm.ChannelSerial, it.Serial, it.Append) {
					return page, nil
				}
			}
		}
		return page, nil
	}

	for i := hi - 1; i >= lo; i-- {
		cm := cs.byCS[cs.order[i]]
		items := storage.CMItems(cm, wantKind)
		for j := len(items) - 1; j >= 0; j-- {
			if cursor != "" && items[j].Serial >= cursor {
				continue
			}
			if !emit(cm.ChannelSerial, items[j].Serial, items[j].Append) {
				return page, nil
			}
		}
	}
	return page, nil
}
