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
)

// Options configures a Storage. Zero values pick sensible defaults.
type Options struct {
	// SeriesID is the per-process series identifier embedded in every
	// minted channelSerial. Empty means generate one at New time.
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
	byID    map[string]string                    // Message.id / PresenceMessage.id -> channelSerial
	members map[string]*protocol.PresenceMessage // "<connId>:<clientId>" -> latest member (DESIGN.md §12.5)

	// Mutable-message derived structures (DESIGN.md §13.4), maintained
	// under mu alongside the log. latest is the materialised projection:
	// message identity serial -> latest merged version. versions is the
	// serial→versions index: identity serial -> every version in version
	// (publish) order.
	latest   map[string]*protocol.Message
	versions map[string][]*protocol.Message

	// annotations indexes a target message identity serial to its
	// annotations in stream (publish) order — the annotations-for-message
	// scan (DESIGN.md §14.4), the memory analogue of the postgres
	// channel_messages serial index. The annotation cms also live on the
	// shared log (order/byCS) so they flow to the appender and are
	// kind-skipped by message/presence history.
	annotations map[string][]*protocol.Annotation
}

func newChannelStore(gen *serial.Generator, appender storage.Appender) *channelStore {
	return &channelStore{
		gen:         gen,
		appender:    appender,
		byCS:        make(map[string]*protocol.ChannelMessage),
		byID:        make(map[string]string),
		members:     make(map[string]*protocol.PresenceMessage),
		latest:      make(map[string]*protocol.Message),
		versions:    make(map[string][]*protocol.Message),
		annotations: make(map[string][]*protocol.Annotation),
	}
}

// Store implements storage.ChannelStore. The whole operation
// (idempotency check + mint + insert + appender delivery) is guarded
// by a single mutex acquire, so concurrent publishes with the same id
// are serialised: one wins, the rest see the duplicate and return
// the original. The appender is fired synchronously after the insert
// for fresh publishes only; idempotent returns do not re-fire the
// appender (the original was already delivered).
func (cs *channelStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
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
		if m.ID == "" {
			continue
		}
		if existingCS, ok := cs.byID[m.ID]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	channelSerial := cs.gen.Mint()
	for i, m := range msgs {
		m.Serial = serial.MessageSerial(channelSerial, i)
		m.Action = protocol.MessageCreate
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
		if m.ID != "" {
			cs.byID[m.ID] = channelSerial
		}
		// Register the create as the first version + projection entry,
		// so mutations can resolve their target and collapsed history /
		// single-message reads find it (DESIGN.md §13.4).
		cs.latest[m.Serial] = m
		cs.versions[m.Serial] = []*protocol.Message{m}
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// Mutate applies an update/delete/append to an existing message under the
// single channel mutex, mirroring Store's idempotency + appender
// discipline (DESIGN.md §13.2). It validates the target exists, merges,
// mints a fresh version cm carrying the complete merged Message, and
// updates the latest projection + versions index atomically.
func (cs *channelStore) Mutate(ctx context.Context, mut *protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.Action.IsMutation() {
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

	if mut.ID != "" {
		if existingCS, ok := cs.byID[mut.ID]; ok {
			return cs.byCS[existingCS], true, nil
		}
	}

	current, ok := cs.latest[mut.Serial]
	if !ok {
		return nil, false, storage.ErrTargetNotFound
	}

	channelSerial := cs.gen.Mint()
	version, err := storage.MergeVersion(current, mut, serial.MessageSerial(channelSerial, 0))
	if err != nil {
		return nil, false, err
	}
	cm := &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Messages:      []*protocol.Message{version},
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	if mut.ID != "" {
		cs.byID[mut.ID] = channelSerial
	}
	cs.latest[mut.Serial] = version
	cs.versions[mut.Serial] = append(cs.versions[mut.Serial], version)

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// LatestVersion returns the projection entry for serial, or
// ErrTargetNotFound (DESIGN.md §13.4).
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*protocol.Message, error) {
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
	// Collapse append runs so history reflects the aggregate, not each
	// delta (DESIGN.md §13.3, §13.4); the log keeps every append cm.
	return storage.PaginateVersions(storage.CollapseAppendVersions(all), q), nil
}

// StoreAnnotation persists an annotation publish on the same stream as
// messages and presence (DESIGN.md §14.1), validating that every
// annotation's target message resolves in the latest-version projection
// (ErrTargetNotFound otherwise, like a mutation) before minting. The
// annotation cm lands on the shared log and is indexed by its target
// serial so annotations-for-message reads are O(target). Idempotency
// shares the byID index with messages/presence. The returned cm is the
// TASK-66 summary-fold seam.
func (cs *channelStore) StoreAnnotation(ctx context.Context, annotations []*protocol.Annotation) (*protocol.ChannelMessage, bool, error) {
	if len(annotations) == 0 {
		return nil, false, errors.New("storage/memory: StoreAnnotation with no annotations")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, a := range annotations {
		if a.ID == "" {
			continue
		}
		if existingCS, ok := cs.byID[a.ID]; ok {
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
		a.Serial = serial.MessageSerial(channelSerial, i)
		// Fold the annotation into its target's summary projection and stamp
		// the post-fold snapshot onto the annotation for delivery (DESIGN.md
		// §14.2). Both happen under the channel mutex, atomically with the
		// log write, exactly like the presence membership fold.
		cs.foldSummary(a)
	}
	cm := &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Annotations:   annotations,
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	for _, a := range annotations {
		if a.ID != "" {
			cs.byID[a.ID] = channelSerial
		}
		cs.annotations[a.MessageSerial] = append(cs.annotations[a.MessageSerial], a)
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
}

// foldSummary folds one annotation into its target message's summary on the
// latest-version projection and stamps the post-fold snapshot onto the
// annotation for delivery (DESIGN.md §14.2). It runs under cs.mu with the
// target already validated to exist. The projection Message is replaced by a
// shallow copy carrying the new summary so a reader holding the prior
// pointer is unaffected, and the annotation's snapshot is a clone so a later
// fold in the same batch cannot disturb it.
func (cs *channelStore) foldSummary(a *protocol.Annotation) {
	cur := cs.latest[a.MessageSerial]
	if cur == nil {
		return
	}
	folded := cur.Summary.Apply(a)
	updated := *cur
	updated.Summary = folded
	cs.latest[a.MessageSerial] = &updated
	a.Summary = folded.Clone()
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
func (cs *channelStore) StorePresence(ctx context.Context, presence []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/memory: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, p := range presence {
		if p.ID == "" {
			continue
		}
		if existingCS, ok := cs.byID[p.ID]; ok {
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
		if p.ID != "" {
			cs.byID[p.ID] = channelSerial
		}
		key := storage.MemberKey(p.ConnectionID, p.ClientID)
		switch p.Action {
		case protocol.PresenceLeave, protocol.PresenceAbsent:
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
func (cs *channelStore) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	out := make([]*protocol.PresenceMessage, 0, len(cs.members))
	for _, p := range cs.members {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Serial < out[j].Serial })

	var asOf string
	if n := len(cs.order); n > 0 {
		asOf = cs.order[n-1]
	}
	return out, asOf, nil
}

// collapsedHistory returns the latest version of each message positioned
// at its create serial (DESIGN.md §13.4), driving the default REST
// message history. Called with cs.mu held. Identities sort by create
// position (identity == createSerial:idx); time bounds and the cursor
// compare against the identity, and a multi-message create batch is
// regrouped under its shared create channelSerial (reversed within the
// batch for backwards, matching the raw scan).
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
	emit := func(m *protocol.Message) bool {
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
	emit := func(channelSerial string, put func(dst *protocol.ChannelMessage)) bool {
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
				if !emit(cm.ChannelSerial, it.Append) {
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
			if !emit(cm.ChannelSerial, items[j].Append) {
				return page, nil
			}
		}
	}
	return page, nil
}
