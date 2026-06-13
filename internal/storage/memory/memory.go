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
}

func newChannelStore(gen *serial.Generator, appender storage.Appender) *channelStore {
	return &channelStore{
		gen:      gen,
		appender: appender,
		byCS:     make(map[string]*protocol.ChannelMessage),
		byID:     make(map[string]string),
		members:  make(map[string]*protocol.PresenceMessage),
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
	}
	cm := &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Messages:      msgs,
	}

	cs.byCS[channelSerial] = cm
	cs.order = append(cs.order, channelSerial)
	for _, m := range msgs {
		if m.ID != "" {
			cs.byID[m.ID] = channelSerial
		}
	}

	if cs.appender != nil {
		cs.appender.Append(cm)
	}
	return cm, false, nil
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
		p.Serial = serial.MessageSerial(channelSerial, i)
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
	lower, upper := serial.TimestampBounds(q.Start, q.End)

	lo := 0
	if lower != "" {
		lo = sort.SearchStrings(cs.order, lower)
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
