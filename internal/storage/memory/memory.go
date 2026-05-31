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
func (s *Storage) Channel(name string, appender storage.Appender) storage.ChannelStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs
	}
	cs := newChannelStore(s.gen, appender)
	s.channels[name] = cs
	return cs
}

// Close is a no-op for the memory backend.
func (s *Storage) Close() error {
	return nil
}

// channelStore holds the per-channel state: an ordered list of
// channelSerials, a map for O(1) lookup, an idempotency index, and
// the Appender that will receive freshly-stored ChannelMessages.
type channelStore struct {
	gen      *serial.Generator
	appender storage.Appender

	mu    sync.Mutex
	order []string // append-only, sorted (serials are monotonic)
	byCS  map[string]*protocol.ChannelMessage
	byID  map[string]string // Message.id -> channelSerial
}

func newChannelStore(gen *serial.Generator, appender storage.Appender) *channelStore {
	return &channelStore{
		gen:      gen,
		appender: appender,
		byCS:     make(map[string]*protocol.ChannelMessage),
		byID:     make(map[string]string),
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

// History implements storage.ChannelStore.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	start := 0
	if q.AfterChannelSerial != "" {
		// First index whose serial > AfterChannelSerial.
		start = sort.SearchStrings(cs.order, q.AfterChannelSerial)
		if start < len(cs.order) && cs.order[start] == q.AfterChannelSerial {
			start++
		}
	}

	end := len(cs.order)
	hasMore := false
	if q.Limit > 0 && end-start > q.Limit {
		end = start + q.Limit
		hasMore = true
	}

	page := make([]*protocol.ChannelMessage, 0, end-start)
	for i := start; i < end; i++ {
		page = append(page, cs.byCS[cs.order[i]])
	}
	return storage.HistoryPage{ChannelMessages: page, HasMore: hasMore}, nil
}
