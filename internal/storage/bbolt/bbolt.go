// Package bbolt is the on-disk storage backend (DESIGN.md §6.2).
// State lives in a single bolt file at the configured data path with
// two top-level buckets:
//
//   - messages: keyed "<channel>\0<channelSerial>", value is the
//     msgpack-encoded protocol.ChannelMessage. bbolt's byte-order
//     iteration over a "<channel>\0" prefix yields a channel's
//     ChannelMessages in publish order.
//   - ids: keyed "<channel>\0<Message.id>", value is the channelSerial
//     the ID landed in. bbolt has no secondary indexes, so this is
//     the manual equivalent of Postgres's partial UNIQUE
//     idempotency index.
//
// Per-process seriesId is regenerated on every Open — the same
// rationale as the Postgres backend (DESIGN.md §8). Generator
// monotonic state is not persisted; restart monotonicity falls out
// because wall-clock time advances.
package bbolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

var (
	messagesBucket = []byte("messages")
	idsBucket      = []byte("ids")
)

// keySep separates the channel name from the rest of a composite key.
// NUL never appears in channel names in any Ably protocol use case, so
// it's a safe in-band separator.
const keySep = byte(0)

// Options configures the bbolt backend.
type Options struct {
	// Path is the file path for the bolt DB. Required.
	Path string

	// Now is the clock used by the serial generator. Nil means
	// time.Now().UnixMilli — overridden by tests for determinism.
	Now func() int64
}

// Storage is the bbolt-backed storage.Storage.
type Storage struct {
	db  *bolt.DB
	gen *serial.Generator

	mu       sync.Mutex
	channels map[string]*channelStore
}

// Open opens (or creates) the bolt file at opts.Path, ensures the two
// top-level buckets exist, and returns a Storage ready for use. The
// seriesId is freshly generated per process.
func Open(opts Options) (*Storage, error) {
	if opts.Path == "" {
		return nil, errors.New("storage/bbolt: Open requires a Path")
	}
	db, err := bolt.Open(opts.Path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("storage/bbolt: open %q: %w", opts.Path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(messagesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(idsBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage/bbolt: bootstrap buckets: %w", err)
	}

	return &Storage{
		db:       db,
		gen:      serial.NewGenerator(serial.NewSeriesID(), opts.Now),
		channels: make(map[string]*channelStore),
	}, nil
}

// Channel returns the ChannelStore for name, binding it to appender
// on first access. Subsequent calls with the same name return the
// same instance and ignore the new appender.
func (s *Storage) Channel(name string, appender storage.Appender) storage.ChannelStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs
	}
	cs := &channelStore{
		db:       s.db,
		gen:      s.gen,
		name:     name,
		appender: appender,
	}
	s.channels[name] = cs
	return cs
}

// Close closes the underlying bolt DB.
func (s *Storage) Close() error {
	return s.db.Close()
}

// channelKey returns a composite key "<channel>\0<suffix>" suitable
// for either the messages or ids bucket.
func channelKey(channel, suffix string) []byte {
	b := make([]byte, 0, len(channel)+1+len(suffix))
	b = append(b, channel...)
	b = append(b, keySep)
	b = append(b, suffix...)
	return b
}

// channelPrefix returns "<channel>\0" — the lex-bound for a
// channel's range scan.
func channelPrefix(channel string) []byte {
	b := make([]byte, 0, len(channel)+1)
	b = append(b, channel...)
	b = append(b, keySep)
	return b
}

// channelStore is the per-channel facet. Concurrency is controlled by
// bolt (one writer at a time per DB) and by the shared generator's
// internal mutex.
type channelStore struct {
	db       *bolt.DB
	gen      *serial.Generator
	name     string
	appender storage.Appender
}

func (cs *channelStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/bbolt: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err := cs.db.Update(func(tx *bolt.Tx) error {
		messages := tx.Bucket(messagesBucket)
		ids := tx.Bucket(idsBucket)

		// Idempotency: any contained ID that's already indexed makes
		// this whole publish a duplicate.
		for _, m := range msgs {
			if m.ID == "" {
				continue
			}
			if existingCS := ids.Get(channelKey(cs.name, m.ID)); existingCS != nil {
				blob := messages.Get(channelKey(cs.name, string(existingCS)))
				if blob == nil {
					return fmt.Errorf("storage/bbolt: id index points to missing ChannelMessage %q", existingCS)
				}
				var original protocol.ChannelMessage
				if err := msgpack.Unmarshal(blob, &original); err != nil {
					return fmt.Errorf("storage/bbolt: decode original ChannelMessage: %w", err)
				}
				resultCM = &original
				idempotent = true
				return nil
			}
		}

		// Not a duplicate — mint a fresh channelSerial, stamp each
		// Message, persist.
		channelSerial := cs.gen.Mint()
		for i, m := range msgs {
			m.Serial = serial.MessageSerial(channelSerial, i)
		}
		cm := &protocol.ChannelMessage{
			ChannelSerial: channelSerial,
			Messages:      msgs,
		}
		blob, err := msgpack.Marshal(cm)
		if err != nil {
			return fmt.Errorf("storage/bbolt: encode ChannelMessage: %w", err)
		}
		if err := messages.Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		for _, m := range msgs {
			if m.ID == "" {
				continue
			}
			if err := ids.Put(channelKey(cs.name, m.ID), []byte(channelSerial)); err != nil {
				return err
			}
		}

		resultCM = cm
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	// Fire the appender outside the bolt tx — Append takes Channel's
	// mu, and we want the bolt write lock released ASAP. Idempotent
	// returns do not re-fire (the original was delivered on its
	// first persist).
	if !idempotent && cs.appender != nil {
		cs.appender.Append(resultCM)
	}
	return resultCM, idempotent, nil
}

// History runs a direction-aware range scan over the channel's
// ChannelMessages. Time bounds (q.Start / q.End) are translated to
// byte-comparable lower/upper key bounds against the
// "<channel>\0<channelSerial>" composite key; bolt's cursor walks
// forwards (Seek+Next) or backwards (Seek+Prev) within those bounds.
//
// Pagination operates at Message granularity: q.Cursor is a
// Message.Serial (`<channelSerial>:<idx>`) and q.Limit caps the
// Message count (not the ChannelMessage count). A multi-message batch
// can be split across pages — the head/tail ChannelMessage in a page
// may carry only a subset of its persisted Messages. The persisted
// blob is never mutated.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	prefix := channelPrefix(cs.name)
	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	forwards := q.Direction == storage.DirectionForwards
	limit := q.Limit
	cursor := q.Cursor

	lowerKey := prefix
	if timeLower != "" {
		lowerKey = channelKey(cs.name, timeLower)
	}
	upperKey := nextPrefix(prefix)
	if timeUpper != "" {
		upperKey = channelKey(cs.name, timeUpper)
	}

	var page storage.HistoryPage
	count := 0

	// emitMessage appends m onto the trailing ChannelMessage when its
	// channelSerial matches, or starts a fresh entry otherwise. It
	// returns false once Limit is reached, signalling outer-loop exit.
	emitMessage := func(channelSerial string, m *protocol.Message) bool {
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
		current.Messages = append(current.Messages, m)
		count++
		return true
	}

	err := cs.db.View(func(tx *bolt.Tx) error {
		messages := tx.Bucket(messagesBucket)
		if messages == nil {
			return nil
		}
		c := messages.Cursor()

		decode := func(k, v []byte) (*protocol.ChannelMessage, error) {
			cm := &protocol.ChannelMessage{}
			if err := msgpack.Unmarshal(v, cm); err != nil {
				return nil, fmt.Errorf("storage/bbolt: decode ChannelMessage %q: %w", k, err)
			}
			return cm, nil
		}

		if forwards {
			for k, v := c.Seek(lowerKey); k != nil; k, v = c.Next() {
				if !bytes.HasPrefix(k, prefix) || bytes.Compare(k, upperKey) >= 0 {
					break
				}
				cm, err := decode(k, v)
				if err != nil {
					return err
				}
				for idx, m := range cm.Messages {
					if cursor != "" && serial.MessageSerial(cm.ChannelSerial, idx) <= cursor {
						continue
					}
					if !emitMessage(cm.ChannelSerial, m) {
						return nil
					}
				}
			}
			return nil
		}

		// Backwards: position at the last key strictly < upperKey, then
		// walk Prev() until lowerKey or the prefix is exhausted. Within
		// each batch, iterate Messages in reverse idx order.
		k, v := c.Seek(upperKey)
		if k == nil {
			k, v = c.Last()
		} else {
			k, v = c.Prev()
		}
		for ; k != nil; k, v = c.Prev() {
			if !bytes.HasPrefix(k, prefix) || bytes.Compare(k, lowerKey) < 0 {
				break
			}
			cm, err := decode(k, v)
			if err != nil {
				return err
			}
			for idx := len(cm.Messages) - 1; idx >= 0; idx-- {
				if cursor != "" && serial.MessageSerial(cm.ChannelSerial, idx) >= cursor {
					continue
				}
				if !emitMessage(cm.ChannelSerial, cm.Messages[idx]) {
					return nil
				}
			}
		}
		return nil
	})
	if err != nil {
		return storage.HistoryPage{}, err
	}
	return page, nil
}

// nextPrefix returns the smallest byte string strictly greater than
// every key starting with p — i.e. p with its last byte incremented.
// Callers ensure p is non-empty and its last byte is not 0xff (true
// for channelPrefix, which always ends in keySep == 0).
func nextPrefix(p []byte) []byte {
	out := make([]byte, len(p))
	copy(out, p)
	out[len(out)-1]++
	return out
}
