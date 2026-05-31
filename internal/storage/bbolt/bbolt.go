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

func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	var page storage.HistoryPage
	err := cs.db.View(func(tx *bolt.Tx) error {
		messages := tx.Bucket(messagesBucket)
		if messages == nil {
			return nil
		}

		prefix := channelPrefix(cs.name)
		var seekKey []byte
		if q.AfterChannelSerial == "" {
			seekKey = prefix
		} else {
			seekKey = channelKey(cs.name, q.AfterChannelSerial)
		}

		cur := messages.Cursor()
		k, v := cur.Seek(seekKey)
		if q.AfterChannelSerial != "" && k != nil && bytes.Equal(k, seekKey) {
			k, v = cur.Next()
		}
		for ; k != nil; k, v = cur.Next() {
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			if q.Limit > 0 && len(page.ChannelMessages) >= q.Limit {
				page.HasMore = true
				return nil
			}
			cm := &protocol.ChannelMessage{}
			if err := msgpack.Unmarshal(v, cm); err != nil {
				return fmt.Errorf("storage/bbolt: decode ChannelMessage %q: %w", k, err)
			}
			page.ChannelMessages = append(page.ChannelMessages, cm)
		}
		return nil
	})
	if err != nil {
		return storage.HistoryPage{}, err
	}
	return page, nil
}
