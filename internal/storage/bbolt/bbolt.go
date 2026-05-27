// Package bbolt is the on-disk storage backend (DESIGN.md §6.2).
// State lives in a single bolt file at the configured data path,
// with one top-level bucket per channel plus a process-wide _meta
// bucket holding the serial generator's monotonic state.
package bbolt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

// Bucket key constants. Channel buckets are named "ch/<channel>" so
// they sort under a known prefix and can't collide with the meta
// bucket.
var (
	metaBucketName      = []byte("_meta")
	metaSeriesIDKey     = []byte("seriesId")
	metaLastTsKey       = []byte("lastTs")
	metaLastCounterKey  = []byte("lastCounter")
	channelBucketPrefix = []byte("ch/")
	messagesSubBucket   = []byte("messages")
	idsSubBucket        = []byte("ids")
)

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

// Open opens (or creates) the bolt file at opts.Path and loads the
// serial generator's state from the _meta bucket. A fresh DB has its
// seriesId minted and persisted before the first Append.
func Open(opts Options) (*Storage, error) {
	if opts.Path == "" {
		return nil, errors.New("storage/bbolt: Open requires a Path")
	}
	db, err := bolt.Open(opts.Path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("storage/bbolt: open %q: %w", opts.Path, err)
	}

	var (
		seriesID   string
		lastTs     int64
		lastCounter int
	)
	err = db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucketName)
		if err != nil {
			return err
		}
		if v := meta.Get(metaSeriesIDKey); v != nil {
			seriesID = string(v)
		} else {
			seriesID = serial.NewSeriesID()
			if err := meta.Put(metaSeriesIDKey, []byte(seriesID)); err != nil {
				return err
			}
		}
		if v := meta.Get(metaLastTsKey); v != nil {
			lastTs = int64(binary.BigEndian.Uint64(v))
		}
		if v := meta.Get(metaLastCounterKey); v != nil {
			lastCounter = int(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage/bbolt: load _meta: %w", err)
	}

	gen := serial.NewGenerator(seriesID, opts.Now)
	if lastTs != 0 || lastCounter != 0 {
		gen.Restore(lastTs, lastCounter)
	}

	return &Storage{
		db:       db,
		gen:      gen,
		channels: make(map[string]*channelStore),
	}, nil
}

// Channel returns the ChannelStore for name. Successive calls with
// the same name return the same instance.
func (s *Storage) Channel(name string) storage.ChannelStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs
	}
	cs := &channelStore{
		db:         s.db,
		gen:        s.gen,
		bucketName: channelBucketName(name),
	}
	s.channels[name] = cs
	return cs
}

// Close closes the underlying bolt DB.
func (s *Storage) Close() error {
	return s.db.Close()
}

func channelBucketName(name string) []byte {
	b := make([]byte, 0, len(channelBucketPrefix)+len(name))
	b = append(b, channelBucketPrefix...)
	b = append(b, name...)
	return b
}

// channelStore is the per-channel facet. All operations run inside a
// bolt tx, so concurrency is controlled by bolt (one writer at a
// time) and by the shared generator's internal mutex.
type channelStore struct {
	db         *bolt.DB
	gen        *serial.Generator
	bucketName []byte
}

func (cs *channelStore) AppendChannelMessage(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/bbolt: AppendChannelMessage with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err := cs.db.Update(func(tx *bolt.Tx) error {
		cb, err := tx.CreateBucketIfNotExists(cs.bucketName)
		if err != nil {
			return err
		}
		messages, err := cb.CreateBucketIfNotExists(messagesSubBucket)
		if err != nil {
			return err
		}
		ids, err := cb.CreateBucketIfNotExists(idsSubBucket)
		if err != nil {
			return err
		}

		// Idempotency: any contained ID that's already indexed makes
		// this whole publish a duplicate.
		for _, m := range msgs {
			if m.ID == "" {
				continue
			}
			if existingCS := ids.Get([]byte(m.ID)); existingCS != nil {
				blob := messages.Get(existingCS)
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
		if err := messages.Put([]byte(channelSerial), blob); err != nil {
			return err
		}
		for _, m := range msgs {
			if m.ID == "" {
				continue
			}
			if err := ids.Put([]byte(m.ID), []byte(channelSerial)); err != nil {
				return err
			}
		}

		// Persist the generator's state so a fresh process keeps
		// minting monotonically. The channelSerial itself encodes
		// (ts, counter), so we just parse it back.
		ts, counter, perr := parseTimestampCounter(channelSerial)
		if perr != nil {
			return perr
		}
		meta := tx.Bucket(metaBucketName)
		if err := putUint64(meta, metaLastTsKey, uint64(ts)); err != nil {
			return err
		}
		if err := putUint64(meta, metaLastCounterKey, uint64(counter)); err != nil {
			return err
		}

		resultCM = cm
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return resultCM, idempotent, nil
}

func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	var page storage.HistoryPage
	err := cs.db.View(func(tx *bolt.Tx) error {
		cb := tx.Bucket(cs.bucketName)
		if cb == nil {
			return nil
		}
		messages := cb.Bucket(messagesSubBucket)
		if messages == nil {
			return nil
		}

		cur := messages.Cursor()
		var k, v []byte
		if q.AfterChannelSerial == "" {
			k, v = cur.First()
		} else {
			k, v = cur.Seek([]byte(q.AfterChannelSerial))
			if k != nil && string(k) == q.AfterChannelSerial {
				k, v = cur.Next()
			}
		}
		for ; k != nil; k, v = cur.Next() {
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

// parseTimestampCounter pulls (ts, counter) back out of a freshly
// minted channelSerial of the form "<14-digit ts>-<3-digit ctr>@<series>".
func parseTimestampCounter(cs string) (int64, int, error) {
	if len(cs) < 18 || cs[14] != '-' || cs[18] != '@' {
		return 0, 0, fmt.Errorf("storage/bbolt: malformed channelSerial %q", cs)
	}
	ts, err := strconv.ParseInt(cs[:14], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("storage/bbolt: parse timestamp from %q: %w", cs, err)
	}
	ctr, err := strconv.Atoi(cs[15:18])
	if err != nil {
		return 0, 0, fmt.Errorf("storage/bbolt: parse counter from %q: %w", cs, err)
	}
	return ts, ctr, nil
}

func putUint64(b *bolt.Bucket, key []byte, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return b.Put(key, buf[:])
}
