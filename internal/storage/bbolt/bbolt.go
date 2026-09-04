// Package bbolt is the on-disk storage backend (DESIGN.md §6.2).
// State lives in a single bolt file at the configured data path with
// two top-level buckets:
//
//   - channel_messages: the append-only log, keyed
//     "<channel>\0<channelSerial>", value is the protobuf-encoded
//     protocol.ChannelMessage (a message or presence cm). bbolt's
//     byte-order iteration over a "<channel>\0" prefix yields a
//     channel's ChannelMessages in publish order.
//   - ids: keyed "<channel>\0<Message.id>", value is the channelSerial
//     the ID landed in. bbolt has no secondary indexes, so this is
//     the manual equivalent of Postgres's partial UNIQUE
//     idempotency index.
//
// The presence membership set is held in memory (per channelStore),
// NOT persisted: presence is connection-scoped and no connection
// survives a process restart, so the set is correctly empty on Open
// (DESIGN.md §12.5). Presence history still persists as ordinary cms
// in channel_messages.
//
// The materialised LiveObjects set IS persisted, in the objects bucket
// (DESIGN.md §15.3). It is the opposite case to presence: an object
// belongs to the channel rather than to a connection, so a restart that
// dropped it would lose state no client can re-establish.
//
// A channel's seriesId and its generator's monotonic state both
// survive a restart, recovered from the initials bucket and the last
// persisted cm when the channel is next materialised (DESIGN.md §8).
// The series has to, because a change of it tells a client the
// channel's ordering restarted, and reopening a file whose log is
// intact restarts nothing.
package bbolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/server-protocol/go/wire"
)

var (
	channelMessagesBucket = []byte("channel_messages")
	idsBucket             = []byte("ids")
	// latestBucket is the materialised messages projection (DESIGN.md
	// §13.4): keyed "<channel>\0<identity>", value the protobuf-encoded
	// latest merged wire.Message (a delete leaves a tombstone whose
	// Action is delete). Persisted so collapsed history and single-message
	// reads survive restarts.
	latestBucket = []byte("latest")
	// versionsBucket is the serial→versions index (DESIGN.md §13.4):
	// keyed "<channel>\0<identity>\0<versionSerial>", value the
	// protobuf-encoded version wire.Message. A prefix scan over
	// "<channel>\0<identity>\0" yields every version in version order.
	versionsBucket = []byte("versions")
	// initialsBucket maps channel name → the immutable initial serial
	// minted when that channel was first materialised. Persisted so the
	// invariant "initial < every cm in this channel" survives process
	// restarts — otherwise a fresh process-local generator would mint
	// a seed at the current wall-clock time, AFTER existing pre-restart
	// cms.
	initialsBucket = []byte("initials")
	// annotationsBucket is the annotations-for-message index (DESIGN.md
	// §14.4): keyed "<channel>\0<target>\0<annotationSerial>", value the
	// protobuf-encoded wire.Annotation. A prefix scan over
	// "<channel>\0<target>\0" yields a message's annotations in stream
	// order — the bbolt analogue of the postgres channel_messages serial
	// index. The annotation cms also live in channel_messages so they flow
	// through the appender and are kind-skipped by message/presence history.
	annotationsBucket = []byte("annotations")
	// objectsBucket is the materialised LiveObjects set (DESIGN.md §15.3):
	// keyed "<channel>\0<objectId>", value the protobuf-encoded
	// wire.StateObject. bbolt's byte-order iteration over a "<channel>\0"
	// prefix yields a channel's objects in object-id order, which is the
	// order a state sync pages them in.
	objectsBucket = []byte("objects")
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
	db *bolt.DB
	// now is the clock every channel's generator reads.
	now func() int64

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
		if _, err := tx.CreateBucketIfNotExists(channelMessagesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(idsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(initialsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(latestBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(versionsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(annotationsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(objectsBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage/bbolt: bootstrap buckets: %w", err)
	}

	return &Storage{
		db:       db,
		now:      opts.Now,
		channels: make(map[string]*channelStore),
	}, nil
}

// Channel returns the ChannelStore for name, binding it to appender on
// first access. Subsequent calls with the same name return the same
// instance and ignore the new appender. On first creation the initial
// channelSerial is loaded from the persisted initials bucket (or
// minted fresh and persisted if the channel is brand-new); the
// current channelSerial is the latest persisted cm's serial, or the
// initial if the channel has no persisted cms. Both are handed to
// appender.Initialize before this call returns.
//
// The channel's generator is built from those two: it carries on the
// series the channel was first seeded with, and resumes from where the
// last persisted cm left off. Both matter across a restart — the
// series because a change of it tells clients the channel's ordering
// restarted, and the monotonic state because a restart within the same
// millisecond would otherwise re-mint a serial already on the log.
func (s *Storage) Channel(_ context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs, ok := s.channels[name]; ok {
		return cs, nil
	}

	current, initial, err := s.loadOrMintInitial(name)
	if err != nil {
		return nil, err
	}
	gen, err := s.generatorFor(current)
	if err != nil {
		return nil, fmt.Errorf("storage/bbolt: channel %q: %w", name, err)
	}

	cs := &channelStore{
		db:       s.db,
		gen:      gen,
		name:     name,
		appender: appender,
	}
	s.channels[name] = cs

	if appender != nil {
		appender.Initialize(current, initial)
	}
	return cs, nil
}

// generatorFor returns the generator that continues the series current
// belongs to, seeded so its first Mint is strictly greater than it.
func (s *Storage) generatorFor(current string) (*serial.Generator, error) {
	ts, counter, seriesID, err := serial.SplitChannelSerial(current)
	if err != nil {
		return nil, err
	}
	gen := serial.NewGenerator(seriesID, s.now)
	gen.Restore(ts, counter)
	return gen, nil
}

// loadOrMintInitial returns the channel's (current, initial) serials.
// initial is loaded from the initials bucket if present; otherwise a
// fresh seed is minted and persisted. current is the latest persisted
// cm's serial within the channel's prefix, or initial if there are no
// persisted cms.
//
// The seed is where a brand-new channel's series comes from, and it is
// persisted alongside the initial serial that carries it, so every
// later Open of this file finds the same one.
func (s *Storage) loadOrMintInitial(name string) (current, initial string, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		initials := tx.Bucket(initialsBucket)
		if v := initials.Get([]byte(name)); v != nil {
			initial = string(v)
		} else {
			initial = serial.NewGenerator(serial.NewSeriesID(), s.now).Mint()
			if perr := initials.Put([]byte(name), []byte(initial)); perr != nil {
				return fmt.Errorf("persist initial for %q: %w", name, perr)
			}
		}
		// current: latest cm's channelSerial in the messages bucket, or
		// initial if no cms exist for this channel.
		messages := tx.Bucket(channelMessagesBucket)
		prefix := channelPrefix(name)
		c := messages.Cursor()
		// Seek to the lex successor of the channel's prefix range, then
		// step back to land on the channel's last key (if any).
		k, _ := c.Seek(nextPrefix(prefix))
		if k == nil {
			k, _ = c.Last()
		} else {
			k, _ = c.Prev()
		}
		if k != nil && bytes.HasPrefix(k, prefix) {
			current = string(k[len(prefix):])
		} else {
			current = initial
		}
		return nil
	})
	if err != nil {
		return "", "", fmt.Errorf("storage/bbolt: load initial for %q: %w", name, err)
	}
	return current, initial, nil
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

// versionKey returns the versions-bucket key for one version of a
// message: "<channel>\0<identity>\0<versionSerial>". The shared keySep
// (NUL) never appears in serials, so a prefix scan over
// versionPrefix(channel, identity) yields a message's versions in
// version order.
func versionKey(channel, identity, versionSerial string) []byte {
	b := make([]byte, 0, len(channel)+1+len(identity)+1+len(versionSerial))
	b = append(b, channel...)
	b = append(b, keySep)
	b = append(b, identity...)
	b = append(b, keySep)
	b = append(b, versionSerial...)
	return b
}

// versionPrefix returns "<channel>\0<identity>\0" — the lex bound for a
// message's version range scan.
func versionPrefix(channel, identity string) []byte {
	b := make([]byte, 0, len(channel)+1+len(identity)+1)
	b = append(b, channel...)
	b = append(b, keySep)
	b = append(b, identity...)
	b = append(b, keySep)
	return b
}

// putVersion records m as the current content of its message identity, and —
// unless m carries an append — as an entry in its version chain. It upserts the
// latest-version projection and appends to the versions index within tx. m must
// already carry its Serial (identity) and Version.
//
// An append is not a version of the message (DESIGN.md §13.3). It changes what
// the message currently says, which the projection records, and it stays on the
// log for live and resume fan-out — but a version chain of one entry per
// increment is not what a streamed append is.
func putVersion(tx *bolt.Tx, channel string, m *wire.Message) error {
	blob, err := storage.EncodeMessage(m)
	if err != nil {
		return err
	}
	if err := tx.Bucket(latestBucket).Put(channelKey(channel, m.Serial), blob); err != nil {
		return fmt.Errorf("storage/bbolt: put latest: %w", err)
	}
	if m.HasAppend() {
		return nil
	}
	if err := tx.Bucket(versionsBucket).Put(versionKey(channel, m.Serial, m.VersionOrSerial()), blob); err != nil {
		return fmt.Errorf("storage/bbolt: put version: %w", err)
	}
	return nil
}

// channelStore is the per-channel facet. Concurrency is controlled by
// bolt (one writer at a time per DB) and by the generator's internal
// mutex.
type channelStore struct {
	db       *bolt.DB
	gen      *serial.Generator
	name     string
	appender storage.Appender

	// members is the in-memory presence set, guarded by mu. Not
	// persisted — empty on Open (DESIGN.md §12.5). Lazily allocated.
	mu      sync.Mutex
	members map[string]*wire.PresenceMessage

	// occupancy is this process's contribution to the channel's occupancy
	// (DESIGN.md §16.2), nil when it serves no holders. Like the membership
	// set it is held in memory and not persisted: occupancy is what is
	// attached right now, and nothing is attached to a freshly-opened file.
	// This is the opposite case to the objects bucket, which is persisted
	// because an object outlives every connection that touched it (§15.3).
	occupancy *wire.ChannelOccupancy
}

func (cs *channelStore) Store(ctx context.Context, msgs []*wire.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/bbolt: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Resolve the batch id and stamp each Message.ID = "<batchID>:<idx>"
	// (DESIGN.md §8) before the write tx, so the ids bucket keys on the
	// batch-derived ids.
	batchID, err := storage.StampMessageIDs(msgs)
	if err != nil {
		return nil, false, err
	}

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err = cs.db.Update(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		ids := tx.Bucket(idsBucket)

		// Idempotency: any contained ID that's already indexed makes
		// this whole publish a duplicate.
		for _, m := range msgs {
			if m.GetId() == "" {
				continue
			}
			if existingCS := ids.Get(channelKey(cs.name, m.GetId())); existingCS != nil {
				blob := messages.Get(channelKey(cs.name, string(existingCS)))
				if blob == nil {
					return fmt.Errorf("storage/bbolt: id index points to missing ChannelMessage %q", existingCS)
				}
				original, err := storage.DecodeChannelMessage(blob)
				if err != nil {
					return err
				}
				resultCM = original
				idempotent = true
				return nil
			}
		}

		// Not a duplicate — mint a fresh channelSerial, stamp each
		// Message (serial, create action, create version), persist.
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
		blob, err := storage.EncodeChannelMessage(cm)
		if err != nil {
			return err
		}
		if err := messages.Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		for _, m := range msgs {
			if m.GetId() != "" {
				if err := ids.Put(channelKey(cs.name, m.GetId()), []byte(channelSerial)); err != nil {
					return err
				}
			}
			// Register the create as the first version + projection entry
			// (DESIGN.md §13.4).
			if err := putVersion(tx, cs.name, m); err != nil {
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

// StoreSummary logs the summaries of freshly-annotated messages so they reach
// subscribers, without recording them as versions (see storage.ChannelStore).
func (cs *channelStore) StoreSummary(ctx context.Context, summaries []*wire.Message) (*protocol.ChannelMessage, error) {
	if len(summaries) == 0 {
		return nil, errors.New("storage/bbolt: StoreSummary with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	var resultCM *protocol.ChannelMessage
	err := cs.db.Update(func(tx *bolt.Tx) error {
		channelSerial := cs.gen.Mint()
		cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: summaries}
		blob, err := storage.EncodeChannelMessage(cm)
		if err != nil {
			return err
		}
		if err := tx.Bucket(channelMessagesBucket).Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		resultCM = cm
		return nil
	})
	if err != nil {
		return nil, err
	}

	if cs.appender != nil {
		cs.appender.Append(resultCM)
	}
	return resultCM, nil
}

// Mutate applies an update/delete/append to an existing message
// (DESIGN.md §13.2): validate the target exists in the projection, merge,
// mint a fresh version cm carrying the complete merged Message, persist
// it on the log, and update the latest projection + versions index in the
// same bolt transaction. The appender fires after commit, like Store.
func (cs *channelStore) Mutate(ctx context.Context, mut *wire.Message, merge storage.MergeFunc) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.IsMutation() {
		return nil, false, errors.New("storage/bbolt: Mutate requires a mutation action")
	}
	if mut.Serial == "" {
		return nil, false, errors.New("storage/bbolt: Mutate requires a target serial")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err := cs.db.Update(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		ids := tx.Bucket(idsBucket)

		if mut.GetId() != "" {
			if existingCS := ids.Get(channelKey(cs.name, mut.GetId())); existingCS != nil {
				blob := messages.Get(channelKey(cs.name, string(existingCS)))
				if blob == nil {
					return fmt.Errorf("storage/bbolt: id index points to missing ChannelMessage %q", existingCS)
				}
				original, err := storage.DecodeChannelMessage(blob)
				if err != nil {
					return err
				}
				resultCM = original
				idempotent = true
				return nil
			}
		}

		curBlob := tx.Bucket(latestBucket).Get(channelKey(cs.name, mut.Serial))
		if curBlob == nil {
			return storage.ErrTargetNotFound
		}
		current, err := storage.DecodeMessage(curBlob)
		if err != nil {
			return err
		}

		channelSerial := cs.gen.Mint()
		version, err := merge(current, mut, serial.MessageSerial(channelSerial, 0))
		if err != nil {
			return err
		}
		cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: []*wire.Message{version}}
		blob, err := storage.EncodeChannelMessage(cm)
		if err != nil {
			return err
		}
		if err := messages.Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		if mut.GetId() != "" {
			if err := ids.Put(channelKey(cs.name, mut.GetId()), []byte(channelSerial)); err != nil {
				return err
			}
		}
		if err := putVersion(tx, cs.name, version); err != nil {
			return err
		}
		resultCM = cm
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if !idempotent && cs.appender != nil {
		cs.appender.Append(resultCM)
	}
	return resultCM, idempotent, nil
}

// LatestVersion returns the projection entry for serial, or
// ErrTargetNotFound (DESIGN.md §13.4).
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*wire.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out *wire.Message
	err := cs.db.View(func(tx *bolt.Tx) error {
		blob := tx.Bucket(latestBucket).Get(channelKey(cs.name, serial))
		if blob == nil {
			return storage.ErrTargetNotFound
		}
		m, err := storage.DecodeMessage(blob)
		if err != nil {
			return err
		}
		out = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Versions returns every version of serial ordered by version, paginated
// via the shared HistoryQuery shape (DESIGN.md §13.4). A prefix scan over
// the versions bucket yields the ascending-by-version list, which
// storage.PaginateVersions then slices.
func (cs *channelStore) Versions(ctx context.Context, serial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	var all []*wire.Message
	err := cs.db.View(func(tx *bolt.Tx) error {
		prefix := versionPrefix(cs.name, serial)
		c := tx.Bucket(versionsBucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			m, err := storage.DecodeMessage(v)
			if err != nil {
				return fmt.Errorf("storage/bbolt: version %q: %w", k, err)
			}
			all = append(all, m)
		}
		return nil
	})
	if err != nil {
		return storage.HistoryPage{}, err
	}
	if len(all) == 0 {
		return storage.HistoryPage{}, storage.ErrTargetNotFound
	}
	return storage.PaginateVersions(all, q), nil
}

// annotationKey returns the annotations-bucket key for one annotation of a
// message: "<channel>\0<target>\0<annotationSerial>". Shares the composite
// layout of versionKey; a prefix scan over annotationPrefix(channel,
// target) yields a message's annotations in stream order.
func annotationKey(channel, target, annotationSerial string) []byte {
	return versionKey(channel, target, annotationSerial)
}

// annotationPrefix returns "<channel>\0<target>\0" — the lex bound for a
// message's annotation range scan.
func annotationPrefix(channel, target string) []byte {
	return versionPrefix(channel, target)
}

// StoreAnnotation persists an annotation publish onto the channel_messages
// log (kind = annotation) and indexes each annotation under its target in
// the annotations bucket for annotations-for-message reads (DESIGN.md
// §14.1, §14.4). Every target must resolve in the latest-version
// projection (ErrTargetNotFound otherwise, like a mutation). Idempotency
// shares the ids bucket with messages/presence. The appender fires after
// commit, like Store. The returned cm is the annotation summary-fold seam.
func (cs *channelStore) StoreAnnotation(ctx context.Context, annotations []*wire.Annotation, fold storage.FoldFunc) (*protocol.ChannelMessage, bool, error) {
	if len(annotations) == 0 {
		return nil, false, errors.New("storage/bbolt: StoreAnnotation with no annotations")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err := cs.db.Update(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		ids := tx.Bucket(idsBucket)

		for _, a := range annotations {
			if a.GetId() == "" {
				continue
			}
			if existingCS := ids.Get(channelKey(cs.name, a.GetId())); existingCS != nil {
				blob := messages.Get(channelKey(cs.name, string(existingCS)))
				if blob == nil {
					return fmt.Errorf("storage/bbolt: id index points to missing ChannelMessage %q", existingCS)
				}
				original, err := storage.DecodeChannelMessage(blob)
				if err != nil {
					return err
				}
				resultCM = original
				idempotent = true
				return nil
			}
		}

		// Target existence: every annotation must reference a message that
		// resolves in the latest-version projection (DESIGN.md §14.1).
		latest := tx.Bucket(latestBucket)
		for _, a := range annotations {
			if latest.Get(channelKey(cs.name, a.MessageSerial)) == nil {
				return storage.ErrTargetNotFound
			}
		}

		channelSerial := cs.gen.Mint()
		for i, a := range annotations {
			a.Serial = storage.AnnotationSerial(channelSerial, i)
			// Fold the annotation into its target's summary projection (the
			// latest bucket), atomically within this write tx (DESIGN.md
			// §14.2).
			if err := foldSummaryTx(latest, cs.name, a, fold); err != nil {
				return err
			}
		}
		cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Annotations: annotations}
		blob, err := storage.EncodeChannelMessage(cm)
		if err != nil {
			return err
		}
		if err := messages.Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		annBucket := tx.Bucket(annotationsBucket)
		for _, a := range annotations {
			if a.GetId() != "" {
				if err := ids.Put(channelKey(cs.name, a.GetId()), []byte(channelSerial)); err != nil {
					return err
				}
			}
			aBlob, err := storage.EncodeAnnotation(a)
			if err != nil {
				return err
			}
			if err := annBucket.Put(annotationKey(cs.name, a.MessageSerial, a.Serial.ToTimeserialString()), aBlob); err != nil {
				return err
			}
		}
		resultCM = cm
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if !idempotent && cs.appender != nil {
		cs.appender.Append(resultCM)
	}
	return resultCM, idempotent, nil
}

// foldSummaryTx folds one annotation into its target message's summary on
// the latest-version projection (the latest bucket) (DESIGN.md §14.2). It runs
// inside the StoreAnnotation write tx with the target already validated to
// exist, so the summary persists on the projection payload and every later
// message read carries it.
func foldSummaryTx(latest *bolt.Bucket, channel string, a *wire.Annotation, fold storage.FoldFunc) error {
	blob := latest.Get(channelKey(channel, a.MessageSerial))
	if blob == nil {
		return nil
	}
	m, err := storage.DecodeMessage(blob)
	if err != nil {
		return err
	}
	fold(m, a)
	updated, err := storage.EncodeMessage(m)
	if err != nil {
		return err
	}
	return latest.Put(channelKey(channel, a.MessageSerial), updated)
}

// Annotations returns the annotations attached to messageSerial in stream
// order via a prefix scan over the annotations bucket, paginated by
// storage.PaginateAnnotations (DESIGN.md §14.4). An unknown target yields
// an empty page.
func (cs *channelStore) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	var all []*wire.Annotation
	err := cs.db.View(func(tx *bolt.Tx) error {
		prefix := annotationPrefix(cs.name, messageSerial)
		c := tx.Bucket(annotationsBucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			a, err := storage.DecodeAnnotation(v)
			if err != nil {
				return fmt.Errorf("storage/bbolt: annotation %q: %w", k, err)
			}
			all = append(all, a)
		}
		return nil
	})
	if err != nil {
		return storage.HistoryPage{}, err
	}
	return storage.PaginateAnnotations(all, q), nil
}

// StorePresence persists a presence publish onto the channel_messages
// log (so it appears in presence history) and folds it into the
// in-memory membership set. The membership set is process-lifetime, not
// persisted (DESIGN.md §12.5). Idempotency shares the ids bucket with
// messages. cs.mu is held across the persist + fold so a concurrent
// Members observes a consistent set; the appender fires after unlock.
func (cs *channelStore) StorePresence(ctx context.Context, presence []*wire.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/bbolt: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err := cs.db.Update(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		ids := tx.Bucket(idsBucket)

		for _, p := range presence {
			if p.GetId() == "" {
				continue
			}
			if existingCS := ids.Get(channelKey(cs.name, p.GetId())); existingCS != nil {
				blob := messages.Get(channelKey(cs.name, string(existingCS)))
				if blob == nil {
					return fmt.Errorf("storage/bbolt: id index points to missing ChannelMessage %q", existingCS)
				}
				original, err := storage.DecodeChannelMessage(blob)
				if err != nil {
					return err
				}
				resultCM = original
				idempotent = true
				return nil
			}
		}

		channelSerial := cs.gen.Mint()
		for i, p := range presence {
			storage.StampPresenceMember(p, channelSerial, i)
		}
		cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Presence: presence}
		blob, err := storage.EncodeChannelMessage(cm)
		if err != nil {
			return err
		}
		if err := messages.Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		for _, p := range presence {
			if p.GetId() == "" {
				continue
			}
			if err := ids.Put(channelKey(cs.name, p.GetId()), []byte(channelSerial)); err != nil {
				return err
			}
		}
		resultCM = cm
		return nil
	})
	if err != nil {
		cs.mu.Unlock()
		return nil, false, err
	}

	if !idempotent {
		if cs.members == nil {
			cs.members = make(map[string]*wire.PresenceMessage)
		}
		for _, p := range resultCM.Presence {
			key := storage.MemberKey(p.ConnectionId, p.GetClientId())
			switch p.Action {
			case wire.PresenceMessage_LEAVE, wire.PresenceMessage_ABSENT:
				delete(cs.members, key)
			default: // Enter, Update, Present
				cs.members[key] = p
			}
		}
	}
	cs.mu.Unlock()

	if !idempotent && cs.appender != nil {
		cs.appender.Append(resultCM)
	}
	return resultCM, idempotent, nil
}

// Members returns the in-memory membership set (sorted by Serial) plus
// the channel's current watermark — the last channelSerial persisted in
// the log, or empty if the channel has no cms.
func (cs *channelStore) Members(ctx context.Context, q storage.MembersQuery) (storage.MembersPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.MembersPage{}, err
	}

	cs.mu.Lock()
	out := make([]*wire.PresenceMessage, 0, len(cs.members))
	for _, p := range cs.members {
		out = append(out, p)
	}
	cs.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		return storage.MemberKey(out[i].ConnectionId, out[i].GetClientId()) <
			storage.MemberKey(out[j].ConnectionId, out[j].GetClientId())
	})

	var asOf string
	err := cs.db.View(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		prefix := channelPrefix(cs.name)
		c := messages.Cursor()
		k, _ := c.Seek(nextPrefix(prefix))
		if k == nil {
			k, _ = c.Last()
		} else {
			k, _ = c.Prev()
		}
		if k != nil && bytes.HasPrefix(k, prefix) {
			asOf = string(k[len(prefix):])
		}
		return nil
	})
	if err != nil {
		return storage.MembersPage{}, fmt.Errorf("storage/bbolt: Members watermark: %w", err)
	}

	out, next := storage.PageMembers(out, q)
	return storage.MembersPage{Members: out, AsOfSerial: asOf, NextCursor: next}, nil
}

// StoreState persists a LiveObjects publish onto the channel_messages log and
// applies its operations to the objects bucket, both inside one write tx
// (DESIGN.md §15.2, §15.3). Idempotency shares the ids bucket with messages
// and presence.
//
// The apply runs inside the tx, between loading the objects the publish names
// and writing back the ones it changed: bolt allows one writer at a time, so
// nothing can have changed them in between. cs.mu is held across the tx for
// the same reason StorePresence holds it — a state publish and a presence
// publish on the same channel must not interleave their serial mints.
func (cs *channelStore) StoreState(ctx context.Context, state []*wire.StateMessage, apply storage.ApplyFunc) (*protocol.ChannelMessage, bool, error) {
	if len(state) == 0 {
		return nil, false, errors.New("storage/bbolt: StoreState with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	var (
		resultCM   *protocol.ChannelMessage
		idempotent bool
	)
	err := cs.db.Update(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		ids := tx.Bucket(idsBucket)
		objects := tx.Bucket(objectsBucket)

		for _, sm := range state {
			if sm.GetId() == "" {
				continue
			}
			if existingCS := ids.Get(channelKey(cs.name, sm.GetId())); existingCS != nil {
				blob := messages.Get(channelKey(cs.name, string(existingCS)))
				if blob == nil {
					return fmt.Errorf("storage/bbolt: id index points to missing ChannelMessage %q", existingCS)
				}
				original, err := storage.DecodeChannelMessage(blob)
				if err != nil {
					return err
				}
				resultCM = original
				idempotent = true
				return nil
			}
		}

		channelSerial := cs.gen.Mint()
		for i, sm := range state {
			storage.StampStateMessage(sm, channelSerial, i)
		}

		named := make([]*wire.StateObject, 0, len(state))
		for _, id := range storage.StateObjectIDs(state) {
			blob := objects.Get(channelKey(cs.name, id))
			if blob == nil {
				continue
			}
			obj, err := storage.DecodeStateObject(blob)
			if err != nil {
				return err
			}
			named = append(named, obj)
		}
		changed, err := apply(named, state)
		if err != nil {
			return err
		}

		cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, State: state}
		blob, err := storage.EncodeChannelMessage(cm)
		if err != nil {
			return err
		}
		if err := messages.Put(channelKey(cs.name, channelSerial), blob); err != nil {
			return err
		}
		for _, sm := range state {
			if sm.GetId() == "" {
				continue
			}
			if err := ids.Put(channelKey(cs.name, sm.GetId()), []byte(channelSerial)); err != nil {
				return err
			}
		}
		for _, obj := range changed {
			objBlob, err := storage.EncodeStateObject(obj)
			if err != nil {
				return err
			}
			if err := objects.Put(channelKey(cs.name, obj.GetObjectId()), objBlob); err != nil {
				return err
			}
		}
		resultCM = cm
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if !idempotent && cs.appender != nil {
		cs.appender.Append(resultCM)
	}
	return resultCM, idempotent, nil
}

// Objects pages the materialised object set with a prefix scan over the
// objects bucket, which bolt walks in key order — so the page comes out
// ordered by object id, and the cursor is the id to Seek to next.
func (cs *channelStore) Objects(ctx context.Context, q storage.ObjectsQuery) (storage.ObjectsPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ObjectsPage{}, err
	}

	var page storage.ObjectsPage
	err := cs.db.View(func(tx *bolt.Tx) error {
		prefix := channelPrefix(cs.name)
		c := tx.Bucket(objectsBucket).Cursor()

		seek := prefix
		if q.After != "" {
			seek = channelKey(cs.name, q.After)
		}
		for k, v := c.Seek(seek); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			id := string(k[len(prefix):])
			if q.After != "" && id <= q.After {
				continue
			}
			if q.Limit > 0 && len(page.Objects) == q.Limit {
				// One past the page: where the next one starts.
				page.NextCursor = page.Objects[len(page.Objects)-1].GetObjectId()
				break
			}
			obj, err := storage.DecodeStateObject(v)
			if err != nil {
				return err
			}
			page.Objects = append(page.Objects, obj)
		}

		messages := tx.Bucket(channelMessagesBucket)
		mc := messages.Cursor()
		k, _ := mc.Seek(nextPrefix(prefix))
		if k == nil {
			k, _ = mc.Last()
		} else {
			k, _ = mc.Prev()
		}
		if k != nil && bytes.HasPrefix(k, prefix) {
			page.AsOfSerial = string(k[len(prefix):])
		}
		return nil
	})
	if err != nil {
		return storage.ObjectsPage{}, fmt.Errorf("storage/bbolt: Objects: %w", err)
	}
	return page, nil
}

// StoreOccupancy records this process's contribution and signals the appender.
// Like the membership set it stays in memory, so there is no write tx: bolt is
// one process, and the contribution is therefore also the aggregate.
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

	wantKind := q.Kind.Normalize()
	if q.Collapse && wantKind == storage.KindMessage {
		return cs.collapsedHistory(q)
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
	if q.AfterChannelSerial != "" {
		// Strict lower bound on channelSerial — the lex successor of the
		// channel key for AfterChannelSerial (append NUL) skips that
		// serial's own rows.
		afterKey := append(channelKey(cs.name, q.AfterChannelSerial), 0)
		if bytes.Compare(afterKey, lowerKey) > 0 {
			lowerKey = afterKey
		}
	}
	upperKey := nextPrefix(prefix)
	if timeUpper != "" {
		upperKey = channelKey(cs.name, timeUpper)
	}
	if q.EndChannelSerial != "" {
		// Inclusive upper bound on channelSerial — convert to an
		// exclusive byte-key upper by appending a NUL byte, the lex
		// successor of the channel-key prefix for EndChannelSerial.
		endKey := append(channelKey(cs.name, q.EndChannelSerial), 0)
		if bytes.Compare(endKey, upperKey) < 0 {
			upperKey = endKey
		}
	}

	var page storage.HistoryPage
	count := 0

	// emit appends one item (Message or PresenceMessage, via put) onto
	// the trailing ChannelMessage when its channelSerial matches, or
	// starts a fresh entry otherwise. Returns false once Limit is hit.
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

	err := cs.db.View(func(tx *bolt.Tx) error {
		messages := tx.Bucket(channelMessagesBucket)
		if messages == nil {
			return nil
		}
		c := messages.Cursor()

		decode := func(k, v []byte) (*protocol.ChannelMessage, error) {
			cm, err := storage.DecodeChannelMessage(v)
			if err != nil {
				return nil, fmt.Errorf("storage/bbolt: ChannelMessage %q: %w", k, err)
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
				for _, it := range storage.CMItems(cm, wantKind) {
					if cursor != "" && it.Serial <= cursor {
						continue
					}
					if !emit(cm.ChannelSerial, it.Serial, it.Append) {
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
			items := storage.CMItems(cm, wantKind)
			for idx := len(items) - 1; idx >= 0; idx-- {
				if cursor != "" && items[idx].Serial >= cursor {
					continue
				}
				if !emit(cm.ChannelSerial, items[idx].Serial, items[idx].Append) {
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

// collapsedHistory returns the latest version of each message positioned
// at its create serial (DESIGN.md §13.4), backing the default REST
// message history. It range-scans the latest projection bucket (keyed by
// identity, so naturally ordered by create position), applies the time
// bounds / cursor / limit against the identity, and regroups each entry
// under its create channelSerial — reversing within a batch for the
// backwards direction, matching the raw scan. q.EndChannelSerial (the
// fromSerial/untilAttached bound) caps entries to their CREATE
// channelSerial <= the bound (storage.CreateChannelSerial(identity)),
// not a raw compare of identity itself, since identity carries a
// ":idx" suffix the bound doesn't have.
func (cs *channelStore) collapsedHistory(q storage.HistoryQuery) (storage.HistoryPage, error) {
	prefix := channelPrefix(cs.name)
	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
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
		if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == ccs {
			page.ChannelMessages[n-1].Messages = append(page.ChannelMessages[n-1].Messages, m)
		} else {
			page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
				ChannelSerial: ccs,
				Messages:      []*wire.Message{m},
			})
		}
		page.LastSerial = m.Serial
		count++
		return true
	}

	err := cs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(latestBucket).Cursor()
		// identity == "<channel>\0<createSerial>:<idx>"; time bounds and
		// cursor compare against the identity (sans the channel prefix).
		decodeAt := func(k, v []byte) (string, *wire.Message, error) {
			identity := string(k[len(prefix):])
			m, err := storage.DecodeMessage(v)
			if err != nil {
				return "", nil, fmt.Errorf("storage/bbolt: projection %q: %w", k, err)
			}
			return identity, m, nil
		}
		inBounds := func(identity string) bool {
			if timeLower != "" && identity < timeLower {
				return false
			}
			if timeUpper != "" && identity >= timeUpper {
				return false
			}
			if q.EndChannelSerial != "" && storage.CreateChannelSerial(identity) > q.EndChannelSerial {
				return false
			}
			return true
		}

		if forwards {
			for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
				identity, m, err := decodeAt(k, v)
				if err != nil {
					return err
				}
				if !inBounds(identity) || (cursor != "" && identity <= cursor) {
					continue
				}
				if !emit(m) {
					return nil
				}
			}
			return nil
		}

		// Backwards: position past the channel's range, step back.
		k, v := c.Seek(nextPrefix(prefix))
		if k == nil {
			k, v = c.Last()
		} else {
			k, v = c.Prev()
		}
		for ; k != nil && bytes.HasPrefix(k, prefix); k, v = c.Prev() {
			identity, m, err := decodeAt(k, v)
			if err != nil {
				return err
			}
			if !inBounds(identity) || (cursor != "" && identity >= cursor) {
				continue
			}
			if !emit(m) {
				return nil
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
