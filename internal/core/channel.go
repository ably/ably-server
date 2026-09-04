// Package core implements the per-channel state and per-process
// channel manager — DESIGN.md §5.1.
package core

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/wire"
)

// entry is a node in a Channel's linked list of ChannelMessages. Each
// entry is one atomic publish carrying one or more Messages. Streams
// tail the list at their own pace; entry.notify is closed once
// entry.next has been set, which wakes all parked streams. The list is
// grow-only — older entries become eligible for GC once no stream
// retains a reference.
//
// The sentinel head (the entry installed at construction, before any
// Append) carries a serial-only ChannelMessage once Initialize has
// run: the channel's initial watermark. That ChannelMessage has no
// Messages, so Stream.Next never returns it as a delivered cm — it
// is only inspected via Stream.ChannelSerial.
type entry struct {
	cm     *protocol.ChannelMessage
	notify chan struct{}
	next   *entry
}

// Channel holds the live ChannelMessage list for one channel name and
// the storage facet that backs it. It owns no goroutine; concurrency
// is serialised by mu around Append.
//
// Publish() is the orchestration entry point: it calls Store on the
// underlying storage and lets the storage backend drive the local
// Append via the Appender callback we registered at construction
// time. The same Append path is used for foreign publishes arriving
// via the Postgres broker in cluster mode (DESIGN.md §7).
//
// A Channel is created in a not-ready state; storage.Storage.Channel
// calls Initialize on it before returning to seed the sentinel's
// watermark serial, record the channel's immutable initial serial,
// and close ready. Attach blocks on ready, so a caller cannot observe
// an empty channelSerial.
type Channel struct {
	name          string
	store         storage.ChannelStore
	ready         chan struct{}
	initialSerial string // immutable after Initialize; sorts strictly less than every cm in this channel

	mu   sync.Mutex
	tail *entry // never nil: a sentinel is installed at construction

	// occupancy fires when storage says the channel's aggregate occupancy
	// moved; occupancyGen is what it carries, a counter whose only job is to
	// differ from the value before it so that watchers are woken.
	occupancy    *live.Value[uint64]
	occupancyGen atomic.Uint64
}

// newChannel constructs a Channel in the not-ready state. The list
// starts with a sentinel entry (cm = nil) that Initialize will then
// populate with the watermark serial.
func newChannel(name string) *Channel {
	return &Channel{
		name:      name,
		ready:     make(chan struct{}),
		tail:      &entry{notify: make(chan struct{})},
		occupancy: live.NewValue(uint64(0)),
	}
}

// Name returns the channel name.
func (c *Channel) Name() string {
	return c.name
}

// Publish runs the full publish-and-link sequence: hand msgs to the
// underlying storage backend, which mints the channelSerial, persists,
// and (in cluster mode) emits a NOTIFY. The link onto the live list
// always arrives via the Appender callback the Channel registered at
// construction — synchronously in single-process backends,
// asynchronously via the LISTEN goroutine in cluster mode. The
// (cm, idempotent, err) tuple is forwarded verbatim from storage.
func (c *Channel) Publish(ctx context.Context, msgs []*wire.Message) (*protocol.ChannelMessage, bool, error) {
	return c.store.Store(ctx, msgs)
}

// PublishPresence runs the presence-publish sequence: hand the presence
// messages to the storage backend (which mints the channelSerial, stamps
// each Serial, folds the membership set, and persists), then the link
// onto the live list arrives via the Appender callback exactly as for a
// message publish (DESIGN.md §12.2). The (cm, idempotent, err) tuple is
// forwarded verbatim from storage.
func (c *Channel) PublishPresence(ctx context.Context, presence []*wire.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	return c.store.StorePresence(ctx, presence)
}

// PublishAnnotation runs the annotation-publish sequence: hand the
// annotations to the storage backend (which validates each target,
// mints the channelSerial, stamps each Serial, persists the annotation cm
// on the shared stream, and — the seam for the summary fold),
// then the link onto the live list arrives via the Appender callback
// exactly as for a message publish (DESIGN.md §14.1). The
// (cm, idempotent, err) tuple is forwarded verbatim — notably
// storage.ErrTargetNotFound when a target message does not exist.
func (c *Channel) PublishAnnotation(ctx context.Context, annotations []*wire.Annotation, fold storage.FoldFunc) (*protocol.ChannelMessage, bool, error) {
	return c.store.StoreAnnotation(ctx, annotations, fold)
}

// PublishSummaries publishes the summaries of messages whose annotations have
// just been folded, so subscribers are told what a message now says about them
// (DESIGN.md §14.2). It is a publish like any other: logged, given a serial,
// and delivered through the Appender.
func (c *Channel) PublishSummaries(ctx context.Context, summaries []*wire.Message) (*protocol.ChannelMessage, error) {
	return c.store.StoreSummary(ctx, summaries)
}

// Annotations returns the annotations attached to the message identified
// by messageSerial, paginated per q (DESIGN.md §14.4).
func (c *Channel) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	return c.store.Annotations(ctx, messageSerial, q)
}

// Mutate applies an update/delete/append to an existing message,
// delegating to the storage backend (which validates the target, merges,
// mints the new version, persists, and updates the projection/versions
// index). The link onto the live list arrives via the Appender callback
// exactly as for a publish, so subscribers see the new version in stream
// order (DESIGN.md §13.2). The (cm, idempotent, err) tuple is forwarded
// verbatim — notably storage.ErrTargetNotFound for an unknown target.
func (c *Channel) Mutate(ctx context.Context, mut *wire.Message, merge storage.MergeFunc) (*protocol.ChannelMessage, bool, error) {
	return c.store.Mutate(ctx, mut, merge)
}

// LatestVersion returns the current latest version of the message
// identified by serial, or storage.ErrTargetNotFound (DESIGN.md §13.4).
func (c *Channel) LatestVersion(ctx context.Context, serial string) (*wire.Message, error) {
	return c.store.LatestVersion(ctx, serial)
}

// Versions returns every version of the message identified by serial,
// paginated per q (DESIGN.md §13.4).
func (c *Channel) Versions(ctx context.Context, serial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	return c.store.Versions(ctx, serial, q)
}

// History delegates to the underlying ChannelStore. Backends return
// ChannelMessages in the order requested by q.Direction (see
// storage.HistoryQuery); the REST and resume paths flatten the page
// without further reordering.
func (c *Channel) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	return c.store.History(ctx, q)
}

// Members returns the channel's current presence set plus the
// channelSerial the set is current as-of, delegating to the storage
// backend. Backs presence sync on attach (DESIGN.md §12.4).
func (c *Channel) Members(ctx context.Context, q storage.MembersQuery) (storage.MembersPage, error) {
	return c.store.Members(ctx, q)
}

// PublishState runs the LiveObjects publish sequence: hand the state
// messages to the storage backend (which mints the channelSerial, stamps
// each Serial, applies the operations to the channel's materialised
// object set via apply, and persists both), then the link onto the live
// list arrives via the Appender callback exactly as for a message
// publish (DESIGN.md §15.2). The (cm, idempotent, err) tuple is
// forwarded verbatim.
func (c *Channel) PublishState(ctx context.Context, state []*wire.StateMessage, apply storage.ApplyFunc) (*protocol.ChannelMessage, bool, error) {
	return c.store.StoreState(ctx, state, apply)
}

// Objects returns one page of the channel's materialised LiveObjects set
// plus the channelSerial the set is current as-of, delegating to the
// storage backend. Backs the state sync a client is served on attach
// (DESIGN.md §15.3).
func (c *Channel) Objects(ctx context.Context, q storage.ObjectsQuery) (storage.ObjectsPage, error) {
	return c.store.Objects(ctx, q)
}

// StoreOccupancy records what this node currently contributes to the
// channel's occupancy, delegating to the storage backend, which then
// signals every node holding the channel that the aggregate moved
// (DESIGN.md §16.2).
func (c *Channel) StoreOccupancy(ctx context.Context, counts *wire.ChannelOccupancy) error {
	return c.store.StoreOccupancy(ctx, counts)
}

// Occupancy returns the channel's occupancy across every node
// contributing to it (DESIGN.md §16.2).
func (c *Channel) Occupancy(ctx context.Context) (*wire.ChannelOccupancy, error) {
	return c.store.Occupancy(ctx)
}

// OccupancyUpdates fires each time storage reports that the channel's
// aggregate occupancy may have moved, so whoever is reporting occupancy
// re-reads it. The value is a generation counter and means nothing on its
// own — what a watcher acts on is that it changed (DESIGN.md §16.3).
//
// It is a watched value rather than a callback because several holders
// watch it independently and none of them own the channel.
func (c *Channel) OccupancyUpdates() *live.Value[uint64] {
	return c.occupancy
}

// OccupancyChanged is how storage delivers that signal. Implements
// storage.Appender.
func (c *Channel) OccupancyChanged() {
	c.occupancy.Set(c.occupancyGen.Add(1))
}

// Initialize seeds the sentinel with the channel's current watermark
// serial, records the channel's immutable initial serial, and marks
// the channel ready. The storage backend calls this exactly once
// before any Append. Subsequent calls are no-ops.
//
// current is the cursor fresh attachments use as their attach point
// (== latest persisted cm's serial, or the freshly-minted seed for an
// empty channel). initial is the channel's immutable seed — strictly
// less than every cm ever persisted on this channel — used as the
// attach point for rewinds that cover the entire channel history.
//
// Implements storage.Appender.
func (c *Channel) Initialize(current, initial string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
		// Double-init: the contract says backends call this once. Honour
		// idempotency defensively so a re-Channel call doesn't panic.
		return
	default:
	}
	c.tail.cm = &protocol.ChannelMessage{ChannelSerial: current}
	c.initialSerial = initial
	close(c.ready)
}

// InitialChannelSerial returns the channel's immutable initial serial,
// recorded at Initialize time. Used as the ATTACHED.channelSerial for
// rewind ATTACHes that cover the entire channel history (no
// predecessor cm exists in storage to use instead).
//
// Safe to call only after the channel is ready (Attach has unblocked).
func (c *Channel) InitialChannelSerial() string {
	return c.initialSerial
}

// Append links an already-minted ChannelMessage at the tail as a
// single entry, waking any parked streams. It satisfies the
// storage.Appender interface — the storage backend calls this to
// deliver a persisted cm to subscribers (the publisher's own publish
// in memory/bbolt; every node's publish in cluster mode).
//
// A no-op when cm is nil or carries no items — a presence cm, an
// annotation cm or a state cm (DESIGN.md §12.2, §14.1, §15.2) links onto
// the list exactly like a message cm.
func (c *Channel) Append(cm *protocol.ChannelMessage) {
	if cm == nil || (len(cm.Messages) == 0 && len(cm.Presence) == 0 && len(cm.Annotations) == 0 && len(cm.State) == 0) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	e := &entry{cm: cm, notify: make(chan struct{})}
	c.tail.next = e
	close(c.tail.notify)
	c.tail = e
}

// Attach blocks until the channel is ready (Initialize has run), then
// returns a Stream positioned at the current tail. The Stream's first
// Next call blocks until the next ChannelMessage is appended.
//
// Returns ctx.Err() if the context cancels before the channel readies.
func (c *Channel) Attach(ctx context.Context) (*Stream, error) {
	select {
	case <-c.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return &Stream{cursor: c.tail}, nil
}

// Stream is an attachment's per-channel view of the linked list. Its
// methods are not safe for concurrent use; an attachment is expected
// to drive a single Stream from one goroutine.
type Stream struct {
	cursor *entry
}

// ChannelSerial returns the cursor's current position — the
// channelSerial at the cursor (the sentinel's watermark before any
// cm has been delivered, or the last delivered cm's serial after).
// Never empty for a Stream returned by a ready Channel.
func (s *Stream) ChannelSerial() string {
	return s.cursor.cm.ChannelSerial
}

// Next blocks until the next ChannelMessage is available, advances
// the cursor to that entry, and returns the ChannelMessage. Returns
// ctx.Err() if the context is cancelled.
func (s *Stream) Next(ctx context.Context) (*protocol.ChannelMessage, error) {
	select {
	case <-s.cursor.notify:
		s.cursor = s.cursor.next
		return s.cursor.cm, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Ready is closed once a further ChannelMessage has been linked after the
// cursor, so a caller that cannot block can wait on it instead. Advance is
// then Next without the wait: it moves the cursor if there is something to
// move to and reports whether it did.
//
// Together they are the same traversal Next performs, split so that a caller
// driving several streams from one goroutine can select across them.
func (s *Stream) Ready() <-chan struct{} { return s.cursor.notify }

func (s *Stream) Advance() bool {
	select {
	case <-s.cursor.notify:
		s.cursor = s.cursor.next
		return true
	default:
		return false
	}
}

// Message is the ChannelMessage at the cursor.
func (s *Stream) Message() *protocol.ChannelMessage { return s.cursor.cm }
