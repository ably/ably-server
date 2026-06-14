// Package core implements the per-channel state and per-process
// channel manager — DESIGN.md §5.1.
package core

import (
	"context"
	"sync"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
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
}

// newChannel constructs a Channel in the not-ready state. The list
// starts with a sentinel entry (cm = nil) that Initialize will then
// populate with the watermark serial.
func newChannel(name string) *Channel {
	return &Channel{
		name:  name,
		ready: make(chan struct{}),
		tail:  &entry{notify: make(chan struct{})},
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
func (c *Channel) Publish(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	return c.store.Store(ctx, msgs)
}

// PublishPresence runs the presence-publish sequence: hand the presence
// messages to the storage backend (which mints the channelSerial, stamps
// each Serial, folds the membership set, and persists), then the link
// onto the live list arrives via the Appender callback exactly as for a
// message publish (DESIGN.md §12.2). The (cm, idempotent, err) tuple is
// forwarded verbatim from storage.
func (c *Channel) PublishPresence(ctx context.Context, presence []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	return c.store.StorePresence(ctx, presence)
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
func (c *Channel) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	return c.store.Members(ctx)
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
// A no-op when cm is nil or carries neither Messages nor Presence — a
// presence cm (Presence populated, Messages empty) links onto the list
// exactly like a message cm (DESIGN.md §12.2).
func (c *Channel) Append(cm *protocol.ChannelMessage) {
	if cm == nil || (len(cm.Messages) == 0 && len(cm.Presence) == 0) {
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
