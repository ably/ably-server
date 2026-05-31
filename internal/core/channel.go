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
type entry struct {
	cm     *protocol.ChannelMessage
	notify chan struct{}
	next   *entry
}

// Channel holds the live ChannelMessage list for one channel name. It
// owns no goroutine; concurrency is serialised by mu around append.
// Serial minting and persistence live in the underlying storage —
// Channel itself only links already-minted ChannelMessages into the
// live list so attachments can tail them.
type Channel struct {
	name  string
	store storage.ChannelStore

	mu   sync.Mutex
	tail *entry // never nil: a sentinel is installed at construction
}

// newChannel constructs a Channel backed by store. The list starts
// with a sentinel entry (no ChannelMessage) so Attach is safe before
// any Append.
func newChannel(name string, store storage.ChannelStore) *Channel {
	return &Channel{
		name:  name,
		store: store,
		tail:  &entry{notify: make(chan struct{})},
	}
}

// Name returns the channel name.
func (c *Channel) Name() string {
	return c.name
}

// Append links an already-minted ChannelMessage onto the tail as a
// single entry, waking any parked streams. It performs no minting and
// no stamping of contained Message.Serials — both are storage's
// responsibility upstream.
//
// Used directly by the cluster broker's NOTIFY listener once it has
// fetched the canonical row; intra-process publishes go through
// AppendChannelMessage which calls Append after the storage write.
//
// A no-op when cm is nil or carries no Messages.
func (c *Channel) Append(cm *protocol.ChannelMessage) {
	if cm == nil || len(cm.Messages) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	e := &entry{cm: cm, notify: make(chan struct{})}
	c.tail.next = e
	close(c.tail.notify)
	c.tail = e
}

// AppendChannelMessage performs one atomic publish: it hands msgs to
// the underlying storage backend (which mints the channelSerial,
// stamps each Message.Serial, and persists the resulting cm) and on a
// non-idempotent return links the cm into the live list before
// returning. The (cm, idempotent, err) tuple is forwarded from
// storage; idempotent=true means a prior publish carrying one of the
// msgs' IDs was matched and nothing new was linked.
func (c *Channel) AppendChannelMessage(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	cm, idempotent, err := c.store.AppendChannelMessage(ctx, msgs)
	if err != nil {
		return nil, false, err
	}
	if !idempotent {
		c.Append(cm)
	}
	return cm, idempotent, nil
}

// Attach returns a Stream positioned at the current tail. The Stream's
// first Next call blocks until the next ChannelMessage is appended.
func (c *Channel) Attach() *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &Stream{cursor: c.tail}
}

// Stream is an attachment's per-channel view of the linked list. Its
// methods are not safe for concurrent use; an attachment is expected
// to drive a single Stream from one goroutine.
type Stream struct {
	cursor *entry
}

// ChannelSerial returns the cursor's current position — the
// channelSerial of the last delivered ChannelMessage, or an empty
// string if the stream is parked at the sentinel (no ChannelMessage
// delivered yet).
func (s *Stream) ChannelSerial() string {
	if s.cursor.cm == nil {
		return ""
	}
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
