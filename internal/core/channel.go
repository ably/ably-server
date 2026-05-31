// Package core implements the per-channel state and per-process
// channel manager — DESIGN.md §5.1.
package core

import (
	"context"
	"sync"

	"github.com/ably/ably-server/internal/protocol"
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
// owns no goroutine; concurrency is serialised by mu around Append.
//
// Channel is intentionally storage-agnostic. Callers (the realtime
// connection's publish path, the REST publish handler, and the cluster
// broker's NOTIFY listener) ask the storage backend for a minted
// ChannelMessage and then pass it to Append to link it onto the live
// list.
type Channel struct {
	name string

	mu   sync.Mutex
	tail *entry // never nil: a sentinel is installed at construction
}

// newChannel constructs a Channel. The list starts with a sentinel
// entry (no ChannelMessage) so Attach is safe before any Append.
func newChannel(name string) *Channel {
	return &Channel{
		name: name,
		tail: &entry{notify: make(chan struct{})},
	}
}

// Name returns the channel name.
func (c *Channel) Name() string {
	return c.name
}

// Append links an already-minted ChannelMessage at the tail as a
// single entry, waking any parked streams. Storage upstream is
// responsible for minting the ChannelSerial and stamping each
// contained Message.Serial; Append performs neither.
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
