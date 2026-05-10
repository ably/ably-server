// Package core implements the per-channel state and per-process
// channel manager — DESIGN.md §5.1.
package core

import (
	"context"
	"strconv"
	"sync"

	"github.com/ably/ably-server/internal/protocol"
)

// entry is a node in a Channel's linked list of published messages.
// Streams tail the list at their own pace; entry.notify is closed once
// entry.next has been set, which wakes all parked streams. The list is
// grow-only — older entries become eligible for GC once no stream
// retains a reference.
type entry struct {
	msg    *protocol.Message
	serial int64
	notify chan struct{}
	next   *entry
}

// Channel holds the live message list for one channel name. It owns
// no goroutine; concurrency is serialised by mu around append.
type Channel struct {
	name string

	mu         sync.Mutex
	tail       *entry // never nil: a sentinel is installed at construction
	nextSerial int64
}

// newChannel constructs a Channel. The list starts with a sentinel
// entry (serial 0, no msg) so Attach is safe before any Append.
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

// Append publishes msg to the channel. All streams parked on the
// previous tail's notify are woken; a fresh attachment from this point
// on parks on the new tail instead.
func (c *Channel) Append(msg *protocol.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextSerial++
	e := &entry{
		msg:    msg,
		serial: c.nextSerial,
		notify: make(chan struct{}),
	}
	c.tail.next = e
	close(c.tail.notify)
	c.tail = e
}

// Attach returns a Stream positioned at the current tail. The Stream's
// first Next call blocks until the next message is appended.
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

// ChannelSerial returns the cursor's current position in wire form —
// the serial of the last delivered message (or the attach point if
// none has been delivered yet).
func (s *Stream) ChannelSerial() string {
	return strconv.FormatInt(s.cursor.serial, 10)
}

// Next blocks until the next message is available, advances the cursor
// to that entry, and returns the message. Returns ctx.Err() if the
// context is cancelled.
func (s *Stream) Next(ctx context.Context) (*protocol.Message, error) {
	select {
	case <-s.cursor.notify:
		s.cursor = s.cursor.next
		return s.cursor.msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
