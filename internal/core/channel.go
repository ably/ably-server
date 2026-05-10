// Package core implements the per-channel state and per-process
// channel manager — DESIGN.md §5.1.
package core

import (
	"sync"

	"github.com/ably/ably-server/internal/protocol"
)

// entry is a node in a Channel's linked list of published messages.
// Attachments tail the list at their own pace; entry.notify is closed
// once entry.next has been set, which wakes all parked attachments.
// The list is grow-only — older entries become eligible for GC once
// no attachment retains a reference.
type entry struct {
	msg    *protocol.Message
	notify chan struct{}
	next   *entry
}

// Channel holds the live message list for one channel name. It owns
// no goroutine; concurrency is serialised by mu around append.
type Channel struct {
	name string

	mu   sync.Mutex
	tail *entry // never nil: a sentinel is installed at construction
}

// newChannel constructs a Channel. The list starts with a sentinel
// entry (no msg) so Tail is safe to call before any Append.
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

// Append publishes msg to the channel. All attachments parked on the
// previous tail's notify are woken; a fresh attachment that calls Tail
// from this point on parks on the new tail instead.
func (c *Channel) Append(msg *protocol.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &entry{
		msg:    msg,
		notify: make(chan struct{}),
	}
	c.tail.next = e
	close(c.tail.notify)
	c.tail = e
}

// Tail returns the current tail entry. Attachments use this as their
// starting cursor: the first iteration of the read loop parks on
// tail.notify, which wakes when the next message is appended.
func (c *Channel) Tail() *entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tail
}
