package core

import (
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// isClosed reports whether ch is closed (non-blocking).
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestChannelAppendBuildsList(t *testing.T) {
	c := newChannel("test")
	head := c.Tail() // sentinel; notify open, next nil

	msgs := []*protocol.Message{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}}
	for _, m := range msgs {
		c.Append(m)
	}

	// Walk forward from the sentinel; verify each msg in order.
	e := head
	for i, want := range msgs {
		if !isClosed(e.notify) {
			t.Fatalf("entry %d: notify not closed", i)
		}
		if e.next == nil {
			t.Fatalf("entry %d: next is nil", i)
		}
		e = e.next
		if e.msg.ID != want.ID {
			t.Fatalf("entry %d: msg.ID = %q, want %q", i, e.msg.ID, want.ID)
		}
	}

	// The final entry's notify is still open — no successor yet.
	if isClosed(e.notify) {
		t.Fatal("final entry's notify is closed; expected open until next append")
	}
}

func TestChannelTailIsLatestOpenEntry(t *testing.T) {
	c := newChannel("test")
	c.Append(&protocol.Message{ID: "m1"})
	c.Append(&protocol.Message{ID: "m2"})

	tail := c.Tail()
	if isClosed(tail.notify) {
		t.Fatal("tail.notify is closed; expected open")
	}
	if tail.msg.ID != "m2" {
		t.Fatalf("tail.msg.ID = %q, want %q", tail.msg.ID, "m2")
	}
}

func TestChannelNotifyWakesWaiter(t *testing.T) {
	c := newChannel("test")
	head := c.Tail()

	got := make(chan *entry, 1)
	go func() {
		<-head.notify
		got <- head.next
	}()

	c.Append(&protocol.Message{ID: "m1"})

	select {
	case e := <-got:
		if e == nil {
			t.Fatal("waiter woke with nil next")
		}
		if e.msg.ID != "m1" {
			t.Fatalf("waiter saw msg.ID = %q, want %q", e.msg.ID, "m1")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not wake within 1s")
	}
}

func TestChannelNotifyWakesAllWaiters(t *testing.T) {
	c := newChannel("test")
	head := c.Tail()

	const waiters = 5
	woken := make(chan struct{}, waiters)
	for range waiters {
		go func() {
			<-head.notify
			woken <- struct{}{}
		}()
	}

	c.Append(&protocol.Message{ID: "m1"})

	deadline := time.After(time.Second)
	for i := range waiters {
		select {
		case <-woken:
		case <-deadline:
			t.Fatalf("only %d/%d waiters woke", i, waiters)
		}
	}
}

func TestChannelAppendIsConcurrentSafe(t *testing.T) {
	c := newChannel("test")
	head := c.Tail()

	const writers = 10
	const perWriter = 100

	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			for range perWriter {
				c.Append(&protocol.Message{ID: "x"})
			}
		}()
	}
	wg.Wait()

	// Walk the list from the sentinel; every Append must be reachable.
	count := 0
	for e := head; e.next != nil; e = e.next {
		count++
	}
	if want := writers * perWriter; count != want {
		t.Fatalf("reachable entries = %d, want %d", count, want)
	}
}
