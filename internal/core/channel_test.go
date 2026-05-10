package core

import (
	"context"
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
	head := c.tail // sentinel; notify open, next nil

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
		if e.serial != int64(i+1) {
			t.Fatalf("entry %d: serial = %d, want %d", i, e.serial, i+1)
		}
	}

	// The final entry's notify is still open — no successor yet.
	if isClosed(e.notify) {
		t.Fatal("final entry's notify is closed; expected open until next append")
	}
}

func TestChannelNotifyWakesWaiter(t *testing.T) {
	c := newChannel("test")
	head := c.tail

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
	head := c.tail

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
	head := c.tail

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

func TestStreamAttachOnEmptyChannelBlocksUntilAppend(t *testing.T) {
	c := newChannel("test")
	s := c.Attach()

	if got := s.ChannelSerial(); got != "0" {
		t.Errorf("initial ChannelSerial = %q, want %q", got, "0")
	}

	got := make(chan *protocol.Message, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := s.Next(ctx)
		if err != nil {
			t.Errorf("Next: %v", err)
			return
		}
		got <- m
	}()

	c.Append(&protocol.Message{ID: "m1"})

	select {
	case m := <-got:
		if m.ID != "m1" {
			t.Errorf("Next msg.ID = %q, want %q", m.ID, "m1")
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return within 1s")
	}

	if got := s.ChannelSerial(); got != "1" {
		t.Errorf("post-Next ChannelSerial = %q, want %q", got, "1")
	}
}

func TestStreamAttachAfterAppendsParksAtTail(t *testing.T) {
	c := newChannel("test")
	c.Append(&protocol.Message{ID: "m1"})
	c.Append(&protocol.Message{ID: "m2"})

	s := c.Attach()
	if got := s.ChannelSerial(); got != "2" {
		t.Fatalf("ChannelSerial = %q, want %q", got, "2")
	}

	// Attaching at the tail means Next blocks until a fresh append.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Next(ctx); err == nil {
		t.Fatal("Next returned with no fresh append; expected ctx error")
	}

	// Now publish a third message; Next should observe it.
	go c.Append(&protocol.Message{ID: "m3"})

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	m, err := s.Next(ctx2)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if m.ID != "m3" {
		t.Errorf("msg.ID = %q, want %q", m.ID, "m3")
	}
	if got := s.ChannelSerial(); got != "3" {
		t.Errorf("ChannelSerial = %q, want %q", got, "3")
	}
}

func TestStreamNextRespectsContext(t *testing.T) {
	c := newChannel("test")
	s := c.Attach()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Next(ctx); err == nil {
		t.Fatal("Next returned nil error after ctx cancellation")
	}
}
