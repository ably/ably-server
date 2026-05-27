package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
)

// stepClock returns a clock func that advances by 1 ms on every call,
// starting at start.
func stepClock(start int64) func() int64 {
	var ts atomic.Int64
	ts.Store(start - 1)
	return func() int64 { return ts.Add(1) }
}

// newTestChannel returns a Channel with a deterministic generator —
// fixed seriesId and a monotonic stepping clock — so tests can assert
// on serial values directly.
func newTestChannel(name string) *Channel {
	return newChannel(name, serial.NewGenerator("testseries0", stepClock(1000)))
}

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
	c := newTestChannel("test")
	head := c.tail // sentinel; notify open, next nil

	msgs := []*protocol.Message{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}}
	for _, m := range msgs {
		c.Append(m)
	}

	// Walk forward from the sentinel; verify each msg in order and that
	// every message has a non-empty, monotonically-ordered Serial.
	e := head
	prev := ""
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
		if e.msg.Serial == "" {
			t.Fatalf("entry %d: msg.Serial is empty", i)
		}
		if e.msg.Serial <= prev {
			t.Fatalf("entry %d: serial %q not greater than previous %q", i, e.msg.Serial, prev)
		}
		prev = e.msg.Serial
	}

	// The final entry's notify is still open — no successor yet.
	if isClosed(e.notify) {
		t.Fatal("final entry's notify is closed; expected open until next append")
	}
}

func TestChannelAppendStampsSerialOnMessage(t *testing.T) {
	c := newTestChannel("test")
	m := &protocol.Message{ID: "m1"}
	c.Append(m)
	if m.Serial != "00000000001000-000@testseries0:000" {
		t.Errorf("m.Serial = %q, want %q", m.Serial, "00000000001000-000@testseries0:000")
	}
}

func TestChannelAppendBatchSharesPrefix(t *testing.T) {
	c := newTestChannel("test")
	a := &protocol.Message{ID: "a"}
	b := &protocol.Message{ID: "b"}
	d := &protocol.Message{ID: "d"}
	c.Append(a, b, d)

	want := []string{
		"00000000001000-000@testseries0:000",
		"00000000001000-000@testseries0:001",
		"00000000001000-000@testseries0:002",
	}
	for i, m := range []*protocol.Message{a, b, d} {
		if m.Serial != want[i] {
			t.Errorf("msg %d Serial = %q, want %q", i, m.Serial, want[i])
		}
	}
}

func TestChannelNotifyWakesWaiter(t *testing.T) {
	c := newTestChannel("test")
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
	c := newTestChannel("test")
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
	c := newTestChannel("test")
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
	c := newTestChannel("test")
	s := c.Attach()

	if got := s.ChannelSerial(); got != "" {
		t.Errorf("initial ChannelSerial = %q, want empty (no message delivered yet)", got)
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

	if got := s.ChannelSerial(); got == "" {
		t.Error("post-Next ChannelSerial is empty; want the delivered message's serial")
	}
}

func TestStreamAttachAfterAppendsParksAtTail(t *testing.T) {
	c := newTestChannel("test")
	c.Append(&protocol.Message{ID: "m1"})
	c.Append(&protocol.Message{ID: "m2"})

	s := c.Attach()
	atTail := s.ChannelSerial()
	if atTail == "" {
		t.Fatal("ChannelSerial after appends is empty; want the tail message's serial")
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
	if got := s.ChannelSerial(); got == "" || got <= atTail {
		t.Errorf("ChannelSerial after Next = %q, want a serial greater than %q", got, atTail)
	}
}

func TestStreamNextRespectsContext(t *testing.T) {
	c := newTestChannel("test")
	s := c.Attach()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Next(ctx); err == nil {
		t.Fatal("Next returned nil error after ctx cancellation")
	}
}
