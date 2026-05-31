package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// stepClock returns a clock func that advances by 1 ms on every call,
// starting at start.
func stepClock(start int64) func() int64 {
	var ts atomic.Int64
	ts.Store(start - 1)
	return func() int64 { return ts.Add(1) }
}

// newTestChannel returns a Channel backed by a fresh in-memory store
// with a deterministic SeriesID and stepping clock, so tests can
// assert on serial values directly.
func newTestChannel(name string) *Channel {
	store := memory.New(memory.Options{
		SeriesID: "testseries0",
		Now:      stepClock(1000),
	})
	return newChannel(name, store.Channel(name))
}

// publish is a thin test helper around Channel.AppendChannelMessage —
// fails the test on error and otherwise returns the resulting cm.
func publish(t *testing.T, c *Channel, msgs ...*protocol.Message) *protocol.ChannelMessage {
	t.Helper()
	cm, _, err := c.AppendChannelMessage(context.Background(), msgs)
	if err != nil {
		t.Fatalf("AppendChannelMessage: %v", err)
	}
	return cm
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
	// Each publish is its own ChannelMessage — one entry per call.
	for _, m := range msgs {
		publish(t, c, m)
	}

	// Walk forward from the sentinel; verify each ChannelMessage in
	// order and that every channelSerial is non-empty and
	// monotonically ordered.
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
		if e.cm == nil {
			t.Fatalf("entry %d: cm is nil", i)
		}
		if len(e.cm.Messages) != 1 {
			t.Fatalf("entry %d: Messages length = %d, want 1", i, len(e.cm.Messages))
		}
		if e.cm.Messages[0].ID != want.ID {
			t.Fatalf("entry %d: msg.ID = %q, want %q", i, e.cm.Messages[0].ID, want.ID)
		}
		if e.cm.ChannelSerial == "" {
			t.Fatalf("entry %d: ChannelSerial is empty", i)
		}
		if e.cm.ChannelSerial <= prev {
			t.Fatalf("entry %d: channelSerial %q not greater than previous %q", i, e.cm.ChannelSerial, prev)
		}
		prev = e.cm.ChannelSerial
	}

	// The final entry's notify is still open — no successor yet.
	if isClosed(e.notify) {
		t.Fatal("final entry's notify is closed; expected open until next append")
	}
}

func TestChannelAppendStampsChannelSerialAndMessageSerial(t *testing.T) {
	c := newTestChannel("test")
	m := &protocol.Message{ID: "m1"}
	publish(t, c, m)

	tail := c.tail
	wantCS := "00000000001000-000@testseries0"
	if tail.cm.ChannelSerial != wantCS {
		t.Errorf("ChannelSerial = %q, want %q", tail.cm.ChannelSerial, wantCS)
	}
	wantMS := "00000000001000-000@testseries0:000"
	if m.Serial != wantMS {
		t.Errorf("m.Serial = %q, want %q", m.Serial, wantMS)
	}
}

func TestChannelAppendBatchSharesChannelSerial(t *testing.T) {
	c := newTestChannel("test")
	a := &protocol.Message{ID: "a"}
	b := &protocol.Message{ID: "b"}
	d := &protocol.Message{ID: "d"}
	publish(t, c, a, b, d)

	wantCS := "00000000001000-000@testseries0"
	if c.tail.cm.ChannelSerial != wantCS {
		t.Errorf("ChannelSerial = %q, want %q", c.tail.cm.ChannelSerial, wantCS)
	}
	if len(c.tail.cm.Messages) != 3 {
		t.Fatalf("Messages length = %d, want 3", len(c.tail.cm.Messages))
	}

	wantSerials := []string{
		"00000000001000-000@testseries0:000",
		"00000000001000-000@testseries0:001",
		"00000000001000-000@testseries0:002",
	}
	for i, m := range []*protocol.Message{a, b, d} {
		if m.Serial != wantSerials[i] {
			t.Errorf("msg %d Serial = %q, want %q", i, m.Serial, wantSerials[i])
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

	publish(t, c, &protocol.Message{ID: "m1"})

	select {
	case e := <-got:
		if e == nil {
			t.Fatal("waiter woke with nil next")
		}
		if e.cm.Messages[0].ID != "m1" {
			t.Fatalf("waiter saw msg.ID = %q, want %q", e.cm.Messages[0].ID, "m1")
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

	publish(t, c, &protocol.Message{ID: "m1"})

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
				// Empty ID — storage idempotency would dedup a shared
				// non-empty ID across goroutines, defeating the test.
				publish(t, c, &protocol.Message{})
			}
		}()
	}
	wg.Wait()

	// Walk the list from the sentinel; every publish must be reachable
	// as one ChannelMessage entry.
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
		t.Errorf("initial ChannelSerial = %q, want empty (no ChannelMessage delivered yet)", got)
	}

	got := make(chan *protocol.ChannelMessage, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		cm, err := s.Next(ctx)
		if err != nil {
			t.Errorf("Next: %v", err)
			return
		}
		got <- cm
	}()

	publish(t, c, &protocol.Message{ID: "m1"})

	select {
	case cm := <-got:
		if len(cm.Messages) != 1 {
			t.Fatalf("delivered Messages length = %d, want 1", len(cm.Messages))
		}
		if cm.Messages[0].ID != "m1" {
			t.Errorf("Next msg.ID = %q, want %q", cm.Messages[0].ID, "m1")
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return within 1s")
	}

	if got := s.ChannelSerial(); got == "" {
		t.Error("post-Next ChannelSerial is empty; want the delivered ChannelMessage's channelSerial")
	}
}

func TestStreamAttachAfterAppendsParksAtTail(t *testing.T) {
	c := newTestChannel("test")
	publish(t, c, &protocol.Message{ID: "m1"})
	publish(t, c, &protocol.Message{ID: "m2"})

	s := c.Attach()
	atTail := s.ChannelSerial()
	if atTail == "" {
		t.Fatal("ChannelSerial after appends is empty; want the tail ChannelMessage's channelSerial")
	}

	// Attaching at the tail means Next blocks until a fresh append.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Next(ctx); err == nil {
		t.Fatal("Next returned with no fresh append; expected ctx error")
	}

	// Now publish a third message; Next should observe it. We can't
	// use the publish helper here — t.Fatalf is unsafe from a goroutine
	// not spawned by the test runner.
	go func() {
		_, _, _ = c.AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "m3"}})
	}()

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	cm, err := s.Next(ctx2)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if cm.Messages[0].ID != "m3" {
		t.Errorf("msg.ID = %q, want %q", cm.Messages[0].ID, "m3")
	}
	if got := s.ChannelSerial(); got == "" || got <= atTail {
		t.Errorf("ChannelSerial after Next = %q, want a channelSerial greater than %q", got, atTail)
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

func TestStreamNextReturnsAtomicBatchAsOneChannelMessage(t *testing.T) {
	c := newTestChannel("test")
	s := c.Attach()

	// One publish with 3 messages is one ChannelMessage delivered as
	// a single Next return. Inline rather than via the publish helper:
	// t.Fatalf is unsafe from non-test goroutines.
	go func() {
		_, _, _ = c.AppendChannelMessage(context.Background(), []*protocol.Message{
			{ID: "a"},
			{ID: "b"},
			{ID: "c"},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) != 3 {
		t.Fatalf("Messages length = %d, want 3", len(cm.Messages))
	}
	for i, want := range []string{"a", "b", "c"} {
		if cm.Messages[i].ID != want {
			t.Errorf("msg %d ID = %q, want %q", i, cm.Messages[i].ID, want)
		}
	}

	// A subsequent Next should park (no more entries until the next
	// publish).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if _, err := s.Next(ctx2); err == nil {
		t.Fatal("Next returned without a fresh publish; expected ctx error")
	}
}
