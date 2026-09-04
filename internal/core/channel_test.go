package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/server-protocol/go/wire"
)

// newCM builds a deterministic ChannelMessage for tests. The serial
// argument is the channelSerial; per-message serials are stamped as
// "<serial>:<idx>". Channel doesn't mint or stamp anything itself —
// storage does that upstream — so tests construct cms directly.
func newCM(serial string, ids ...string) *protocol.ChannelMessage {
	msgs := make([]*wire.Message, len(ids))
	for i, id := range ids {
		msgs[i] = &wire.Message{
			Id:     new(id),
			Serial: fmt.Sprintf("%s:%03d", serial, i),
		}
	}
	return &protocol.ChannelMessage{ChannelSerial: serial, Messages: msgs}
}

// newReadyChannel constructs a Channel and drives it through
// Initialize with the given seed serial, mirroring what the storage
// backend does in production. Tests that exercise Attach use this so
// they don't have to wire up a real storage to get a ready channel.
func newReadyChannel(name, seedSerial string) *Channel {
	c := newChannel(name)
	c.Initialize(seedSerial, seedSerial)
	return c
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
	c := newChannel("test")
	head := c.tail // sentinel; notify open, next nil

	cms := []*protocol.ChannelMessage{
		newCM("001", "m1"),
		newCM("002", "m2"),
		newCM("003", "m3"),
	}
	for _, cm := range cms {
		c.Append(cm)
	}

	// Walk forward from the sentinel; each Appended cm should be
	// reachable as one entry, in order.
	e := head
	for i, want := range cms {
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
		if e.cm.ChannelSerial != want.ChannelSerial {
			t.Fatalf("entry %d: ChannelSerial = %q, want %q", i, e.cm.ChannelSerial, want.ChannelSerial)
		}
		if e.cm.Messages[0].GetId() != want.Messages[0].GetId() {
			t.Fatalf("entry %d: msg.GetId() = %q, want %q", i, e.cm.Messages[0].GetId(), want.Messages[0].GetId())
		}
	}

	// The final entry's notify is still open — no successor yet.
	if isClosed(e.notify) {
		t.Fatal("final entry's notify is closed; expected open until next append")
	}
}

func TestChannelAppendIsNoOpOnNilOrEmpty(t *testing.T) {
	c := newChannel("test")
	head := c.tail

	c.Append(nil)
	c.Append(&protocol.ChannelMessage{ChannelSerial: "001"}) // no Messages

	// Nothing should have been linked; the sentinel's notify is still
	// open and next is nil.
	if isClosed(head.notify) {
		t.Error("notify closed on a no-op Append")
	}
	if head.next != nil {
		t.Error("next set on a no-op Append")
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

	c.Append(newCM("001", "m1"))

	select {
	case e := <-got:
		if e == nil {
			t.Fatal("waiter woke with nil next")
		}
		if e.cm.Messages[0].GetId() != "m1" {
			t.Fatalf("waiter saw msg.GetId() = %q, want %q", e.cm.Messages[0].GetId(), "m1")
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

	c.Append(newCM("001", "m1"))

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

	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			for range perWriter {
				serial := fmt.Sprintf("%010d", counter.Add(1))
				c.Append(newCM(serial, "x"))
			}
		}()
	}
	wg.Wait()

	// Walk the list from the sentinel; every Append must be reachable
	// as one entry.
	count := 0
	for e := head; e.next != nil; e = e.next {
		count++
	}
	if want := writers * perWriter; count != want {
		t.Fatalf("reachable entries = %d, want %d", count, want)
	}
}

func TestStreamAttachOnEmptyChannelExposesWatermark(t *testing.T) {
	c := newReadyChannel("test", "seed-serial")
	s, err := c.Attach(context.Background())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if got := s.ChannelSerial(); got != "seed-serial" {
		t.Errorf("initial ChannelSerial = %q, want %q (watermark from Initialize)", got, "seed-serial")
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

	c.Append(newCM("001", "m1"))

	select {
	case cm := <-got:
		if len(cm.Messages) != 1 {
			t.Fatalf("delivered Messages length = %d, want 1", len(cm.Messages))
		}
		if cm.Messages[0].GetId() != "m1" {
			t.Errorf("Next msg.GetId() = %q, want %q", cm.Messages[0].GetId(), "m1")
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return within 1s")
	}

	if got := s.ChannelSerial(); got != "001" {
		t.Errorf("post-Next ChannelSerial = %q, want %q", got, "001")
	}
}

func TestStreamAttachAfterAppendsParksAtTail(t *testing.T) {
	c := newReadyChannel("test", "seed-serial")
	c.Append(newCM("001", "m1"))
	c.Append(newCM("002", "m2"))

	s, err := c.Attach(context.Background())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if got := s.ChannelSerial(); got != "002" {
		t.Fatalf("ChannelSerial after appends = %q, want %q", got, "002")
	}

	// Attaching at the tail means Next blocks until a fresh append.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Next(ctx); err == nil {
		t.Fatal("Next returned with no fresh append; expected ctx error")
	}

	// Now append a third entry; Next should observe it.
	go c.Append(newCM("003", "m3"))

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	cm, err := s.Next(ctx2)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if cm.Messages[0].GetId() != "m3" {
		t.Errorf("msg.GetId() = %q, want %q", cm.Messages[0].GetId(), "m3")
	}
	if got := s.ChannelSerial(); got != "003" {
		t.Errorf("ChannelSerial after Next = %q, want %q", got, "003")
	}
}

func TestStreamNextRespectsContext(t *testing.T) {
	c := newReadyChannel("test", "seed-serial")
	s, err := c.Attach(context.Background())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Next(ctx); err == nil {
		t.Fatal("Next returned nil error after ctx cancellation")
	}
}

func TestStreamNextReturnsAtomicBatchAsOneChannelMessage(t *testing.T) {
	c := newReadyChannel("test", "seed-serial")
	s, err := c.Attach(context.Background())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// One Append carrying 3 messages is one ChannelMessage delivered
	// as a single Next return.
	go c.Append(newCM("001", "a", "b", "c"))

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
		if cm.Messages[i].GetId() != want {
			t.Errorf("msg %d ID = %q, want %q", i, cm.Messages[i].GetId(), want)
		}
	}

	// A subsequent Next should park (no more entries until the next
	// Append).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if _, err := s.Next(ctx2); err == nil {
		t.Fatal("Next returned without a fresh Append; expected ctx error")
	}
}

func TestChannelAttachBlocksUntilInitialize(t *testing.T) {
	c := newChannel("test")

	attached := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Attach(ctx)
		attached <- err
	}()

	// Without Initialize, Attach should not return promptly.
	select {
	case err := <-attached:
		t.Fatalf("Attach returned before Initialize: err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	c.Initialize("seed", "seed")

	select {
	case err := <-attached:
		if err != nil {
			t.Fatalf("Attach after Initialize: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Attach did not return within 1s of Initialize")
	}
}

func TestChannelAttachRespectsContextBeforeInitialize(t *testing.T) {
	c := newChannel("test")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.Attach(ctx); err == nil {
		t.Fatal("Attach returned nil error on cancelled ctx before Initialize")
	}
}
