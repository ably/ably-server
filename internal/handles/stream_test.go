package handles

import (
	"testing"
	"time"

	"github.com/ably/server-protocol/go/wire"
)

// TestStream_DeliversPublishesInOrder drives the shared stream interface over
// this server's live list: a publish that lands after the attachment started
// reading wakes it through Ready, comes out of Next in order, and carries the
// channelSerial the client would resume from.
func TestStream_DeliversPublishesInOrder(t *testing.T) {
	held := newTestChannel(t, "stream-test")
	ch := held.stored

	s := held.messages().Stream()
	if s == nil {
		t.Fatal("Stream returned nil")
	}

	// Attaching is asynchronous: the channel is not readable until storage
	// has told it where to start, so a caller waits as the attachment does.
	select {
	case <-s.Attached():
	case <-time.After(2 * time.Second):
		t.Fatal("stream never became attached")
	}
	if err := s.Error(); err != nil {
		t.Fatalf("stream failed to attach: %s", err)
	}

	// Nothing published yet, so there is nothing to advance to.
	if s.Next() {
		t.Error("Next advanced before anything was published")
	}

	for _, body := range []string{"one", "two"} {
		if _, _, err := ch.Publish(t.Context(), []*wire.Message{{Name: new("ev"), Data: wire.MessageStrData(body)}}); err != nil {
			t.Fatalf("Publish(%s): %s", body, err)
		}
	}

	// Ready fires for the first publish.
	select {
	case <-s.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready did not fire after a publish")
	}

	// A publish reaches the cache over the channel's own tail, so the stream
	// is read until both have arrived rather than once: Next says only that
	// there is nothing there yet.
	var got []string
	var serials []*wire.Timeserial
	deadline := time.Now().Add(2 * time.Second)
	for len(got) < 2 && time.Now().Before(deadline) {
		for s.Next() {
			msg := s.Message()
			if msg == nil {
				continue
			}
			if !msg.IsMessage() {
				t.Errorf("a publish came through as action %v, want a message", msg.Action)
			}
			for _, m := range msg.Messages {
				got = append(got, m.Data.GetStr())
			}
			serials = append(serials, s.ChannelSerial())
		}
	}

	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("got %v, want [one two]", got)
	}
	if len(serials) != 2 || serials[0] == nil || serials[1] == nil {
		t.Fatalf("got serials %v, want two parsed serials", serials)
	}
	if !serials[0].Before(serials[1]) {
		t.Errorf("serials out of order: %s then %s", serials[0].ToTimeserialString(), serials[1].ToTimeserialString())
	}
}

// TestStream_PresenceCarriesItsAction checks the discrimination this server
// does by slice and the wire does by action field.
func TestStream_PresenceCarriesItsAction(t *testing.T) {
	held := newTestChannel(t, "stream-presence")
	ch := held.stored
	s := held.messages().Stream()
	select {
	case <-s.Attached():
	case <-time.After(2 * time.Second):
		t.Fatal("stream never became attached")
	}

	if _, _, err := ch.PublishPresence(t.Context(), []*wire.PresenceMessage{
		presenceEnter("conn-a", "alice"),
	}); err != nil {
		t.Fatalf("PublishPresence: %s", err)
	}

	select {
	case <-s.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready did not fire after a presence publish")
	}
	if !s.Next() {
		t.Fatal("Next did not advance to the presence publish")
	}
	msg := s.Message()
	if !msg.IsPresence() {
		t.Fatalf("presence came through as action %v", msg.Action)
	}
	if len(msg.Presence) != 1 || msg.Presence[0].GetClientId() != "alice" {
		t.Errorf("got presence %v, want one member alice", msg.Presence)
	}
}
