package handles

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/core"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/wire"
)

// publishTestMessages publishes one message per body and returns the serial
// each landed at.
func publishTestMessages(t *testing.T, ch *core.Channel, bodies ...string) []*wire.Timeserial {
	t.Helper()
	serials := make([]*wire.Timeserial, 0, len(bodies))
	for _, body := range bodies {
		cm, _, err := ch.Publish(t.Context(), []*wire.Message{{Name: new("ev"), Data: wire.MessageStrData(body)}})
		if err != nil {
			t.Fatalf("Publish(%s): %s", body, err)
		}
		serial, err := wire.TimeserialFromString(cm.ChannelSerial)
		if err != nil {
			t.Fatalf("parse %q: %s", cm.ChannelSerial, err)
		}
		serials = append(serials, serial)
	}
	return serials
}

// drain reads the stream until it stops advancing, returning each message's
// data. It waits for the stream to attach first.
func drain(t *testing.T, s *channel.Stream) []string {
	t.Helper()
	select {
	case <-s.Attached():
	case <-time.After(2 * time.Second):
		t.Fatal("stream never became attached")
	}

	var got []string
	for s.Next() {
		msg := s.Message()
		if msg == nil {
			continue
		}
		for _, m := range msg.Messages {
			got = append(got, m.Data.GetStr())
		}
	}
	return got
}

// TestRewind_ServesMessagesPublishedBeforeTheCacheExisted attaches behind the
// live edge on a channel whose history is entirely in storage.
//
// This is the case a cache holding only what it has seen cannot answer: the
// messages were published before it existed, so serving them means reaching
// into storage for them. What the client asked for and where it lands are the
// cache's; finding the messages is this server's.
func TestRewind_ServesMessagesPublishedBeforeTheCacheExisted(t *testing.T) {
	// The channel comes live after the fact, so its cache holds none of them.
	held, _ := newTestChannelWithHistory(t, "rewind-cold", "one", "two", "three", "four")

	got := drain(t, held.messages().StreamFromOffset(2, nil))
	if len(got) != 2 || got[0] != "three" || got[1] != "four" {
		t.Fatalf("rewind=2 gave %v, want [three four]", got)
	}
}

// TestRewind_ResumeFromASerialDeliversWhatFollowedIt is the resume path: a
// client presents the serial it last saw and is sent what came after it,
// and only that.
func TestRewind_ResumeFromASerialDeliversWhatFollowedIt(t *testing.T) {
	held, serials := newTestChannelWithHistory(t, "rewind-resume", "one", "two", "three")

	s := held.messages().StreamFromChannelSerial(serials[0], nil)
	got := drain(t, s)
	if len(got) != 2 || got[0] != "two" || got[1] != "three" {
		t.Fatalf("resume from the first message gave %v, want [two three]", got)
	}
	if err := s.Error(); err != nil {
		t.Errorf("resume failed with %s", err)
	}
	if s.AttachFlags()&channel.RESUMED == 0 {
		t.Error("a resume that found its point did not report RESUMED")
	}
}

// TestRewind_ResumeFromAnUnknownSerialSaysContinuityWasLost checks the other
// half: a client presenting a serial this channel never had still attaches,
// and is told that it did not resume.
//
// The attach is not refused. It carries an error and leaves RESUMED unset,
// which is what tells the client its continuity is broken and it should go
// and read history for itself — and it is given everything the server still
// holds rather than only what followed a point it cannot find.
func TestRewind_ResumeFromAnUnknownSerialSaysContinuityWasLost(t *testing.T) {
	held, _ := newTestChannelWithHistory(t, "rewind-unknown", "one", "two")

	// A serial from before this channel began, which nothing on it follows.
	stale := wire.MustTimeserialFromString("00000000000001-000@" + held.stored.InitialChannelSerial()[19:])
	s := held.messages().StreamFromChannelSerial(stale, nil)
	got := drain(t, s)

	if s.Error() == nil {
		t.Error("resuming from a serial the channel never had was reported as a clean resume")
	}
	if s.AttachFlags()&channel.RESUMED != 0 {
		t.Error("a resume that could not find its point still reported RESUMED")
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Errorf("got %v, want everything the channel still holds", got)
	}
}

// TestRewind_SharesOneCacheAcrossAttachments checks that the channel manager
// hands every attachment on a channel the same cache. That sharing is the
// reason the cache exists: the second rewind is answered from what the first
// one fetched.
func TestRewind_SharesOneCacheAcrossAttachments(t *testing.T) {
	m, store := newTestChannels(t)

	first := getTestChannel(t, m, store, "shared-cache")
	second := getTestChannel(t, m, store, "shared-cache")

	if first.messages() != second.messages() {
		t.Error("two attachments on one channel were given different caches")
	}
}
