package bbolt_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

func TestBBoltChannelStoreContract(t *testing.T) {
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		s, err := bbolt.Open(bbolt.Options{Path: filepath.Join(t.TempDir(), "ably.db")})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestBBoltSurvivesProcessRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ably.db")
	ctx := context.Background()

	// First "process": publish on two channels.
	s1, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	var fooSerials []string
	for i := range 3 {
		cm, _, err := s1.Channel("foo").AppendChannelMessage(ctx, []*protocol.Message{{Name: "x", Data: i}})
		if err != nil {
			t.Fatalf("foo publish %d: %v", i, err)
		}
		fooSerials = append(fooSerials, cm.ChannelSerial)
	}
	barCM, _, err := s1.Channel("bar").AppendChannelMessage(ctx, []*protocol.Message{{Name: "y"}})
	if err != nil {
		t.Fatalf("bar publish: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Second "process": same path; history must show prior publishes.
	s2, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	page, err := s2.Channel("foo").History(ctx, storage.HistoryQuery{})
	if err != nil {
		t.Fatalf("foo History: %v", err)
	}
	if got := len(page.ChannelMessages); got != len(fooSerials) {
		t.Fatalf("foo History len = %d, want %d", got, len(fooSerials))
	}
	for i, cm := range page.ChannelMessages {
		if cm.ChannelSerial != fooSerials[i] {
			t.Errorf("page[%d] = %q, want %q", i, cm.ChannelSerial, fooSerials[i])
		}
	}

	// A new publish on foo gets a channelSerial > all prior ones —
	// the generator's state was restored from _meta.
	fresh, _, err := s2.Channel("foo").AppendChannelMessage(ctx, []*protocol.Message{{Name: "z"}})
	if err != nil {
		t.Fatalf("post-restart publish: %v", err)
	}
	for _, prior := range append([]string{}, append(fooSerials, barCM.ChannelSerial)...) {
		if fresh.ChannelSerial <= prior {
			t.Errorf("post-restart channelSerial %q not greater than prior %q", fresh.ChannelSerial, prior)
		}
	}
}

func TestBBoltSeriesIDPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ably.db")

	s1, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	cm1, _, err := s1.Channel("foo").AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
	if err != nil {
		t.Fatalf("publish #1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	s2, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	cm2, _, err := s2.Channel("foo").AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "y"}})
	if err != nil {
		t.Fatalf("publish #2: %v", err)
	}

	// Same seriesId on both sides of the restart.
	series1 := seriesIDOf(t, cm1.ChannelSerial)
	series2 := seriesIDOf(t, cm2.ChannelSerial)
	if series1 != series2 {
		t.Errorf("seriesId changed across restart: %q vs %q", series1, series2)
	}
}

// seriesIDOf extracts the seriesId from a channelSerial of the form
// "<ts>-<ctr>@<seriesId>".
func seriesIDOf(t *testing.T, cs string) string {
	t.Helper()
	for i := 0; i < len(cs); i++ {
		if cs[i] == '@' {
			return cs[i+1:]
		}
	}
	t.Fatalf("malformed channelSerial %q (no @)", cs)
	return ""
}
