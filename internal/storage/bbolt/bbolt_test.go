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

// mustChannel materialises a ChannelStore for name, failing on error.
// Used inside this _test file's higher-level flows; the contract
// suite has its own helper of the same shape.
func mustChannel(t *testing.T, s storage.Storage, name string) storage.ChannelStore {
	t.Helper()
	ch, err := s.Channel(context.Background(), name, nil)
	if err != nil {
		t.Fatalf("Channel(%q): %v", name, err)
	}
	return ch
}

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

// TestBBoltSurvivesProcessRestart proves that data persists across a
// Close+Open cycle on the same file: history of pre-restart publishes
// remains readable, and a post-restart publish succeeds and is itself
// visible via history.
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
		cm, _, err := mustChannel(t, s1, "foo").Store(ctx, []*protocol.Message{{Name: "x", Data: i}})
		if err != nil {
			t.Fatalf("foo publish %d: %v", i, err)
		}
		fooSerials = append(fooSerials, cm.ChannelSerial)
	}
	if _, _, err := mustChannel(t, s1, "bar").Store(ctx, []*protocol.Message{{Name: "y"}}); err != nil {
		t.Fatalf("bar publish: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Second "process": same path; history must show prior publishes
	// in the same order, and a fresh publish must persist alongside.
	s2, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	page, err := mustChannel(t, s2, "foo").History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
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

	// A post-restart publish persists and shows up at the tail.
	fresh, _, err := mustChannel(t, s2, "foo").Store(ctx, []*protocol.Message{{Name: "z"}})
	if err != nil {
		t.Fatalf("post-restart publish: %v", err)
	}
	page, err = mustChannel(t, s2, "foo").History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
	if err != nil {
		t.Fatalf("foo History #2: %v", err)
	}
	if got := len(page.ChannelMessages); got != len(fooSerials)+1 {
		t.Fatalf("post-restart History len = %d, want %d", got, len(fooSerials)+1)
	}
	if got := page.ChannelMessages[len(page.ChannelMessages)-1].ChannelSerial; got != fresh.ChannelSerial {
		t.Errorf("tail ChannelSerial = %q, want %q", got, fresh.ChannelSerial)
	}
}
