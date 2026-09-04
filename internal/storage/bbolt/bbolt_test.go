package bbolt_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/storagetest"
	"github.com/ably/server-protocol/go/wire"
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
//
// TestBBoltPresenceMembersNotPersisted proves the §12.5 invariant for
// the disk backend: the membership set is in-memory only, so it is
// empty after a Close+Open cycle (presence is connection-scoped and no
// connection survives a restart) — while the presence *history* does
// persist as an ordinary cm on the channel_messages log.
func TestBBoltPresenceMembersNotPersisted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ably.db")
	ctx := context.Background()

	s1, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if _, _, err := mustChannel(t, s1, "room").StorePresence(ctx, []*wire.PresenceMessage{
		{Action: wire.PresenceMessage_ENTER, ConnectionId: "conn-1", ClientId: new("alice"), Data: wire.MessageStrData("hi")},
	}); err != nil {
		t.Fatalf("StorePresence: %v", err)
	}
	memberPage, err := mustChannel(t, s1, "room").Members(ctx, storage.MembersQuery{})
	members := memberPage.Members
	if err != nil {
		t.Fatalf("Members #1: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("pre-restart members = %d, want 1", len(members))
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Second "process": the membership set starts empty...
	s2, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	memberPage, err = mustChannel(t, s2, "room").Members(ctx, storage.MembersQuery{})
	members = memberPage.Members
	if err != nil {
		t.Fatalf("Members #2: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("post-restart members = %d, want 0 (membership is not persisted)", len(members))
	}

	// ...but the presence history persists across the restart.
	page, err := mustChannel(t, s2, "room").History(ctx, storage.HistoryQuery{
		Kind: storage.KindPresence, Direction: storage.DirectionForwards,
	})
	if err != nil {
		t.Fatalf("presence History: %v", err)
	}
	if len(page.ChannelMessages) != 1 || len(page.ChannelMessages[0].Presence) != 1 {
		t.Fatalf("presence history = %d cms, want 1 with one presence item", len(page.ChannelMessages))
	}
	if got := page.ChannelMessages[0].Presence[0].GetClientId(); got != "alice" {
		t.Errorf("persisted presence clientId = %q, want alice", got)
	}
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
		cm, _, err := mustChannel(t, s1, "foo").Store(ctx, []*wire.Message{{Name: new("x"), Data: wire.MessageStrData(strconv.Itoa(i))}})
		if err != nil {
			t.Fatalf("foo publish %d: %v", i, err)
		}
		fooSerials = append(fooSerials, cm.ChannelSerial)
	}
	if _, _, err := mustChannel(t, s1, "bar").Store(ctx, []*wire.Message{{Name: new("y")}}); err != nil {
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
	fresh, _, err := mustChannel(t, s2, "foo").Store(ctx, []*wire.Message{{Name: new("z")}})
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

// TestBBoltChannelSeriesSurvivesRestart asserts that reopening the file
// carries on a channel's existing series rather than starting a new one.
//
// The series is how a server tells a client that the serials before it
// are not ordered against the ones after it. A restart over an intact
// log has restarted nothing, so saying otherwise would hand every
// resuming client a discontinuity that did not happen.
func TestBBoltChannelSeriesSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ably.db")
	ctx := context.Background()

	s1, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	before, _, err := mustChannel(t, s1, "room").Store(ctx, []*wire.Message{{Name: new("x")}})
	if err != nil {
		t.Fatalf("publish before restart: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	s2, err := bbolt.Open(bbolt.Options{Path: path})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	after, _, err := mustChannel(t, s2, "room").Store(ctx, []*wire.Message{{Name: new("y")}})
	if err != nil {
		t.Fatalf("publish after restart: %v", err)
	}

	_, _, wantSeries, err := serial.SplitChannelSerial(before.ChannelSerial)
	if err != nil {
		t.Fatalf("split %q: %v", before.ChannelSerial, err)
	}
	_, _, gotSeries, err := serial.SplitChannelSerial(after.ChannelSerial)
	if err != nil {
		t.Fatalf("split %q: %v", after.ChannelSerial, err)
	}
	if gotSeries != wantSeries {
		t.Errorf("series after restart = %q, want %q (%q then %q)",
			gotSeries, wantSeries, before.ChannelSerial, after.ChannelSerial)
	}
	if after.ChannelSerial <= before.ChannelSerial {
		t.Errorf("post-restart serial %q is not after %q", after.ChannelSerial, before.ChannelSerial)
	}
}

// TestBBoltSerialsStayMonotonicAcrossRestartWithinOneMillisecond pins
// the case a fresh generator got wrong: a channel reopened while the
// clock still reads the millisecond its last publish was minted in.
// The generator resumes from the last persisted serial, so the next
// mint follows it instead of colliding with it.
func TestBBoltSerialsStayMonotonicAcrossRestartWithinOneMillisecond(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ably.db")
	ctx := context.Background()
	frozen := func() int64 { return 1726585978590 }

	s1, err := bbolt.Open(bbolt.Options{Path: path, Now: frozen})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	var serials []string
	for range 3 {
		cm, _, err := mustChannel(t, s1, "room").Store(ctx, []*wire.Message{{Name: new("x")}})
		if err != nil {
			t.Fatalf("publish before restart: %v", err)
		}
		serials = append(serials, cm.ChannelSerial)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	s2, err := bbolt.Open(bbolt.Options{Path: path, Now: frozen})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	for range 3 {
		cm, _, err := mustChannel(t, s2, "room").Store(ctx, []*wire.Message{{Name: new("y")}})
		if err != nil {
			t.Fatalf("publish after restart: %v", err)
		}
		serials = append(serials, cm.ChannelSerial)
	}

	for i := 1; i < len(serials); i++ {
		if serials[i] <= serials[i-1] {
			t.Fatalf("serials not monotonic across restart: %q then %q (all: %v)",
				serials[i-1], serials[i], serials)
		}
	}
}
