// Package storagetest exposes a contract-test suite that every
// storage.ChannelStore implementation should satisfy. Backends import
// this package from their _test.go and call RunChannelStoreTests with
// a factory that produces a fresh storage for each subtest.
package storagetest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Factory builds a fresh Storage for one subtest. It is called once
// per subtest, so backends may use t.TempDir() / t.Cleanup() to bound
// per-test resources.
type Factory func(t *testing.T) storage.Storage

// RunChannelStoreTests runs the shared ChannelStore contract against
// the storage produced by f.
func RunChannelStoreTests(t *testing.T, f Factory) {
	t.Helper()

	t.Run("AppendStampsChannelSerialAndMessageSerials", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")
		cm, idempotent, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
		if err != nil {
			t.Fatalf("AppendChannelMessage: %v", err)
		}
		if idempotent {
			t.Fatal("idempotent=true on a fresh publish")
		}
		if cm.ChannelSerial == "" {
			t.Error("ChannelSerial is empty")
		}
		if len(cm.Messages) != 1 {
			t.Fatalf("Messages length = %d, want 1", len(cm.Messages))
		}
		want := cm.ChannelSerial + ":000"
		if cm.Messages[0].Serial != want {
			t.Errorf("Messages[0].Serial = %q, want %q", cm.Messages[0].Serial, want)
		}
	})

	t.Run("AppendStampsBatchWithSharedChannelSerial", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")
		msgs := []*protocol.Message{{Name: "a"}, {Name: "b"}, {Name: "c"}}
		cm, _, err := ch.AppendChannelMessage(context.Background(), msgs)
		if err != nil {
			t.Fatalf("AppendChannelMessage: %v", err)
		}
		if len(cm.Messages) != 3 {
			t.Fatalf("Messages length = %d, want 3", len(cm.Messages))
		}
		for i, m := range cm.Messages {
			want := cm.ChannelSerial + ":" + threeDigit(i)
			if m.Serial != want {
				t.Errorf("Messages[%d].Serial = %q, want %q", i, m.Serial, want)
			}
		}
	})

	t.Run("AppendsAreMonotonicallyOrdered", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")
		var prev string
		for i := range 5 {
			cm, _, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			if cm.ChannelSerial <= prev {
				t.Fatalf("publish %d channelSerial %q not > previous %q", i, cm.ChannelSerial, prev)
			}
			prev = cm.ChannelSerial
		}
	})

	t.Run("IdempotentReturnsOriginalOnRepeatID", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		first, idemp, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "dup", Data: "v1"}})
		if err != nil || idemp {
			t.Fatalf("first publish: err=%v idempotent=%v", err, idemp)
		}

		// Re-publish with the same ID but different payload — should
		// return the original ChannelMessage and not re-append.
		second, idemp, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "dup", Data: "v2"}})
		if err != nil {
			t.Fatalf("second publish: %v", err)
		}
		if !idemp {
			t.Fatal("idempotent=false on duplicate id")
		}
		if second.ChannelSerial != first.ChannelSerial {
			t.Errorf("returned ChannelSerial = %q, want original %q", second.ChannelSerial, first.ChannelSerial)
		}
		if second.Messages[0].Data != "v1" {
			t.Errorf("returned Data = %v, want %v (original, not the replay)", second.Messages[0].Data, "v1")
		}

		// History should still show only the one entry.
		page, err := ch.History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 1 {
			t.Errorf("History len = %d, want 1 (duplicate must not persist)", len(page.ChannelMessages))
		}
	})

	t.Run("IdempotencyMatchesAnyMessageInBatch", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		first, _, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "a"}, {ID: "b"}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}

		// A new publish where any contained id matches should be
		// treated as a duplicate.
		second, idemp, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "c"}, {ID: "b"}})
		if err != nil {
			t.Fatalf("second publish: %v", err)
		}
		if !idemp {
			t.Error("idempotent=false; expected match on shared id 'b'")
		}
		if second.ChannelSerial != first.ChannelSerial {
			t.Errorf("returned ChannelSerial = %q, want original %q", second.ChannelSerial, first.ChannelSerial)
		}
	})

	t.Run("EmptyIDsAreAlwaysNew", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		first, _, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
		second, idemp, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "y"}})
		if err != nil {
			t.Fatalf("second publish: %v", err)
		}
		if idemp {
			t.Fatal("idempotent=true on a publish with no IDs")
		}
		if second.ChannelSerial == first.ChannelSerial {
			t.Error("two empty-id publishes got the same channelSerial")
		}
	})

	t.Run("AppendRejectsEmptyBatch", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")
		if _, _, err := ch.AppendChannelMessage(context.Background(), nil); err == nil {
			t.Error("AppendChannelMessage(nil) returned no error")
		}
		if _, _, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{}); err == nil {
			t.Error("AppendChannelMessage([]) returned no error")
		}
	})

	t.Run("HistoryFromStartReturnsAllInOrder", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		var serials []string
		for i := range 4 {
			cm, _, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x", Data: i}})
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			serials = append(serials, cm.ChannelSerial)
		}

		page, err := ch.History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 4 {
			t.Fatalf("History len = %d, want 4", len(page.ChannelMessages))
		}
		for i, cm := range page.ChannelMessages {
			if cm.ChannelSerial != serials[i] {
				t.Errorf("page[%d].ChannelSerial = %q, want %q", i, cm.ChannelSerial, serials[i])
			}
		}
		if page.HasMore {
			t.Error("HasMore=true with no limit")
		}
	})

	t.Run("HistoryAfterSerialExcludesThatSerial", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		var serials []string
		for range 4 {
			cm, _, _ := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
			serials = append(serials, cm.ChannelSerial)
		}

		page, err := ch.History(context.Background(), storage.HistoryQuery{AfterChannelSerial: serials[1]})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 2 {
			t.Fatalf("History len = %d, want 2 (serials[2] and serials[3])", len(page.ChannelMessages))
		}
		if page.ChannelMessages[0].ChannelSerial != serials[2] {
			t.Errorf("first result = %q, want %q", page.ChannelMessages[0].ChannelSerial, serials[2])
		}
	})

	t.Run("HistoryAfterUnknownSerialReturnsLaterEntries", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		cm, _, _ := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})

		// An "unknown" serial that sorts before cm's serial — history
		// should return cm.
		page, err := ch.History(context.Background(), storage.HistoryQuery{AfterChannelSerial: "00000000000000-000@zzzzzzzzzz"})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 1 || page.ChannelMessages[0].ChannelSerial != cm.ChannelSerial {
			t.Errorf("History returned %+v, want [%q]", channelSerialsOf(page), cm.ChannelSerial)
		}
	})

	t.Run("HistoryAfterHeadReturnsEmpty", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		cm, _, _ := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})

		page, err := ch.History(context.Background(), storage.HistoryQuery{AfterChannelSerial: cm.ChannelSerial})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 0 {
			t.Errorf("History len = %d, want 0", len(page.ChannelMessages))
		}
	})

	t.Run("HistoryWithLimitSetsHasMore", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		for range 5 {
			_, _, _ = ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
		}

		page, err := ch.History(context.Background(), storage.HistoryQuery{Limit: 2})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 2 {
			t.Errorf("len = %d, want 2", len(page.ChannelMessages))
		}
		if !page.HasMore {
			t.Error("HasMore=false with limit < total")
		}

		// Limit equal to total: no more.
		page, err = ch.History(context.Background(), storage.HistoryQuery{Limit: 5})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if page.HasMore {
			t.Error("HasMore=true with limit == total")
		}
	})

	t.Run("ChannelsAreIsolated", func(t *testing.T) {
		s := f(t)

		fooCM, _, err := s.Channel("foo").AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "x"}})
		if err != nil {
			t.Fatalf("foo publish: %v", err)
		}
		// Same ID, different channel — must NOT be idempotent.
		barCM, idemp, err := s.Channel("bar").AppendChannelMessage(context.Background(), []*protocol.Message{{ID: "x"}})
		if err != nil {
			t.Fatalf("bar publish: %v", err)
		}
		if idemp {
			t.Error("idempotent=true across channels; idempotency must be per-channel")
		}
		if barCM.ChannelSerial == fooCM.ChannelSerial {
			t.Error("different channels minted the same channelSerial")
		}

		// History on foo must not include bar's publish.
		page, err := s.Channel("foo").History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("foo History: %v", err)
		}
		if len(page.ChannelMessages) != 1 {
			t.Errorf("foo History len = %d, want 1", len(page.ChannelMessages))
		}
	})

	t.Run("SameChannelInstanceReturned", func(t *testing.T) {
		s := f(t)
		a := s.Channel("foo")
		b := s.Channel("foo")

		// Publish via a; History via b must see it.
		cm, _, err := a.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		page, err := b.History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 1 || page.ChannelMessages[0].ChannelSerial != cm.ChannelSerial {
			t.Errorf("Channel(\"foo\") handed out different stores: a wrote %q, b sees %+v", cm.ChannelSerial, channelSerialsOf(page))
		}
	})

	t.Run("ConcurrentAppendsAreUniqueAndOrdered", func(t *testing.T) {
		s := f(t)
		ch := s.Channel("foo")

		const workers = 10
		const perWorker = 20

		var wg sync.WaitGroup
		mu := sync.Mutex{}
		var allSerials []string
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				for range perWorker {
					cm, _, err := ch.AppendChannelMessage(context.Background(), []*protocol.Message{{Name: "x"}})
					if err != nil {
						t.Errorf("publish: %v", err)
						return
					}
					mu.Lock()
					allSerials = append(allSerials, cm.ChannelSerial)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		// All distinct.
		seen := make(map[string]struct{}, len(allSerials))
		for _, sv := range allSerials {
			if _, dup := seen[sv]; dup {
				t.Errorf("duplicate channelSerial: %q", sv)
			}
			seen[sv] = struct{}{}
		}

		// History returns the same number of entries, ordered.
		page, err := ch.History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != workers*perWorker {
			t.Errorf("History len = %d, want %d", len(page.ChannelMessages), workers*perWorker)
		}
		var prev string
		for i, cm := range page.ChannelMessages {
			if cm.ChannelSerial <= prev {
				t.Errorf("page[%d] not monotonic: %q <= %q", i, cm.ChannelSerial, prev)
			}
			prev = cm.ChannelSerial
		}
	})
}

// threeDigit zero-pads i to a 3-digit string ("042"). Mirrors the
// padding the serial package uses for the per-message idx suffix.
func threeDigit(i int) string {
	return fmt.Sprintf("%03d", i)
}

func channelSerialsOf(p storage.HistoryPage) []string {
	out := make([]string, 0, len(p.ChannelMessages))
	for _, cm := range p.ChannelMessages {
		out = append(out, cm.ChannelSerial)
	}
	return out
}
