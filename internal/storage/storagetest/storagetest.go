// Package storagetest exposes a contract-test suite that every
// storage.ChannelStore implementation should satisfy. Backends import
// this package from their _test.go and call RunChannelStoreTests with
// a factory that produces a fresh storage for each subtest.
package storagetest

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

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

	t.Run("ChannelInitializesAppenderWithWatermark", func(t *testing.T) {
		s := f(t)
		a := newCapturingAppender()
		if _, err := s.Channel(context.Background(), "foo", a); err != nil {
			t.Fatalf("Channel: %v", err)
		}
		got := a.initialized()
		if got == "" {
			t.Fatal("appender.Initialize was not called (or called with empty serial)")
		}
	})

	t.Run("ChannelInitializeNotRecalledOnSecondCall", func(t *testing.T) {
		s := f(t)
		a := newCapturingAppender()
		if _, err := s.Channel(context.Background(), "foo", a); err != nil {
			t.Fatalf("first Channel: %v", err)
		}
		// Second call with a different appender — the backend is
		// documented to ignore the new appender, so the original
		// appender's Initialize count should remain at 1 and the new
		// appender should not be Initialize'd at all (or if it is,
		// initialize counts independently — but the channelStore→
		// appender binding does not change).
		b := newCapturingAppender()
		if _, err := s.Channel(context.Background(), "foo", b); err != nil {
			t.Fatalf("second Channel: %v", err)
		}
		if got := a.initializeCount(); got != 1 {
			t.Errorf("original appender Initialize count = %d, want 1", got)
		}
	})

	t.Run("AppendStampsChannelSerialAndMessageSerials", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		cm, idempotent, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
		if err != nil {
			t.Fatalf("Store: %v", err)
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
		ch := mustChannel(t, s, "foo")
		msgs := []*protocol.Message{{Name: "a"}, {Name: "b"}, {Name: "c"}}
		cm, _, err := ch.Store(context.Background(), msgs)
		if err != nil {
			t.Fatalf("Store: %v", err)
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
		ch := mustChannel(t, s, "foo")
		var prev string
		for i := range 5 {
			cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
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
		ch := mustChannel(t, s, "foo")

		first, idemp, err := ch.Store(context.Background(), []*protocol.Message{{ID: "dup", Data: "v1"}})
		if err != nil || idemp {
			t.Fatalf("first publish: err=%v idempotent=%v", err, idemp)
		}

		// Re-publish with the same ID but different payload — should
		// return the original ChannelMessage and not re-append.
		second, idemp, err := ch.Store(context.Background(), []*protocol.Message{{ID: "dup", Data: "v2"}})
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
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 1 {
			t.Errorf("History len = %d, want 1 (duplicate must not persist)", len(page.ChannelMessages))
		}
	})

	t.Run("IdempotencyMatchesAnyMessageInBatch", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		first, _, err := ch.Store(context.Background(), []*protocol.Message{{ID: "a"}, {ID: "b"}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}

		// A new publish where any contained id matches should be
		// treated as a duplicate.
		second, idemp, err := ch.Store(context.Background(), []*protocol.Message{{ID: "c"}, {ID: "b"}})
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
		ch := mustChannel(t, s, "foo")

		first, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
		second, idemp, err := ch.Store(context.Background(), []*protocol.Message{{Name: "y"}})
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
		ch := mustChannel(t, s, "foo")
		if _, _, err := ch.Store(context.Background(), nil); err == nil {
			t.Error("Store(nil) returned no error")
		}
		if _, _, err := ch.Store(context.Background(), []*protocol.Message{}); err == nil {
			t.Error("Store([]) returned no error")
		}
	})

	t.Run("HistoryForwardsReturnsAllInOrder", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var serials []string
		for i := range 4 {
			cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x", Data: i}})
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			serials = append(serials, cm.ChannelSerial)
		}

		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
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

	t.Run("HistoryBackwardsReturnsNewestFirst", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var serials []string
		for i := range 4 {
			cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x", Data: i}})
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			serials = append(serials, cm.ChannelSerial)
		}

		// Default (zero-value Direction) is backwards, matching Ably.
		page, err := ch.History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 4 {
			t.Fatalf("History len = %d, want 4", len(page.ChannelMessages))
		}
		for i, cm := range page.ChannelMessages {
			want := serials[len(serials)-1-i]
			if cm.ChannelSerial != want {
				t.Errorf("page[%d].ChannelSerial = %q, want %q (newest first)", i, cm.ChannelSerial, want)
			}
		}
	})

	t.Run("HistoryBackwardsReversesWithinBatch", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// Publish two batches; observe that backwards reverses BOTH
		// across publishes AND within each publish (Ably-verified).
		_, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "a0"}, {Name: "a1"}, {Name: "a2"}})
		if err != nil {
			t.Fatalf("publish batch1: %v", err)
		}
		_, _, err = ch.Store(context.Background(), []*protocol.Message{{Name: "b0"}, {Name: "b1"}})
		if err != nil {
			t.Fatalf("publish batch2: %v", err)
		}

		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionBackwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 2 {
			t.Fatalf("ChannelMessages len = %d, want 2", len(page.ChannelMessages))
		}
		// Flatten to a name sequence and compare against the
		// full-reversal Ably emits: [b1, b0, a2, a1, a0].
		var got []string
		for _, cm := range page.ChannelMessages {
			for _, m := range cm.Messages {
				got = append(got, m.Name)
			}
		}
		want := []string{"b1", "b0", "a2", "a1", "a0"}
		if !equalStrings(got, want) {
			t.Errorf("backwards flatten = %v, want %v", got, want)
		}
	})

	t.Run("HistoryForwardsPreservesWithinBatch", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		_, _, _ = ch.Store(context.Background(), []*protocol.Message{{Name: "a0"}, {Name: "a1"}, {Name: "a2"}})
		_, _, _ = ch.Store(context.Background(), []*protocol.Message{{Name: "b0"}, {Name: "b1"}})

		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		var got []string
		for _, cm := range page.ChannelMessages {
			for _, m := range cm.Messages {
				got = append(got, m.Name)
			}
		}
		want := []string{"a0", "a1", "a2", "b0", "b1"}
		if !equalStrings(got, want) {
			t.Errorf("forwards flatten = %v, want %v", got, want)
		}
	})

	t.Run("HistoryDoesNotMutatePersistedState", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		_, _, _ = ch.Store(context.Background(), []*protocol.Message{{Name: "a0"}, {Name: "a1"}, {Name: "a2"}})

		// Backwards: must reverse Messages in the returned page WITHOUT
		// reordering the persisted batch.
		if _, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionBackwards}); err != nil {
			t.Fatalf("backwards History: %v", err)
		}
		// Subsequent forwards History must still see natural idx order.
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("forwards History: %v", err)
		}
		if len(page.ChannelMessages) != 1 || len(page.ChannelMessages[0].Messages) != 3 {
			t.Fatalf("unexpected page shape: %d ChannelMessages, first len=%d",
				len(page.ChannelMessages), len(page.ChannelMessages[0].Messages))
		}
		gotNames := []string{
			page.ChannelMessages[0].Messages[0].Name,
			page.ChannelMessages[0].Messages[1].Name,
			page.ChannelMessages[0].Messages[2].Name,
		}
		wantNames := []string{"a0", "a1", "a2"}
		if !equalStrings(gotNames, wantNames) {
			t.Errorf("post-backwards forwards Messages = %v, want %v (persisted state must not be mutated)", gotNames, wantNames)
		}
	})

	t.Run("HistoryCursorForwardsExcludesCursorMessage", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var cms []*protocol.ChannelMessage
		for range 4 {
			cm, _, _ := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
			cms = append(cms, cm)
		}

		// Cursor is a Message.Serial — pass the single Message's
		// Serial from cms[1] (it has exactly one Message at idx 0).
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Cursor:    cms[1].Messages[0].Serial,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 2 {
			t.Fatalf("History len = %d, want 2 (cms[2] and cms[3])", len(page.ChannelMessages))
		}
		if page.ChannelMessages[0].ChannelSerial != cms[2].ChannelSerial {
			t.Errorf("first result = %q, want %q", page.ChannelMessages[0].ChannelSerial, cms[2].ChannelSerial)
		}
	})

	t.Run("HistoryCursorBackwardsExcludesCursorMessage", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var cms []*protocol.ChannelMessage
		for range 4 {
			cm, _, _ := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
			cms = append(cms, cm)
		}

		// Backwards cursor at cms[2]'s Message.Serial returns cms[1], cms[0].
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionBackwards,
			Cursor:    cms[2].Messages[0].Serial,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 2 {
			t.Fatalf("History len = %d, want 2 (cms[1] and cms[0] in reverse)", len(page.ChannelMessages))
		}
		if page.ChannelMessages[0].ChannelSerial != cms[1].ChannelSerial {
			t.Errorf("first result = %q, want %q", page.ChannelMessages[0].ChannelSerial, cms[1].ChannelSerial)
		}
		if page.ChannelMessages[1].ChannelSerial != cms[0].ChannelSerial {
			t.Errorf("second result = %q, want %q", page.ChannelMessages[1].ChannelSerial, cms[0].ChannelSerial)
		}
	})

	t.Run("HistoryCursorMidBatchForwards", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// Publish a single 5-message batch.
		cm, _, _ := ch.Store(context.Background(), []*protocol.Message{
			{Name: "m0"}, {Name: "m1"}, {Name: "m2"}, {Name: "m3"}, {Name: "m4"},
		})

		// Cursor at idx 1 (m1) — forwards should return m2, m3, m4 in
		// one partial ChannelMessage with ChannelSerial == cm.ChannelSerial.
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Cursor:    cm.Messages[1].Serial,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		got := flattenNames(page)
		want := []string{"m2", "m3", "m4"}
		if !equalStrings(got, want) {
			t.Errorf("mid-batch forwards = %v, want %v", got, want)
		}
		if len(page.ChannelMessages) != 1 || page.ChannelMessages[0].ChannelSerial != cm.ChannelSerial {
			t.Errorf("expected one partial batch with channelSerial %q, got %+v",
				cm.ChannelSerial, channelSerialsOf(page))
		}
	})

	t.Run("HistoryCursorMidBatchBackwards", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		cm, _, _ := ch.Store(context.Background(), []*protocol.Message{
			{Name: "m0"}, {Name: "m1"}, {Name: "m2"}, {Name: "m3"}, {Name: "m4"},
		})

		// Cursor at idx 3 (m3), backwards — emit m2, m1, m0 (reverse idx).
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionBackwards,
			Cursor:    cm.Messages[3].Serial,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		got := flattenNames(page)
		want := []string{"m2", "m1", "m0"}
		if !equalStrings(got, want) {
			t.Errorf("mid-batch backwards = %v, want %v", got, want)
		}
	})

	t.Run("HistoryCursorBeforeAnyMessageReturnsAll", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		cm, _, _ := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})

		// A cursor that sorts before any persisted Message.Serial.
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Cursor:    "00000000000000-000@zzzzzzzzzz:000",
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 1 || page.ChannelMessages[0].ChannelSerial != cm.ChannelSerial {
			t.Errorf("History returned %+v, want [%q]", channelSerialsOf(page), cm.ChannelSerial)
		}
	})

	t.Run("HistoryCursorAtLastMessageReturnsEmpty", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		cm, _, _ := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})

		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Cursor:    cm.Messages[0].Serial,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 0 {
			t.Errorf("History len = %d, want 0", len(page.ChannelMessages))
		}
	})

	t.Run("HistoryLimitCountsMessagesNotBatches", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// One atomic batch of 5 messages — Ably's `limit` should slice it.
		_, _, _ = ch.Store(context.Background(), []*protocol.Message{
			{Name: "m0"}, {Name: "m1"}, {Name: "m2"}, {Name: "m3"}, {Name: "m4"},
		})

		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Limit:     2,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		got := flattenNames(page)
		if !equalStrings(got, []string{"m0", "m1"}) {
			t.Errorf("forwards limit=2 names = %v, want [m0 m1] (Ably splits batches)", got)
		}
		if !page.HasMore {
			t.Error("HasMore=false with limit < batch size")
		}

		page, err = ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionBackwards,
			Limit:     2,
		})
		if err != nil {
			t.Fatalf("backwards History: %v", err)
		}
		got = flattenNames(page)
		if !equalStrings(got, []string{"m4", "m3"}) {
			t.Errorf("backwards limit=2 names = %v, want [m4 m3]", got)
		}
	})

	t.Run("HistoryLimitForwardsSetsHasMore", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var serials []string
		for range 5 {
			cm, _, _ := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
			serials = append(serials, cm.ChannelSerial)
		}

		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Limit:     2,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 2 {
			t.Errorf("len = %d, want 2", len(page.ChannelMessages))
		}
		if !page.HasMore {
			t.Error("HasMore=false with limit < total")
		}
		if page.ChannelMessages[0].ChannelSerial != serials[0] {
			t.Errorf("first = %q, want %q (oldest first for forwards)", page.ChannelMessages[0].ChannelSerial, serials[0])
		}

		page, err = ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Limit:     5,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if page.HasMore {
			t.Error("HasMore=true with limit == total")
		}
	})

	t.Run("HistoryTimeBoundsFilter", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// 10ms sleeps encourage (but do not require) distinct timestamps
		// across publishes. Assertions are derived from each cm's own
		// serial, so the test stays correct when two publishes happen
		// to share a ms.
		var cms []*protocol.ChannelMessage
		for i := range 5 {
			if i > 0 {
				time.Sleep(10 * time.Millisecond)
			}
			cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x", Data: i}})
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			cms = append(cms, cm)
		}

		startTS := parseSerialTimestamp(t, cms[1].ChannelSerial)
		endTS := parseSerialTimestamp(t, cms[3].ChannelSerial)

		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Start:     startTS,
			End:       endTS,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		for _, cm := range page.ChannelMessages {
			cmTS := parseSerialTimestamp(t, cm.ChannelSerial)
			if cmTS < startTS || cmTS > endTS {
				t.Errorf("returned cm %q ts=%d outside [%d, %d]", cm.ChannelSerial, cmTS, startTS, endTS)
			}
		}
		got := serialSet(page)
		for _, want := range cms[1:4] {
			if !got[want.ChannelSerial] {
				t.Errorf("missing in-range cm %q", want.ChannelSerial)
			}
		}
	})

	t.Run("HistoryEndBeforeStartIsEmpty", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		for range 3 {
			_, _, _ = ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
		}
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction: storage.DirectionForwards,
			Start:     2_000_000_000_000,
			End:       1_000_000_000_000,
		})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 0 {
			t.Errorf("End<Start returned %d entries, want 0", len(page.ChannelMessages))
		}
	})

	t.Run("HistoryEmptyChannelReturnsEmpty", func(t *testing.T) {
		s := f(t)
		// Open the channel via Channel(...) so the store exists, then
		// query without ever publishing.
		page, err := mustChannel(t, s, "foo").History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 0 {
			t.Errorf("empty channel History len = %d, want 0", len(page.ChannelMessages))
		}
		if page.HasMore {
			t.Error("HasMore=true on empty channel")
		}
	})

	t.Run("HistoryLimitBackwardsTakesNewest", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var serials []string
		for range 5 {
			cm, _, _ := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
			serials = append(serials, cm.ChannelSerial)
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
		// Newest two, newest-first.
		if page.ChannelMessages[0].ChannelSerial != serials[4] {
			t.Errorf("first = %q, want %q (newest)", page.ChannelMessages[0].ChannelSerial, serials[4])
		}
		if page.ChannelMessages[1].ChannelSerial != serials[3] {
			t.Errorf("second = %q, want %q", page.ChannelMessages[1].ChannelSerial, serials[3])
		}
	})

	t.Run("ChannelsAreIsolated", func(t *testing.T) {
		s := f(t)

		fooCM, _, err := mustChannel(t, s, "foo").Store(context.Background(), []*protocol.Message{{ID: "x"}})
		if err != nil {
			t.Fatalf("foo publish: %v", err)
		}
		// Same ID, different channel — must NOT be idempotent.
		barCM, idemp, err := mustChannel(t, s, "bar").Store(context.Background(), []*protocol.Message{{ID: "x"}})
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
		page, err := mustChannel(t, s, "foo").History(context.Background(), storage.HistoryQuery{})
		if err != nil {
			t.Fatalf("foo History: %v", err)
		}
		if len(page.ChannelMessages) != 1 {
			t.Errorf("foo History len = %d, want 1", len(page.ChannelMessages))
		}
	})

	t.Run("SameChannelInstanceReturned", func(t *testing.T) {
		s := f(t)
		a := mustChannel(t, s, "foo")
		b := mustChannel(t, s, "foo")

		// Publish via a; History via b must see it.
		cm, _, err := a.Store(context.Background(), []*protocol.Message{{Name: "x"}})
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
		ch := mustChannel(t, s, "foo")

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
					cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x"}})
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
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
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

// capturingAppender records Initialize and Append calls so the
// contract tests can assert on the backend's appender-callback
// behaviour without needing a real core.Channel.
type capturingAppender struct {
	mu             sync.Mutex
	initSerial     string
	initCount      int
	appends        []*protocol.ChannelMessage
}

func newCapturingAppender() *capturingAppender {
	return &capturingAppender{}
}

func (a *capturingAppender) Initialize(channelSerial string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.initSerial = channelSerial
	a.initCount++
}

func (a *capturingAppender) Append(cm *protocol.ChannelMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appends = append(a.appends, cm)
}

func (a *capturingAppender) initialized() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.initSerial
}

func (a *capturingAppender) initializeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.initCount
}

// mustChannel materialises a ChannelStore for name, failing the test
// on error. Storage backends call appender.Initialize during this
// call; tests that pass a nil appender skip that callback.
func mustChannel(t *testing.T, s storage.Storage, name string) storage.ChannelStore {
	t.Helper()
	ch, err := s.Channel(context.Background(), name, nil)
	if err != nil {
		t.Fatalf("Channel(%q): %v", name, err)
	}
	return ch
}

// equalStrings reports whether a and b have the same length and
// element-wise content. Tests use this when comparing flattened
// Message-name sequences across directions.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if s != b[i] {
			return false
		}
	}
	return true
}

func channelSerialsOf(p storage.HistoryPage) []string {
	out := make([]string, 0, len(p.ChannelMessages))
	for _, cm := range p.ChannelMessages {
		out = append(out, cm.ChannelSerial)
	}
	return out
}

// parseSerialTimestamp pulls the 14-digit ms-since-epoch prefix out of
// a channelSerial. See internal/serial for the format.
func parseSerialTimestamp(t *testing.T, channelSerial string) int64 {
	t.Helper()
	if len(channelSerial) < 14 {
		t.Fatalf("channelSerial %q too short to contain a timestamp", channelSerial)
	}
	v, err := strconv.ParseInt(channelSerial[:14], 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp from %q: %v", channelSerial, err)
	}
	return v
}

// serialSet returns the set of ChannelSerials present in a page, for
// containment assertions in tests.
func serialSet(p storage.HistoryPage) map[string]bool {
	out := make(map[string]bool, len(p.ChannelMessages))
	for _, cm := range p.ChannelMessages {
		out[cm.ChannelSerial] = true
	}
	return out
}

// flattenNames returns the concatenated Message.Name sequence across
// every ChannelMessage in the page, preserving the page's order. Used
// to verify direction-aware emit order at Message granularity.
func flattenNames(p storage.HistoryPage) []string {
	var out []string
	for _, cm := range p.ChannelMessages {
		for _, m := range cm.Messages {
			out = append(out, m.Name)
		}
	}
	return out
}
