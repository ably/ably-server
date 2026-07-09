// Package storagetest exposes a contract-test suite that every
// storage.ChannelStore implementation should satisfy. Backends import
// this package from their _test.go and call RunChannelStoreTests with
// a factory that produces a fresh storage for each subtest.
package storagetest

import (
	"context"
	"errors"
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

	t.Run("GeneratesBatchIDAndStampsMessageIDs", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// A publish with no message ids is stamped with a fresh 8-char
		// base64 batch id, and each Message.ID = "<batchID>:<idx>"
		// (DESIGN.md §8, TASK-18 AC#1).
		cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "a"}, {Name: "b"}})
		if err != nil {
			t.Fatalf("Store: %v", err)
		}
		if len(cm.ID) != 8 {
			t.Errorf("ChannelMessage.ID = %q, want an 8-char base64 batch id", cm.ID)
		}
		for i, m := range cm.Messages {
			want := cm.ID + ":" + strconv.Itoa(i)
			if m.ID != want {
				t.Errorf("Messages[%d].ID = %q, want %q", i, m.ID, want)
			}
		}
	})

	t.Run("RejectsMismatchedClientBatchIDs", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// A client-supplied multi-message batch whose ids do not conform
		// to "<batchID>:<idx>" is rejected (TASK-18 AC#2).
		_, _, err := ch.Store(context.Background(), []*protocol.Message{{ID: "x:0"}, {ID: "y:1"}})
		if !errors.Is(err, storage.ErrInvalidMessageID) {
			t.Fatalf("Store err = %v, want ErrInvalidMessageID", err)
		}
		// Nothing should have been persisted.
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 0 {
			t.Errorf("History len = %d, want 0 (rejected publish must not persist)", len(page.ChannelMessages))
		}
	})

	t.Run("IdempotentByBatchID", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// A conforming client-supplied batch is idempotent on its batch
		// id: republishing the same "<batchID>:<idx>" ids returns the
		// original (the shared batch id collides on the first message id).
		first, _, err := ch.Store(context.Background(), []*protocol.Message{{ID: "b:0"}, {ID: "b:1"}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
		if first.ID != "b" {
			t.Errorf("ChannelMessage.ID = %q, want %q", first.ID, "b")
		}

		second, idemp, err := ch.Store(context.Background(), []*protocol.Message{{ID: "b:0"}, {ID: "b:1"}})
		if err != nil {
			t.Fatalf("second publish: %v", err)
		}
		if !idemp {
			t.Error("idempotent=false; expected match on batch id 'b'")
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

	t.Run("HistoryEndChannelSerialIsInclusiveUpperBound", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var cms []*protocol.ChannelMessage
		for i := range 5 {
			cm, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "x", Data: i}})
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			cms = append(cms, cm)
		}

		// Cap at cms[2]: forwards should return cms[0..2] (inclusive),
		// backwards should return cms[2..0] in reverse.
		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction:        storage.DirectionForwards,
			EndChannelSerial: cms[2].ChannelSerial,
		})
		if err != nil {
			t.Fatalf("forwards History: %v", err)
		}
		want := []string{cms[0].ChannelSerial, cms[1].ChannelSerial, cms[2].ChannelSerial}
		if got := channelSerialsOf(page); !equalStrings(got, want) {
			t.Errorf("forwards = %v, want %v", got, want)
		}

		page, err = ch.History(context.Background(), storage.HistoryQuery{
			Direction:        storage.DirectionBackwards,
			EndChannelSerial: cms[2].ChannelSerial,
		})
		if err != nil {
			t.Fatalf("backwards History: %v", err)
		}
		want = []string{cms[2].ChannelSerial, cms[1].ChannelSerial, cms[0].ChannelSerial}
		if got := channelSerialsOf(page); !equalStrings(got, want) {
			t.Errorf("backwards = %v, want %v", got, want)
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

	t.Run("PresenceEnterPopulatesMembers", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		mustEnter(t, ch, "conn-1", "alice", "hi")
		mustEnter(t, ch, "conn-2", "bob", "yo")

		members, asOf, err := ch.Members(context.Background())
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if asOf == "" {
			t.Error("asOf serial empty after presence publishes")
		}
		got := membersByKey(members)
		if len(got) != 2 {
			t.Fatalf("members = %d, want 2", len(got))
		}
		if got[storage.MemberKey("conn-1", "alice")] == nil {
			t.Error("alice (conn-1) missing from members")
		}
		if got[storage.MemberKey("conn-2", "bob")] == nil {
			t.Error("bob (conn-2) missing from members")
		}
	})

	t.Run("PresenceUpdateReplacesMember", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		mustEnter(t, ch, "conn-1", "alice", "v1")
		mustPresence(t, ch, &protocol.PresenceMessage{
			Action: protocol.PresenceUpdate, ConnectionID: "conn-1", ClientID: "alice", Data: "v2",
		})
		members, _, err := ch.Members(context.Background())
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if len(members) != 1 {
			t.Fatalf("members = %d, want 1 (update replaces, not adds)", len(members))
		}
		if members[0].Data != "v2" {
			t.Errorf("member Data = %v, want v2 (latest wins)", members[0].Data)
		}
	})

	t.Run("PresenceLeaveRemovesMember", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		mustEnter(t, ch, "conn-1", "alice", "hi")
		mustPresence(t, ch, &protocol.PresenceMessage{
			Action: protocol.PresenceLeave, ConnectionID: "conn-1", ClientID: "alice",
		})
		members, _, err := ch.Members(context.Background())
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if len(members) != 0 {
			t.Errorf("members = %d, want 0 after leave", len(members))
		}
	})

	t.Run("PresenceSameClientDistinctConnsAreDistinctMembers", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		mustEnter(t, ch, "conn-1", "alice", "")
		mustEnter(t, ch, "conn-2", "alice", "")
		members, _, err := ch.Members(context.Background())
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if len(members) != 2 {
			t.Errorf("members = %d, want 2 (same clientId over two connections)", len(members))
		}
	})

	t.Run("PresenceIdempotentByID", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		first, idemp, err := ch.StorePresence(context.Background(), []*protocol.PresenceMessage{
			{ID: "p1", Action: protocol.PresenceEnter, ConnectionID: "conn-1", ClientID: "alice", Data: "v1"},
		})
		if err != nil || idemp {
			t.Fatalf("first StorePresence: err=%v idempotent=%v", err, idemp)
		}
		second, idemp, err := ch.StorePresence(context.Background(), []*protocol.PresenceMessage{
			{ID: "p1", Action: protocol.PresenceEnter, ConnectionID: "conn-1", ClientID: "alice", Data: "v2"},
		})
		if err != nil {
			t.Fatalf("second StorePresence: %v", err)
		}
		if !idemp {
			t.Error("idempotent=false on duplicate presence id")
		}
		if second.ChannelSerial != first.ChannelSerial {
			t.Errorf("returned serial = %q, want original %q", second.ChannelSerial, first.ChannelSerial)
		}
		members, _, _ := ch.Members(context.Background())
		if len(members) != 1 || members[0].Data != "v1" {
			t.Errorf("members = %+v, want single alice with original v1", members)
		}
	})

	t.Run("PresenceAndMessageStreamsAreKindFiltered", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		if _, _, err := ch.Store(context.Background(), []*protocol.Message{{Name: "m"}}); err != nil {
			t.Fatalf("Store: %v", err)
		}
		mustEnter(t, ch, "conn-1", "alice", "hi")

		// Message history (default kind) excludes the presence cm.
		msgPage, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("message History: %v", err)
		}
		if len(msgPage.ChannelMessages) != 1 || len(msgPage.ChannelMessages[0].Messages) != 1 {
			t.Fatalf("message history = %v, want exactly one message cm", channelSerialsOf(msgPage))
		}
		for _, cm := range msgPage.ChannelMessages {
			if len(cm.Presence) != 0 {
				t.Error("message history leaked a presence cm")
			}
		}

		// Presence history excludes the message cm.
		presPage, err := ch.History(context.Background(), storage.HistoryQuery{Kind: storage.KindPresence, Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("presence History: %v", err)
		}
		if len(presPage.ChannelMessages) != 1 || len(presPage.ChannelMessages[0].Presence) != 1 {
			t.Fatalf("presence history = %v, want exactly one presence cm", channelSerialsOf(presPage))
		}
		if got := presPage.ChannelMessages[0].Presence[0].ClientID; got != "alice" {
			t.Errorf("presence history clientId = %q, want alice", got)
		}
	})

	// ---- Mutable messages (DESIGN.md §13) ------------------------------

	t.Run("CreateStampsActionAndVersion", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		m := mustCreate(t, ch, &protocol.Message{Name: "x", Data: "v1", ClientID: "alice"})
		if m.Action != protocol.MessageCreate {
			t.Errorf("create action = %v, want create", m.Action)
		}
		if m.Version == nil || m.Version.Serial != m.Serial {
			t.Errorf("create version = %+v, want version.serial == serial %q", m.Version, m.Serial)
		}
		if m.Version != nil && m.Version.ClientID != "alice" {
			t.Errorf("create version.clientId = %q, want alice", m.Version.ClientID)
		}
	})

	t.Run("MutateUpdateProducesNewVersionStableIdentity", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Name: "greeting", Data: "v1", ClientID: "alice"})

		updated := mustMutate(t, ch, &protocol.Message{
			Action: protocol.MessageUpdate, Serial: created.Serial, Data: "v2", ClientID: "bob",
			Version: &protocol.MessageVersion{Description: "edit"},
		})
		if updated.Serial != created.Serial {
			t.Errorf("updated serial = %q, want stable identity %q", updated.Serial, created.Serial)
		}
		if updated.Action != protocol.MessageUpdate {
			t.Errorf("updated action = %v, want update", updated.Action)
		}
		if updated.Data != "v2" {
			t.Errorf("updated data = %v, want v2", updated.Data)
		}
		if updated.Version == nil || updated.Version.Serial == created.Serial || updated.Version.Serial <= created.Version.Serial {
			t.Errorf("updated version.serial = %v, want fresh and > create's %q", updated.Version, created.Version.Serial)
		}
		if updated.Version != nil && (updated.Version.ClientID != "bob" || updated.Version.Description != "edit") {
			t.Errorf("updated version metadata = %+v, want operator bob / 'edit'", updated.Version)
		}

		latest, err := ch.LatestVersion(context.Background(), created.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		if latest.Data != "v2" || latest.ClientID != "alice" {
			t.Errorf("latest = data %v / creator %q, want v2 / alice (creator carried forward)", latest.Data, latest.ClientID)
		}
	})

	t.Run("MutateShallowMixinCarriesForwardUnsuppliedFields", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Name: "title", Data: "body", ClientID: "alice"})

		// Update only data — name must carry forward.
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: created.Serial, Data: "body2", ClientID: "alice"})
		latest, _ := ch.LatestVersion(context.Background(), created.Serial)
		if latest.Name != "title" || latest.Data != "body2" {
			t.Errorf("after data-only update: name=%q data=%v, want title/body2", latest.Name, latest.Data)
		}

		// Update only name — data must carry forward.
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: created.Serial, Name: "title2", ClientID: "alice"})
		latest, _ = ch.LatestVersion(context.Background(), created.Serial)
		if latest.Name != "title2" || latest.Data != "body2" {
			t.Errorf("after name-only update: name=%q data=%v, want title2/body2", latest.Name, latest.Data)
		}
	})

	t.Run("MutateAppendConcatenatesData", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Data: "Hello", ClientID: "alice"})
		appended := mustMutate(t, ch, &protocol.Message{Action: protocol.MessageAppend, Serial: created.Serial, Data: ", world", ClientID: "alice"})
		if appended.Data != "Hello, world" {
			t.Errorf("appended data = %v, want %q", appended.Data, "Hello, world")
		}
		// Stored/fanned out as a full aggregated update carrying the
		// incremental delta in Alt (DESIGN.md §13.3).
		if appended.Action != protocol.MessageUpdate {
			t.Errorf("append action = %v, want update (aggregate)", appended.Action)
		}
		delta := appended.Alt[protocol.DeltaAppend]
		if delta == nil {
			t.Fatalf("append version carries no %q delta", protocol.DeltaAppend)
		}
		if delta.Action != protocol.MessageAppend || delta.Data != ", world" {
			t.Errorf("delta = action %v data %v, want append / %q", delta.Action, delta.Data, ", world")
		}
		if delta.Serial != created.Serial {
			t.Errorf("delta serial = %q, want stable identity %q", delta.Serial, created.Serial)
		}
	})

	t.Run("MutateAppendRejectsIncompatibleData", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		strMsg := mustCreate(t, ch, &protocol.Message{Data: "hello", ClientID: "alice"})
		if _, _, err := ch.Mutate(context.Background(), &protocol.Message{
			Action: protocol.MessageAppend, Serial: strMsg.Serial, Data: []byte("x"), ClientID: "alice",
		}); !errors.Is(err, storage.ErrIncompatibleAppend) {
			t.Errorf("binary-onto-string append err = %v, want ErrIncompatibleAppend", err)
		}
		// The rejected append must not have applied.
		if latest, _ := ch.LatestVersion(context.Background(), strMsg.Serial); latest.Data != "hello" {
			t.Errorf("data after rejected append = %v, want unchanged hello", latest.Data)
		}

		// Binary-onto-binary concatenates.
		binMsg := mustCreate(t, ch, &protocol.Message{Data: []byte("ab"), ClientID: "alice"})
		appended := mustMutate(t, ch, &protocol.Message{Action: protocol.MessageAppend, Serial: binMsg.Serial, Data: []byte("cd"), ClientID: "alice"})
		if got, ok := appended.Data.([]byte); !ok || string(got) != "abcd" {
			t.Errorf("binary append data = %v, want abcd", appended.Data)
		}
	})

	t.Run("MutateAppendVersionHistoryCollapses", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Data: "a", ClientID: "alice"})
		for _, chunk := range []string{"b", "c", "d"} {
			mustMutate(t, ch, &protocol.Message{Action: protocol.MessageAppend, Serial: created.Serial, Data: chunk, ClientID: "alice"})
		}

		// The aggregate is the full concatenation and stays queryable.
		latest, err := ch.LatestVersion(context.Background(), created.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		if latest.Data != "abcd" {
			t.Errorf("aggregate data = %v, want abcd", latest.Data)
		}

		// Version history reflects the aggregate, not each append: the
		// run of appends collapses to a single version after the create.
		vers, err := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Versions: %v", err)
		}
		if n := itemCountVersions(vers); n != 2 {
			t.Fatalf("versions after create+3 appends = %d, want 2 (create + collapsed aggregate)", n)
		}
		agg := vers.ChannelMessages[len(vers.ChannelMessages)-1].Messages[0]
		if agg.Data != "abcd" {
			t.Errorf("collapsed aggregate data = %v, want abcd", agg.Data)
		}

		// An update between append runs is a checkpoint: it and a later
		// append run both survive the collapse.
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: created.Serial, Data: "X", ClientID: "alice"})
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageAppend, Serial: created.Serial, Data: "Y", ClientID: "alice"})
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageAppend, Serial: created.Serial, Data: "Z", ClientID: "alice"})
		vers, _ = ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if n := itemCountVersions(vers); n != 4 {
			t.Errorf("versions after create+appends+update+appends = %d, want 4 (create, aggregate, update, aggregate)", n)
		}
		if final, _ := ch.LatestVersion(context.Background(), created.Serial); final.Data != "XYZ" {
			t.Errorf("final aggregate = %v, want XYZ", final.Data)
		}
	})

	t.Run("MutateDeleteIsSoftTombstone", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Name: "n", Data: "secret", ClientID: "alice"})
		deleted := mustMutate(t, ch, &protocol.Message{Action: protocol.MessageDelete, Serial: created.Serial, ClientID: "mod"})
		if deleted.Action != protocol.MessageDelete {
			t.Errorf("delete action = %v, want delete", deleted.Action)
		}
		if deleted.Data != nil || deleted.Name != "" {
			t.Errorf("tombstone carries data=%v name=%q, want both cleared", deleted.Data, deleted.Name)
		}

		// Soft: the message and all versions remain queryable.
		latest, err := ch.LatestVersion(context.Background(), created.Serial)
		if err != nil {
			t.Fatalf("LatestVersion after delete: %v (must stay queryable)", err)
		}
		if latest.Action != protocol.MessageDelete {
			t.Errorf("latest action after delete = %v, want delete", latest.Action)
		}
		vers, err := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Versions after delete: %v", err)
		}
		if n := itemCountVersions(vers); n != 2 {
			t.Errorf("versions after delete = %d, want 2 (create + delete)", n)
		}
	})

	t.Run("MutateTargetNotFound", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		_, _, err := ch.Mutate(context.Background(), &protocol.Message{
			Action: protocol.MessageUpdate, Serial: "00000000000001-000@nonexistent:000", Data: "x", ClientID: "alice",
		})
		if !errors.Is(err, storage.ErrTargetNotFound) {
			t.Errorf("Mutate on unknown target err = %v, want ErrTargetNotFound", err)
		}
		if _, err := ch.LatestVersion(context.Background(), "00000000000001-000@nonexistent:000"); !errors.Is(err, storage.ErrTargetNotFound) {
			t.Errorf("LatestVersion unknown err = %v, want ErrTargetNotFound", err)
		}
	})

	t.Run("MutateIdempotentByID", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Data: "v1", ClientID: "alice"})

		first, idemp, err := ch.Mutate(context.Background(), &protocol.Message{
			ID: "edit-1", Action: protocol.MessageUpdate, Serial: created.Serial, Data: "v2", ClientID: "alice",
		})
		if err != nil || idemp {
			t.Fatalf("first mutate: err=%v idempotent=%v", err, idemp)
		}
		second, idemp, err := ch.Mutate(context.Background(), &protocol.Message{
			ID: "edit-1", Action: protocol.MessageUpdate, Serial: created.Serial, Data: "v3", ClientID: "alice",
		})
		if err != nil {
			t.Fatalf("second mutate: %v", err)
		}
		if !idemp {
			t.Error("idempotent=false on duplicate mutation id")
		}
		if second.ChannelSerial != first.ChannelSerial {
			t.Errorf("returned serial = %q, want original %q", second.ChannelSerial, first.ChannelSerial)
		}
		latest, _ := ch.LatestVersion(context.Background(), created.Serial)
		if latest.Data != "v2" {
			t.Errorf("latest data = %v, want v2 (duplicate mutation must not apply)", latest.Data)
		}
		vers, _ := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if n := itemCountVersions(vers); n != 2 {
			t.Errorf("versions = %d, want 2 (create + one applied edit)", n)
		}
	})

	t.Run("VersionsOrderedByVersion", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Data: "v1", ClientID: "alice"})
		u1 := mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: created.Serial, Data: "v2", ClientID: "alice"})
		u2 := mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: created.Serial, Data: "v3", ClientID: "alice"})

		fwd, err := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Versions forwards: %v", err)
		}
		gotFwd := versionSerials(fwd)
		wantFwd := []string{created.Version.Serial, u1.Version.Serial, u2.Version.Serial}
		if !equalStrings(gotFwd, wantFwd) {
			t.Errorf("versions forwards = %v, want %v", gotFwd, wantFwd)
		}

		bwd, _ := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionBackwards})
		gotBwd := versionSerials(bwd)
		wantBwd := []string{u2.Version.Serial, u1.Version.Serial, created.Version.Serial}
		if !equalStrings(gotBwd, wantBwd) {
			t.Errorf("versions backwards = %v, want %v", gotBwd, wantBwd)
		}

		// Limit + HasMore at version granularity.
		lim, _ := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 2})
		if got := versionSerials(lim); !equalStrings(got, wantFwd[:2]) || !lim.HasMore {
			t.Errorf("versions limit=2 = %v hasMore=%v, want %v hasMore=true", got, lim.HasMore, wantFwd[:2])
		}
	})

	t.Run("CollapsedHistoryShowsLatestAtCreatePosition", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		m1 := mustCreate(t, ch, &protocol.Message{Name: "m1", Data: "1", ClientID: "alice"})
		m2 := mustCreate(t, ch, &protocol.Message{Name: "m2", Data: "2", ClientID: "alice"})
		m3 := mustCreate(t, ch, &protocol.Message{Name: "m3", Data: "3", ClientID: "alice"})
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: m2.Serial, Data: "2-edited", ClientID: "alice"})

		// Collapsed forwards: latest of each, positioned at create order.
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
		if err != nil {
			t.Fatalf("collapsed History: %v", err)
		}
		var gotData []any
		var gotSerials, gotChannelSerials []string
		for _, cm := range page.ChannelMessages {
			for _, m := range cm.Messages {
				gotData = append(gotData, m.Data)
				gotSerials = append(gotSerials, m.Serial)
				gotChannelSerials = append(gotChannelSerials, cm.ChannelSerial)
			}
		}
		if len(gotData) != 3 {
			t.Fatalf("collapsed messages = %d, want 3 (one per message)", len(gotData))
		}
		if gotData[0] != "1" || gotData[1] != "2-edited" || gotData[2] != "3" {
			t.Errorf("collapsed data = %v, want [1 2-edited 3]", gotData)
		}
		// Stable identities, positioned at create serials.
		wantSerials := []string{m1.Serial, m2.Serial, m3.Serial}
		if !equalStrings(gotSerials, wantSerials) {
			t.Errorf("collapsed serials = %v, want %v (stable identities)", gotSerials, wantSerials)
		}
		wantCS := []string{
			storage.CreateChannelSerial(m1.Serial),
			storage.CreateChannelSerial(m2.Serial),
			storage.CreateChannelSerial(m3.Serial),
		}
		if !equalStrings(gotChannelSerials, wantCS) {
			t.Errorf("collapsed channelSerials = %v, want create positions %v", gotChannelSerials, wantCS)
		}
	})

	t.Run("CollapsedVsRawMessageHistory", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Data: "v1", ClientID: "alice"})
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageUpdate, Serial: created.Serial, Data: "v2", ClientID: "alice"})

		// Raw stream (Collapse=false): both the create and the update cm
		// appear in stream order — live/resume must see every version.
		raw, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("raw History: %v", err)
		}
		if len(raw.ChannelMessages) != 2 {
			t.Errorf("raw history cms = %d, want 2 (create + update version)", len(raw.ChannelMessages))
		}

		// Collapsed: one entry, latest content.
		col, _ := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
		if len(col.ChannelMessages) != 1 || len(col.ChannelMessages[0].Messages) != 1 {
			t.Fatalf("collapsed cms = %d, want 1", len(col.ChannelMessages))
		}
		if col.ChannelMessages[0].Messages[0].Data != "v2" {
			t.Errorf("collapsed data = %v, want v2", col.ChannelMessages[0].Messages[0].Data)
		}
	})

	t.Run("CollapsedDeletedShowsAsTombstone", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &protocol.Message{Data: "v1", ClientID: "alice"})
		mustMutate(t, ch, &protocol.Message{Action: protocol.MessageDelete, Serial: created.Serial, ClientID: "mod"})

		col, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
		if err != nil {
			t.Fatalf("collapsed History: %v", err)
		}
		if len(col.ChannelMessages) != 1 || len(col.ChannelMessages[0].Messages) != 1 {
			t.Fatalf("collapsed cms = %d, want 1 (deleted message still positioned)", len(col.ChannelMessages))
		}
		if got := col.ChannelMessages[0].Messages[0]; got.Action != protocol.MessageDelete {
			t.Errorf("collapsed deleted action = %v, want delete (tombstone)", got.Action)
		}
	})
}

// mustCreate publishes a single create message and returns the stamped
// Message (with its serial + version) from the persisted cm.
func mustCreate(t *testing.T, ch storage.ChannelStore, m *protocol.Message) *protocol.Message {
	t.Helper()
	cm, _, err := ch.Store(context.Background(), []*protocol.Message{m})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	return cm.Messages[0]
}

// mustMutate applies a mutation and returns the persisted merged version.
func mustMutate(t *testing.T, ch storage.ChannelStore, mut *protocol.Message) *protocol.Message {
	t.Helper()
	cm, _, err := ch.Mutate(context.Background(), mut)
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	return cm.Messages[0]
}

// itemCountVersions totals the Messages across a versions page.
func itemCountVersions(page storage.HistoryPage) int {
	n := 0
	for _, cm := range page.ChannelMessages {
		n += len(cm.Messages)
	}
	return n
}

// versionSerials flattens a versions page to the per-version serials
// (Version.Serial), in page order.
func versionSerials(page storage.HistoryPage) []string {
	var out []string
	for _, cm := range page.ChannelMessages {
		for _, m := range cm.Messages {
			out = append(out, storage.VersionSerial(m))
		}
	}
	return out
}

// mustEnter publishes a single ENTER for (connID, clientID) with data.
func mustEnter(t *testing.T, ch storage.ChannelStore, connID, clientID string, data any) {
	t.Helper()
	mustPresence(t, ch, &protocol.PresenceMessage{
		Action: protocol.PresenceEnter, ConnectionID: connID, ClientID: clientID, Data: data,
	})
}

// mustPresence calls StorePresence and fails the test on error.
func mustPresence(t *testing.T, ch storage.ChannelStore, pms ...*protocol.PresenceMessage) *protocol.ChannelMessage {
	t.Helper()
	cm, _, err := ch.StorePresence(context.Background(), pms)
	if err != nil {
		t.Fatalf("StorePresence: %v", err)
	}
	return cm
}

// membersByKey indexes a membership set by its (connectionId, clientId) key.
func membersByKey(members []*protocol.PresenceMessage) map[string]*protocol.PresenceMessage {
	out := make(map[string]*protocol.PresenceMessage, len(members))
	for _, p := range members {
		out[storage.MemberKey(p.ConnectionID, p.ClientID)] = p
	}
	return out
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
	mu          sync.Mutex
	initCurrent string
	initInitial string
	initCount   int
	appends     []*protocol.ChannelMessage
}

func newCapturingAppender() *capturingAppender {
	return &capturingAppender{}
}

func (a *capturingAppender) Initialize(current, initial string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.initCurrent = current
	a.initInitial = initial
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
	return a.initCurrent
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
