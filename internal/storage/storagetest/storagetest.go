// Package storagetest exposes a contract-test suite that every
// storage.ChannelStore implementation should satisfy. Backends import
// this package from their _test.go and call RunChannelStoreTests with
// a factory that produces a fresh storage for each subtest.
package storagetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/state/statebuilder"
	"github.com/ably/server-protocol/go/wire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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
		cm, idempotent, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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
		msgs := []*wire.Message{{Name: new("a")}, {Name: new("b")}, {Name: new("c")}}
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
			cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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

		first, idemp, err := ch.Store(context.Background(), []*wire.Message{{Id: new("dup"), Data: wire.MessageStrData("v1")}})
		if err != nil || idemp {
			t.Fatalf("first publish: err=%v idempotent=%v", err, idemp)
		}

		// Re-publish with the same ID but different payload — should
		// return the original ChannelMessage and not re-append.
		second, idemp, err := ch.Store(context.Background(), []*wire.Message{{Id: new("dup"), Data: wire.MessageStrData("v2")}})
		if err != nil {
			t.Fatalf("second publish: %v", err)
		}
		if !idemp {
			t.Fatal("idempotent=false on duplicate id")
		}
		if second.ChannelSerial != first.ChannelSerial {
			t.Errorf("returned ChannelSerial = %q, want original %q", second.ChannelSerial, first.ChannelSerial)
		}
		if second.Messages[0].Data.AsStr() != "v1" {
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
		// base64 batch id, and each Message.GetId() = "<batchID>:<idx>"
		// (DESIGN.md §8).
		cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("a")}, {Name: new("b")}})
		if err != nil {
			t.Fatalf("Store: %v", err)
		}
		if len(cm.ID) != 8 {
			t.Errorf("ChannelMessage.GetId() = %q, want an 8-char base64 batch id", cm.ID)
		}
		for i, m := range cm.Messages {
			want := cm.ID + ":" + strconv.Itoa(i)
			if m.GetId() != want {
				t.Errorf("Messages[%d].ID = %q, want %q", i, m.GetId(), want)
			}
		}
	})

	t.Run("RejectsMismatchedClientBatchIDs", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// A client-supplied multi-message batch whose ids do not conform
		// to "<batchID>:<idx>" is rejected.
		_, _, err := ch.Store(context.Background(), []*wire.Message{{Id: new("x:0")}, {Id: new("y:1")}})
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
		first, _, err := ch.Store(context.Background(), []*wire.Message{{Id: new("b:0")}, {Id: new("b:1")}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
		if first.ID != "b" {
			t.Errorf("ChannelMessage.GetId() = %q, want %q", first.ID, "b")
		}

		second, idemp, err := ch.Store(context.Background(), []*wire.Message{{Id: new("b:0")}, {Id: new("b:1")}})
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

		first, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
		second, idemp, err := ch.Store(context.Background(), []*wire.Message{{Name: new("y")}})
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
		if _, _, err := ch.Store(context.Background(), []*wire.Message{}); err == nil {
			t.Error("Store([]) returned no error")
		}
	})

	t.Run("HistoryForwardsReturnsAllInOrder", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var serials []string
		for i := range 4 {
			cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x"), Data: wire.MessageStrData(strconv.Itoa(i))}})
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
			cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x"), Data: wire.MessageStrData(strconv.Itoa(i))}})
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
		_, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("a0")}, {Name: new("a1")}, {Name: new("a2")}})
		if err != nil {
			t.Fatalf("publish batch1: %v", err)
		}
		_, _, err = ch.Store(context.Background(), []*wire.Message{{Name: new("b0")}, {Name: new("b1")}})
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
				got = append(got, m.GetName())
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

		_, _, _ = ch.Store(context.Background(), []*wire.Message{{Name: new("a0")}, {Name: new("a1")}, {Name: new("a2")}})
		_, _, _ = ch.Store(context.Background(), []*wire.Message{{Name: new("b0")}, {Name: new("b1")}})

		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		var got []string
		for _, cm := range page.ChannelMessages {
			for _, m := range cm.Messages {
				got = append(got, m.GetName())
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

		_, _, _ = ch.Store(context.Background(), []*wire.Message{{Name: new("a0")}, {Name: new("a1")}, {Name: new("a2")}})

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
			page.ChannelMessages[0].Messages[0].GetName(),
			page.ChannelMessages[0].Messages[1].GetName(),
			page.ChannelMessages[0].Messages[2].GetName(),
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
			cm, _, _ := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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
			cm, _, _ := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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
		cm, _, _ := ch.Store(context.Background(), []*wire.Message{
			{Name: new("m0")}, {Name: new("m1")}, {Name: new("m2")}, {Name: new("m3")}, {Name: new("m4")},
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

		cm, _, _ := ch.Store(context.Background(), []*wire.Message{
			{Name: new("m0")}, {Name: new("m1")}, {Name: new("m2")}, {Name: new("m3")}, {Name: new("m4")},
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

		cm, _, _ := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})

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

		cm, _, _ := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})

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
		_, _, _ = ch.Store(context.Background(), []*wire.Message{
			{Name: new("m0")}, {Name: new("m1")}, {Name: new("m2")}, {Name: new("m3")}, {Name: new("m4")},
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
			cm, _, _ := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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
			cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x"), Data: wire.MessageStrData(strconv.Itoa(i))}})
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
			cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x"), Data: wire.MessageStrData(strconv.Itoa(i))}})
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
			_, _, _ = ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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
			cm, _, _ := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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

		fooCM, _, err := mustChannel(t, s, "foo").Store(context.Background(), []*wire.Message{{Id: new("x")}})
		if err != nil {
			t.Fatalf("foo publish: %v", err)
		}
		// Same ID, different channel — must NOT be idempotent.
		barCM, idemp, err := mustChannel(t, s, "bar").Store(context.Background(), []*wire.Message{{Id: new("x")}})
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
		cm, _, err := a.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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
					cm, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("x")}})
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

	// A limit is a bound on the read, not a slice taken afterwards: syncing a
	// channel with many thousands of members is what paging is for, and a
	// backend answering with the whole set defeats it.
	t.Run("MembersPagesToTheLimit", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		for _, member := range []struct{ conn, client string }{
			{"conn-1", "alice"}, {"conn-2", "bob"}, {"conn-3", "carol"}, {"conn-4", "dave"},
		} {
			mustEnter(t, ch, member.conn, member.client, "hi")
		}

		seen := map[string]bool{}
		q := storage.MembersQuery{Limit: 2}
		for {
			page, err := ch.Members(context.Background(), q)
			if err != nil {
				t.Fatalf("Members: %v", err)
			}
			if len(page.Members) > q.Limit {
				t.Fatalf("page held %d members, want at most %d", len(page.Members), q.Limit)
			}
			for _, m := range page.Members {
				key := storage.MemberKey(m.ConnectionId, m.GetClientId())
				if seen[key] {
					t.Fatalf("member %s came back on more than one page", key)
				}
				seen[key] = true
			}
			if page.NextCursor == "" {
				break
			}
			if len(seen) > 4 {
				t.Fatal("paging returned more members than were entered")
			}
			q.After = page.NextCursor
		}

		if len(seen) != 4 {
			t.Fatalf("paging reached %d members, want 4", len(seen))
		}
	})

	t.Run("PresenceEnterPopulatesMembers", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		mustEnter(t, ch, "conn-1", "alice", "hi")
		mustEnter(t, ch, "conn-2", "bob", "yo")

		chPage, err := ch.Members(context.Background(), storage.MembersQuery{})
		members, asOf := chPage.Members, chPage.AsOfSerial
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
		mustPresence(t, ch, &wire.PresenceMessage{
			Action: wire.PresenceMessage_UPDATE, ConnectionId: "conn-1", ClientId: new("alice"), Data: wire.MessageStrData("v2"),
		})
		chPage, err := ch.Members(context.Background(), storage.MembersQuery{})
		members, _ := chPage.Members, chPage.AsOfSerial
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if len(members) != 1 {
			t.Fatalf("members = %d, want 1 (update replaces, not adds)", len(members))
		}
		if members[0].Data.AsStr() != "v2" {
			t.Errorf("member Data = %v, want v2 (latest wins)", members[0].Data)
		}
	})

	t.Run("PresenceLeaveRemovesMember", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		mustEnter(t, ch, "conn-1", "alice", "hi")
		mustPresence(t, ch, &wire.PresenceMessage{
			Action: wire.PresenceMessage_LEAVE, ConnectionId: "conn-1", ClientId: new("alice"),
		})
		chPage, err := ch.Members(context.Background(), storage.MembersQuery{})
		members, _ := chPage.Members, chPage.AsOfSerial
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
		chPage, err := ch.Members(context.Background(), storage.MembersQuery{})
		members, _ := chPage.Members, chPage.AsOfSerial
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
		first, idemp, err := ch.StorePresence(context.Background(), []*wire.PresenceMessage{
			{Id: new("p1"), Action: wire.PresenceMessage_ENTER, ConnectionId: "conn-1", ClientId: new("alice"), Data: wire.MessageStrData("v1")},
		})
		if err != nil || idemp {
			t.Fatalf("first StorePresence: err=%v idempotent=%v", err, idemp)
		}
		second, idemp, err := ch.StorePresence(context.Background(), []*wire.PresenceMessage{
			{Id: new("p1"), Action: wire.PresenceMessage_ENTER, ConnectionId: "conn-1", ClientId: new("alice"), Data: wire.MessageStrData("v2")},
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
		chPage, _ := ch.Members(context.Background(), storage.MembersQuery{})
		members, _ := chPage.Members, chPage.AsOfSerial
		if len(members) != 1 || members[0].Data.AsStr() != "v1" {
			t.Errorf("members = %+v, want single alice with original v1", members)
		}
	})

	t.Run("PresenceAndMessageStreamsAreKindFiltered", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		if _, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("m")}}); err != nil {
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
		if got := presPage.ChannelMessages[0].Presence[0].GetClientId(); got != "alice" {
			t.Errorf("presence history clientId = %q, want alice", got)
		}
	})

	t.Run("StateStampsSerialsOnEveryOperation", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		_, ops := mapCreateAndSet("k")
		cm := mustState(t, ch, ops...)

		if cm.ChannelSerial == "" {
			t.Fatal("ChannelSerial is empty")
		}
		if len(cm.State) != 2 {
			t.Fatalf("State length = %d, want 2", len(cm.State))
		}
		for i, sm := range cm.State {
			want := storage.StateSerial(cm.ChannelSerial, i)
			if got := sm.GetSerial(); !got.Equals(want) {
				t.Errorf("State[%d].Serial = %s, want %s", i, got.ToTimeserialString(), want.ToTimeserialString())
			}
			if got, want := sm.GetSiteCode(), want.SiteCode(); got != want {
				t.Errorf("State[%d].SiteCode = %q, want %q", i, got, want)
			}
		}
	})

	t.Run("StateOperationsMaterialiseObjects", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		objectID, ops := mapCreateAndSet("k")
		mustState(t, ch, ops...)

		page, err := ch.Objects(context.Background(), storage.ObjectsQuery{})
		if err != nil {
			t.Fatalf("Objects: %v", err)
		}
		if page.AsOfSerial == "" {
			t.Error("asOf serial empty after a state publish")
		}
		got := objectsByID(page.Objects)
		if got[objectID] == nil {
			t.Fatalf("the created object %q is missing from the set: %v", objectID, objectIDsOf(page.Objects))
		}
		if got[objectID].GetMap() == nil {
			t.Errorf("the created object is not a map: %+v", got[objectID])
		}
		// The set on the root is what makes the object reachable, so the
		// root has to have been materialised too, carrying the reference.
		root := got[wire.RootObjectID]
		if root == nil {
			t.Fatalf("the root the set named is missing from the set: %v", objectIDsOf(page.Objects))
		}
		if ref := root.GetMap().GetEntries()["k"].GetData().GetObjectId(); ref != objectID {
			t.Errorf("root entry %q points at %q, want %q", "k", ref, objectID)
		}
	})

	t.Run("StateFoldsSuccessiveOperationsOnOneObject", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		// Two publishes, each setting a different key on the root: the
		// second must build on the first rather than replace it, which is
		// what makes the stored set a fold of the stream rather than a
		// record of its last publish.
		mustState(t, ch, rootSet("a", "1"))
		mustState(t, ch, rootSet("b", "2"))

		page, err := ch.Objects(context.Background(), storage.ObjectsQuery{})
		if err != nil {
			t.Fatalf("Objects: %v", err)
		}
		entries := objectsByID(page.Objects)[wire.RootObjectID].GetMap().GetEntries()
		if got := entries["a"].GetData().GetString_(); got != "1" {
			t.Errorf("root[a] = %q, want 1 (the earlier operation was dropped)", got)
		}
		if got := entries["b"].GetData().GetString_(); got != "2" {
			t.Errorf("root[b] = %q, want 2", got)
		}
	})

	t.Run("StateAppliesOnlyToTheObjectsTheOperationsName", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		firstID, first := mapCreateAndSet("a")
		mustState(t, ch, first...)
		before := objectsByID(mustObjects(t, ch))[firstID]

		secondID, second := mapCreateAndSet("b")
		mustState(t, ch, second...)
		after := objectsByID(mustObjects(t, ch))

		if after[secondID] == nil {
			t.Fatalf("the second object %q is missing from the set", secondID)
		}
		if !proto.Equal(before, after[firstID]) {
			t.Errorf("an object no operation named was changed: %+v became %+v", before, after[firstID])
		}
	})

	t.Run("ObjectsPageToTheLimit", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		created := map[string]bool{}
		for i := range 4 {
			id, ops := mapCreateAndSet("k" + strconv.Itoa(i))
			mustState(t, ch, ops...)
			created[id] = true
		}

		seen := map[string]bool{}
		q := storage.ObjectsQuery{Limit: 2}
		for {
			page, err := ch.Objects(context.Background(), q)
			if err != nil {
				t.Fatalf("Objects: %v", err)
			}
			if len(page.Objects) > q.Limit {
				t.Fatalf("page held %d objects, want at most %d", len(page.Objects), q.Limit)
			}
			for _, o := range page.Objects {
				if seen[o.GetObjectId()] {
					t.Fatalf("object %s came back on more than one page", o.GetObjectId())
				}
				seen[o.GetObjectId()] = true
			}
			if page.NextCursor == "" {
				break
			}
			if len(seen) > len(created)+8 {
				t.Fatal("paging never ended")
			}
			q.After = page.NextCursor
		}

		for id := range created {
			if !seen[id] {
				t.Errorf("object %s was never reached by paging", id)
			}
		}
	})

	t.Run("StateIdempotentByID", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		first := rootSet("k", "v1")
		first.Id = new("s1")
		firstCM := mustState(t, ch, first)

		second := rootSet("k", "v2")
		second.Id = new("s1")
		secondCM, idemp, err := ch.StoreState(context.Background(), []*wire.StateMessage{second}, testApply)
		if err != nil {
			t.Fatalf("second StoreState: %v", err)
		}
		if !idemp {
			t.Error("idempotent=false on duplicate state id")
		}
		if secondCM.ChannelSerial != firstCM.ChannelSerial {
			t.Errorf("returned serial = %q, want original %q", secondCM.ChannelSerial, firstCM.ChannelSerial)
		}

		entries := objectsByID(mustObjects(t, ch))[wire.RootObjectID].GetMap().GetEntries()
		if got := entries["k"].GetData().GetString_(); got != "v1" {
			t.Errorf("root[k] = %q, want the original v1 — the duplicate was applied", got)
		}
	})

	t.Run("StateAndMessageStreamsAreKindFiltered", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		if _, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("m")}}); err != nil {
			t.Fatalf("Store: %v", err)
		}
		mustEnter(t, ch, "conn-1", "alice", "hi")
		mustState(t, ch, rootSet("k", "v"))

		for _, kind := range []storage.Kind{storage.KindMessage, storage.KindPresence} {
			page, err := ch.History(context.Background(), storage.HistoryQuery{Kind: kind, Direction: storage.DirectionForwards})
			if err != nil {
				t.Fatalf("%s History: %v", kind, err)
			}
			for _, cm := range page.ChannelMessages {
				if len(cm.State) != 0 {
					t.Errorf("%s history leaked a state cm", kind)
				}
			}
		}

		statePage, err := ch.History(context.Background(), storage.HistoryQuery{Kind: storage.KindState, Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("state History: %v", err)
		}
		if len(statePage.ChannelMessages) != 1 || len(statePage.ChannelMessages[0].State) != 1 {
			t.Fatalf("state history = %v, want exactly one state cm", channelSerialsOf(statePage))
		}
		if got := statePage.ChannelMessages[0].State[0].GetOperation().GetObjectId(); got != wire.RootObjectID {
			t.Errorf("state history objectId = %q, want %q", got, wire.RootObjectID)
		}
	})

	t.Run("StateRejectsEmptyPublish", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")
		if _, _, err := ch.StoreState(context.Background(), nil, testApply); err == nil {
			t.Error("StoreState with no messages was accepted")
		}
	})

	t.Run("OccupancyOfAnUntouchedChannelIsZero", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		occ, err := ch.Occupancy(context.Background())
		if err != nil {
			t.Fatalf("Occupancy: %v", err)
		}
		if occ == nil {
			t.Fatal("Occupancy answered nil for a channel nothing has occupied")
		}
		if occ.GetConnections() != 0 || occ.GetChannelMode() != 0 {
			t.Errorf("occupancy = %+v, want the zero value", occ)
		}
	})

	t.Run("OccupancyStoredIsReadBack", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		mustOccupancy(t, ch, &wire.ChannelOccupancy{
			ChannelMode: int32(channel.MESSAGE_SUBSCRIBE),
			Connections: 3,
			Subscribers: 3,
		})

		occ := readOccupancy(t, ch)
		if occ.GetConnections() != 3 || occ.GetSubscribers() != 3 {
			t.Errorf("occupancy = %+v, want 3 connections and 3 subscribers", occ)
		}
		if occ.GetChannelMode() != int32(channel.MESSAGE_SUBSCRIBE) {
			t.Errorf("channelMode = %d, want MESSAGE_SUBSCRIBE (%d)", occ.GetChannelMode(), channel.MESSAGE_SUBSCRIBE)
		}
	})

	t.Run("OccupancyIsReplacedNotAccumulated", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		// A contribution is the whole of what the node serves, not a delta, so
		// storing twice must leave the second value rather than their sum.
		mustOccupancy(t, ch, &wire.ChannelOccupancy{Connections: 3, Subscribers: 3})
		mustOccupancy(t, ch, &wire.ChannelOccupancy{Connections: 1, Subscribers: 1})

		if occ := readOccupancy(t, ch); occ.GetConnections() != 1 {
			t.Errorf("connections = %d after replacing 3 with 1, want 1", occ.GetConnections())
		}
	})

	t.Run("AnEmptyOccupancyWithdrawsTheContribution", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		mustOccupancy(t, ch, &wire.ChannelOccupancy{
			ChannelMode: int32(channel.MESSAGE_SUBSCRIBE), Connections: 2, Subscribers: 2,
		})
		mustOccupancy(t, ch, nil)

		occ := readOccupancy(t, ch)
		if occ.GetConnections() != 0 || occ.GetSubscribers() != 0 || occ.GetChannelMode() != 0 {
			t.Errorf("occupancy = %+v after withdrawing, want the zero value", occ)
		}
	})

	t.Run("OccupancyCountsThePresenceSet", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "room")

		// presenceMembers is not a node's contribution — it is the size of the
		// membership set, which storage already holds.
		mustEnter(t, ch, "conn-1", "alice", "hi")
		mustEnter(t, ch, "conn-2", "bob", "hi")

		if occ := readOccupancy(t, ch); occ.GetPresenceMembers() != 2 {
			t.Errorf("presenceMembers = %d, want 2", occ.GetPresenceMembers())
		}
	})

	t.Run("StoringOccupancySignalsTheAppender", func(t *testing.T) {
		s := f(t)
		a := newCapturingAppender()
		ch, err := s.Channel(context.Background(), "room", a)
		if err != nil {
			t.Fatalf("Channel: %v", err)
		}

		mustOccupancy(t, ch, &wire.ChannelOccupancy{Connections: 1, Subscribers: 1})

		// The signal may cross a notification round-trip, so it is waited for
		// rather than read once.
		deadline := time.Now().Add(10 * time.Second)
		for a.occupancyChangeCount() == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if a.occupancyChangeCount() == 0 {
			t.Error("storing a contribution never signalled the appender, so nothing would re-read the aggregate")
		}
	})

	t.Run("EmptyStringDataSurvivesStorage", func(t *testing.T) {
		// An empty-string (and empty []byte) payload must survive the
		// storage round-trip, so a client that published "" reads back an
		// empty value rather than an absent one (DESIGN.md §6).
		s := f(t)
		ch := mustChannel(t, s, "foo")
		if _, _, err := ch.Store(context.Background(), []*wire.Message{{Name: new("s"), Data: wire.MessageStrData("")}, {Name: new("b"), Data: wire.MessageBinData([]byte{})}}); err != nil {
			t.Fatalf("Store: %v", err)
		}
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		msgs := page.ChannelMessages
		if len(msgs) != 1 || len(msgs[0].Messages) != 2 {
			t.Fatalf("history shape = %v, want one cm with two messages", channelSerialsOf(page))
		}
		for i, m := range msgs[0].Messages {
			if m.Data == nil {
				t.Errorf("message %d empty Data round-tripped as nil (dropped by storage encoding)", i)
			}
		}

		// Presence data "" shares the same storage round-trip.
		room := mustChannel(t, s, "room")
		mustEnter(t, room, "conn-1", "alice", "")
		roomPage, err := room.Members(context.Background(), storage.MembersQuery{})
		members, _ := roomPage.Members, roomPage.AsOfSerial
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if len(members) != 1 {
			t.Fatalf("members = %d, want 1", len(members))
		}
		if members[0].Data == nil {
			t.Errorf("empty presence Data round-tripped as nil (dropped by storage encoding)")
		}
	})

	t.Run("ExtrasRoundTripsThroughStorage", func(t *testing.T) {
		// Client-supplied extras must survive the storage payload encoding
		// verbatim for messages, presence and annotations, so
		// history/presence/annotation reads carry it back
		// (DESIGN.md §8). The AI Transport SDK sets extras.ai on ~every message.
		extras := map[string]any{"headers": map[string]any{"some": "metadata"}}
		s := f(t)
		ch := mustChannel(t, s, "foo")

		// Message publish -> history.
		created := mustCreate(t, ch, &wire.Message{Name: new("m"), Data: wire.MessageStrData("body"), ClientId: new("alice"), Extras: mustStruct(t, extras)})
		if !reflect.DeepEqual(created.Extras.AsMap(), extras) {
			t.Errorf("stored message extras = %#v, want %#v", created.Extras.AsMap(), extras)
		}
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(page.ChannelMessages) != 1 || len(page.ChannelMessages[0].Messages) != 1 ||
			!reflect.DeepEqual(page.ChannelMessages[0].Messages[0].Extras.AsMap(), extras) {
			t.Errorf("history dropped/altered message extras: %+v", page.ChannelMessages)
		}

		// Presence enter -> members.
		room := mustChannel(t, s, "room")
		mustPresence(t, room, &wire.PresenceMessage{
			Action: wire.PresenceMessage_ENTER, ConnectionId: "conn-1", ClientId: new("alice"), Extras: mustStruct(t, extras),
		})
		roomPage, err := room.Members(context.Background(), storage.MembersQuery{})
		members, _ := roomPage.Members, roomPage.AsOfSerial
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if len(members) != 1 || !reflect.DeepEqual(members[0].Extras.AsMap(), extras) {
			t.Errorf("presence member extras = %#v, want %#v", members, extras)
		}

		// Annotation publish -> annotations read.
		acm, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: "👍", MessageSerial: created.Serial, Extras: mustStruct(t, extras)},
		}, testFold)
		if err != nil {
			t.Fatalf("StoreAnnotation: %v", err)
		}
		if !reflect.DeepEqual(acm.Annotations[0].Extras.AsMap(), extras) {
			t.Errorf("stored annotation extras = %#v, want %#v", acm.Annotations[0].Extras.AsMap(), extras)
		}
		apage, err := ch.Annotations(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Annotations: %v", err)
		}
		if len(apage.ChannelMessages) != 1 || len(apage.ChannelMessages[0].Annotations) != 1 ||
			!reflect.DeepEqual(apage.ChannelMessages[0].Annotations[0].Extras.AsMap(), extras) {
			t.Errorf("annotations read dropped/altered extras: %+v", apage.ChannelMessages)
		}
	})

	// ---- Mutable messages (DESIGN.md §13) ------------------------------

	t.Run("CreateStampsActionAndVersion", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		m := mustCreate(t, ch, &wire.Message{Name: new("x"), Data: wire.MessageStrData("v1"), ClientId: new("alice")})
		if m.Action != wire.MessageAction_MESSAGE_CREATE {
			t.Errorf("create action = %v, want create", m.Action)
		}
		if m.Version == nil || m.Version.Serial != m.Serial {
			t.Errorf("create version = %+v, want version.serial == serial %q", m.Version, m.Serial)
		}
		if m.Version != nil && m.Version.GetClientId() != "alice" {
			t.Errorf("create version.clientId = %q, want alice", m.Version.GetClientId())
		}
	})

	t.Run("MutateUpdateProducesNewVersionStableIdentity", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Name: new("greeting"), Data: wire.MessageStrData("v1"), ClientId: new("alice")})

		updated := mustMutate(t, ch, &wire.Message{
			Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("v2"), ClientId: new("bob"),
			Version: &wire.Message_Version{Description: new("edit"), Metadata: map[string]string{"reason": "testing"}},
		})
		if updated.Serial != created.Serial {
			t.Errorf("updated serial = %q, want stable identity %q", updated.Serial, created.Serial)
		}
		if updated.Action != wire.MessageAction_MESSAGE_UPDATE {
			t.Errorf("updated action = %v, want update", updated.Action)
		}
		if updated.Data.AsStr() != "v2" {
			t.Errorf("updated data = %v, want v2", updated.Data)
		}
		if updated.Version == nil || updated.Version.Serial == created.Serial || updated.Version.Serial <= created.Version.Serial {
			t.Errorf("updated version.serial = %v, want fresh and > create's %q", updated.Version, created.Version.Serial)
		}
		if updated.Version != nil && (updated.Version.GetClientId() != "bob" || updated.Version.GetDescription() != "edit") {
			t.Errorf("updated version metadata = %+v, want operator bob / 'edit'", updated.Version)
		}

		latest, err := ch.LatestVersion(context.Background(), created.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		if latest.Data.AsStr() != "v2" || latest.GetClientId() != "alice" {
			t.Errorf("latest = data %v / creator %q, want v2 / alice (creator carried forward)", latest.Data, latest.GetClientId())
		}
		// The full operation envelope (operator clientId + description +
		// metadata) must persist with the version and project on read-back.
		if latest.Version == nil ||
			latest.Version.GetClientId() != "bob" ||
			latest.Version.GetDescription() != "edit" ||
			!reflect.DeepEqual(latest.Version.Metadata, map[string]string{"reason": "testing"}) ||
			latest.Version.Timestamp == 0 {
			t.Errorf("read-back version = %+v, want operator bob / 'edit' / {reason:testing} / non-zero ts", latest.Version)
		}
	})

	t.Run("MutateShallowMixinCarriesForwardUnsuppliedFields", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		origExtras := map[string]any{"headers": map[string]any{"some": "metadata"}}
		created := mustCreate(t, ch, &wire.Message{Name: new("title"), Data: wire.MessageStrData("body"), ClientId: new("alice"), Extras: mustStruct(t, origExtras)})

		// Update only data — name and extras must carry forward.
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("body2"), ClientId: new("alice")})
		latest, _ := ch.LatestVersion(context.Background(), created.Serial)
		if latest.GetName() != "title" || latest.Data.AsStr() != "body2" {
			t.Errorf("after data-only update: name=%q data=%v, want title/body2", latest.GetName(), latest.Data)
		}
		if !reflect.DeepEqual(latest.Extras.AsMap(), origExtras) {
			t.Errorf("after data-only update: extras=%#v, want carried-forward %#v", latest.Extras, origExtras)
		}

		// Update only name — data and extras must carry forward.
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Name: new("title2"), ClientId: new("alice")})
		latest, _ = ch.LatestVersion(context.Background(), created.Serial)
		if latest.GetName() != "title2" || latest.Data.AsStr() != "body2" {
			t.Errorf("after name-only update: name=%q data=%v, want title2/body2", latest.GetName(), latest.Data)
		}
		if !reflect.DeepEqual(latest.Extras.AsMap(), origExtras) {
			t.Errorf("after name-only update: extras=%#v, want carried-forward %#v", latest.Extras, origExtras)
		}

		// A supplied extras replaces the whole object (shallow-mixin, §13.2).
		newExtras := map[string]any{"push": map[string]any{"notification": map[string]any{"title": "hi"}}}
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, ClientId: new("alice"), Extras: mustStruct(t, newExtras)})
		latest, _ = ch.LatestVersion(context.Background(), created.Serial)
		if !reflect.DeepEqual(latest.Extras.AsMap(), newExtras) {
			t.Errorf("after extras-replacing update: extras=%#v, want %#v", latest.Extras, newExtras)
		}
	})

	t.Run("MutateAppendConcatenatesData", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("Hello"), ClientId: new("alice")})
		appended := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_APPEND, Serial: created.Serial, Data: wire.MessageStrData(", world"), ClientId: new("alice")})
		if appended.Data.AsStr() != "Hello, world" {
			t.Errorf("appended data = %v, want %q", appended.Data, "Hello, world")
		}
		// Stored/fanned out as a full aggregated update carrying the
		// incremental delta in Alt (DESIGN.md §13.3).
		if appended.Action != wire.MessageAction_MESSAGE_UPDATE {
			t.Errorf("append action = %v, want update (aggregate)", appended.Action)
		}
		delta := appended.Alt[wire.DeltaAppend]
		if delta == nil {
			t.Fatalf("append version carries no %q delta", wire.DeltaAppend)
		}
		if delta.Action != wire.MessageAction_MESSAGE_APPEND || delta.Data.AsStr() != ", world" {
			t.Errorf("delta = action %v data %v, want append / %q", delta.Action, delta.Data, ", world")
		}
		if delta.Serial != created.Serial {
			t.Errorf("delta serial = %q, want stable identity %q", delta.Serial, created.Serial)
		}
	})

	t.Run("CreateStampsTopLevelTimestamp", func(t *testing.T) {
		// Every message carries a top-level create timestamp on the wire (§8);
		// it is omitempty, so a zero value is dropped and an SDK reads it as
		// absent. It must equal the create serial's timestamp.
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("hi"), ClientId: new("alice")})
		wantTS, _ := serial.Timestamp(created.Serial)
		if created.Timestamp != uint64(wantTS) || created.Timestamp == 0 {
			t.Errorf("create top-level timestamp = %d, want create-serial ts %d (non-zero)", created.Timestamp, wantTS)
		}
		if latest, _ := ch.LatestVersion(context.Background(), created.Serial); latest.Timestamp != uint64(wantTS) {
			t.Errorf("LatestVersion top-level timestamp = %d, want %d", latest.Timestamp, wantTS)
		}
	})

	t.Run("MutateCarriesTopLevelCreateTimestampForward", func(t *testing.T) {
		// An update/delete/append delivery keeps the ORIGINAL create timestamp
		// at the top level; only version.timestamp carries the operation time
		// (§8, §13.2, mirroring the reference's buildUpdateMessage).
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("a"), ClientId: new("alice")})
		createTS := created.Timestamp
		if createTS == 0 {
			t.Fatal("create carried no top-level timestamp")
		}
		updated := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("b"), ClientId: new("alice")})
		if updated.Timestamp != createTS {
			t.Errorf("update top-level timestamp = %d, want carried-forward create ts %d", updated.Timestamp, createTS)
		}
		if updated.Version == nil || updated.Version.Timestamp < createTS {
			t.Errorf("update version.timestamp = %v, want the (later) operation time", updated.Version)
		}
	})

	t.Run("MutateAppendDeltaCarriesIdentityForward", func(t *testing.T) {
		// The append delta must repeat the message's identity — name, extras,
		// and the top-level create timestamp — not just the incremental data
		// (DESIGN.md §13.3). A subscriber routes append frames by name/extras
		// exactly as the create; a streaming publisher omits the name on each
		// append (relying on carry-forward), so a name-less delta is invisible
		// to a name-filtering subscriber even though it arrives on the wire.
		s := f(t)
		ch := mustChannel(t, s, "foo")
		extras := map[string]any{"ai": map[string]any{"transport": map[string]any{"step-id": "wf-step-X"}}}
		created := mustCreate(t, ch, &wire.Message{Name: new("ai-output"), Data: wire.MessageStrData("INIT "), Extras: mustStruct(t, extras), ClientId: new("alice")})
		// The append supplies its own extras (the streaming shape) but NO name.
		appended := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_APPEND, Serial: created.Serial, Data: wire.MessageStrData("DEAD partial answer"), Extras: mustStruct(t, extras), ClientId: new("alice")})

		// The rolled-up aggregate keeps the name and the create timestamp.
		if appended.GetName() != "ai-output" {
			t.Errorf("aggregate name = %q, want %q carried forward", appended.GetName(), "ai-output")
		}
		if appended.Timestamp != created.Timestamp {
			t.Errorf("aggregate top-level timestamp = %d, want carried-forward create ts %d", appended.Timestamp, created.Timestamp)
		}

		delta := appended.Alt[wire.DeltaAppend]
		if delta == nil {
			t.Fatalf("append version carries no %q delta", wire.DeltaAppend)
		}
		if delta.GetName() != "ai-output" {
			t.Errorf("delta name = %q, want %q carried forward from the create", delta.GetName(), "ai-output")
		}
		if !reflect.DeepEqual(delta.Extras.AsMap(), extras) {
			t.Errorf("delta extras = %#v, want the supplied/carried-forward %#v", delta.Extras.AsMap(), extras)
		}
		if delta.Timestamp != created.Timestamp {
			t.Errorf("delta top-level timestamp = %d, want carried-forward create ts %d", delta.Timestamp, created.Timestamp)
		}
		if delta.Data.AsStr() != "DEAD partial answer" {
			t.Errorf("delta data = %v, want the incremental slice only", delta.Data)
		}
	})

	t.Run("MutateAppendRejectsIncompatibleData", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		strMsg := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("hello"), ClientId: new("alice")})
		if _, _, err := ch.Mutate(context.Background(), &wire.Message{
			Action: wire.MessageAction_MESSAGE_APPEND, Serial: strMsg.Serial, Data: wire.MessageBinData([]byte("x")), ClientId: new("alice"),
		}, testMerge); func() bool { _, ok := storage.AsProtocolError(err); return !ok }() {
			t.Errorf("binary-onto-string append err = %v, want the protocol's refusal", err)
		}
		// The rejected append must not have applied.
		if latest, _ := ch.LatestVersion(context.Background(), strMsg.Serial); latest.Data.AsStr() != "hello" {
			t.Errorf("data after rejected append = %v, want unchanged hello", latest.Data)
		}

		// Binary-onto-binary concatenates.
		binMsg := mustCreate(t, ch, &wire.Message{Data: wire.MessageBinData([]byte("ab")), ClientId: new("alice")})
		appended := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_APPEND, Serial: binMsg.Serial, Data: wire.MessageBinData([]byte("cd")), ClientId: new("alice")})
		if got := appended.Data.AsBin(); string(got) != "abcd" {
			t.Errorf("binary append data = %v, want abcd", appended.Data)
		}
	})

	t.Run("MutateAppendIsNotAVersion", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("a"), ClientId: new("alice")})
		for _, chunk := range []string{"b", "c", "d"} {
			mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_APPEND, Serial: created.Serial, Data: wire.MessageStrData(chunk), ClientId: new("alice")})
		}

		// The aggregate is the full concatenation and stays queryable: an
		// append changes what the message currently says.
		latest, err := ch.LatestVersion(context.Background(), created.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		if latest.Data.AsStr() != "abcd" {
			t.Errorf("aggregate data = %v, want abcd", latest.Data)
		}

		// An append is not a version of it, so the chain is the create alone
		// (DESIGN.md §13.3) — a streamed append does not lengthen the chain
		// with an entry per increment.
		vers, err := ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Versions: %v", err)
		}
		if n := itemCountVersions(vers); n != 1 {
			t.Fatalf("versions after create+3 appends = %d, want 1 (the create; an append is not a version)", n)
		}

		// An update is a version, and an append after it still is not.
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("X"), ClientId: new("alice")})
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_APPEND, Serial: created.Serial, Data: wire.MessageStrData("Y"), ClientId: new("alice")})
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_APPEND, Serial: created.Serial, Data: wire.MessageStrData("Z"), ClientId: new("alice")})
		vers, _ = ch.Versions(context.Background(), created.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if n := itemCountVersions(vers); n != 2 {
			t.Errorf("versions after create+appends+update+appends = %d, want 2 (the create and the update)", n)
		}
		if final, _ := ch.LatestVersion(context.Background(), created.Serial); final.Data.AsStr() != "XYZ" {
			t.Errorf("final aggregate = %v, want XYZ", final.Data)
		}

		// Every append is still on the log, which is what live and resume
		// fan-out read.
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if n := itemCountVersions(page); n != 7 {
			t.Errorf("log entries = %d, want 7 (create + 3 appends + update + 2 appends)", n)
		}
	})

	t.Run("MutateDeleteIsSoftTombstone", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Name: new("n"), Data: wire.MessageStrData("secret"), ClientId: new("alice")})
		deleted := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_DELETE, Serial: created.Serial, ClientId: new("mod")})
		if deleted.Action != wire.MessageAction_MESSAGE_DELETE {
			t.Errorf("delete action = %v, want delete", deleted.Action)
		}
		// A delete is a soft tombstone: its deletedness is carried by
		// action=delete, and a delete that supplies no body carries the
		// current name/data forward (shallow-mixin, matching the reference).
		if deleted.Data.AsStr() != "secret" || deleted.GetName() != "n" {
			t.Errorf("dataless delete carries data=%v name=%q, want secret/n carried forward", deleted.Data, deleted.GetName())
		}
		// The operator clientId is recorded on the version, distinct from the
		// creator carried forward at the top level.
		if deleted.GetClientId() != "alice" {
			t.Errorf("tombstone creator clientId = %q, want alice (carried forward)", deleted.GetClientId())
		}
		if deleted.Version == nil || deleted.Version.GetClientId() != "mod" {
			t.Errorf("tombstone version clientId = %v, want mod (operator)", deleted.Version)
		}

		// Soft: the message and all versions remain queryable.
		latest, err := ch.LatestVersion(context.Background(), created.Serial)
		if err != nil {
			t.Fatalf("LatestVersion after delete: %v (must stay queryable)", err)
		}
		if latest.Action != wire.MessageAction_MESSAGE_DELETE {
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
		_, _, err := ch.Mutate(context.Background(), &wire.Message{
			Action: wire.MessageAction_MESSAGE_UPDATE, Serial: "00000000000001-000@nonexistent:000", Data: wire.MessageStrData("x"), ClientId: new("alice"),
		}, testMerge)
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
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("v1"), ClientId: new("alice")})

		first, idemp, err := ch.Mutate(context.Background(), &wire.Message{
			Id: new("edit-1"), Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("v2"), ClientId: new("alice"),
		}, testMerge)
		if err != nil || idemp {
			t.Fatalf("first mutate: err=%v idempotent=%v", err, idemp)
		}
		second, idemp, err := ch.Mutate(context.Background(), &wire.Message{
			Id: new("edit-1"), Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("v3"), ClientId: new("alice"),
		}, testMerge)
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
		if latest.Data.AsStr() != "v2" {
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
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("v1"), ClientId: new("alice")})
		u1 := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("v2"), ClientId: new("alice")})
		u2 := mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("v3"), ClientId: new("alice")})

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
		m1 := mustCreate(t, ch, &wire.Message{Name: new("m1"), Data: wire.MessageStrData("1"), ClientId: new("alice")})
		m2 := mustCreate(t, ch, &wire.Message{Name: new("m2"), Data: wire.MessageStrData("2"), ClientId: new("alice")})
		m3 := mustCreate(t, ch, &wire.Message{Name: new("m3"), Data: wire.MessageStrData("3"), ClientId: new("alice")})
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: m2.Serial, Data: wire.MessageStrData("2-edited"), ClientId: new("alice")})

		// Collapsed forwards: latest of each, positioned at create order.
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
		if err != nil {
			t.Fatalf("collapsed History: %v", err)
		}
		var gotData []any
		var gotSerials, gotChannelSerials []string
		for _, cm := range page.ChannelMessages {
			for _, m := range cm.Messages {
				gotData = append(gotData, m.Data.AsStr())
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
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("v1"), ClientId: new("alice")})
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: created.Serial, Data: wire.MessageStrData("v2"), ClientId: new("alice")})

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
		if col.ChannelMessages[0].Messages[0].Data.AsStr() != "v2" {
			t.Errorf("collapsed data = %v, want v2", col.ChannelMessages[0].Messages[0].Data)
		}
	})

	// GET .../messages (the default Collapse=true view) honours
	// the REST fromSerial/untilAttached bound via q.EndChannelSerial. This
	// mirrors HistoryEndChannelSerialIsInclusiveUpperBound above but for
	// the collapsed view, whose entries are keyed by identity
	// (createSerial:idx), not by ChannelMessage.ChannelSerial, so the bound
	// must be compared against storage.CreateChannelSerial(identity) — a
	// backend that forgets this and only wires EndChannelSerial into the
	// raw scan will fail this test by returning every message regardless
	// of the bound.
	t.Run("CollapsedHistoryEndChannelSerialBoundsByCreateSerial", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		var msgs []*wire.Message
		for i := range 5 {
			msgs = append(msgs, mustCreate(t, ch, &wire.Message{Name: new("x"), Data: wire.MessageStrData(strconv.Itoa(i)), ClientId: new("alice")}))
		}
		bound := storage.CreateChannelSerial(msgs[2].Serial)

		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction:        storage.DirectionForwards,
			Collapse:         true,
			EndChannelSerial: bound,
		})
		if err != nil {
			t.Fatalf("forwards collapsed History: %v", err)
		}
		want := []string{msgs[0].Serial, msgs[1].Serial, msgs[2].Serial}
		if got := flattenSerials(page); !equalStrings(got, want) {
			t.Errorf("forwards = %v, want %v", got, want)
		}

		page, err = ch.History(context.Background(), storage.HistoryQuery{
			Direction:        storage.DirectionBackwards,
			Collapse:         true,
			EndChannelSerial: bound,
		})
		if err != nil {
			t.Fatalf("backwards collapsed History: %v", err)
		}
		want = []string{msgs[2].Serial, msgs[1].Serial, msgs[0].Serial}
		if got := flattenSerials(page); !equalStrings(got, want) {
			t.Errorf("backwards = %v, want %v", got, want)
		}
	})

	// An edit lands at a fresh, later channelSerial than its message's
	// create position (DESIGN.md §13.4). The untilAttached bound must
	// still admit that message — it stays positioned at its create
	// serial — even though the edit itself happened after the attach
	// point. A backend that compared the bound against the edit's own
	// channelSerial instead of storage.CreateChannelSerial would wrongly
	// drop it.
	t.Run("CollapsedHistoryEndChannelSerialUsesCreateNotEditSerial", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")

		m1 := mustCreate(t, ch, &wire.Message{Name: new("m1"), Data: wire.MessageStrData("v1"), ClientId: new("alice")})
		m2 := mustCreate(t, ch, &wire.Message{Name: new("m2"), Data: wire.MessageStrData("2"), ClientId: new("alice")})
		bound := storage.CreateChannelSerial(m2.Serial)

		// Edit m1 after the bound — its version cm lands past m2's
		// create point but m1 should still surface (at "v1-edited").
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_UPDATE, Serial: m1.Serial, Data: wire.MessageStrData("v1-edited"), ClientId: new("alice")})
		// Created after the bound — must be excluded.
		mustCreate(t, ch, &wire.Message{Name: new("m3"), Data: wire.MessageStrData("3"), ClientId: new("alice")})

		page, err := ch.History(context.Background(), storage.HistoryQuery{
			Direction:        storage.DirectionForwards,
			Collapse:         true,
			EndChannelSerial: bound,
		})
		if err != nil {
			t.Fatalf("collapsed History: %v", err)
		}
		if got := flattenSerials(page); !equalStrings(got, []string{m1.Serial, m2.Serial}) {
			t.Errorf("serials = %v, want [%s %s]", got, m1.Serial, m2.Serial)
		}
		if got := flattenNames(page); !equalStrings(got, []string{"m1", "m2"}) {
			t.Errorf("names = %v, want [m1 m2]", got)
		}
		for _, cm := range page.ChannelMessages {
			for _, m := range cm.Messages {
				if m.Serial == m1.Serial && m.Data.AsStr() != "v1-edited" {
					t.Errorf("m1 data = %v, want v1-edited (edit content, create position)", m.Data)
				}
			}
		}
	})

	// ---- Annotations (DESIGN.md §14) -----------------------------------

	t.Run("StoreAnnotationStampsSerialsAndTarget", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})

		cm, idemp, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
		}, testFold)
		if err != nil || idemp {
			t.Fatalf("StoreAnnotation: err=%v idempotent=%v", err, idemp)
		}
		if cm.ChannelSerial == "" || len(cm.Annotations) != 1 {
			t.Fatalf("annotation cm shape = %+v", cm)
		}
		if cm.Annotations[0].Serial.ToTimeserialString() != cm.ChannelSerial+":000" {
			t.Errorf("annotation serial = %q, want %q", cm.Annotations[0].Serial.ToTimeserialString(), cm.ChannelSerial+":000")
		}
		if cm.Annotations[0].MessageSerial != target.Serial {
			t.Errorf("annotation messageSerial = %q, want target %q", cm.Annotations[0].MessageSerial, target.Serial)
		}
	})

	t.Run("StoreAnnotationRejectsUnknownTarget", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		_, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:distinct.v1", MessageSerial: "00000000000001-000@nope:000"},
		}, testFold)
		if !errors.Is(err, storage.ErrTargetNotFound) {
			t.Errorf("StoreAnnotation unknown target err = %v, want ErrTargetNotFound", err)
		}
	})

	t.Run("AnnotationsForMessageInStreamOrder", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		a := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("a"), ClientId: new("alice")})
		b := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("b"), ClientId: new("alice")})

		var want []string
		for _, name := range []string{"👍", "❤️", "🎉"} {
			cm, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
				{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: name, MessageSerial: a.Serial},
			}, testFold)
			if err != nil {
				t.Fatalf("StoreAnnotation: %v", err)
			}
			want = append(want, cm.Annotations[0].Serial.ToTimeserialString())
		}
		// An annotation on a different message must not leak.
		if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: "🚀", MessageSerial: b.Serial},
		}, testFold); err != nil {
			t.Fatalf("StoreAnnotation on b: %v", err)
		}

		page, err := ch.Annotations(context.Background(), a.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Annotations: %v", err)
		}
		var got []string
		for _, cm := range page.ChannelMessages {
			for _, an := range cm.Annotations {
				got = append(got, an.Serial.ToTimeserialString())
			}
		}
		if !equalStrings(got, want) {
			t.Errorf("annotations for a = %v, want %v (stream order, target-filtered)", got, want)
		}
	})

	t.Run("AnnotationsPaginateAndHasMore", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})
		var serials []string
		for i := range 3 {
			cm, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
				{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: strconv.Itoa(i), MessageSerial: target.Serial},
			}, testFold)
			if err != nil {
				t.Fatalf("StoreAnnotation %d: %v", i, err)
			}
			serials = append(serials, cm.Annotations[0].Serial.ToTimeserialString())
		}
		page, err := ch.Annotations(context.Background(), target.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 2})
		if err != nil {
			t.Fatalf("Annotations: %v", err)
		}
		if !page.HasMore {
			t.Error("HasMore=false with limit < total")
		}
		var got []string
		for _, cm := range page.ChannelMessages {
			for _, an := range cm.Annotations {
				got = append(got, an.Serial.ToTimeserialString())
			}
		}
		if !equalStrings(got, serials[:2]) {
			t.Errorf("first page = %v, want %v", got, serials[:2])
		}
		// Continue from the boundary.
		next, err := ch.Annotations(context.Background(), target.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards, Cursor: got[len(got)-1]})
		if err != nil {
			t.Fatalf("Annotations next: %v", err)
		}
		var gotNext []string
		for _, cm := range next.ChannelMessages {
			for _, an := range cm.Annotations {
				gotNext = append(gotNext, an.Serial.ToTimeserialString())
			}
		}
		if !equalStrings(gotNext, serials[2:]) {
			t.Errorf("next page = %v, want %v", gotNext, serials[2:])
		}
	})

	t.Run("AnnotationsUnknownTargetIsEmpty", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		page, err := ch.Annotations(context.Background(), "00000000000001-000@nope:000", storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Annotations: %v", err)
		}
		if len(page.ChannelMessages) != 0 {
			t.Errorf("unknown-target annotations = %d, want 0", len(page.ChannelMessages))
		}
	})

	t.Run("AnnotationIdempotentByID", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})
		first, idemp, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Id: new("ann-1"), Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: "👍", MessageSerial: target.Serial},
		}, testFold)
		if err != nil || idemp {
			t.Fatalf("first StoreAnnotation: err=%v idempotent=%v", err, idemp)
		}
		second, idemp, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Id: new("ann-1"), Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: "❤️", MessageSerial: target.Serial},
		}, testFold)
		if err != nil {
			t.Fatalf("second StoreAnnotation: %v", err)
		}
		if !idemp {
			t.Error("idempotent=false on duplicate annotation id")
		}
		if second.ChannelSerial != first.ChannelSerial {
			t.Errorf("returned serial = %q, want original %q", second.ChannelSerial, first.ChannelSerial)
		}
		page, _ := ch.Annotations(context.Background(), target.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if n := len(page.ChannelMessages); n != 1 {
			t.Errorf("annotations = %d, want 1 (duplicate must not persist)", n)
		}
	})

	t.Run("AnnotationsSkippedByMessageAndPresenceHistory", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})
		mustEnter(t, ch, "conn-1", "alice", "hi")
		if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:multiple.v1", Name: "👍", MessageSerial: target.Serial},
		}, testFold); err != nil {
			t.Fatalf("StoreAnnotation: %v", err)
		}

		// Message history: only the create, no annotation cm.
		msgPage, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("message History: %v", err)
		}
		for _, cm := range msgPage.ChannelMessages {
			if len(cm.Annotations) != 0 {
				t.Error("message history leaked an annotation cm")
			}
		}
		if n := len(msgPage.ChannelMessages); n != 1 {
			t.Errorf("message history cms = %d, want 1 (create only)", n)
		}

		// Presence history skips the annotation cm too.
		presPage, err := ch.History(context.Background(), storage.HistoryQuery{Kind: storage.KindPresence, Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("presence History: %v", err)
		}
		for _, cm := range presPage.ChannelMessages {
			if len(cm.Annotations) != 0 {
				t.Error("presence history leaked an annotation cm")
			}
		}
	})

	t.Run("SummaryFoldLandsOnTheProjection", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})

		if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
		}, testFold); err != nil {
			t.Fatalf("StoreAnnotation: %v", err)
		}

		// The projection carries the current summary, which is where every
		// read of the message finds it.
		latest, err := ch.LatestVersion(context.Background(), target.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		if !wantDistinct(latest, "reaction:distinct.v1", "👍", "bob") {
			t.Errorf("projection summary = %#v, want 👍:[bob]", summaryOf(latest))
		}

		// A second client accumulates; reads reflect the latest summary.
		if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("carol"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
		}, testFold); err != nil {
			t.Fatalf("StoreAnnotation 2: %v", err)
		}
		latest, _ = ch.LatestVersion(context.Background(), target.Serial)
		if !wantDistinct(latest, "reaction:distinct.v1", "👍", "bob", "carol") {
			t.Errorf("projection summary after 2nd = %#v, want 👍:[bob,carol]", summaryOf(latest))
		}

		// Collapsed message history carries the summary too.
		col, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
		if err != nil {
			t.Fatalf("collapsed History: %v", err)
		}
		if len(col.ChannelMessages) != 1 || len(col.ChannelMessages[0].Messages) != 1 {
			t.Fatalf("collapsed cms = %d, want 1", len(col.ChannelMessages))
		}
		if !wantDistinct(col.ChannelMessages[0].Messages[0], "reaction:distinct.v1", "👍", "bob", "carol") {
			t.Errorf("history summary = %#v, want 👍:[bob,carol]", summaryOf(col.ChannelMessages[0].Messages[0]))
		}
	})

	t.Run("SummaryIsPublishedButIsNotAVersion", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})

		if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
		}, testFold); err != nil {
			t.Fatalf("StoreAnnotation: %v", err)
		}

		latest, err := ch.LatestVersion(context.Background(), target.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		summary := latest.Clone()
		summary.Action = wire.MessageAction_MESSAGE_SUMMARY
		cm, err := ch.StoreSummary(context.Background(), []*wire.Message{summary})
		if err != nil {
			t.Fatalf("StoreSummary: %v", err)
		}
		if cm.ChannelSerial == "" {
			t.Error("summary cm has no channelSerial; subscribers order by it and resume from it")
		}

		// It is on the log, so it reaches subscribers and replays on resume.
		page, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		var summaries int
		for _, entry := range page.ChannelMessages {
			for _, m := range entry.Messages {
				if m.Action == wire.MessageAction_MESSAGE_SUMMARY {
					summaries++
				}
			}
		}
		if summaries != 1 {
			t.Errorf("summaries on the log = %d, want 1", summaries)
		}

		// It is not a version of the message, and it has not replaced what the
		// message says either.
		vers, err := ch.Versions(context.Background(), target.Serial, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("Versions: %v", err)
		}
		if n := itemCountVersions(vers); n != 1 {
			t.Errorf("versions = %d, want 1 (the create; a summary is not a version)", n)
		}
		if again, _ := ch.LatestVersion(context.Background(), target.Serial); again.Action == wire.MessageAction_MESSAGE_SUMMARY {
			t.Error("the summary replaced the message on the projection")
		}
	})

	t.Run("SummaryFoldAccumulatesAcrossABatch", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})

		// Two creates in one batch on the same target fold one after the
		// other, so the projection ends up carrying both (DESIGN.md §14.2).
		if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:total.v1", MessageSerial: target.Serial},
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("carol"), Type: "reaction:total.v1", MessageSerial: target.Serial},
		}, testFold); err != nil {
			t.Fatalf("StoreAnnotation batch: %v", err)
		}
		latest, err := ch.LatestVersion(context.Background(), target.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		agg := summaryOf(latest)["reaction:total.v1"]
		if agg == nil || agg.GetTotalV1() == nil || agg.GetTotalV1().Total != 2 {
			t.Errorf("batch summary = %#v, want a total of 2", summaryOf(latest))
		}
	})

	t.Run("SummaryFoldDeleteRemovesContribution", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		target := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("post"), ClientId: new("alice")})

		for _, a := range []*wire.Annotation{
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("bob"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
			{Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new("carol"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
			{Action: wire.Annotation_ANNOTATION_DELETE, ClientId: new("bob"), Type: "reaction:distinct.v1", Name: "👍", MessageSerial: target.Serial},
		} {
			if _, _, err := ch.StoreAnnotation(context.Background(), []*wire.Annotation{a}, testFold); err != nil {
				t.Fatalf("StoreAnnotation: %v", err)
			}
		}
		latest, err := ch.LatestVersion(context.Background(), target.Serial)
		if err != nil {
			t.Fatalf("LatestVersion: %v", err)
		}
		if !wantDistinct(latest, "reaction:distinct.v1", "👍", "carol") {
			t.Errorf("after delete summary = %#v, want 👍:[carol]", summaryOf(latest))
		}
	})

	t.Run("CollapsedDeletedShowsAsTombstone", func(t *testing.T) {
		s := f(t)
		ch := mustChannel(t, s, "foo")
		created := mustCreate(t, ch, &wire.Message{Data: wire.MessageStrData("v1"), ClientId: new("alice")})
		mustMutate(t, ch, &wire.Message{Action: wire.MessageAction_MESSAGE_DELETE, Serial: created.Serial, ClientId: new("mod")})

		col, err := ch.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
		if err != nil {
			t.Fatalf("collapsed History: %v", err)
		}
		if len(col.ChannelMessages) != 1 || len(col.ChannelMessages[0].Messages) != 1 {
			t.Fatalf("collapsed cms = %d, want 1 (deleted message still positioned)", len(col.ChannelMessages))
		}
		if got := col.ChannelMessages[0].Messages[0]; got.Action != wire.MessageAction_MESSAGE_DELETE {
			t.Errorf("collapsed deleted action = %v, want delete (tombstone)", got.Action)
		}
	})
}

// mustCreate publishes a single create message and returns the stamped
// Message (with its serial + version) from the persisted cm.
func mustCreate(t *testing.T, ch storage.ChannelStore, m *wire.Message) *wire.Message {
	t.Helper()
	cm, _, err := ch.Store(context.Background(), []*wire.Message{m})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	return cm.Messages[0]
}

// mustMutate applies a mutation and returns the persisted merged version.
func mustMutate(t *testing.T, ch storage.ChannelStore, mut *wire.Message) *wire.Message {
	t.Helper()
	cm, _, err := ch.Mutate(context.Background(), mut, testMerge)
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
			out = append(out, m.VersionOrSerial())
		}
	}
	return out
}

// mustEnter publishes a single ENTER for (connID, clientID) with data.
func mustEnter(t *testing.T, ch storage.ChannelStore, connID, clientID, data string) {
	t.Helper()
	mustPresence(t, ch, &wire.PresenceMessage{
		Action: wire.PresenceMessage_ENTER, ConnectionId: connID, ClientId: new(clientID), Data: wire.MessageStrData(data),
	})
}

// testMerge and testFold are what an edit and an annotation mean, as the
// server hands them to storage: the shared module's, so the contract suite
// exercises the same answers the server serves.
var (
	testMerge = core.MergeVersion(0, logging.Nop)
	testFold  = core.FoldSummary(logging.Nop)
)

// summaryOf is a message's annotation summary, which the wire carries under
// the annotations it is a fold of.
func summaryOf(m *wire.Message) channel.Summary {
	return m.GetAnnotations().GetSummary()
}

// wantDistinct reports whether a message's summary records exactly the given
// clients against one value of a distinct.v1 annotation type.
func wantDistinct(m *wire.Message, typ, value string, clients ...string) bool {
	agg := summaryOf(m)[typ]
	if agg == nil || agg.GetDistinctV1() == nil {
		return false
	}
	list := agg.GetDistinctV1().Values[value]
	return list != nil && equalStrings(list.ClientIds, clients) && int(list.Total) == len(clients)
}

// mustStruct renders a free-form extras map as the struct the wire carries.
func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("structpb.NewStruct(%#v): %v", m, err)
	}
	return s
}

// mustPresence calls StorePresence and fails the test on error.
func mustPresence(t *testing.T, ch storage.ChannelStore, pms ...*wire.PresenceMessage) *protocol.ChannelMessage {
	t.Helper()
	cm, _, err := ch.StorePresence(context.Background(), pms)
	if err != nil {
		t.Fatalf("StorePresence: %v", err)
	}
	return cm
}

// testApply is what an object operation means, as the server hands it to
// storage: the shared module's, so the contract suite exercises the same
// answers the server serves.
var testApply = core.ApplyOperations(logging.Nop)

// mustState calls StoreState and fails the test on error.
func mustState(t *testing.T, ch storage.ChannelStore, state ...*wire.StateMessage) *protocol.ChannelMessage {
	t.Helper()
	cm, _, err := ch.StoreState(context.Background(), state, testApply)
	if err != nil {
		t.Fatalf("StoreState: %v", err)
	}
	return cm
}

// mustObjects reads the whole materialised object set.
func mustObjects(t *testing.T, ch storage.ChannelStore) []*wire.StateObject {
	t.Helper()
	page, err := ch.Objects(context.Background(), storage.ObjectsQuery{})
	if err != nil {
		t.Fatalf("Objects: %v", err)
	}
	return page.Objects
}

// mapCreateAndSet is one object as a client makes one: a map create, and the
// set on the root under key that makes it reachable. It returns the object id
// the create derives, which is what the set points at.
func mapCreateAndSet(key string) (string, []*wire.StateMessage) {
	create := statebuilder.NewMapOp(wire.StateOperation_MAP_CREATE, wire.Map_LWW)
	//lint:ignore SA1019 the nonce is a V1 field on the wire but is still what an object id is derived from
	create.Nonce = statebuilder.RandomNonce()
	objectID := statebuilder.InferObjectId(time.Now(), create)

	return objectID, []*wire.StateMessage{
		{Operation: create},
		{Operation: &wire.StateOperation{
			Action:   wire.StateOperation_MAP_SET,
			ObjectId: wire.RootObjectID,
			MapSet:   &wire.MapSet{Key: key, Value: &wire.StateData{ObjectId: &objectID}},
		}},
	}
}

// rootSet is a single operation setting a string under key on the root.
func rootSet(key, value string) *wire.StateMessage {
	return &wire.StateMessage{Operation: &wire.StateOperation{
		Action:   wire.StateOperation_MAP_SET,
		ObjectId: wire.RootObjectID,
		MapSet:   &wire.MapSet{Key: key, Value: &wire.StateData{String_: &value}},
	}}
}

// objectsByID indexes an object set by object id.
func objectsByID(objects []*wire.StateObject) map[string]*wire.StateObject {
	out := make(map[string]*wire.StateObject, len(objects))
	for _, o := range objects {
		out[o.GetObjectId()] = o
	}
	return out
}

// objectIDsOf is an object set's ids, for a failure message.
func objectIDsOf(objects []*wire.StateObject) []string {
	out := make([]string, 0, len(objects))
	for _, o := range objects {
		out = append(out, o.GetObjectId())
	}
	return out
}

// mustOccupancy stores one contribution and fails the test on error.
func mustOccupancy(t *testing.T, ch storage.ChannelStore, counts *wire.ChannelOccupancy) {
	t.Helper()
	if err := ch.StoreOccupancy(context.Background(), counts); err != nil {
		t.Fatalf("StoreOccupancy: %v", err)
	}
}

// readOccupancy reads the channel's aggregate and fails the test on error.
func readOccupancy(t *testing.T, ch storage.ChannelStore) *wire.ChannelOccupancy {
	t.Helper()
	occ, err := ch.Occupancy(context.Background())
	if err != nil {
		t.Fatalf("Occupancy: %v", err)
	}
	return occ
}

// membersByKey indexes a membership set by its (connectionId, clientId) key.
func membersByKey(members []*wire.PresenceMessage) map[string]*wire.PresenceMessage {
	out := make(map[string]*wire.PresenceMessage, len(members))
	for _, p := range members {
		out[storage.MemberKey(p.ConnectionId, p.GetClientId())] = p
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
	// occupancyChanges counts the signals the backend sent, which is how a
	// test asserts that storing a contribution told the channel about it.
	occupancyChanges int
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

func (a *capturingAppender) OccupancyChanged() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.occupancyChanges++
}

func (a *capturingAppender) initialized() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.initCurrent
}

func (a *capturingAppender) occupancyChangeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.occupancyChanges
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

// flattenNames returns the concatenated Message.GetName() sequence across
// every ChannelMessage in the page, preserving the page's order. Used
// to verify direction-aware emit order at Message granularity.
func flattenNames(p storage.HistoryPage) []string {
	var out []string
	for _, cm := range p.ChannelMessages {
		for _, m := range cm.Messages {
			out = append(out, m.GetName())
		}
	}
	return out
}

// flattenSerials returns the concatenated Message.Serial sequence
// across every ChannelMessage in the page, preserving the page's
// order — the collapsed-history counterpart to channelSerialsOf (which
// reads cm.ChannelSerial, not the per-message identity).
func flattenSerials(p storage.HistoryPage) []string {
	var out []string
	for _, cm := range p.ChannelMessages {
		for _, m := range cm.Messages {
			out = append(out, m.Serial)
		}
	}
	return out
}
