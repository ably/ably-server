package storage

import (
	"errors"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
)

func TestStampMessageIDs(t *testing.T) {
	t.Run("GeneratesBatchIDForUnsetIDs", func(t *testing.T) {
		msgs := []*protocol.Message{{Name: "a"}, {Name: "b"}, {Name: "c"}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if len(batchID) != 8 {
			t.Errorf("batchID = %q, want an 8-char base64 id", batchID)
		}
		// idx is unpadded to match Ably's wire shape ("<batchID>:0").
		want := []string{batchID + ":0", batchID + ":1", batchID + ":2"}
		for i, m := range msgs {
			if m.ID != want[i] {
				t.Errorf("msgs[%d].ID = %q, want %q", i, m.ID, want[i])
			}
		}
	})

	t.Run("SingleMessageClientIDKeptAsBatchID", func(t *testing.T) {
		msgs := []*protocol.Message{{ID: "dup"}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if batchID != "dup" {
			t.Errorf("batchID = %q, want %q", batchID, "dup")
		}
		if msgs[0].ID != "dup" {
			t.Errorf("msgs[0].ID = %q, want it left unchanged", msgs[0].ID)
		}
	})

	t.Run("SingleMessageTrimsTrailingZeroIndex", func(t *testing.T) {
		msgs := []*protocol.Message{{ID: "base:0"}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if batchID != "base" {
			t.Errorf("batchID = %q, want %q", batchID, "base")
		}
	})

	t.Run("ConformingMultiBatchAccepted", func(t *testing.T) {
		msgs := []*protocol.Message{{ID: "b:0"}, {ID: "b:1"}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if batchID != "b" {
			t.Errorf("batchID = %q, want %q", batchID, "b")
		}
	})

	t.Run("MismatchedMultiBatchRejected", func(t *testing.T) {
		for _, tc := range [][]*protocol.Message{
			{{ID: "b:0"}, {ID: "b:2"}}, // wrong idx
			{{ID: "b:0"}, {ID: "c:1"}}, // wrong base
			{{ID: "b:1"}, {ID: "b:2"}}, // first not ":0"
			{{ID: "b:0"}, {Name: "x"}}, // some ids missing
		} {
			if _, err := StampMessageIDs(tc); !errors.Is(err, ErrInvalidMessageID) {
				t.Errorf("StampMessageIDs(%v) err = %v, want ErrInvalidMessageID", tc, err)
			}
		}
	})
}
