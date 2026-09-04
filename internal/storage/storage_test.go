package storage

import (
	"errors"
	"testing"

	"github.com/ably/server-protocol/go/wire"
)

func TestStampMessageIDs(t *testing.T) {
	t.Run("GeneratesBatchIDForUnsetIDs", func(t *testing.T) {
		msgs := []*wire.Message{{Name: new("a")}, {Name: new("b")}, {Name: new("c")}}
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
			if m.GetId() != want[i] {
				t.Errorf("msgs[%d].GetId() = %q, want %q", i, m.GetId(), want[i])
			}
		}
	})

	t.Run("SingleMessageClientIDKeptAsBatchID", func(t *testing.T) {
		msgs := []*wire.Message{{Id: new("dup")}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if batchID != "dup" {
			t.Errorf("batchID = %q, want %q", batchID, "dup")
		}
		if msgs[0].GetId() != "dup" {
			t.Errorf("msgs[0].GetId() = %q, want it left unchanged", msgs[0].GetId())
		}
	})

	t.Run("SingleMessageTrimsTrailingZeroIndex", func(t *testing.T) {
		msgs := []*wire.Message{{Id: new("base:0")}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if batchID != "base" {
			t.Errorf("batchID = %q, want %q", batchID, "base")
		}
	})

	t.Run("ConformingMultiBatchAccepted", func(t *testing.T) {
		msgs := []*wire.Message{{Id: new("b:0")}, {Id: new("b:1")}}
		batchID, err := StampMessageIDs(msgs)
		if err != nil {
			t.Fatalf("StampMessageIDs: %v", err)
		}
		if batchID != "b" {
			t.Errorf("batchID = %q, want %q", batchID, "b")
		}
	})

	t.Run("MismatchedMultiBatchRejected", func(t *testing.T) {
		for _, tc := range [][]*wire.Message{
			{{Id: new("b:0")}, {Id: new("b:2")}}, // wrong idx
			{{Id: new("b:0")}, {Id: new("c:1")}}, // wrong base
			{{Id: new("b:1")}, {Id: new("b:2")}}, // first not ":0"
			{{Id: new("b:0")}, {Name: new("x")}}, // some ids missing
		} {
			if _, err := StampMessageIDs(tc); !errors.Is(err, ErrInvalidMessageID) {
				t.Errorf("StampMessageIDs(%v) err = %v, want ErrInvalidMessageID", tc, err)
			}
		}
	})
}
