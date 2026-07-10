package protocol

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestAnnotationRoundTrip pins the Annotation wire shape: JSON and msgpack
// both round-trip every field, and the field names match Ably's
// (DESIGN.md §14.1, TASK-63 AC#1).
func TestAnnotationRoundTrip(t *testing.T) {
	original := &Annotation{
		ID:            "client-key",
		Serial:        "00000000000042-000@abc:000",
		Action:        AnnotationDelete,
		ClientID:      "alice",
		ConnectionID:  "conn-1",
		Type:          "reaction:multiple.v1",
		Name:          "👍",
		MessageSerial: "00000000000001-000@abc:000",
		Count:         3,
		Data:          "payload",
		Encoding:      "utf-8",
		Timestamp:     1700000000000,
	}

	t.Run("json", func(t *testing.T) {
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var got Annotation
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got, *original) {
			t.Fatalf("json round-trip mismatch:\n got %+v\nwant %+v", got, *original)
		}
		// Field names pinned to Ably's wire shape.
		for _, want := range []string{`"messageSerial":`, `"clientId":`, `"connectionId":`, `"type":`, `"count":`, `"action":`} {
			if !strings.Contains(string(data), want) {
				t.Errorf("JSON %s missing field %s", data, want)
			}
		}
	})

	t.Run("msgpack", func(t *testing.T) {
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		enc.UseCompactInts(true)
		if err := enc.Encode(original); err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Annotation
		if err := msgpack.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !reflect.DeepEqual(got, *original) {
			t.Fatalf("msgpack round-trip mismatch:\n got %+v\nwant %+v", got, *original)
		}
	})
}

// TestAnnotationActionAlwaysEncoded ensures action=0 (annotation.create) is
// emitted on the wire (no omitempty), matching Message/Presence actions.
func TestAnnotationActionAlwaysEncoded(t *testing.T) {
	data, err := json.Marshal(&Annotation{Type: "reaction:distinct.v1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"action":0`) {
		t.Errorf("create annotation JSON = %s, want action:0 present", data)
	}
	if AnnotationCreate != 0 || AnnotationDelete != 1 {
		t.Errorf("annotation action values = create:%d delete:%d, want 0/1", AnnotationCreate, AnnotationDelete)
	}
	if ActionAnnotation != 21 {
		t.Errorf("ANNOTATION protocol action = %d, want 21", ActionAnnotation)
	}
}

func TestAnnotationModeFlagBits(t *testing.T) {
	if FlagAnnotationPublish != 1<<21 {
		t.Errorf("ANNOTATION_PUBLISH = %d, want 1<<21", FlagAnnotationPublish)
	}
	if FlagAnnotationSubscribe != 1<<22 {
		t.Errorf("ANNOTATION_SUBSCRIBE = %d, want 1<<22", FlagAnnotationSubscribe)
	}
}

func TestParseAnnotationType(t *testing.T) {
	cases := []struct {
		in        string
		name, agg string
		ok        bool
	}{
		{"reaction:distinct.v1", "reaction", "distinct.v1", true},
		{"bare", "bare", "", true},
		{"", "", "", false},
		{":distinct.v1", "", "", false},
		{"a:b:c", "", "", false},
	}
	for _, c := range cases {
		name, agg, ok := ParseAnnotationType(c.in)
		if ok != c.ok || (ok && (name != c.name || agg != c.agg)) {
			t.Errorf("ParseAnnotationType(%q) = (%q,%q,%v), want (%q,%q,%v)", c.in, name, agg, ok, c.name, c.agg, c.ok)
		}
	}
}

func TestAnnotationValidate(t *testing.T) {
	cases := []struct {
		name    string
		a       Annotation
		wantErr bool
	}{
		{"missing messageSerial", Annotation{Type: "reaction:distinct.v1", ClientID: "a"}, true},
		{"missing type", Annotation{MessageSerial: "s"}, true},
		{"malformed type", Annotation{MessageSerial: "s", Type: "a:b:c", ClientID: "a"}, true},
		{"unknown aggregation", Annotation{MessageSerial: "s", Type: "reaction:bogus.v1", ClientID: "a"}, true},
		{"unaggregated ok", Annotation{MessageSerial: "s", Type: "bare", ClientID: "a"}, false},
		{"identified distinct ok", Annotation{MessageSerial: "s", Type: "reaction:distinct.v1", ClientID: "a"}, false},
		{"anonymous distinct rejected", Annotation{MessageSerial: "s", Type: "reaction:distinct.v1"}, true},
		{"anonymous multiple ok", Annotation{MessageSerial: "s", Type: "reaction:multiple.v1"}, false},
		{"anonymous total ok", Annotation{MessageSerial: "s", Type: "flag:total.v1"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := c.a
			err := a.Validate()
			if (err != nil) != c.wantErr {
				t.Fatalf("Validate() err = %v, wantErr %v", err, c.wantErr)
			}
			if err != nil && err.Code != 40000 {
				t.Errorf("Validate() error code = %d, want 40000", err.Code)
			}
		})
	}
}

// TestAnnotationCountDefaultsToOne pins the multiple.v1 count default
// (Ably PSRFC-13, DESIGN.md §14.1).
func TestAnnotationCountDefaultsToOne(t *testing.T) {
	a := Annotation{MessageSerial: "s", Type: "reaction:multiple.v1", ClientID: "alice", Count: 0}
	if err := a.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if a.Count != 1 {
		t.Errorf("multiple.v1 count = %d, want defaulted to 1", a.Count)
	}
	// distinct.v1 does not default count.
	b := Annotation{MessageSerial: "s", Type: "reaction:distinct.v1", ClientID: "alice", Count: 0}
	if err := b.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if b.Count != 0 {
		t.Errorf("distinct.v1 count = %d, want left at 0", b.Count)
	}
}
