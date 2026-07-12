package protocol

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// fullMessage / fullPresenceMessage / fullAnnotation build values with
// every wire field populated non-zero, so the parity test sees every
// omitempty field emitted.
func fullMessage() *Message {
	return &Message{
		ID:           "b:0",
		Serial:       "s:000",
		Action:       MessageUpdate,
		ClientID:     "alice",
		ConnectionID: "conn-1",
		Name:         "greeting",
		Data:         "hello",
		Encoding:     "utf-8",
		Timestamp:    1700000000000,
		Version:      &MessageVersion{Serial: "s:000", Timestamp: 1700000000000, ClientID: "alice"},
		Summary:      Summary{"reaction:total.v1": {Method: "total.v1", Total: &TotalAggregation{Total: 1}}},
		Alt:          map[string]*Message{DeltaAppend: {Action: MessageAppend, Data: ", world", Serial: "s:000"}},
	}
}

func fullPresenceMessage() *PresenceMessage {
	return &PresenceMessage{
		ID:           "p:0",
		Serial:       "s:000",
		Action:       PresenceEnter,
		ClientID:     "alice",
		ConnectionID: "conn-1",
		Data:         "hello",
		Encoding:     "utf-8",
		Timestamp:    1700000000000,
	}
}

func fullAnnotation() *Annotation {
	return &Annotation{
		ID:            "a:0",
		Serial:        "s:000",
		Action:        AnnotationCreate,
		ClientID:      "alice",
		ConnectionID:  "conn-1",
		Type:          "reaction:multiple.v1",
		Name:          "👍",
		MessageSerial: "t:000",
		Count:         3,
		Data:          "hello",
		Encoding:      "utf-8",
		Timestamp:     1700000000000,
		// Summary is server-internal (msgpack:"-") and must NOT be emitted.
		Summary: Summary{"reaction:total.v1": {Method: "total.v1", Total: &TotalAggregation{Total: 1}}},
	}
}

// TestMsgpackEncoderFieldParity is the drift guard for TASK-97: the custom
// msgpack encoders (msgpackcodec.go) hand-list each type's wire fields, so
// a field added to the struct without being added to its encoder would
// silently vanish from the wire and storage. This enumerates each struct's
// msgpack tags by reflection and fails if the encoded map's key set differs
// — so the omission breaks the build's tests, not the wire.
func TestMsgpackEncoderFieldParity(t *testing.T) {
	cases := []struct {
		name string
		val  any
	}{
		{"Message", fullMessage()},
		{"PresenceMessage", fullPresenceMessage()},
		{"Annotation", fullAnnotation()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := msgpackTagNames(reflect.TypeOf(tc.val).Elem())

			data, err := msgpack.Marshal(tc.val)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var m map[string]any
			if err := unmarshalMsgpackMap(data, &m); err != nil {
				t.Fatalf("decode map: %v", err)
			}
			got := make([]string, 0, len(m))
			for k := range m {
				got = append(got, k)
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("encoder field set drift for %s:\n encoded keys = %v\n struct tags   = %v\n"+
					"update the custom encoder in msgpackcodec.go to match the struct", tc.name, got, want)
			}
		})
	}
}

// msgpackTagNames returns the wire field names declared by a struct's
// msgpack tags, excluding fields tagged "-" (server-internal).
func msgpackTagNames(t reflect.Type) []string {
	var names []string
	for i := range t.NumField() {
		tag := t.Field(i).Tag.Get("msgpack")
		if tag == "" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = t.Field(i).Name
		}
		names = append(names, name)
	}
	return names
}

// TestEmptyDataSurvivesRoundTrip is TASK-97 AC#3: an empty-string (and
// empty []byte) Data must survive both wire formats AND the msgpack
// storage payload encoding — msgpack's omitempty would otherwise drop an
// interface-held "" (DESIGN.md §6). Covers Message, PresenceMessage and
// Annotation for consistency (each shares the storage round-trip).
func TestEmptyDataSurvivesRoundTrip(t *testing.T) {
	t.Run("Message", func(t *testing.T) {
		for _, data := range []any{"", []byte{}} {
			m := &Message{Serial: "s:000", Action: MessageCreate, Data: data}
			// Storage payload path: msgpack.Marshal directly on the value.
			mp, err := msgpack.Marshal(m)
			if err != nil {
				t.Fatalf("msgpack Marshal: %v", err)
			}
			var got Message
			if err := msgpack.Unmarshal(mp, &got); err != nil {
				t.Fatalf("msgpack Unmarshal: %v", err)
			}
			if got.Data == nil {
				t.Errorf("msgpack storage round-trip dropped Data %#v (got nil)", data)
			}
			assertKeyPresent(t, "data", mp)

			// Wire path, both formats, wrapped in a ProtocolMessage.
			for _, f := range []Format{FormatJSON, FormatMsgpack} {
				wire, err := Marshal(&ProtocolMessage{Action: ActionMessage, Channel: "c", Messages: []*Message{m}}, f)
				if err != nil {
					t.Fatalf("%s Marshal: %v", f, err)
				}
				var out ProtocolMessage
				if err := Unmarshal(wire, f, &out); err != nil {
					t.Fatalf("%s Unmarshal: %v", f, err)
				}
				if len(out.Messages) != 1 || out.Messages[0].Data == nil {
					t.Errorf("%s wire round-trip dropped Data %#v", f, data)
				}
			}
		}
	})

	t.Run("PresenceMessage", func(t *testing.T) {
		p := &PresenceMessage{Action: PresenceEnter, ClientID: "alice", Data: ""}
		mp, err := msgpack.Marshal(p)
		if err != nil {
			t.Fatalf("msgpack Marshal: %v", err)
		}
		assertKeyPresent(t, "data", mp)
		var got PresenceMessage
		if err := msgpack.Unmarshal(mp, &got); err != nil {
			t.Fatalf("msgpack Unmarshal: %v", err)
		}
		if got.Data == nil {
			t.Error("presence msgpack round-trip dropped empty Data")
		}
	})

	t.Run("Annotation", func(t *testing.T) {
		a := &Annotation{Action: AnnotationCreate, Type: "reaction:multiple.v1", MessageSerial: "t:000", Data: ""}
		mp, err := msgpack.Marshal(a)
		if err != nil {
			t.Fatalf("msgpack Marshal: %v", err)
		}
		assertKeyPresent(t, "data", mp)
		var got Annotation
		if err := msgpack.Unmarshal(mp, &got); err != nil {
			t.Fatalf("msgpack Unmarshal: %v", err)
		}
		if got.Data == nil {
			t.Error("annotation msgpack round-trip dropped empty Data")
		}
	})
}

// assertKeyPresent fails if the msgpack blob's top-level map lacks key.
func assertKeyPresent(t *testing.T, key string, blob []byte) {
	t.Helper()
	var m map[string]any
	if err := unmarshalMsgpackMap(blob, &m); err != nil {
		t.Fatalf("decode map: %v", err)
	}
	if _, ok := m[key]; !ok {
		t.Errorf("msgpack map %v missing key %q", m, key)
	}
}

// unmarshalMsgpackMap decodes blob into m, disabling interface-map key
// remapping so keys stay plain strings.
func unmarshalMsgpackMap(blob []byte, m *map[string]any) error {
	dec := msgpack.NewDecoder(bytes.NewReader(blob))
	return dec.Decode(m)
}

// toInt64 coerces a msgpack-decoded numeric (any int/uint kind) to int64.
func toInt64(v any) (int64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	default:
		return 0, false
	}
}
