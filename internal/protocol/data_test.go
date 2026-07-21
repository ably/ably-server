package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// deadbeef is a 4-byte binary payload and its canonical base64 form, used
// throughout to exercise the binary transcoding rules.
var (
	deadbeef       = []byte{0xde, 0xad, 0xbe, 0xef}
	deadbeefBase64 = "3q2+7w==" // base64.StdEncoding of deadbeef
)

// TestDataFromExternal pins the ingress normalisation rules (DataFromExternal),
// the `Data any` adaptation of the reference's ablyrpc.DataFromExternal.
func TestDataFromExternal(t *testing.T) {
	cases := []struct {
		name     string
		in       any
		inEnc    string
		wantData any
		wantEnc  string
	}{
		{"plain string", "foo", "", "foo", ""},
		{"outer base64 → bytes, suffix stripped", deadbeefBase64, "base64", deadbeef, ""},
		{"nested base64 suffix stripped, prefix kept", base64.StdEncoding.EncodeToString([]byte("foo")), "utf-8/base64", []byte("foo"), "utf-8"},
		{"raw bytes kept", deadbeef, "", deadbeef, ""},
		{"object → json string, encoding json", map[string]any{"foo": float64(42)}, "", `{"foo":42}`, "json"},
		{"array → json string, encoding json", []any{float64(1), "x"}, "", `[1,"x"]`, "json"},
		{"bool → string", true, "", "true", ""},
		{"number → string", float64(42), "", "42", ""},
		{"nil → nil, encoding preserved", nil, "keep", nil, "keep"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, enc, err := DataFromExternal(tc.in, tc.inEnc)
			if err != nil {
				t.Fatalf("DataFromExternal: %v", err)
			}
			if !reflect.DeepEqual(data, tc.wantData) {
				t.Errorf("data = %#v, want %#v", data, tc.wantData)
			}
			if enc != tc.wantEnc {
				t.Errorf("encoding = %q, want %q", enc, tc.wantEnc)
			}
		})
	}
}

// TestDataFromExternalEmptySliceNotNil pins the empty-slice-not-nil quirk
// (reference AsAny): an empty binary payload must be a non-nil []byte, since
// a msgpack nil breaks ably-js decode.
func TestDataFromExternalEmptySliceNotNil(t *testing.T) {
	data, enc, err := DataFromExternal("", "base64")
	if err != nil {
		t.Fatalf("DataFromExternal: %v", err)
	}
	b, ok := data.([]byte)
	if !ok {
		t.Fatalf("data type = %T, want []byte", data)
	}
	if b == nil {
		t.Error("empty base64 decoded to nil []byte, want non-nil empty slice")
	}
	if len(b) != 0 {
		t.Errorf("len = %d, want 0", len(b))
	}
	if enc != "" {
		t.Errorf("encoding = %q, want empty", enc)
	}
}

func TestGetOuterEncoding(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"json", "json"},
		{"base64", "base64"},
		{"utf-8/cipher+aes-256-cbc/base64", "/base64"},
		{"utf-8/base64", "/base64"},
	}
	for _, tc := range cases {
		if got := getOuterEncoding(tc.in); got != tc.want {
			t.Errorf("getOuterEncoding(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestMessageJSONBinaryEgress pins the egress rule: over JSON a []byte payload
// is emitted as a base64 string with "base64" appended to the encoding, while
// a string payload and its encoding are emitted unchanged.
func TestMessageJSONBinaryEgress(t *testing.T) {
	cases := []struct {
		name     string
		data     any
		enc      string
		wantData string
		wantEnc  string
	}{
		{"bytes, empty encoding", deadbeef, "", deadbeefBase64, "base64"},
		{"bytes, existing encoding", deadbeef, "utf-8/cipher+aes-256-cbc", deadbeefBase64, "utf-8/cipher+aes-256-cbc/base64"},
		{"string unaffected", "foo", "", "foo", ""},
		{"json string unaffected", `{"a":1}`, "json", `{"a":1}`, "json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(&Message{Serial: "s:0", Action: MessageCreate, Data: tc.data, Encoding: tc.enc})
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var wire struct {
				Data     string `json:"data"`
				Encoding string `json:"encoding"`
			}
			if err := json.Unmarshal(b, &wire); err != nil {
				t.Fatalf("Unmarshal wire: %v", err)
			}
			if wire.Data != tc.wantData {
				t.Errorf("wire data = %q, want %q", wire.Data, tc.wantData)
			}
			if wire.Encoding != tc.wantEnc {
				t.Errorf("wire encoding = %q, want %q (raw: %s)", wire.Encoding, tc.wantEnc, b)
			}
		})
	}
}

// TestMessageJSONIngress pins that a JSON base64 payload is normalised to raw
// bytes with the base64 suffix stripped on decode.
func TestMessageJSONIngress(t *testing.T) {
	var m Message
	if err := json.Unmarshal([]byte(`{"data":"`+deadbeefBase64+`","encoding":"base64"}`), &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	b, ok := m.Data.([]byte)
	if !ok {
		t.Fatalf("Data type = %T, want []byte", m.Data)
	}
	if !bytes.Equal(b, deadbeef) {
		t.Errorf("Data = %x, want %x", b, deadbeef)
	}
	if m.Encoding != "" {
		t.Errorf("Encoding = %q, want empty (suffix stripped)", m.Encoding)
	}
}

// TestMessageMsgpackBinaryIngress pins that a msgpack bin payload decodes to
// raw bytes with the encoding unchanged.
func TestMessageMsgpackBinaryIngress(t *testing.T) {
	blob := mustMsgpackMap(t, map[string]any{"data": deadbeef, "encoding": ""})
	var m Message
	if err := msgpack.Unmarshal(blob, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	b, ok := m.Data.([]byte)
	if !ok {
		t.Fatalf("Data type = %T, want []byte", m.Data)
	}
	if !bytes.Equal(b, deadbeef) {
		t.Errorf("Data = %x, want %x", b, deadbeef)
	}
	if m.Encoding != "" {
		t.Errorf("Encoding = %q, want empty", m.Encoding)
	}
}

// TestMessageMsgpackMapCanonicalisedViaTranscoder pins that a msgpack map data
// payload is canonicalised to a JSON string via the streaming MsgpackToJSON
// transcoder — preserving the msgpack key order, which json.Marshal of a Go
// map would NOT (it sorts keys) — with the encoding set to "json".
func TestMessageMsgpackMapCanonicalisedViaTranscoder(t *testing.T) {
	// Encode a message map {"data": {"b":1,"a":2}} with the inner keys in
	// b-then-a order; the transcoder must preserve that order.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	must(t, enc.EncodeMapLen(1))
	must(t, enc.EncodeString("data"))
	must(t, enc.EncodeMapLen(2))
	must(t, enc.EncodeString("b"))
	must(t, enc.EncodeInt(1))
	must(t, enc.EncodeString("a"))
	must(t, enc.EncodeInt(2))

	var m Message
	if err := msgpack.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	s, ok := m.Data.(string)
	if !ok {
		t.Fatalf("Data type = %T, want string", m.Data)
	}
	if s != `{"b":1,"a":2}` {
		t.Errorf("Data = %q, want %q (transcoder must preserve msgpack key order)", s, `{"b":1,"a":2}`)
	}
	if m.Encoding != "json" {
		t.Errorf("Encoding = %q, want %q", m.Encoding, "json")
	}
}

// TestCrossFormatBinaryFanout pins the full msgpack→JSON transcode: a binary
// payload decoded from msgpack is re-emitted over JSON as base64+suffix, and
// the reverse (JSON base64 → msgpack) emits raw bin bytes (not a base64
// string, not nil).
func TestCrossFormatBinaryFanout(t *testing.T) {
	// msgpack ingress → JSON egress.
	mpIn := mustMsgpackMap(t, map[string]any{"data": deadbeef})
	var m Message
	if err := msgpack.Unmarshal(mpIn, &m); err != nil {
		t.Fatalf("msgpack Unmarshal: %v", err)
	}
	jsonOut, err := json.Marshal(&m)
	if err != nil {
		t.Fatalf("json Marshal: %v", err)
	}
	var wire struct{ Data, Encoding string }
	if err := json.Unmarshal(jsonOut, &wire); err != nil {
		t.Fatalf("wire Unmarshal: %v", err)
	}
	if wire.Data != deadbeefBase64 || wire.Encoding != "base64" {
		t.Errorf("JSON egress = {data:%q, encoding:%q}, want {%q, %q}", wire.Data, wire.Encoding, deadbeefBase64, "base64")
	}

	// JSON ingress → msgpack egress: the stored []byte is emitted as a
	// msgpack bin, not a base64 string.
	var m2 Message
	if err := json.Unmarshal([]byte(`{"data":"`+deadbeefBase64+`","encoding":"base64"}`), &m2); err != nil {
		t.Fatalf("json Unmarshal: %v", err)
	}
	mpOut, err := msgpack.Marshal(&m2)
	if err != nil {
		t.Fatalf("msgpack Marshal: %v", err)
	}
	var decoded map[string]any
	dec := msgpack.NewDecoder(bytes.NewReader(mpOut))
	if err := dec.Decode(&decoded); err != nil {
		t.Fatalf("decode map: %v", err)
	}
	b, ok := decoded["data"].([]byte)
	if !ok {
		t.Fatalf("msgpack data type = %T, want []byte (raw bin)", decoded["data"])
	}
	if !bytes.Equal(b, deadbeef) {
		t.Errorf("msgpack data = %x, want %x", b, deadbeef)
	}
	if enc, present := decoded["encoding"]; present && enc != "" {
		t.Errorf("msgpack encoding = %v, want empty/absent", enc)
	}
}

// TestPresenceAndAnnotationBinaryEgress confirms the binary→base64 JSON egress
// rule applies to presence and annotation payloads too.
func TestPresenceAndAnnotationBinaryEgress(t *testing.T) {
	p, err := json.Marshal(&PresenceMessage{Action: PresenceEnter, Data: deadbeef})
	if err != nil {
		t.Fatalf("presence Marshal: %v", err)
	}
	if !bytes.Contains(p, []byte(`"data":"`+deadbeefBase64+`"`)) || !bytes.Contains(p, []byte(`"encoding":"base64"`)) {
		t.Errorf("presence JSON = %s, want base64 data + base64 encoding", p)
	}
	a, err := json.Marshal(&Annotation{Action: AnnotationCreate, Type: "r:multiple.v1", MessageSerial: "t:0", Data: deadbeef})
	if err != nil {
		t.Fatalf("annotation Marshal: %v", err)
	}
	if !bytes.Contains(a, []byte(`"data":"`+deadbeefBase64+`"`)) || !bytes.Contains(a, []byte(`"encoding":"base64"`)) {
		t.Errorf("annotation JSON = %s, want base64 data + base64 encoding", a)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
}

// mustMsgpackMap encodes a map as a msgpack map with plain string keys.
func mustMsgpackMap(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := msgpack.Marshal(m)
	if err != nil {
		t.Fatalf("marshal map: %v", err)
	}
	return b
}
