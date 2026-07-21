package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// This file adapts the payload normalisation rules from the reference's
// ablyrpc/data.go (go/realtime/lib/ablyrpc/data.go) to our `Data any`
// representation. The reference carries payload as a Data proto union with
// explicit Str/Bin arms; we carry it as an `any` holding either a Go string
// or a []byte. The RULES are identical (Ably's TR3/RSL4/RSL6 payload
// encoding):
//
//   - Over a JSON (text) transport binary data travels as a base64 string
//     with "base64" appended to the encoding; over msgpack it travels as raw
//     bytes with no suffix (jsonDataEncoding + the []byte JSON marshalling).
//   - On ingress an outer-base64 string is decoded to raw bytes and the
//     "base64" suffix stripped, so the STORED form is canonical (raw bytes,
//     no base64 suffix) regardless of the publishing transport
//     (DataFromExternal + getOuterEncoding).
//   - Objects and arrays are canonicalised to a JSON string with encoding
//     "json"; over msgpack this uses the streaming MsgpackToJSON transcoder
//     (msgpack.go) so the JSON text matches node byte-for-byte, including
//     msgpack key order and undefined-skipping (msgpackData).

// DataFromExternal converts an external value (an `any` decoded from JSON or
// msgpack) into the server's canonical Data representation — a Go string or a
// []byte — together with the canonical encoding.
//
// This is the `Data any` equivalent of the reference's
// ablyrpc.DataFromExternal, itself the Go equivalent of node's Data.normalise
// / Data.fromEncoded. The rules are identical:
//
//   - a string whose outer encoding is base64 is decoded to bytes with the
//     base64 suffix stripped;
//   - raw bytes are kept as bytes;
//   - objects and arrays are JSON-encoded to a string and the encoding is
//     deliberately overridden to "json" (avoiding a redundant "json/json"
//     if the caller already specified it on an unenveloped publish);
//   - anything else (a bool or a number) is stringified — node uses
//     String(data); "%v" is the close Go equivalent.
//
// A nil value returns nil data (no payload) with the encoding unchanged.
func DataFromExternal(v any, inEncoding string) (data any, outEncoding string, err error) {
	outEncoding = inEncoding
	if v == nil {
		return
	}
	switch w := v.(type) {
	case string:
		// if string data has an outer base64 encoding, then decode it
		// into bytes and strip off the base64 encoding
		if enc := getOuterEncoding(inEncoding); strings.HasSuffix(enc, "base64") {
			var b []byte
			b, err = base64.StdEncoding.DecodeString(w)
			if err != nil {
				return
			}
			data = b
			outEncoding = strings.TrimSuffix(inEncoding, enc)
			return
		}
		data = w
	case []byte:
		data = w
	case map[string]any, []any:
		// encode objects and arrays as JSON strings
		var b []byte
		b, err = json.Marshal(w)
		if err != nil {
			return
		}
		data = string(b)
		// deliberately override any specified encoding to avoid `json/json` if
		// someone redundantly specifies the encoding in an unenveloped publish
		outEncoding = "json"
	default:
		// anything else will be a bool or a number which get
		// converted to a string (in node with String(data),
		// here using a %v format string, which is a close
		// equivalent)
		data = fmt.Sprintf("%v", w)
	}
	return
}

// getOuterEncoding returns last element of the encoding if it
// contains one, ie json/base64 otherwise it returns the
// encoding as is. It will return the included seperator if it
// contains one.
func getOuterEncoding(enc string) string {
	p := strings.LastIndexByte(enc, '/')
	if p <= 0 {
		return enc
	}

	return enc[p:]
}

// jsonDataEncoding returns the encoding to emit alongside a message's data on
// a JSON (text) transport. Binary data is delivered as a base64 string, so
// "base64" is appended to the canonical (stored) encoding; string data is
// emitted unchanged. This is the egress half of the base64 rule and mirrors
// the reference's ablyrpc.jsonDataEncoding — the base64 suffix is appended at
// JSON emission, never stored.
func jsonDataEncoding(encoding string, data any) string {
	if _, isBin := data.([]byte); !isBin {
		return encoding
	}
	if encoding == "" {
		return "base64"
	}
	return encoding + "/base64"
}

// msgpackData is used to decode msgpack-encoded message data, potentially
// JSON encoding to a string if the data is not a string or bytes (i.e. it's
// an array, map, number, bool, or null).
//
// This is necessary rather than performing the JSON encoding when decoding
// the message's Data field directly, since the decision to JSON encode the
// data needs to be retained in the jsonEncoded field so that the message
// decoder can set the message's encoding field to "json" after the fact.
type msgpackData struct {
	value       any
	jsonEncoded bool
}

// DecodeMsgpack decodes msgpack-encoded message data, which can be any msgpack
// value (i.e. string, bytes, array, map, number, bool, or nil).
//
// msgpack strings and bytes are decoded into string and []byte respectively,
// whereas other values are JSON encoded and stored as a string, with
// m.jsonEncoded being set to true to indicate the JSON encoding has occurred.
func (m *msgpackData) DecodeMsgpack(dec *msgpack.Decoder) error {
	c, err := dec.PeekCode()
	if err != nil {
		return err
	}
	switch {
	case msgpcode.IsString(c):
		data, err := dec.DecodeString()
		if err != nil {
			return err
		}
		m.value = data
		return nil
	case msgpcode.IsBin(c):
		data, err := dec.DecodeBytes()
		if err != nil {
			return err
		}
		m.value = data
		return nil
	default:
		data, err := MsgpackToJSON(dec)
		if err != nil {
			return err
		}
		m.value = data
		// only mark the data as JSON encoded if it's an object or
		// array to remain compatible with node, which does not mark
		// numbers or bools as being JSON encoded.
		if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
			m.jsonEncoded = true
		}
	}
	return nil
}

// normaliseInboundData applies DataFromExternal to a decoded (data, encoding)
// pair, returning the canonical Data representation and encoding. jsonEncoded
// reports that a msgpack object/array was transcoded to a JSON string (via
// msgpackData), in which case the encoding is set to "json" after the fact —
// exactly as the reference's Message.DecodeMsgpack does. It is the single
// ingress seam shared by the JSON and msgpack decoders of Message,
// PresenceMessage and Annotation.
func normaliseInboundData(v any, inEncoding string, jsonEncoded bool) (data any, outEncoding string, err error) {
	data, outEncoding, err = DataFromExternal(v, inEncoding)
	if err != nil {
		return
	}
	if jsonEncoded {
		outEncoding = "json"
	}
	return
}
