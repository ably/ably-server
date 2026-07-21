package protocol

import "github.com/vmihailenco/msgpack/v5"

// Custom msgpack codecs for the payload-bearing wire types (Message,
// PresenceMessage, Annotation). They exist for one reason: the default
// struct encoder honours `omitempty` on the `Data any` field, and
// vmihailenco/msgpack treats an interface holding an empty string (or an
// empty []byte) as empty — so `data:""` is silently dropped. msgpack is
// also the storage payload encoding (DESIGN.md §6), so a dropped empty
// data does not survive the persist/read round-trip even for JSON
// clients, and REST history returns the value as absent. The custom
// encoder emits every field exactly as its msgpack struct tag would,
// except Data, which is emitted whenever it is non-nil (so "" and empty
// []byte survive) and omitted only when nil.
//
// Decoding uses the default struct decoder via a method-less shadow type:
// a msgpack map decodes into the struct by tag regardless of which keys
// are present, so `data:""` round-trips with the library's own interface
// decoding and full type fidelity. The encoder's field list is guarded
// against drift from the struct by the reflection test in
// msgpackcodec_test.go.

// msgpackField is one entry in a type's custom-encoded map: its wire key,
// whether it is present (mirroring the field's omitempty semantics), and
// how to encode its value.
type msgpackField struct {
	key   string
	emit  bool
	value func(*msgpack.Encoder) error
}

// encodeMsgpackMap writes the emitted fields as a msgpack map, matching
// the shape the default struct encoder produces (string keys, one entry
// per non-omitted field).
func encodeMsgpackMap(enc *msgpack.Encoder, fields []msgpackField) error {
	n := 0
	for _, f := range fields {
		if f.emit {
			n++
		}
	}
	if err := enc.EncodeMapLen(n); err != nil {
		return err
	}
	for _, f := range fields {
		if !f.emit {
			continue
		}
		if err := enc.EncodeString(f.key); err != nil {
			return err
		}
		if err := f.value(enc); err != nil {
			return err
		}
	}
	return nil
}

// EncodeMsgpack encodes the Message as a map mirroring its struct tags,
// forcing Data whenever non-nil (see the file comment).
func (m *Message) EncodeMsgpack(enc *msgpack.Encoder) error {
	return encodeMsgpackMap(enc, []msgpackField{
		{"id", m.ID != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.ID) }},
		{"serial", m.Serial != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.Serial) }},
		{"action", true, func(e *msgpack.Encoder) error { return e.EncodeInt(int64(m.Action)) }},
		{"clientId", m.ClientID != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.ClientID) }},
		{"connectionId", m.ConnectionID != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.ConnectionID) }},
		{"connectionKey", m.ConnectionKey != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.ConnectionKey) }},
		{"name", m.Name != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.Name) }},
		{"data", m.Data != nil, func(e *msgpack.Encoder) error { return e.Encode(m.Data) }},
		{"encoding", m.Encoding != "", func(e *msgpack.Encoder) error { return e.EncodeString(m.Encoding) }},
		{"extras", len(m.Extras) > 0, func(e *msgpack.Encoder) error { return e.Encode(m.Extras) }},
		{"timestamp", m.Timestamp != 0, func(e *msgpack.Encoder) error { return e.EncodeInt(m.Timestamp) }},
		{"version", m.Version != nil, func(e *msgpack.Encoder) error { return e.Encode(m.Version) }},
		{"summary", len(m.Summary) > 0, func(e *msgpack.Encoder) error { return e.Encode(m.Summary) }},
		{"alt", len(m.Alt) > 0, func(e *msgpack.Encoder) error { return e.Encode(m.Alt) }},
	})
}

// DecodeMsgpack decodes a Message. It captures the raw msgpack value and
// decodes it twice: once via the method-less shadow type (sidestepping the
// custom-encoder recursion) for every field with the library's full type
// fidelity, and once into a lone msgpackData field to normalise the payload.
// (The two passes avoid a struct with two `data` keys, which msgpack — unlike
// encoding/json — rejects rather than shadowing by depth.) The payload is
// then normalised via DataFromExternal to the canonical Data representation:
// an outer-base64 string is decoded to raw bytes with the suffix stripped, and
// a msgpack object/array is transcoded to a JSON string (via MsgpackToJSON)
// with the encoding set to "json" after the fact. Mirrors the reference's
// ablyrpc.Message.DecodeMsgpack.
func (m *Message) DecodeMsgpack(dec *msgpack.Decoder) error {
	var raw msgpack.RawMessage
	if err := raw.DecodeMsgpack(dec); err != nil {
		return err
	}
	type wire Message
	var w wire
	if err := msgpack.Unmarshal(raw, &w); err != nil {
		return err
	}
	*m = Message(w)
	return decodeMsgpackData(raw, &m.Data, &m.Encoding)
}

// decodeMsgpackData normalises the "data" field of a raw msgpack map into the
// canonical Data representation, writing the result back through the given
// pointers. It is the shared msgpack ingress seam for Message,
// PresenceMessage and Annotation (see Message.DecodeMsgpack).
func decodeMsgpackData(raw msgpack.RawMessage, data *any, encoding *string) error {
	var dv struct {
		Data *msgpackData `msgpack:"data,omitempty"`
	}
	if err := msgpack.Unmarshal(raw, &dv); err != nil {
		return err
	}
	if dv.Data == nil {
		return nil
	}
	norm, enc, err := normaliseInboundData(dv.Data.value, *encoding, dv.Data.jsonEncoded)
	if err != nil {
		return err
	}
	*data = norm
	*encoding = enc
	return nil
}

// EncodeMsgpack encodes the PresenceMessage as a map mirroring its struct
// tags, forcing Data whenever non-nil (see the file comment).
func (p *PresenceMessage) EncodeMsgpack(enc *msgpack.Encoder) error {
	return encodeMsgpackMap(enc, []msgpackField{
		{"id", p.ID != "", func(e *msgpack.Encoder) error { return e.EncodeString(p.ID) }},
		{"serial", p.Serial != "", func(e *msgpack.Encoder) error { return e.EncodeString(p.Serial) }},
		{"action", true, func(e *msgpack.Encoder) error { return e.EncodeInt(int64(p.Action)) }},
		{"clientId", p.ClientID != "", func(e *msgpack.Encoder) error { return e.EncodeString(p.ClientID) }},
		{"connectionId", p.ConnectionID != "", func(e *msgpack.Encoder) error { return e.EncodeString(p.ConnectionID) }},
		{"data", p.Data != nil, func(e *msgpack.Encoder) error { return e.Encode(p.Data) }},
		{"encoding", p.Encoding != "", func(e *msgpack.Encoder) error { return e.EncodeString(p.Encoding) }},
		{"extras", len(p.Extras) > 0, func(e *msgpack.Encoder) error { return e.Encode(p.Extras) }},
		{"timestamp", p.Timestamp != 0, func(e *msgpack.Encoder) error { return e.EncodeInt(p.Timestamp) }},
	})
}

// DecodeMsgpack decodes a PresenceMessage, normalising the Data field via the
// same two-pass scheme as Message.DecodeMsgpack.
func (p *PresenceMessage) DecodeMsgpack(dec *msgpack.Decoder) error {
	var raw msgpack.RawMessage
	if err := raw.DecodeMsgpack(dec); err != nil {
		return err
	}
	type wire PresenceMessage
	var w wire
	if err := msgpack.Unmarshal(raw, &w); err != nil {
		return err
	}
	*p = PresenceMessage(w)
	return decodeMsgpackData(raw, &p.Data, &p.Encoding)
}

// EncodeMsgpack encodes the Annotation as a map mirroring its struct
// tags, forcing Data whenever non-nil (see the file comment). Summary is
// server-internal (msgpack:"-") and is not emitted.
func (a *Annotation) EncodeMsgpack(enc *msgpack.Encoder) error {
	return encodeMsgpackMap(enc, []msgpackField{
		{"id", a.ID != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.ID) }},
		{"serial", a.Serial != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.Serial) }},
		{"action", true, func(e *msgpack.Encoder) error { return e.EncodeInt(int64(a.Action)) }},
		{"clientId", a.ClientID != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.ClientID) }},
		{"connectionId", a.ConnectionID != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.ConnectionID) }},
		{"type", a.Type != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.Type) }},
		{"name", a.Name != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.Name) }},
		{"messageSerial", a.MessageSerial != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.MessageSerial) }},
		{"count", a.Count != 0, func(e *msgpack.Encoder) error { return e.EncodeInt(int64(a.Count)) }},
		{"data", a.Data != nil, func(e *msgpack.Encoder) error { return e.Encode(a.Data) }},
		{"encoding", a.Encoding != "", func(e *msgpack.Encoder) error { return e.EncodeString(a.Encoding) }},
		{"extras", len(a.Extras) > 0, func(e *msgpack.Encoder) error { return e.Encode(a.Extras) }},
		{"timestamp", a.Timestamp != 0, func(e *msgpack.Encoder) error { return e.EncodeInt(a.Timestamp) }},
	})
}

// DecodeMsgpack decodes an Annotation, normalising the Data field via the
// same two-pass scheme as Message.DecodeMsgpack.
func (a *Annotation) DecodeMsgpack(dec *msgpack.Decoder) error {
	var raw msgpack.RawMessage
	if err := raw.DecodeMsgpack(dec); err != nil {
		return err
	}
	type wire Annotation
	var w wire
	if err := msgpack.Unmarshal(raw, &w); err != nil {
		return err
	}
	*a = Annotation(w)
	return decodeMsgpackData(raw, &a.Data, &a.Encoding)
}
