package storage

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ably/ably-server/internal/protocol"

	"github.com/ably/server-protocol/go/wire"
)

// How a publish is written down.
//
// The types being stored are the protocol's, and the protocol already says how
// they are encoded: as protobuf. Storing them any other way means a second
// answer to a question that has one — and a lossy one, because the encodings a
// wire type carries are the ones a client speaks, which drop the fields a
// client is not told about but a server has to remember.

// EncodeMessage, EncodeAnnotation and EncodePresence write one item of a
// publish; the Decode* pair read it back.
func EncodeMessage(m *wire.Message) ([]byte, error) { return encode(m, "message") }

func DecodeMessage(b []byte) (*wire.Message, error) {
	var m wire.Message
	return &m, decode(b, &m, "message")
}

func EncodeAnnotation(a *wire.Annotation) ([]byte, error) { return encode(a, "annotation") }

func DecodeAnnotation(b []byte) (*wire.Annotation, error) {
	var a wire.Annotation
	return &a, decode(b, &a, "annotation")
}

func EncodePresence(p *wire.PresenceMessage) ([]byte, error) { return encode(p, "presence") }

func DecodePresence(b []byte) (*wire.PresenceMessage, error) {
	var p wire.PresenceMessage
	return &p, decode(b, &p, "presence")
}

func EncodeState(s *wire.StateMessage) ([]byte, error) { return encode(s, "state") }

func DecodeState(b []byte) (*wire.StateMessage, error) {
	var s wire.StateMessage
	return &s, decode(b, &s, "state")
}

// EncodeStateObject and DecodeStateObject write one object of a channel's
// materialised LiveObjects set. The set is not part of any publish — it is the
// fold of the state stream, kept so a sync can be served without replaying it
// (DESIGN.md §15.3) — so it is written down on its own.
func EncodeStateObject(o *wire.StateObject) ([]byte, error) { return encode(o, "state object") }

func DecodeStateObject(b []byte) (*wire.StateObject, error) {
	var o wire.StateObject
	return &o, decode(b, &o, "state object")
}

// EncodeChannelMessage writes a whole publish as one record, for a backend
// that stores it whole rather than a row per item.
//
// The channelSerial crosses as the timeserial it is. This server holds it as a
// string because that is what it keys and compares by, and the string is the
// serial's own written form, so nothing is decided here.
func EncodeChannelMessage(cm *protocol.ChannelMessage) ([]byte, error) {
	serial, err := wire.TimeserialFromString(cm.ChannelSerial)
	if err != nil {
		return nil, fmt.Errorf("storage: encode ChannelMessage %q: %w", cm.ChannelSerial, err)
	}
	return encode(&wire.ChannelMessage{
		Id:            cm.ID,
		ChannelSerial: serial,
		Messages:      cm.Messages,
		Presence:      cm.Presence,
		Annotations:   cm.Annotations,
		State:         cm.State,
	}, "ChannelMessage")
}

func DecodeChannelMessage(b []byte) (*protocol.ChannelMessage, error) {
	var stored wire.ChannelMessage
	if err := decode(b, &stored, "ChannelMessage"); err != nil {
		return nil, err
	}
	return &protocol.ChannelMessage{
		ID:            stored.Id,
		ChannelSerial: stored.ChannelSerial.ToTimeserialString(),
		Messages:      stored.Messages,
		Presence:      stored.Presence,
		Annotations:   stored.Annotations,
		State:         stored.State,
	}, nil
}

func encode(m proto.Message, what string) ([]byte, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("storage: encode %s: %w", what, err)
	}
	return b, nil
}

func decode(b []byte, m proto.Message, what string) error {
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("storage: decode %s: %w", what, err)
	}
	return nil
}
