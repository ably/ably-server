package protocol

import "encoding/json"

// PresenceAction is the action carried by a PresenceMessage — the
// presence-stream analogue of a message action. Values match Ably's
// wire enum (DESIGN.md §12.1).
type PresenceAction int8

const (
	PresenceAbsent  PresenceAction = 0
	PresencePresent PresenceAction = 1
	PresenceEnter   PresenceAction = 2
	PresenceLeave   PresenceAction = 3
	PresenceUpdate  PresenceAction = 4
)

var presenceActionNames = map[PresenceAction]string{
	PresenceAbsent:  "absent",
	PresencePresent: "present",
	PresenceEnter:   "enter",
	PresenceLeave:   "leave",
	PresenceUpdate:  "update",
}

func (a PresenceAction) String() string {
	if name, ok := presenceActionNames[a]; ok {
		return name
	}
	return "unknown"
}

// PresenceMessage is a single presence operation — the presence-stream
// analogue of Message (DESIGN.md §12.1). A member's identity in the
// presence set is the pair (ConnectionID, ClientID).
//
// ID and Serial (DESIGN.md §8, §12.1):
//   - ID is the presence-newness key. A genuine op (published by a live
//     connection) is stamped "<connectionId>:<msgSerial>:<index>" at
//     realtime publish, so SDKs order it by (msgSerial, index); a client
//     may instead supply its own id (idempotency intent). A synthesized
//     event (fixture member, teardown/detach LEAVE) stays id-less and is
//     ordered by Timestamp.
//   - Serial is server-assigned on publish in the form
//     `<channelSerial>:<idx>`.
type PresenceMessage struct {
	ID           string         `json:"id,omitempty"           msgpack:"id,omitempty"`
	Serial       string         `json:"serial,omitempty"       msgpack:"serial,omitempty"`
	Action       PresenceAction `json:"action"                 msgpack:"action"`
	ClientID     string         `json:"clientId,omitempty"     msgpack:"clientId,omitempty"`
	ConnectionID string         `json:"connectionId,omitempty" msgpack:"connectionId,omitempty"`
	Data         any            `json:"data,omitempty"         msgpack:"data,omitempty"`
	Encoding     string         `json:"encoding,omitempty"     msgpack:"encoding,omitempty"`
	// Extras is a free-form JSON object attached to the presence message,
	// preserved verbatim through publish, presence sync/fan-out and storage
	// (DESIGN.md §8, §12.1). Carried as a map for the same round-trip reason
	// as Message.Extras.
	Extras    map[string]any `json:"extras,omitempty"       msgpack:"extras,omitempty"`
	Timestamp int64          `json:"timestamp,omitempty"    msgpack:"timestamp,omitempty"`
}

// MarshalJSON encodes a PresenceMessage for a JSON transport, applying the
// binary→base64 egress rule to its payload (see Message.MarshalJSON).
func (p *PresenceMessage) MarshalJSON() ([]byte, error) {
	type Alias PresenceMessage
	return json.Marshal(&struct {
		*Alias
		Encoding string `json:"encoding,omitempty"`
		Data     any    `json:"data,omitempty"`
	}{
		Alias:    (*Alias)(p),
		Encoding: jsonDataEncoding(p.Encoding, p.Data),
		Data:     p.Data,
	})
}

// UnmarshalJSON decodes a JSON PresenceMessage, normalising its payload to
// the canonical Data representation via DataFromExternal (see data.go).
func (p *PresenceMessage) UnmarshalJSON(b []byte) error {
	type Alias PresenceMessage
	v := struct {
		*Alias
		Data any `json:"data,omitempty"`
	}{Alias: (*Alias)(p)}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	data, enc, err := normaliseInboundData(v.Data, p.Encoding, false)
	if err != nil {
		return err
	}
	p.Data = data
	p.Encoding = enc
	return nil
}
