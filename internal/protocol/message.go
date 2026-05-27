package protocol

// Message is a published message payload — the unit of pub/sub on a
// channel.
//
// ID and Serial are *not* the same identifier (see DESIGN.md §8):
//   - ID is client-supplied and optional; it carries idempotency intent
//     so the server can reject duplicate publishes within the retention
//     window.
//   - Serial is server-assigned on publish and is the canonical channel
//     ordering identifier (`<ts>-<ctr>@<series>:<idx>`).
type Message struct {
	ID           string `json:"id,omitempty"           msgpack:"id,omitempty"`
	Serial       string `json:"serial,omitempty"       msgpack:"serial,omitempty"`
	ClientID     string `json:"clientId,omitempty"     msgpack:"clientId,omitempty"`
	ConnectionID string `json:"connectionId,omitempty" msgpack:"connectionId,omitempty"`
	Name         string `json:"name,omitempty"         msgpack:"name,omitempty"`
	Data         any    `json:"data,omitempty"         msgpack:"data,omitempty"`
	Encoding     string `json:"encoding,omitempty"     msgpack:"encoding,omitempty"`
	Timestamp    int64  `json:"timestamp,omitempty"    msgpack:"timestamp,omitempty"`
}

// ProtocolMessage is one frame on the realtime WebSocket connection.
type ProtocolMessage struct {
	Action        Action     `json:"action"                  msgpack:"action"`
	ID            string     `json:"id,omitempty"            msgpack:"id,omitempty"`
	ConnectionID  string     `json:"connectionId,omitempty"  msgpack:"connectionId,omitempty"`
	Channel       string     `json:"channel,omitempty"       msgpack:"channel,omitempty"`
	ChannelSerial string     `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	MsgSerial     int64      `json:"msgSerial,omitempty"     msgpack:"msgSerial,omitempty"`
	Timestamp     int64      `json:"timestamp,omitempty"     msgpack:"timestamp,omitempty"`
	Count         int        `json:"count,omitempty"         msgpack:"count,omitempty"`
	Flags         int64      `json:"flags,omitempty"         msgpack:"flags,omitempty"`
	Messages      []*Message `json:"messages,omitempty"      msgpack:"messages,omitempty"`
}
