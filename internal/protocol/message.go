package protocol

// Message is a published message payload — one Message within a
// ChannelMessage atomic publish (see DESIGN.md §8).
//
// ID and Serial are distinct identifiers:
//   - ID is client-supplied and optional; it carries idempotency intent
//     so the server can reject duplicate publishes within the retention
//     window.
//   - Serial is server-assigned on publish in the form
//     `<channelSerial>:<idx>`, where channelSerial is the containing
//     ChannelMessage's serial and idx is this Message's position
//     within that batch.
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

// ChannelMessage is one atomic publish on a channel: a server-assigned
// channelSerial (the discrete attach/resume point in the channel's
// stream) plus the one or more Messages published in that batch.
//
// One publish (REST request or inbound MESSAGE frame) maps to exactly
// one ChannelMessage; subscribers receive ChannelMessages as the
// atomic delivery unit (one outbound MESSAGE frame per ChannelMessage).
// Storage persists ChannelMessages keyed by ChannelSerial.
type ChannelMessage struct {
	ChannelSerial string     `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	Messages      []*Message `json:"messages,omitempty"      msgpack:"messages,omitempty"`
}

// ProtocolMessage is one frame on the realtime WebSocket connection.
type ProtocolMessage struct {
	Action        Action            `json:"action"                  msgpack:"action"`
	ID            string            `json:"id,omitempty"            msgpack:"id,omitempty"`
	ConnectionID  string            `json:"connectionId,omitempty"  msgpack:"connectionId,omitempty"`
	Channel       string            `json:"channel,omitempty"       msgpack:"channel,omitempty"`
	ChannelSerial string            `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	MsgSerial     int64             `json:"msgSerial,omitempty"     msgpack:"msgSerial,omitempty"`
	Timestamp     int64             `json:"timestamp,omitempty"     msgpack:"timestamp,omitempty"`
	Count         int               `json:"count,omitempty"         msgpack:"count,omitempty"`
	Flags         int64             `json:"flags,omitempty"         msgpack:"flags,omitempty"`
	Messages      []*Message        `json:"messages,omitempty"      msgpack:"messages,omitempty"`
	Error         *ErrorInfo        `json:"error,omitempty"         msgpack:"error,omitempty"`
	Params        map[string]string `json:"params,omitempty"        msgpack:"params,omitempty"`
}

// ErrorInfo describes an error in Ably's standard wire form, attached
// to a ProtocolMessage when the server needs to convey a non-fatal
// problem to the client (e.g. a resume that could not fully replay).
type ErrorInfo struct {
	Message    string `json:"message,omitempty"    msgpack:"message,omitempty"`
	Code       int    `json:"code,omitempty"       msgpack:"code,omitempty"`
	StatusCode int    `json:"statusCode,omitempty" msgpack:"statusCode,omitempty"`
	HRef       string `json:"href,omitempty"       msgpack:"href,omitempty"`
}

// Flags carried on ATTACHED.
const (
	// FlagResumed indicates the channel state was resumed from the
	// client's supplied channelSerial: the gap between the client's
	// cursor and the live tail was replayed in full. Cleared when the
	// server could not satisfy the resume in full (e.g. cap exceeded,
	// retention aged-out) — clients should treat the absence of this
	// flag as a discontinuity.
	FlagResumed int64 = 1 << 2
)
