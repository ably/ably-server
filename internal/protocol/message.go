package protocol

// Message is a published message payload — one Message within a
// ChannelMessage atomic publish (see DESIGN.md §8).
//
// ID and Serial are distinct identifiers:
//   - ID is client-supplied and optional; it carries idempotency intent
//     so the server can reject duplicate publishes within the retention
//     window.
//   - Serial is the stable message IDENTITY (DESIGN.md §8, §13.1): it
//     names the message, unchanged across every version. For a create
//     it is server-assigned in the form `<channelSerial>:<idx>` (the
//     containing ChannelMessage's serial plus this Message's position
//     in the batch); an update/delete/append repeats the target's
//     Serial and carries a fresh Version.
//
// Action and Version split message-vs-version identity (DESIGN.md §13.1).
// Action distinguishes a create from a mutation; Version names a single
// version of the message. For a create, Version.Serial == Serial; each
// subsequent mutation lands at a new channelSerial and gets a fresh,
// strictly-greater Version.Serial. Action carries no omitempty: a create
// must emit action=0 on the wire to match Ably's SDKs.
type Message struct {
	ID           string          `json:"id,omitempty"           msgpack:"id,omitempty"`
	Serial       string          `json:"serial,omitempty"       msgpack:"serial,omitempty"`
	Action       MessageAction   `json:"action"                 msgpack:"action"`
	ClientID     string          `json:"clientId,omitempty"     msgpack:"clientId,omitempty"`
	ConnectionID string          `json:"connectionId,omitempty" msgpack:"connectionId,omitempty"`
	Name         string          `json:"name,omitempty"         msgpack:"name,omitempty"`
	Data         any             `json:"data,omitempty"         msgpack:"data,omitempty"`
	Encoding     string          `json:"encoding,omitempty"     msgpack:"encoding,omitempty"`
	Timestamp    int64           `json:"timestamp,omitempty"    msgpack:"timestamp,omitempty"`
	Version      *MessageVersion `json:"version,omitempty"      msgpack:"version,omitempty"`
}

// MessageVersion names a single version of a message (DESIGN.md §13.1).
// Its wire shape matches Ably's version object. Serial is the
// `<channelSerial>:<idx>` of the publish that produced this version (for
// a create it equals the message's own Serial; for a mutation it is the
// mutation publish's position). Timestamp, ClientID, Description and
// Metadata stamp the operation: ClientID is the operating client (which
// may differ from the message's creator), and Description/Metadata are
// optional operator-supplied annotations.
type MessageVersion struct {
	Serial      string         `json:"serial,omitempty"      msgpack:"serial,omitempty"`
	Timestamp   int64          `json:"timestamp,omitempty"   msgpack:"timestamp,omitempty"`
	ClientID    string         `json:"clientId,omitempty"    msgpack:"clientId,omitempty"`
	Description string         `json:"description,omitempty" msgpack:"description,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"    msgpack:"metadata,omitempty"`
}

// ChannelMessage is one atomic publish on a channel: a server-assigned
// channelSerial (the discrete attach/resume point in the channel's
// stream) plus the one or more Messages published in that batch.
//
// One publish (REST request or inbound MESSAGE frame) maps to exactly
// one ChannelMessage; subscribers receive ChannelMessages as the
// atomic delivery unit (one outbound MESSAGE frame per ChannelMessage).
// Storage persists ChannelMessages keyed by ChannelSerial.
// A ChannelMessage carries either Messages (a data publish) or Presence
// (a presence publish), never both — the two ride one ordered stream
// distinguished by which slice is populated (DESIGN.md §12.1).
type ChannelMessage struct {
	ChannelSerial string             `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	Messages      []*Message         `json:"messages,omitempty"      msgpack:"messages,omitempty"`
	Presence      []*PresenceMessage `json:"presence,omitempty"      msgpack:"presence,omitempty"`
}

// ProtocolMessage is one frame on the realtime WebSocket connection.
type ProtocolMessage struct {
	Action        Action             `json:"action"                  msgpack:"action"`
	ID            string             `json:"id,omitempty"            msgpack:"id,omitempty"`
	ConnectionID  string             `json:"connectionId,omitempty"  msgpack:"connectionId,omitempty"`
	Channel       string             `json:"channel,omitempty"       msgpack:"channel,omitempty"`
	ChannelSerial string             `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	MsgSerial     int64              `json:"msgSerial,omitempty"     msgpack:"msgSerial,omitempty"`
	Timestamp     int64              `json:"timestamp,omitempty"     msgpack:"timestamp,omitempty"`
	Count         int                `json:"count,omitempty"         msgpack:"count,omitempty"`
	Flags         int64              `json:"flags,omitempty"         msgpack:"flags,omitempty"`
	Messages      []*Message         `json:"messages,omitempty"      msgpack:"messages,omitempty"`
	Presence      []*PresenceMessage `json:"presence,omitempty"      msgpack:"presence,omitempty"`
	Error         *ErrorInfo         `json:"error,omitempty"         msgpack:"error,omitempty"`
	Params        map[string]string  `json:"params,omitempty"        msgpack:"params,omitempty"`
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

// Flags carried on ATTACH / ATTACHED. The low bits are server-set
// status flags; the high bits (1<<16 and up) are the channel-mode
// bitfield, matching Ably's wire constants (DESIGN.md §4.2, §12).
const (
	// FlagHasPresence is set on ATTACHED when the channel has a
	// non-empty presence set, signalling the SDK that a SYNC will
	// follow (DESIGN.md §12.4).
	FlagHasPresence int64 = 1 << 0

	// FlagResumed indicates the channel state was resumed from the
	// client's supplied channelSerial: the gap between the client's
	// cursor and the live tail was replayed in full. Cleared when the
	// server could not satisfy the resume in full (e.g. cap exceeded,
	// retention aged-out) — clients should treat the absence of this
	// flag as a discontinuity.
	FlagResumed int64 = 1 << 2

	// Channel-mode flags (DESIGN.md §4.2). ATTACH.flags selects the
	// requested modes; ATTACHED.flags carries the effective set.
	FlagPresence          int64 = 1 << 16 // enter/update/leave presence
	FlagPublish           int64 = 1 << 17 // publish MESSAGE
	FlagSubscribe         int64 = 1 << 18 // receive MESSAGE
	FlagPresenceSubscribe int64 = 1 << 19 // receive PRESENCE + presence sync
)
