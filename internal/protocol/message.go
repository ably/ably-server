package protocol

import "encoding/json"

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
	ID           string        `json:"id,omitempty"           msgpack:"id,omitempty"`
	Serial       string        `json:"serial,omitempty"       msgpack:"serial,omitempty"`
	Action       MessageAction `json:"action"                 msgpack:"action"`
	ClientID     string        `json:"clientId,omitempty"     msgpack:"clientId,omitempty"`
	ConnectionID string        `json:"connectionId,omitempty" msgpack:"connectionId,omitempty"`
	// ConnectionKey is an inbound-only field on a REST publish: it names a
	// live realtime connection to publish on behalf of (DESIGN.md §8, §13).
	// The REST publish path resolves it to that connection's connectionId
	// (and clientId), stamps those, then clears it — connectionKey is never
	// persisted or delivered (mirrors the reference, which resolves it to a
	// derived connectionId and delivers only that).
	ConnectionKey string `json:"connectionKey,omitempty" msgpack:"connectionKey,omitempty"`
	Name          string `json:"name,omitempty"         msgpack:"name,omitempty"`
	Data          any    `json:"data,omitempty"         msgpack:"data,omitempty"`
	Encoding      string `json:"encoding,omitempty"     msgpack:"encoding,omitempty"`
	// Extras is a free-form JSON object the client attaches to a message
	// (headers, push metadata, and — for the AI Transport SDK — extras.ai).
	// The server treats it as opaque and preserves it verbatim through
	// publish, fan-out, storage, history and mutations (DESIGN.md §8, §13.2).
	// It is a map rather than a raw-JSON carrier so it round-trips both the
	// JSON and msgpack wire/storage encodings — json.RawMessage would not
	// survive the msgpack storage payload. This mirrors the reference's
	// extras object (ablyrpc holds it as structpb.Struct, wire-encoded as a
	// plain object under either format).
	Extras    map[string]any  `json:"extras,omitempty"       msgpack:"extras,omitempty"`
	Timestamp int64           `json:"timestamp,omitempty"    msgpack:"timestamp,omitempty"`
	Version   *MessageVersion `json:"version,omitempty"      msgpack:"version,omitempty"`
	// Summary is the fold of this message's annotations (DESIGN.md §14.2),
	// keyed by annotation type. It rides the latest-version projection so
	// message reads (GET .../messages, .../messages/{serial}, history)
	// carry the current summary, and it is the payload of the outbound
	// MESSAGE/summary (action 4) delivery frame.
	Summary Summary `json:"summary,omitempty" msgpack:"summary,omitempty"`
	// Alt carries alternative in-band representations of this message,
	// keyed by role (DESIGN.md §13.3). Its sole current use is the
	// append delta: an append is persisted and fanned out as a full
	// action=update Message whose Data is the rolled-up aggregate, with
	// Alt[DeltaAppend] holding the incremental append (action=append,
	// just the new data) the server hands a caught-up subscriber instead
	// of the full version. It is a server-internal carrier — persisted so
	// resume and cross-node fan-out can still choose delta vs full — and
	// is resolved away before a frame reaches a client, so it is excluded
	// from the client-facing JSON encoding.
	Alt map[string]*Message `json:"-" msgpack:"alt,omitempty"`
}

// MarshalJSON encodes a Message for a JSON (text) transport, applying the
// binary→base64 egress rule (DESIGN.md §8; see data.go): the Data field is
// emitted via encoding/json's default handling — a []byte payload becomes a
// base64 string — and "base64" is appended to the emitted encoding so a JSON
// subscriber decodes the payload back to bytes. String payloads and the
// canonical (stored) encoding are emitted unchanged. Mirrors the reference's
// ablyrpc.Message.MarshalJSON, where the base64 suffix is appended at JSON
// emission and never stored.
func (m *Message) MarshalJSON() ([]byte, error) {
	type Alias Message
	return json.Marshal(&struct {
		*Alias
		Encoding string `json:"encoding,omitempty"`
		Data     any    `json:"data,omitempty"`
	}{
		Alias:    (*Alias)(m),
		Encoding: jsonDataEncoding(m.Encoding, m.Data),
		Data:     m.Data,
	})
}

// UnmarshalJSON decodes a JSON Message, normalising its payload to the
// server's canonical Data representation via DataFromExternal (see data.go):
// an outer-base64 string is decoded to raw bytes with the base64 suffix
// stripped, and objects/arrays are canonicalised to a JSON string. Mirrors
// the reference's ablyrpc.Message.UnmarshalJSON.
func (m *Message) UnmarshalJSON(b []byte) error {
	type Alias Message
	v := struct {
		*Alias
		Data any `json:"data,omitempty"`
	}{Alias: (*Alias)(m)}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	data, enc, err := normaliseInboundData(v.Data, m.Encoding, false)
	if err != nil {
		return err
	}
	m.Data = data
	m.Encoding = enc
	return nil
}

// DeltaAppend is the Alt key under which an append's incremental delta
// message rides on the full aggregated version (DESIGN.md §13.3). Matches
// Ably's reference constant.
const DeltaAppend = "delta-append"

// HasAppendDelta reports whether m is an append aggregate — a full
// version carrying an incremental append in Alt (DESIGN.md §13.3). It is
// how the delivery path and the version-history collapse distinguish an
// append from an ordinary update, which are otherwise both action=update.
func (m *Message) HasAppendDelta() bool {
	return m != nil && m.Alt[DeltaAppend] != nil
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
// A ChannelMessage carries exactly one of Messages (a data publish),
// Presence (a presence publish), or Annotations (an annotation publish,
// DESIGN.md §14.1) — the three ride one ordered stream distinguished by
// which slice is populated (DESIGN.md §12.1).
//
// ID is the batch identifier for a message publish (DESIGN.md §8): the
// client-supplied idempotency key, or a server-generated 8-char base64
// string when none is supplied. Each contained Message.ID is stamped
// "<ID>:<idx>" (storage.StampMessageIDs), so the batch id is the
// idempotency key indexed by storage.
//
// Empty for a presence cm: presence has no batch key because presence
// identity is per-member, not per-batch. Each contained
// PresenceMessage.ID is instead stamped "<connectionId>:<msgSerial>:
// <index>" at realtime publish, and storage-level idempotency keys off
// those contained ids — so no cm-level id is ever minted for presence.
type ChannelMessage struct {
	ID            string             `json:"id,omitempty"            msgpack:"id,omitempty"`
	ChannelSerial string             `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	Messages      []*Message         `json:"messages,omitempty"      msgpack:"messages,omitempty"`
	Presence      []*PresenceMessage `json:"presence,omitempty"      msgpack:"presence,omitempty"`
	Annotations   []*Annotation      `json:"annotations,omitempty"   msgpack:"annotations,omitempty"`
}

// ProtocolMessage is one frame on the realtime WebSocket connection.
type ProtocolMessage struct {
	Action       Action `json:"action"                  msgpack:"action"`
	ID           string `json:"id,omitempty"            msgpack:"id,omitempty"`
	ConnectionID string `json:"connectionId,omitempty"  msgpack:"connectionId,omitempty"`
	// Channel is a pointer so the server can emit an explicit empty channel
	// (channel:"") while frames that carry no channel omit the field. An
	// ERROR frame with the channel field absent is classified connection-
	// fatal by SDKs, but one carrying the field (even "") is routed to the
	// channel (ably-js transport.ts onProtocolMessage) — so an empty-name
	// ATTACH rejection must set Channel to a pointer-to-"" to reach the wire
	// and fail the channel rather than the connection. pointer+omitempty
	// emits the value whenever it is non-nil (including "") and omits it when
	// nil. Read an inbound frame's channel via GetChannel (absent means "").
	Channel       *string `json:"channel,omitempty"       msgpack:"channel,omitempty"`
	ChannelSerial string  `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	// MsgSerial is the per-connection publish counter (§8), a pointer so
	// the server can emit msgSerial:0 while every non-publish frame omits
	// the field. pointer+omitempty emits the value whenever it is non-nil
	// (including 0) and omits it when nil, so an ACK/NACK always carries an
	// explicit msgSerial — SDKs read it positionally and compute NaN
	// if it is absent — while HEARTBEAT/CONNECTED/MESSAGE
	// deliveries, which never set it, stay free of a spurious msgSerial:0.
	// Read an inbound frame's serial via GetMsgSerial (absent means 0).
	MsgSerial *int64 `json:"msgSerial,omitempty"     msgpack:"msgSerial,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"     msgpack:"timestamp,omitempty"`
	Count     int    `json:"count,omitempty"         msgpack:"count,omitempty"`
	// Res carries the per-message publish results back to the publisher
	// on an ACK (Ably's TR4s shape): one entry per message in the ack
	// window, each holding the server-assigned serials. SDKs read it to
	// populate publish/update results (DESIGN.md §8, §13.1).
	Res         []*PublishResult   `json:"res,omitempty"           msgpack:"res,omitempty"`
	Flags       int64              `json:"flags,omitempty"         msgpack:"flags,omitempty"`
	Messages    []*Message         `json:"messages,omitempty"      msgpack:"messages,omitempty"`
	Presence    []*PresenceMessage `json:"presence,omitempty"      msgpack:"presence,omitempty"`
	Annotations []*Annotation      `json:"annotations,omitempty"   msgpack:"annotations,omitempty"`
	Error       *ErrorInfo         `json:"error,omitempty"         msgpack:"error,omitempty"`
	Params      ChannelParams      `json:"params,omitempty"        msgpack:"params,omitempty"`
	// ConnectionDetails carries the resolved identity and connection
	// limits on the CONNECTED frame (DESIGN.md §2.1, §8).
	ConnectionDetails *ConnectionDetails `json:"connectionDetails,omitempty" msgpack:"connectionDetails,omitempty"`
	// Auth carries a fresh token on an inbound AUTH frame for inband
	// re-authentication (DESIGN.md §2.1, §3); field name/tags match
	// the SDKs' authDetails shape so they encode it unchanged.
	Auth *AuthDetails `json:"auth,omitempty" msgpack:"auth,omitempty"`
}

// GetChannel returns the frame's channel, treating an absent (nil) Channel
// as "". Inbound frames omit the field when there is no channel; a
// pointer-to-"" is a deliberately channel-scoped empty name.
func (m *ProtocolMessage) GetChannel() string {
	if m.Channel == nil {
		return ""
	}
	return *m.Channel
}

// GetMsgSerial returns the frame's msgSerial, treating an absent (nil)
// MsgSerial as 0. Inbound publish-like frames address their ACK by this
// per-connection counter; SDKs — and this server — omit msgSerial:0 on
// the first publish, so absent-means-0 preserves inbound compatibility
// (DESIGN.md §8).
func (m *ProtocolMessage) GetMsgSerial() int64 {
	if m.MsgSerial == nil {
		return 0
	}
	return *m.MsgSerial
}

// AuthDetails carries the token supplied on an inband AUTH ProtocolMessage
// (Ably AD2). AccessToken is the JWT the client presents to re-authenticate
// an established connection (DESIGN.md §3).
type AuthDetails struct {
	AccessToken string `json:"accessToken,omitempty" msgpack:"accessToken,omitempty"`
}

// ConnectionDetails is sent inside the CONNECTED ProtocolMessage and
// tells the SDK its resolved identity plus the limits/params it should
// adopt for this connection (Ably's CD2* / DESIGN.md §2.1, §8). Field
// names and wire tags match the SDKs' connectionDetails shape so they decode
// it unchanged. The two duration fields are whole milliseconds on the
// wire, as SDKs encode durations as integer milliseconds.
type ConnectionDetails struct {
	// ClientID is the connection's resolved clientId (§3.2): a concrete
	// value, "*" for a wildcard bearer, or omitted for an anonymous
	// connection.
	ClientID string `json:"clientId,omitempty" msgpack:"clientId,omitempty"`
	// ConnectionKey is the opaque key an SDK would resume with. Since
	// connection-state resume is a non-goal (§1, §11), it is the
	// process-local connectionId and is not recoverable.
	ConnectionKey string `json:"connectionKey,omitempty" msgpack:"connectionKey,omitempty"`
	// MaxMessageSize is the largest permitted payload of a single publish
	// (bytes). SDKs reject oversize publishes client-side.
	MaxMessageSize int64 `json:"maxMessageSize,omitempty" msgpack:"maxMessageSize,omitempty"`
	// MaxFrameSize is the largest permitted WebSocket frame / POST body
	// (bytes).
	MaxFrameSize int64 `json:"maxFrameSize,omitempty" msgpack:"maxFrameSize,omitempty"`
	// MaxInboundRate is the advisory ceiling on messages per second from
	// this connection.
	MaxInboundRate int64 `json:"maxInboundRate,omitempty" msgpack:"maxInboundRate,omitempty"`
	// ConnectionStateTTLMs is how long (ms) an SDK should treat the
	// connection state as recoverable after an abrupt disconnect (DF1a).
	ConnectionStateTTLMs int64 `json:"connectionStateTtl,omitempty" msgpack:"connectionStateTtl,omitempty"`
	// MaxIdleIntervalMs is the maximum time (ms) the server will leave the
	// server→client direction idle before sending a HEARTBEAT; it equals
	// the server heartbeat cadence (CD2h).
	MaxIdleIntervalMs int64 `json:"maxIdleInterval,omitempty" msgpack:"maxIdleInterval,omitempty"`
}

// PublishResult is one entry in an ACK's Res array (Ably's TR4s): the
// serials the server assigned to one acknowledged message. For a create
// it carries the message's Serial; for a mutation, the new version
// serial.
type PublishResult struct {
	Serials []string `json:"serials,omitempty" msgpack:"serials,omitempty"`
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

	// FlagHasBacklog is set on ATTACHED when the attach replayed any
	// backlog before live delivery — a rewind or a resume gap-fill
	// (DESIGN.md §4.2, §4.3). SDKs surface it as
	// ChannelStateChange.hasBacklog (RTL2i). Cleared on a fresh attach
	// that replayed nothing.
	FlagHasBacklog int64 = 1 << 1

	// FlagResumed indicates the channel state was resumed from the
	// client's supplied channelSerial: the gap between the client's
	// cursor and the live tail was replayed in full. Cleared when the
	// server could not satisfy the resume in full (e.g. cap exceeded,
	// retention aged-out) — clients should treat the absence of this
	// flag as a discontinuity.
	FlagResumed int64 = 1 << 2

	// FlagAttachResume is set by the SDK on an ATTACH that continues an
	// existing attachment (a non-clean attach, RTL4j) — e.g. re-attaching
	// after a connection resume. Like a supplied channelSerial it marks the
	// attach as a resume, which suppresses rewind (DESIGN.md §4.3): a
	// continuation must not replay history the client has already seen.
	FlagAttachResume int64 = 1 << 5

	// Channel-mode flags (DESIGN.md §4.2). ATTACH.flags selects the
	// requested modes; ATTACHED.flags carries the effective set.
	FlagPresence          int64 = 1 << 16 // enter/update/leave presence
	FlagPublish           int64 = 1 << 17 // publish MESSAGE
	FlagSubscribe         int64 = 1 << 18 // receive MESSAGE
	FlagPresenceSubscribe int64 = 1 << 19 // receive PRESENCE + presence sync
	// Annotation modes (DESIGN.md §14.3). ANNOTATION_PUBLISH is part of the
	// no-mode-bits ATTACH default set (§4.2, matching the reference's
	// MODE_DEFAULT); ANNOTATION_SUBSCRIBE is opt-in — raw annotation
	// delivery must be requested explicitly. Bits match Ably's wire
	// constants (1<<20 MAY_HAVE_PRESENCE is internal-only and unused here).
	FlagAnnotationPublish   int64 = 1 << 21 // publish ANNOTATION
	FlagAnnotationSubscribe int64 = 1 << 22 // receive raw ANNOTATION frames
)
