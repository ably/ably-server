package protocol

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
// ID and Serial carry the same split as on Message (DESIGN.md §8):
//   - ID is client-supplied and optional; it carries idempotency intent.
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
