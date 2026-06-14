package protocol

type Action int8

const (
	ActionHeartbeat    Action = 0
	ActionAck          Action = 1
	ActionNack         Action = 2
	ActionConnect      Action = 3
	ActionConnected    Action = 4
	ActionDisconnect   Action = 5
	ActionDisconnected Action = 6
	ActionClose        Action = 7
	ActionClosed       Action = 8
	ActionError        Action = 9
	ActionAttach       Action = 10
	ActionAttached     Action = 11
	ActionDetach       Action = 12
	ActionDetached     Action = 13
	ActionPresence     Action = 14
	ActionMessage      Action = 15
	ActionSync         Action = 16
	ActionAuth         Action = 17
)

var actionNames = map[Action]string{
	ActionHeartbeat:    "heartbeat",
	ActionAck:          "ack",
	ActionNack:         "nack",
	ActionConnect:      "connect",
	ActionConnected:    "connected",
	ActionDisconnect:   "disconnect",
	ActionDisconnected: "disconnected",
	ActionClose:        "close",
	ActionClosed:       "closed",
	ActionError:        "error",
	ActionAttach:       "attach",
	ActionAttached:     "attached",
	ActionDetach:       "detach",
	ActionDetached:     "detached",
	ActionPresence:     "presence",
	ActionMessage:      "message",
	ActionSync:         "sync",
	ActionAuth:         "auth",
}

func (a Action) String() string {
	if name, ok := actionNames[a]; ok {
		return name
	}
	return "unknown"
}

// MessageAction is the action carried by a Message — the data-stream
// analogue of PresenceAction. Values are pinned to Ably's MessageAction
// wire enum (ably-go's constants): a create is the default original
// publish; update/delete/append are mutations of an existing message
// (DESIGN.md §13.1). The values summary (3), meta (4) and others Ably
// defines for object messages / annotations are intentionally omitted —
// this server only models the mutable-message subset.
type MessageAction int8

const (
	// MessageCreate is an original publish (the default). Every message
	// the publish path produces carries this; it must be emitted on the
	// wire even at its zero value, so Message.Action has no omitempty.
	MessageCreate MessageAction = 0
	// MessageUpdate replaces fields of an existing message with a new
	// version (DESIGN.md §13.2).
	MessageUpdate MessageAction = 1
	// MessageDelete soft-deletes an existing message — a tombstone
	// version (DESIGN.md §13.2).
	MessageDelete MessageAction = 2
	// MessageAppend concatenates onto an existing message's data
	// (DESIGN.md §13.3). Pinned to Ably's value 5.
	MessageAppend MessageAction = 5
)

var messageActionNames = map[MessageAction]string{
	MessageCreate: "create",
	MessageUpdate: "update",
	MessageDelete: "delete",
	MessageAppend: "append",
}

func (a MessageAction) String() string {
	if name, ok := messageActionNames[a]; ok {
		return name
	}
	return "unknown"
}

// IsMutation reports whether a is an update, delete or append — i.e. a
// mutation of an existing message rather than an original create.
func (a MessageAction) IsMutation() bool {
	switch a {
	case MessageUpdate, MessageDelete, MessageAppend:
		return true
	default:
		return false
	}
}
