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
