package handles

import (
	"github.com/ably/ably-server/internal/protocol"

	"github.com/ably/server-protocol/go/wire"
)

// How a stored publish crosses into the wire types the protocol code reads.
// This is the storage boundary and nothing more: every field either has a
// counterpart or is this server's own, and the ones that are its own do not
// cross.

// channelMessage is one stored publish as the wire carries it.
//
// The two models discriminate a publish differently: the wire says what kind
// it is in an action field, which the shared code branches on, while this
// server says it by which of its three slices is populated. Deriving one from
// the other is exact, because a publish here is only ever of one kind — Append
// links messages, presence and annotations as separate entries.
func channelMessage(cm *protocol.ChannelMessage) *wire.ChannelMessage {
	if cm == nil {
		return nil
	}
	serial, _ := wire.TimeserialFromString(cm.ChannelSerial)
	out := &wire.ChannelMessage{
		Id:            cm.ID,
		ChannelSerial: serial,
		Action:        channelMessageAction(cm),
		ConnectionId:  publishingConnection(cm),
	}
	out.Messages = cm.Messages
	out.Presence = cm.Presence
	out.Annotations = cm.Annotations
	out.State = cm.State
	return out
}

// publishingConnection is the connection a stored publish came from.
//
// This server records it on each item rather than on the publish, because that
// is where it reads it back from; the wire carries it once for the publish,
// which is the same thing said once — a channel message is one publish, so one
// connection. It has to cross, because it is how a connection that asked not
// to be sent its own messages is recognised as their author.
func publishingConnection(cm *protocol.ChannelMessage) string {
	for _, m := range cm.Messages {
		if m.ConnectionId != "" {
			return m.ConnectionId
		}
	}
	for _, p := range cm.Presence {
		if p.ConnectionId != "" {
			return p.ConnectionId
		}
	}
	for _, sm := range cm.State {
		if sm.ConnectionId != "" {
			return sm.ConnectionId
		}
	}
	return ""
}

func channelMessageAction(cm *protocol.ChannelMessage) wire.ProtocolMessageAction {
	switch {
	case len(cm.Presence) > 0:
		return wire.ProtocolMessageAction_ACTION_PRESENCE
	case len(cm.Annotations) > 0:
		return wire.ProtocolMessageAction_ACTION_ANNOTATION
	case len(cm.State) > 0:
		return wire.ProtocolMessageAction_ACTION_STATE
	default:
		return wire.ProtocolMessageAction_ACTION_MESSAGE
	}
}
