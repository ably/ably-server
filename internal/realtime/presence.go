package realtime

import (
	"context"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// wildcardClientID is the §3.2 marker meaning "the bearer may assume any
// clientId". It is never itself a member identity.
const wildcardClientID = "*"

// teardownLeaveTimeout bounds the synthesised-LEAVE publishes done when a
// connection terminates; the connection's own context is already gone by
// then, so these run on a fresh, bounded context.
const teardownLeaveTimeout = 5 * time.Second

// handlePresence processes an inbound PRESENCE frame: enter / update /
// leave for one or more members (DESIGN.md §12.2). It authorises against
// the attachment's PRESENCE mode, resolves and validates each clientId
// (§12.3), stamps the connectionId, publishes via the channel, and
// ACK/NACKs on the msgSerial.
func (c *connection) handlePresence(ctx context.Context, msg *protocol.ProtocolMessage) {
	if msg.Channel == "" || len(msg.Presence) == 0 {
		c.logger.Warn("PRESENCE with empty channel or no payload; rejecting", "msgSerial", msg.MsgSerial)
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}

	// Presence requires an attachment holding the PRESENCE mode.
	a, ok := c.attachments[msg.Channel]
	if !ok || !a.hasMode(protocol.FlagPresence) {
		c.logger.Warn("PRESENCE without an attached PRESENCE-mode channel; rejecting",
			"channel", msg.Channel, "msgSerial", msg.MsgSerial)
		c.nack(ctx, msg.MsgSerial, &protocol.ErrorInfo{
			Message:    "presence requires an attachment with the presence mode",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// Resolve + validate every member's clientId, stamp connectionId.
	for _, p := range msg.Presence {
		cid, ok := resolvePresenceClientID(c.clientID, p.ClientID)
		if !ok {
			c.logger.Warn("PRESENCE clientId rejected", "channel", msg.Channel,
				"connClientId", c.clientID, "msgClientId", p.ClientID, "msgSerial", msg.MsgSerial)
			c.nack(ctx, msg.MsgSerial, &protocol.ErrorInfo{
				Message:    "invalid clientId for presence",
				Code:       91000,
				StatusCode: 400,
			})
			return
		}
		p.ClientID = cid
		p.ConnectionID = c.id
	}

	if _, _, err := a.channel.PublishPresence(ctx, msg.Presence); err != nil {
		c.logger.Warn("presence publish failed; NACKing", "channel", msg.Channel, "msgSerial", msg.MsgSerial, "err", err)
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}

	// Track membership for teardown LEAVE (DESIGN.md §12.5).
	for _, p := range msg.Presence {
		c.recordPresence(msg.Channel, p.ClientID, p.Action)
	}

	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionAck,
		MsgSerial: msg.MsgSerial,
		Count:     len(msg.Presence),
	})
}

// resolvePresenceClientID applies the §12.3 rules and returns the
// clientId to stamp on the member, or ok=false if the operation is not
// permitted. connClientID is the connection's resolved clientId:
// "" (anonymous), wildcardClientID (the bearer may assume any clientId),
// or a concrete value.
func resolvePresenceClientID(connClientID, msgClientID string) (string, bool) {
	switch connClientID {
	case "":
		// Anonymous connections cannot enter presence.
		return "", false
	case wildcardClientID:
		// A wildcard bearer must select a concrete clientId; "*" is never
		// itself a member identity.
		if msgClientID == "" || msgClientID == wildcardClientID {
			return "", false
		}
		return msgClientID, true
	default:
		// Concrete clientId: the member may omit it (we stamp ours) or
		// supply the matching value; anything else is rejected.
		if msgClientID == "" || msgClientID == connClientID {
			return connClientID, true
		}
		return "", false
	}
}

// recordPresence updates the per-connection entered set after a
// successful presence operation: ENTER/UPDATE/PRESENT add the member,
// LEAVE/ABSENT remove it.
func (c *connection) recordPresence(channel, clientID string, action protocol.PresenceAction) {
	switch action {
	case protocol.PresenceLeave, protocol.PresenceAbsent:
		if set, ok := c.entered[channel]; ok {
			delete(set, clientID)
			if len(set) == 0 {
				delete(c.entered, channel)
			}
		}
	default: // Enter, Update, Present
		set := c.entered[channel]
		if set == nil {
			set = make(map[string]struct{})
			c.entered[channel] = set
		}
		set[clientID] = struct{}{}
	}
}

// leaveChannel synthesises a LEAVE for every member this connection
// entered on channel and clears them from the entered set. Used on
// DETACH. Best-effort: a publish failure is logged, not surfaced.
func (c *connection) leaveChannel(ctx context.Context, channel string) {
	set := c.entered[channel]
	if len(set) == 0 {
		return
	}
	c.publishLeaves(ctx, channel, set)
	delete(c.entered, channel)
}

// emitTeardownLeaves synthesises LEAVE for every member still held by
// this connection across all channels, on a fresh bounded context (the
// connection's own context is already cancelled). Called once as the
// connection terminates (DESIGN.md §12.5).
func (c *connection) emitTeardownLeaves() {
	if len(c.entered) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), teardownLeaveTimeout)
	defer cancel()
	for channel, set := range c.entered {
		c.publishLeaves(ctx, channel, set)
	}
	c.entered = make(map[string]map[string]struct{})
}

// publishLeaves publishes one LEAVE per clientId in set onto channel,
// stamped with this connection's id.
func (c *connection) publishLeaves(ctx context.Context, channel string, set map[string]struct{}) {
	ch, err := c.manager.GetChannel(ctx, channel)
	if err != nil {
		c.logger.Warn("teardown leave: GetChannel failed", "channel", channel, "err", err)
		return
	}
	leaves := make([]*protocol.PresenceMessage, 0, len(set))
	for clientID := range set {
		leaves = append(leaves, &protocol.PresenceMessage{
			Action:       protocol.PresenceLeave,
			ClientID:     clientID,
			ConnectionID: c.id,
		})
	}
	if _, _, err := ch.PublishPresence(ctx, leaves); err != nil {
		c.logger.Warn("teardown leave publish failed", "channel", channel, "err", err)
	}
}

// nack queues a NACK for msgSerial, optionally carrying an ErrorInfo.
func (c *connection) nack(ctx context.Context, msgSerial int64, errInfo *protocol.ErrorInfo) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionNack,
		MsgSerial: msgSerial,
		Error:     errInfo,
	})
}
