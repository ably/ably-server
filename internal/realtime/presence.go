package realtime

import (
	"context"
	"strconv"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/protocol"
)

// wildcardClientID is the §3.2 marker meaning "the bearer may assume any
// clientId". It is never itself a member identity. Aliased to the
// canonical constant in internal/auth so connection resolution and the
// presence/message rules agree.
const wildcardClientID = auth.WildcardClientID

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
	msgSerial := msg.GetMsgSerial()
	if msg.GetChannel() == "" || len(msg.Presence) == 0 {
		c.logger.Warn("PRESENCE with empty channel or no payload; rejecting", "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, nil)
		return
	}

	// Presence requires an attachment holding the PRESENCE mode.
	a, ok := c.attachments[msg.GetChannel()]
	if !ok || !a.hasMode(protocol.FlagPresence) {
		c.logger.Warn("PRESENCE without an attached PRESENCE-mode channel; rejecting",
			"channel", msg.GetChannel(), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "presence requires an attachment with the presence mode",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// Resolve + validate every member's clientId, stamp connectionId, and
	// mint the Ably-form id for genuine (non-synthesized) members.
	for i, p := range msg.Presence {
		cid, ok := resolvePresenceClientID(c.clientID, p.ClientID)
		if !ok {
			c.logger.Warn("PRESENCE clientId rejected", "channel", msg.GetChannel(),
				"connClientId", c.clientID, "msgClientId", p.ClientID, "msgSerial", msgSerial)
			c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
				Message:    "invalid clientId for presence",
				Code:       91000,
				StatusCode: 400,
			})
			return
		}
		p.ClientID = cid
		p.ConnectionID = c.id
		// Stamp the id only when the client did not supply one, mirroring the
		// reference (realtimeattachment.ts presence(): `if (!entry.id)`); a
		// client-supplied id is honoured for idempotency exactly as for
		// messages.
		if p.ID == "" {
			p.ID = presenceID(c.id, msgSerial, i)
		}
	}

	// Track membership for teardown LEAVE (DESIGN.md §12.5) on the read
	// goroutine, which is the only writer of the entered set. This is done
	// optimistically before the store completes; a rare store failure
	// would leave a spurious entry whose only effect is a harmless
	// synthesised LEAVE for a member that never durably entered.
	for _, p := range msg.Presence {
		c.recordPresence(msg.GetChannel(), p.ClientID, p.Action)
	}

	// The presence store runs on the publish worker so its ACK stays
	// ordered with this connection's message/mutation ACKs (msgSerial is
	// shared across all publish kinds) and is emitted only after a durable
	// commit. Count is 1: an ACK acknowledges one protocol
	// message (this PRESENCE frame), not the members it carries.
	channel := msg.GetChannel()
	presence := msg.Presence
	ch := a.channel
	c.enqueuePublish(ctx, func() {
		if _, _, err := ch.PublishPresence(ctx, presence); err != nil {
			c.logger.Warn("presence publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionAck,
			MsgSerial: &msgSerial,
			Count:     1,
		})
	})
}

// presentSnapshot copies members for a SYNC frame, stamping each with
// action PRESENT (DESIGN.md §12.4). The stored members keep their
// enter/update action — Members may return pointers into live backend
// state, so we must copy rather than mutate.
func presentSnapshot(members []*protocol.PresenceMessage) []*protocol.PresenceMessage {
	out := make([]*protocol.PresenceMessage, len(members))
	for i, m := range members {
		cp := *m
		cp.Action = protocol.PresencePresent
		out[i] = &cp
	}
	return out
}

// presenceID mints the Ably-form id a genuine presence message carries:
// "<connectionId>:<msgSerial>:<index>" — the publishing member's
// connectionId, the inbound PRESENCE frame's msgSerial, and the message's
// position in that frame's presence array (DESIGN.md §12.1). SDKs treat a
// message whose id is prefixed by its own connectionId as non-synthesized
// and order members by (msgSerial, index) rather than by timestamp
// (RTP2b2). msgSerial and index are
// plain (unpadded) integers, matching the reference
// (realtimeattachment.ts: `msgId + ':' + i`, msgId == `connectionId + ':'
// + msgSerial`). Server-fabricated events (teardown/detach LEAVEs) carry
// no id and stay genuinely synthesized (RTP2b1).
func presenceID(connectionID string, msgSerial int64, idx int) string {
	return connectionID + ":" + strconv.FormatInt(msgSerial, 10) + ":" + strconv.Itoa(idx)
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

// scheduleTeardownLeaves hands this connection's still-held presence
// members to the server's delayed-leave reaper (DESIGN.md §12.5), which
// synthesises their LEAVE after the grace window unless the same
// connectionId re-enters first. Used when the connection drops abruptly
// (not a clean CLOSE), so a resume within the window preserves the member.
// Only the read goroutine touches entered, and it has stopped, so this is
// race-free.
func (c *connection) scheduleTeardownLeaves() {
	if len(c.entered) == 0 {
		return
	}
	channels := make([]string, 0, len(c.entered))
	for channel := range c.entered {
		channels = append(channels, channel)
	}
	c.srv.scheduleConnectionLeaves(c.id, channels)
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
// Count is 1: a NACK rejects exactly one inbound frame. SDKs correlate
// an ACK/NACK by counting Count messages from MsgSerial, so a missing
// Count (0) leaves the operation uncorrelated and the client hanging.
func (c *connection) nack(ctx context.Context, msgSerial int64, errInfo *protocol.ErrorInfo) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionNack,
		MsgSerial: &msgSerial,
		Count:     1,
		Error:     errInfo,
	})
}
