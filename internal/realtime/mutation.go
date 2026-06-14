package realtime

import (
	"context"
	"errors"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// handleMutation processes an inbound MESSAGE frame carrying a mutation —
// an update, delete or append targeting an existing message (DESIGN.md
// §13.2, §13.6). The mutation reuses the MESSAGE frame (no new
// ProtocolMessage action), distinguished by the Message-level action and
// a target serial. The handler:
//
//   - requires an attachment holding the PUBLISH mode (§13.5);
//   - validates a single message carrying a mutation action and a target;
//   - stamps the operating clientId onto the version;
//   - calls Channel.Mutate (which validates the target, merges, mints the
//     version, persists, and updates the projection/versions index);
//   - ACKs on success, NACKs on failure (a missing/aged-out target NACKs).
//
// Subscribers receive the new version as an ordinary outbound MESSAGE
// frame via the normal attachment cursor (forward()), carrying the action,
// the new version, and the unchanged serial in stream order.
//
// Capability/ownership gating is intentionally absent: the capability
// framework (TASK-12) is unbuilt, so the only gate is the PUBLISH mode,
// matching the rest of the realtime surface. TASK-51 adds ownership once
// TASK-12 lands.
func (c *connection) handleMutation(ctx context.Context, msg *protocol.ProtocolMessage) {
	if len(msg.Messages) != 1 {
		c.logger.Warn("mutation must carry exactly one message; rejecting",
			"channel", msg.Channel, "count", len(msg.Messages), "msgSerial", msg.MsgSerial)
		c.nack(ctx, msg.MsgSerial, &protocol.ErrorInfo{
			Message:    "a mutation must carry exactly one message",
			Code:       40000,
			StatusCode: 400,
		})
		return
	}
	m := msg.Messages[0]
	if !m.Action.IsMutation() || m.Serial == "" {
		c.logger.Warn("mutation missing action or target serial; rejecting",
			"channel", msg.Channel, "action", m.Action.String(), "msgSerial", msg.MsgSerial)
		c.nack(ctx, msg.MsgSerial, &protocol.ErrorInfo{
			Message:    "a mutation requires an action and a target serial",
			Code:       40000,
			StatusCode: 400,
		})
		return
	}

	// A mutation requires an attachment holding the PUBLISH mode
	// (DESIGN.md §13.5).
	a, ok := c.attachments[msg.Channel]
	if !ok || !a.hasMode(protocol.FlagPublish) {
		c.logger.Warn("mutation without an attached PUBLISH-mode channel; rejecting",
			"channel", msg.Channel, "msgSerial", msg.MsgSerial)
		c.nack(ctx, msg.MsgSerial, &protocol.ErrorInfo{
			Message:    "a mutation requires an attachment with the publish mode",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// Stamp the operating clientId onto the version (DESIGN.md §13.1).
	// Only a concrete connection clientId is recorded as the operator;
	// the message's creator clientId is carried forward by the merge.
	if c.clientID != "" && c.clientID != wildcardClientID {
		m.ClientID = c.clientID
	}

	_, _, err := a.channel.Mutate(ctx, m)
	if err != nil {
		if errors.Is(err, storage.ErrTargetNotFound) {
			c.logger.Warn("mutation target not found; NACKing",
				"channel", msg.Channel, "target", m.Serial, "msgSerial", msg.MsgSerial)
			c.nack(ctx, msg.MsgSerial, &protocol.ErrorInfo{
				Message:    "target message not found",
				Code:       40400,
				StatusCode: 404,
			})
			return
		}
		c.logger.Warn("mutation failed; NACKing",
			"channel", msg.Channel, "target", m.Serial, "msgSerial", msg.MsgSerial, "err", err)
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}

	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionAck,
		MsgSerial: msg.MsgSerial,
		Count:     1,
	})
}
