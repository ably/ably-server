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
// No attachment or capability gate is applied — a mutation is a write to
// the channel stream, handled exactly like a create publish, which needs
// no attachment either. DESIGN §13.5's PUBLISH-mode + message-* capability
// gating lands uniformly (for publish and mutate) with the capability
// framework (TASK-12); applying it to mutations alone would diverge from
// the create path.
func (c *connection) handleMutation(ctx context.Context, msg *protocol.ProtocolMessage) {
	msgSerial := msg.MsgSerial
	if len(msg.Messages) != 1 {
		c.logger.Warn("mutation must carry exactly one message; rejecting",
			"channel", msg.Channel, "count", len(msg.Messages), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "a mutation must carry exactly one message",
			Code:       40000,
			StatusCode: 400,
		})
		return
	}
	m := msg.Messages[0]
	if !m.Action.IsMutation() || m.Serial == "" {
		c.logger.Warn("mutation missing action or target serial; rejecting",
			"channel", msg.Channel, "action", m.Action.String(), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "a mutation requires an action and a target serial",
			Code:       40000,
			StatusCode: 400,
		})
		return
	}

	// Stamp the operating clientId onto the version (DESIGN.md §13.1).
	// Only a concrete connection clientId is recorded as the operator;
	// the message's creator clientId is carried forward by the merge.
	if c.clientID != "" && c.clientID != wildcardClientID {
		m.ClientID = c.clientID
	}

	// The mutation store runs on the publish worker, off the read
	// goroutine, ACKing only after a durable commit and preserving this
	// connection's ACK ordering (TASK-20).
	channel := msg.Channel
	c.enqueuePublish(ctx, func() {
		ch, err := c.manager.GetChannel(ctx, channel)
		if err != nil {
			c.logger.Warn("mutation failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		cm, _, err := ch.Mutate(ctx, m)
		if err != nil {
			if errors.Is(err, storage.ErrTargetNotFound) {
				c.logger.Warn("mutation target not found; NACKing",
					"channel", channel, "target", m.Serial, "msgSerial", msgSerial)
				c.nack(ctx, msgSerial, &protocol.ErrorInfo{
					Message:    "target message not found",
					Code:       40400,
					StatusCode: 404,
				})
				return
			}
			c.logger.Warn("mutation failed; NACKing",
				"channel", channel, "target", m.Serial, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		// The ACK carries the new version serial so the SDK can return it
		// as the operation's VersionSerial (DESIGN.md §13.1).
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionAck,
			MsgSerial: msgSerial,
			Count:     1,
			Res:       []*protocol.PublishResult{{Serials: []string{storage.VersionSerial(cm.Messages[0])}}},
		})
	})
}
