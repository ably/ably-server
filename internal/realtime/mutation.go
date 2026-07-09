package realtime

import (
	"context"
	"errors"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
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
// The mutation is authorised against the message-{update,delete}-{own,any}
// capability ops (DESIGN.md §13.5): -any waives ownership, -own requires
// the caller's resolved clientId to equal the target's creator. The check
// runs on the publish worker (where the creator lookup is available) so it
// stays ordered with this connection's other ACK/NACKs.
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
		if errInfo := c.authorizeMutation(ctx, ch, channel, m); errInfo != nil {
			c.logger.Warn("mutation rejected", "channel", channel, "target", m.Serial,
				"action", m.Action.String(), "msgSerial", msgSerial, "code", errInfo.Code)
			c.nack(ctx, msgSerial, errInfo)
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
			if errors.Is(err, storage.ErrIncompatibleAppend) {
				c.logger.Warn("append data incompatible; NACKing",
					"channel", channel, "target", m.Serial, "msgSerial", msgSerial)
				c.nack(ctx, msgSerial, &protocol.ErrorInfo{
					Message:    "append data type is incompatible with the target's current data",
					Code:       40000,
					StatusCode: 400,
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

// authorizeMutation applies the §13.5 capability + ownership check for a
// mutation of m on channel. It returns nil when the mutation is permitted,
// or an ErrorInfo describing the rejection (40160 insufficient capability,
// 40400 when the target does not exist and ownership had to be checked).
// The creator lookup is performed only when the caller holds just the
// -own op — the -any op waives it.
func (c *connection) authorizeMutation(ctx context.Context, ch *core.Channel, channel string, m *protocol.Message) *protocol.ErrorInfo {
	ownOp, anyOp := mutationOps(m.Action)
	switch c.capability().MutationGrant(channel, ownOp, anyOp) {
	case auth.MutationAllowed:
		return nil
	case auth.MutationDeniedCapability:
		return &protocol.ErrorInfo{
			Message:    "insufficient capability for message mutation",
			Code:       40160,
			StatusCode: 401,
		}
	}
	// MutationNeedsOwnership: the caller must own the target message.
	latest, err := ch.LatestVersion(ctx, m.Serial)
	if err != nil {
		if errors.Is(err, storage.ErrTargetNotFound) {
			return &protocol.ErrorInfo{Message: "target message not found", Code: 40400, StatusCode: 404}
		}
		return &protocol.ErrorInfo{Message: "mutation authorization failed", Code: 50000, StatusCode: 500}
	}
	if ownsMessage(c.clientID, latest.ClientID) {
		return nil
	}
	return &protocol.ErrorInfo{
		Message:    "insufficient capability: caller does not own the target message",
		Code:       40160,
		StatusCode: 401,
	}
}

// mutationOps maps a mutation action to its ownership-scoped capability op
// pair (DESIGN.md §13.5): update and append are gated by message-update-*,
// delete by message-delete-*.
func mutationOps(a protocol.MessageAction) (own, any auth.Op) {
	if a == protocol.MessageDelete {
		return auth.OpMessageDeleteOwn, auth.OpMessageDeleteAny
	}
	return auth.OpMessageUpdateOwn, auth.OpMessageUpdateAny
}

// ownsMessage reports whether a caller with the given resolved clientId
// owns a message whose creator clientId is creator (DESIGN.md §13.5). A
// wildcard or anonymous caller owns nothing — ownership requires a
// concrete identity matching the creator.
func ownsMessage(callerClientID, creator string) bool {
	return callerClientID != "" && callerClientID != wildcardClientID && callerClientID == creator
}
