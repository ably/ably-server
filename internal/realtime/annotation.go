package realtime

import (
	"context"
	"errors"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// handleAnnotation processes an inbound ANNOTATION frame: one or more
// annotation.create / annotation.delete operations targeting existing
// messages (DESIGN.md §14.1, §14.3). It authorises against the
// attachment's ANNOTATION_PUBLISH mode (which required the
// annotation-publish capability at attach time, §3.1), resolves and
// validates each annotation's clientId (§3.2, with the anonymous
// multiple.v1/total.v1 exception, §14.1), stamps the connectionId and
// timestamp, publishes via the channel (which validates the target and
// persists), and ACK/NACKs on the msgSerial.
//
// Subscribers holding ANNOTATION_SUBSCRIBE receive each annotation as an
// outbound ANNOTATION frame via the normal attachment cursor (forward()).
func (c *connection) handleAnnotation(ctx context.Context, msg *protocol.ProtocolMessage) {
	msgSerial := msg.PublishSerial()
	if msg.Channel == "" || len(msg.Annotations) == 0 {
		c.logger.Warn("ANNOTATION with empty channel or no payload; rejecting", "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, nil)
		return
	}

	// Publishing an annotation requires an attachment holding the
	// ANNOTATION_PUBLISH mode + the annotation-publish capability (DESIGN.md
	// §3.1, §14.3). The mode was granted at attach time only if the cap was
	// present; re-checking the cap here covers a capability narrowed by an
	// intervening re-auth.
	a, ok := c.attachments[msg.Channel]
	if !ok || !a.hasMode(protocol.FlagAnnotationPublish) || !c.capability().Permits(msg.Channel, auth.OpAnnotationPublish) {
		c.logger.Warn("ANNOTATION without ANNOTATION_PUBLISH mode/capability; rejecting",
			"channel", msg.Channel, "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "insufficient capability to publish annotations",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// Resolve + validate every annotation: stamp the clientId (§3.2), the
	// connectionId, and a server timestamp, then apply §14.1 validation
	// (messageSerial/type present, method known, anonymous method allowed).
	now := time.Now().UnixMilli()
	for _, an := range msg.Annotations {
		cid, ok := auth.MessageClientID(c.clientID, an.ClientID)
		if !ok {
			c.logger.Warn("ANNOTATION clientId rejected", "channel", msg.Channel,
				"connClientId", c.clientID, "annClientId", an.ClientID, "msgSerial", msgSerial)
			c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
				Message:    "invalid clientId for annotation",
				Code:       40012,
				StatusCode: 400,
			})
			return
		}
		an.ClientID = cid
		an.ConnectionID = c.id
		if an.Timestamp == 0 {
			an.Timestamp = now
		}
		if verr := an.Validate(); verr != nil {
			c.logger.Warn("ANNOTATION validation failed; rejecting",
				"channel", msg.Channel, "msgSerial", msgSerial, "reason", verr.Message)
			c.enqueueNack(ctx, msgSerial, verr.ErrorInfo())
			return
		}
	}

	channel := msg.Channel
	annotations := msg.Annotations
	ch := a.channel
	c.enqueuePublish(ctx, func() {
		cm, _, err := ch.PublishAnnotation(ctx, annotations)
		if err != nil {
			if errors.Is(err, storage.ErrTargetNotFound) {
				c.logger.Warn("annotation target not found; NACKing",
					"channel", channel, "msgSerial", msgSerial)
				c.nack(ctx, msgSerial, &protocol.ErrorInfo{
					Message:    "target message not found",
					Code:       40400,
					StatusCode: 404,
				})
				return
			}
			c.logger.Warn("annotation publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		// The ACK carries the assigned annotation serials (Ably's TR4s
		// shape) so the SDK can address them, like a message publish. Count
		// is 1: an ACK acknowledges one protocol frame.
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionAck,
			MsgSerial: &msgSerial,
			Count:     1,
			Res:       []*protocol.PublishResult{{Serials: annotationSerials(cm.Annotations)}},
		})
	})
}

// annotationSerials returns the server-assigned Serial of each annotation
// in idx order — the serials carried in the frame's ACK Res entry.
func annotationSerials(annotations []*protocol.Annotation) []string {
	out := make([]string, len(annotations))
	for i, a := range annotations {
		out[i] = a.Serial
	}
	return out
}
