package realtime

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
)

// connection is one live WebSocket connection. It owns two goroutines:
// the request-handler goroutine runs the read loop, and one goroutine is
// spawned for the write loop (gorilla/websocket requires a single writer
// per connection). Attachments spawn additional goroutines, one per
// channel, that push frames onto the connection's outbound channel.
type connection struct {
	ws                *websocket.Conn
	format            protocol.Format
	id                string
	heartbeatInterval time.Duration
	logger            *slog.Logger
	manager           *core.Manager

	outbound    chan *protocol.ProtocolMessage
	attachments map[string]*attachment
}

// run drives the connection until either side terminates. It returns
// once both loops have exited.
func (c *connection) run(ctx context.Context) {
	defer c.ws.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// CONNECTED is the first frame we emit; buffer is empty here.
	if !c.queue(ctx, &protocol.ProtocolMessage{Action: protocol.ActionConnected, ConnectionID: c.id}) {
		return
	}

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		c.writeLoop(ctx)
	}()

	c.readLoop(ctx)
	cancel()
	<-writeDone
}

// readLoop decodes inbound frames and dispatches on Action.
func (c *connection) readLoop(ctx context.Context) {
	for {
		typ, data, err := c.ws.ReadMessage()
		if err != nil {
			if !isExpectedClose(err) {
				c.logger.Debug("read error", "err", err)
			}
			return
		}

		expected := websocket.TextMessage
		if c.format == protocol.FormatMsgpack {
			expected = websocket.BinaryMessage
		}
		if typ != expected {
			c.logger.Warn("unexpected frame type", "got", typ, "want", expected)
			continue
		}

		var msg protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, c.format, &msg); err != nil {
			c.logger.Warn("decode error", "err", err)
			continue
		}
		c.dispatch(ctx, &msg)
	}
}

func (c *connection) dispatch(ctx context.Context, msg *protocol.ProtocolMessage) {
	switch msg.Action {
	case protocol.ActionAttach:
		c.handleAttach(ctx, msg)
	case protocol.ActionDetach:
		c.handleDetach(ctx, msg.Channel)
	case protocol.ActionMessage:
		c.handleMessage(ctx, msg)
	case protocol.ActionClose:
		c.handleClose(ctx)
	default:
		c.logger.Debug("received frame", "action", msg.Action.String())
	}
}

// handleClose responds to a client-initiated CLOSE with CLOSED. The
// client then closes its end of the WebSocket, which causes readLoop's
// ReadMessage to return and the connection to terminate normally.
func (c *connection) handleClose(ctx context.Context) {
	c.queue(ctx, &protocol.ProtocolMessage{Action: protocol.ActionClosed})
}

// handleAttach starts an attachment for the channel named by msg if
// one does not already exist on this connection. msg.ChannelSerial,
// if non-empty, is the client's resume cursor: the attachment will
// replay the gap from that cursor to the live anchor before entering
// the live MESSAGE forwarding loop (subject to the replay cap).
func (c *connection) handleAttach(ctx context.Context, msg *protocol.ProtocolMessage) {
	name := msg.Channel
	if name == "" {
		c.logger.Warn("ATTACH with empty channel name; ignoring")
		return
	}
	if _, exists := c.attachments[name]; exists {
		return
	}
	ch, err := c.manager.GetChannel(ctx, name)
	if err != nil {
		c.logger.Warn("GetChannel failed", "channel", name, "err", err)
		return
	}
	stream, err := ch.Attach(ctx)
	if err != nil {
		c.logger.Warn("Attach failed", "channel", name, "err", err)
		return
	}
	a := newAttachment(ctx, name, ch, stream, msg.ChannelSerial, msg.Params, c.outbound, c.logger.With("channel", name))
	c.attachments[name] = a
	go a.run()
}

// handleDetach stops the matching attachment (waiting for its goroutine
// to exit so no further MESSAGE frames slip past the DETACHED ack) and
// queues DETACHED. DETACH for a channel with no live attachment is
// idempotent — we still ack so the client can transition cleanly.
func (c *connection) handleDetach(ctx context.Context, name string) {
	if name == "" {
		c.logger.Warn("DETACH with empty channel name; ignoring")
		return
	}
	if a, ok := c.attachments[name]; ok {
		a.stop()
		delete(c.attachments, name)
	}
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:  protocol.ActionDetached,
		Channel: name,
	})
}

// handleMessage publishes the inbound payload to its channel and ACKs
// (or NACKs) the publisher.
func (c *connection) handleMessage(ctx context.Context, msg *protocol.ProtocolMessage) {
	if msg.Channel == "" {
		c.logger.Warn("MESSAGE with empty channel name; rejecting", "msgSerial", msg.MsgSerial)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionNack,
			MsgSerial: msg.MsgSerial,
		})
		return
	}
	if len(msg.Messages) == 0 {
		c.logger.Warn("MESSAGE with no payload; rejecting", "msgSerial", msg.MsgSerial)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionNack,
			MsgSerial: msg.MsgSerial,
		})
		return
	}

	ch, err := c.manager.GetChannel(ctx, msg.Channel)
	if err != nil {
		c.logger.Warn("publish failed; NACKing", "channel", msg.Channel, "msgSerial", msg.MsgSerial, "err", err)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionNack,
			MsgSerial: msg.MsgSerial,
		})
		return
	}
	if _, _, err := ch.Publish(ctx, msg.Messages); err != nil {
		c.logger.Warn("publish failed; NACKing", "channel", msg.Channel, "msgSerial", msg.MsgSerial, "err", err)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionNack,
			MsgSerial: msg.MsgSerial,
		})
		return
	}
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionAck,
		MsgSerial: msg.MsgSerial,
		Count:     len(msg.Messages),
	})
}

// queue pushes a frame onto the outbound channel, blocking under
// backpressure. Returns false if the connection's context has been
// cancelled.
func (c *connection) queue(ctx context.Context, msg *protocol.ProtocolMessage) bool {
	select {
	case c.outbound <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

// writeLoop serialises all outbound frames and emits HEARTBEAT on idle.
func (c *connection) writeLoop(ctx context.Context) {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-c.outbound:
			if err := c.write(msg); err != nil {
				c.logger.Debug("write error", "err", err)
				return
			}
			ticker.Reset(c.heartbeatInterval)

		case <-ticker.C:
			if err := c.write(&protocol.ProtocolMessage{Action: protocol.ActionHeartbeat}); err != nil {
				c.logger.Debug("heartbeat write error", "err", err)
				return
			}
		}
	}
}

func (c *connection) write(msg *protocol.ProtocolMessage) error {
	data, err := protocol.Marshal(msg, c.format)
	if err != nil {
		return err
	}
	wsType := websocket.TextMessage
	if c.format == protocol.FormatMsgpack {
		wsType = websocket.BinaryMessage
	}
	return c.ws.WriteMessage(wsType, data)
}

func isExpectedClose(err error) bool {
	if errors.Is(err, websocket.ErrCloseSent) {
		return true
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		return true
	}
	return false
}
