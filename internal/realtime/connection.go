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
	select {
	case c.outbound <- &protocol.ProtocolMessage{Action: protocol.ActionConnected, ConnectionID: c.id}:
	case <-ctx.Done():
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
		c.handleAttach(ctx, msg.Channel)
	default:
		c.logger.Debug("received frame", "action", msg.Action.String())
	}
}

// handleAttach starts an attachment for name if one does not already
// exist on this connection. The attachment goroutine writes ATTACHED
// followed by a MESSAGE frame for every subsequent publish.
func (c *connection) handleAttach(ctx context.Context, name string) {
	if name == "" {
		c.logger.Warn("ATTACH with empty channel name; ignoring")
		return
	}
	if _, exists := c.attachments[name]; exists {
		return
	}
	ch := c.manager.GetChannel(name)
	a := newAttachment(name, ch.Attach(), c.outbound, c.logger.With("channel", name))
	c.attachments[name] = a
	go a.run(ctx)
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
