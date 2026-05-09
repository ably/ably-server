package realtime

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// connection is one live WebSocket connection. It owns two goroutines:
// the request-handler goroutine runs the read loop, and one goroutine is
// spawned for the write loop (gorilla/websocket requires a single writer
// per connection).
type connection struct {
	ws                *websocket.Conn
	format            protocol.Format
	id                string
	heartbeatInterval time.Duration
	logger            *slog.Logger

	outbound chan *protocol.ProtocolMessage
}

// run drives the connection until either side terminates. It returns
// once both loops have exited.
func (c *connection) run(ctx context.Context) {
	defer c.ws.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// CONNECTED is the first frame we emit.
	c.send(&protocol.ProtocolMessage{
		Action:       protocol.ActionConnected,
		ConnectionID: c.id,
	})

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		c.writeLoop(ctx)
	}()

	c.readLoop()
	cancel()
	<-writeDone
}

// readLoop decodes inbound frames and dispatches on Action. For this
// iteration it only logs them — once attachments and publish land, this
// is where they will be routed.
func (c *connection) readLoop() {
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
		c.logger.Debug("received frame", "action", msg.Action.String())
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

// send queues an outbound frame for the write loop.
func (c *connection) send(msg *protocol.ProtocolMessage) {
	select {
	case c.outbound <- msg:
	default:
		c.logger.Warn("outbound buffer full, dropping frame", "action", msg.Action.String())
	}
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
