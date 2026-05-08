package realtime

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// connection is one live WebSocket connection.
type connection struct {
	ws                *websocket.Conn
	format            protocol.Format
	id                string
	heartbeatInterval time.Duration
	logger            *slog.Logger

	outbound chan *protocol.ProtocolMessage

	closeOnce sync.Once
	done      chan struct{}
}

// run drives the connection's read and write loops until either side
// terminates.
func (c *connection) run(ctx context.Context) {
	c.done = make(chan struct{})
	defer c.close()

	// CONNECTED is the first frame we emit.
	c.send(&protocol.ProtocolMessage{
		Action:       protocol.ActionConnected,
		ConnectionID: c.id,
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.readLoop(cancel) }()
	go func() { defer wg.Done(); c.writeLoop(ctx) }()
	wg.Wait()
}

// readLoop decodes inbound frames and dispatches on Action. For this
// iteration's scope it only logs them — once attachments and publish
// land, this is where they will be routed.
func (c *connection) readLoop(cancel context.CancelFunc) {
	defer cancel()
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

// writeLoop serialises all outbound frames (gorilla/websocket requires a
// single writer per connection) and emits HEARTBEAT on idle.
func (c *connection) writeLoop(ctx context.Context) {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-c.done:
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

// send queues an outbound frame. Used by run() to emit CONNECTED.
func (c *connection) send(msg *protocol.ProtocolMessage) {
	select {
	case c.outbound <- msg:
	default:
		c.logger.Warn("outbound buffer full, dropping frame", "action", msg.Action.String())
	}
}

func (c *connection) close() {
	c.closeOnce.Do(func() {
		if c.done != nil {
			close(c.done)
		}
		_ = c.ws.Close()
	})
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
