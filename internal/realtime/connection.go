package realtime

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
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
	clientID          string // resolved clientId for this connection ("" = anonymous, "*" = wildcard); see DESIGN.md §3.2
	principal         *auth.Principal // verified credential + token claims; clientId resolution (TASK-11) consumes this
	heartbeatInterval time.Duration
	// echo is the connection's `echo` upgrade param (default true). When
	// false, the fan-out skips delivering this connection's own published
	// MESSAGEs back to it (DESIGN.md §2.1); presence is always delivered.
	echo    bool
	logger  *slog.Logger
	manager *core.Manager

	outbound    chan *protocol.ProtocolMessage
	attachments map[string]*attachment

	// entered tracks the presence members this connection has entered,
	// per channel: channel -> set of clientIds. Used to synthesise LEAVE
	// on DETACH and on connection teardown (DESIGN.md §12.5). Only
	// touched from the single read-loop goroutine (dispatch + teardown).
	entered map[string]map[string]struct{}
}

// Connection limits advertised in ConnectionDetails on CONNECTED
// (DESIGN.md §2.1, §8). They are advisory today — the server does not
// enforce them yet — but SDKs adopt them (e.g. rejecting oversize
// publishes client-side against maxMessageSize).
const (
	// defaultMaxMessageSize is Ably's 64 KiB single-publish payload cap.
	defaultMaxMessageSize int64 = 65536
	// defaultMaxFrameSize is Ably's 512 KiB frame / POST-body cap.
	defaultMaxFrameSize int64 = 524288
	// defaultMaxInboundRate is the advisory per-connection publish rate
	// ceiling in messages/second.
	defaultMaxInboundRate int64 = 1000
	// defaultConnectionStateTTL is how long an SDK should treat a
	// dropped connection's state as recoverable (Ably's DF1a default).
	defaultConnectionStateTTL = 120 * time.Second
)

// run drives the connection until either side terminates. It returns
// once both loops have exited.
func (c *connection) run(ctx context.Context) {
	defer c.ws.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// CONNECTED is the first frame we emit; buffer is empty here.
	if !c.queue(ctx, &protocol.ProtocolMessage{
		Action:            protocol.ActionConnected,
		ConnectionID:      c.id,
		ConnectionDetails: c.connectionDetails(),
	}) {
		return
	}

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		c.writeLoop(ctx)
	}()

	c.readLoop(ctx)

	// The read loop has exited — the connection is terminating (client
	// disconnect, network error, or the socket being closed under us on
	// shutdown). Synthesise LEAVE for every presence member this
	// connection still holds, so other subscribers see the departures
	// (DESIGN.md §12.5). Uses a fresh context since ctx is about to be
	// cancelled.
	c.emitTeardownLeaves()

	cancel()
	<-writeDone
}

// connectionDetails builds the ConnectionDetails advertised on CONNECTED
// (DESIGN.md §2.1, §8): the resolved clientId (omitted when anonymous,
// "*" for a wildcard bearer), the connectionKey (the opaque, non-resumable
// connectionId — connection-state resume is a non-goal), the connection
// limits, and maxIdleInterval aligned to the server heartbeat cadence so
// the SDK knows how long a quiet server→client direction is expected.
func (c *connection) connectionDetails() *protocol.ConnectionDetails {
	return &protocol.ConnectionDetails{
		ClientID:             c.clientID,
		ConnectionKey:        c.id,
		MaxMessageSize:       defaultMaxMessageSize,
		MaxFrameSize:         defaultMaxFrameSize,
		MaxInboundRate:       defaultMaxInboundRate,
		ConnectionStateTTLMs: defaultConnectionStateTTL.Milliseconds(),
		MaxIdleIntervalMs:    c.heartbeatInterval.Milliseconds(),
	}
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
	case protocol.ActionPresence:
		c.handlePresence(ctx, msg)
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
	a := newAttachment(ctx, name, ch, stream, msg.ChannelSerial, msg.Flags, msg.Params, c.outbound, c.id, c.echo, c.logger.With("channel", name))
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
	// Detaching from a channel leaves any presence members this
	// connection entered on it (DESIGN.md §12.5).
	c.leaveChannel(ctx, name)
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
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}
	if len(msg.Messages) == 0 {
		c.logger.Warn("MESSAGE with no payload; rejecting", "msgSerial", msg.MsgSerial)
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}
	// Mutations (update/delete/append) reuse the MESSAGE frame,
	// distinguished by a non-create Message-level action (DESIGN.md
	// §13.6); they take the mutation path rather than a fresh publish.
	for _, m := range msg.Messages {
		if m.Action.IsMutation() {
			c.handleMutation(ctx, msg)
			return
		}
	}

	// Resolve and stamp each message's clientId against the connection's
	// identity (DESIGN.md §3.2): a message may omit it (we stamp the
	// connection's), match it, or — for a wildcard connection — assert any
	// concrete identity. Asserting a disallowed identity is rejected.
	for _, m := range msg.Messages {
		cid, ok := auth.MessageClientID(c.clientID, m.ClientID)
		if !ok {
			c.logger.Warn("message clientId not permitted; NACKing",
				"channel", msg.Channel, "msgSerial", msg.MsgSerial, "msgClientId", m.ClientID)
			c.nack(ctx, msg.MsgSerial, nil)
			return
		}
		m.ClientID = cid
		// Stamp the publishing connection so subscribers see the origin
		// (Ably delivers Message.connectionId) and the fan-out can honour
		// echo=false (DESIGN.md §2.1, §8).
		m.ConnectionID = c.id
	}

	ch, err := c.manager.GetChannel(ctx, msg.Channel)
	if err != nil {
		c.logger.Warn("publish failed; NACKing", "channel", msg.Channel, "msgSerial", msg.MsgSerial, "err", err)
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}
	cm, _, err := ch.Publish(ctx, msg.Messages)
	if err != nil {
		c.logger.Warn("publish failed; NACKing", "channel", msg.Channel, "msgSerial", msg.MsgSerial, "err", err)
		c.nack(ctx, msg.MsgSerial, nil)
		return
	}
	// Count is 1: an ACK acknowledges protocol messages (one msgSerial per
	// frame), not the inner messages. We emit one ACK per inbound frame
	// and never batch-ack, so it is always 1. The per-message serials ride
	// the single Res entry (Ably's TR4s) so the publisher still learns
	// every serial it was assigned (DESIGN.md §8).
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionAck,
		MsgSerial: msg.MsgSerial,
		Count:     1,
		Res:       []*protocol.PublishResult{{Serials: messageSerials(cm.Messages)}},
	})
}

// messageSerials returns the server-assigned Serial of each message in
// idx order — the serials carried in the frame's ACK Res entry.
func messageSerials(msgs []*protocol.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Serial
	}
	return out
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
