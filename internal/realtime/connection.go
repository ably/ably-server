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

	// publishQ is the per-connection publish pipeline: one buffered
	// channel of tasks drained in FIFO order by a single publishLoop
	// goroutine (DESIGN.md §5.2). Each inbound MESSAGE/mutation/PRESENCE
	// frame enqueues exactly one task; the worker performs the storage
	// write off the read goroutine and emits the frame's ACK/NACK on
	// completion. A single FIFO worker keeps ACKs in msgSerial order and
	// keeps this connection's channel appends in publish order, while the
	// read loop stays free to decode the next frame (TASK-20).
	publishQ chan func()

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

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		c.publishLoop(ctx)
	}()

	c.readLoop(ctx)

	// The read loop has exited — the connection is terminating (client
	// disconnect, network error, or the socket being closed under us on
	// shutdown). Cancel the context and drain the publish worker first so
	// no in-flight task races the teardown below and no further ACKs are
	// queued for a dying connection.
	cancel()
	<-publishDone

	// Synthesise LEAVE for every presence member this connection still
	// holds, so other subscribers see the departures (DESIGN.md §12.5).
	// The worker has stopped and only the read goroutine ever touches the
	// entered set, so this runs race-free on a fresh, bounded context.
	c.emitTeardownLeaves()

	<-writeDone
}

// publishLoop drains the connection's publish pipeline in FIFO order,
// running one task at a time until the context is cancelled. Serialising
// the tasks keeps this connection's channel appends in publish order and
// its ACK/NACK frames in msgSerial order, while the read goroutine is free
// to decode the next frame (DESIGN.md §5.2, TASK-20).
func (c *connection) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-c.publishQ:
			task()
		}
	}
}

// enqueuePublish hands a task to the publish worker, blocking only under
// backpressure (the buffer is full because earlier writes are still in
// flight) — never on the storage write itself. Returns false if the
// connection's context is cancelled before the task is accepted.
func (c *connection) enqueuePublish(ctx context.Context, task func()) bool {
	select {
	case c.publishQ <- task:
		return true
	case <-ctx.Done():
		return false
	}
}

// enqueueNack routes a validation-rejection NACK through the publish
// worker so it is emitted in msgSerial order behind any publishes still
// in flight on this connection — a directly-queued NACK could otherwise
// overtake an earlier publish's ACK and corrupt the SDK's ack accounting
// (TASK-20, TASK-33).
func (c *connection) enqueueNack(ctx context.Context, msgSerial int64, errInfo *protocol.ErrorInfo) {
	c.enqueuePublish(ctx, func() { c.nack(ctx, msgSerial, errInfo) })
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

// handleMessage validates and stamps the inbound payload on the read
// goroutine, then hands the storage write to the publish worker, which
// ACKs after a durable commit (or NACKs on failure). Validation
// rejections are also enqueued so their NACK stays ordered behind any
// still-pending publishes on this connection (TASK-20).
func (c *connection) handleMessage(ctx context.Context, msg *protocol.ProtocolMessage) {
	msgSerial := msg.MsgSerial
	if msg.Channel == "" {
		c.logger.Warn("MESSAGE with empty channel name; rejecting", "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, nil)
		return
	}
	if len(msg.Messages) == 0 {
		c.logger.Warn("MESSAGE with no payload; rejecting", "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, nil)
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
				"channel", msg.Channel, "msgSerial", msgSerial, "msgClientId", m.ClientID)
			c.enqueueNack(ctx, msgSerial, nil)
			return
		}
		m.ClientID = cid
		// Stamp the publishing connection so subscribers see the origin
		// (Ably delivers Message.connectionId) and the fan-out can honour
		// echo=false (DESIGN.md §2.1, §8).
		m.ConnectionID = c.id
	}

	channel := msg.Channel
	messages := msg.Messages
	c.enqueuePublish(ctx, func() {
		ch, err := c.manager.GetChannel(ctx, channel)
		if err != nil {
			c.logger.Warn("publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		cm, _, err := ch.Publish(ctx, messages)
		if err != nil {
			c.logger.Warn("publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		// Count is 1: an ACK acknowledges protocol messages (one msgSerial
		// per frame), not the inner messages. We emit one ACK per inbound
		// frame and never batch-ack, so it is always 1. The per-message
		// serials ride the single Res entry (Ably's TR4s) so the publisher
		// still learns every serial it was assigned (DESIGN.md §8). The ACK
		// is emitted only now, after storage has durably committed.
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionAck,
			MsgSerial: msgSerial,
			Count:     1,
			Res:       []*protocol.PublishResult{{Serials: messageSerials(cm.Messages)}},
		})
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
			// A server-initiated DISCONNECTED (shutdown, DESIGN.md §11) is
			// the connection's last frame: once it is on the wire, close the
			// socket so the read loop unblocks and normal teardown runs
			// (synthesising presence LEAVEs). Closing here — after the write
			// — guarantees the client receives the frame before the close.
			if msg.Action == protocol.ActionDisconnected {
				_ = c.ws.Close()
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

// disconnect initiates a graceful, server-side close (DESIGN.md §11): it
// enqueues a DISCONNECTED frame, which the write loop flushes before
// closing the socket. If the outbound buffer cannot accept the frame
// (the writer is gone or backed up), the socket is force-closed directly
// so the connection still tears down. Safe to call from the shutdown
// goroutine — it never touches per-connection state owned by the read
// loop.
func (c *connection) disconnect() {
	select {
	case c.outbound <- &protocol.ProtocolMessage{
		Action: protocol.ActionDisconnected,
		Error: &protocol.ErrorInfo{
			Message:    "server is shutting down; please reconnect",
			Code:       80003, // ErrDisconnected — a retryable disconnect
			StatusCode: 503,
		},
	}:
	default:
		c.forceClose()
	}
}

// forceClose closes the underlying socket immediately, unblocking the
// read loop. Used for stragglers still open at the shutdown deadline.
func (c *connection) forceClose() {
	_ = c.ws.Close()
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
