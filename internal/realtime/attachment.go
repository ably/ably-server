package realtime

import (
	"context"
	"log/slog"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
)

// attachment is the (connection, channel) pair on this node. It owns
// one goroutine that walks the channel's Stream and pushes frames
// (ATTACHED, then MESSAGE per published message) onto the connection's
// outbound chan. The goroutine exits when the attachment's context is
// cancelled — either because the connection is closing, or because the
// client sent a DETACH.
type attachment struct {
	channelName string
	stream      *core.Stream
	out         chan<- *protocol.ProtocolMessage
	logger      *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// newAttachment derives a cancellable context from parent and returns
// an attachment ready to be run.
func newAttachment(parent context.Context, name string, stream *core.Stream, out chan<- *protocol.ProtocolMessage, logger *slog.Logger) *attachment {
	ctx, cancel := context.WithCancel(parent)
	return &attachment{
		channelName: name,
		stream:      stream,
		out:         out,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
	}
}

// run forwards stream ChannelMessages to the connection until the
// attachment's context is cancelled. done is closed on exit. One
// MESSAGE frame is emitted per ChannelMessage, carrying the whole
// batch in Messages[] under the batch's ChannelSerial.
func (a *attachment) run() {
	defer close(a.done)

	if !a.send(&protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       a.channelName,
		ChannelSerial: a.stream.ChannelSerial(),
	}) {
		return
	}

	for {
		cm, err := a.stream.Next(a.ctx)
		if err != nil {
			return
		}
		if !a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionMessage,
			Channel:       a.channelName,
			ChannelSerial: cm.ChannelSerial,
			Messages:      cm.Messages,
		}) {
			return
		}
	}
}

// stop cancels the attachment and waits for its goroutine to exit, so
// callers can safely queue a DETACHED frame after stop returns
// knowing no further MESSAGE frames will arrive on the outbound chan.
func (a *attachment) stop() {
	a.cancel()
	<-a.done
}

// send pushes a frame onto the connection's outbound chan, blocking
// under backpressure. Returns false if the attachment's context is
// cancelled.
func (a *attachment) send(msg *protocol.ProtocolMessage) bool {
	select {
	case a.out <- msg:
		return true
	case <-a.ctx.Done():
		return false
	}
}
