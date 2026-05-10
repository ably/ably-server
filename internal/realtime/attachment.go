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
// outbound chan.
type attachment struct {
	channelName string
	stream      *core.Stream
	out         chan<- *protocol.ProtocolMessage
	logger      *slog.Logger
}

func newAttachment(name string, stream *core.Stream, out chan<- *protocol.ProtocolMessage, logger *slog.Logger) *attachment {
	return &attachment{
		channelName: name,
		stream:      stream,
		out:         out,
		logger:      logger,
	}
}

// run forwards stream messages to the connection until ctx is
// cancelled.
func (a *attachment) run(ctx context.Context) {
	if !a.send(ctx, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       a.channelName,
		ChannelSerial: a.stream.ChannelSerial(),
	}) {
		return
	}

	for {
		msg, err := a.stream.Next(ctx)
		if err != nil {
			return
		}
		if !a.send(ctx, &protocol.ProtocolMessage{
			Action:        protocol.ActionMessage,
			Channel:       a.channelName,
			ChannelSerial: a.stream.ChannelSerial(),
			Messages:      []*protocol.Message{msg},
		}) {
			return
		}
	}
}

// send pushes a frame onto the connection's outbound chan, blocking
// under backpressure. Returns false if the context is cancelled.
func (a *attachment) send(ctx context.Context, msg *protocol.ProtocolMessage) bool {
	select {
	case a.out <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}
