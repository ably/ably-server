package realtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// defaultReplayCap caps the number of Messages replayed per ATTACH
// resume. A client that has missed more than this many messages still
// receives the most recent defaultReplayCap, with ATTACHED.Error set
// and FlagResumed cleared so the SDK can surface a discontinuity.
const defaultReplayCap = 1000

// attachment is the (connection, channel) pair on this node. It owns
// one goroutine that walks the channel's Stream and pushes frames
// (ATTACHED, then MESSAGE per published message) onto the connection's
// outbound chan. The goroutine exits when the attachment's context is
// cancelled — either because the connection is closing, or because the
// client sent a DETACH.
type attachment struct {
	channelName  string
	channel      *core.Channel
	stream       *core.Stream
	resumeFrom   string // client-supplied channelSerial, "" for a fresh attach
	replayCap    int
	out          chan<- *protocol.ProtocolMessage
	logger       *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// newAttachment derives a cancellable context from parent and returns
// an attachment ready to be run. resumeFrom may be empty for a fresh
// attach; if non-empty, run() will replay the gap before entering the
// live Stream loop.
func newAttachment(parent context.Context, name string, channel *core.Channel, stream *core.Stream, resumeFrom string, out chan<- *protocol.ProtocolMessage, logger *slog.Logger) *attachment {
	ctx, cancel := context.WithCancel(parent)
	return &attachment{
		channelName: name,
		channel:     channel,
		stream:      stream,
		resumeFrom:  resumeFrom,
		replayCap:   defaultReplayCap,
		out:         out,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
	}
}

// run sends ATTACHED, optionally replays the gap between the client's
// resume cursor and the live tail, then forwards stream
// ChannelMessages to the connection until the attachment's context is
// cancelled. done is closed on exit.
func (a *attachment) run() {
	defer close(a.done)

	anchor := a.stream.ChannelSerial()
	replay, resumed, errInfo := a.computeReplay(anchor)

	flags := int64(0)
	if resumed {
		flags = protocol.FlagResumed
	}

	attached := &protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       a.channelName,
		ChannelSerial: a.attachedChannelSerial(anchor),
		Flags:         flags,
		Error:         errInfo,
	}
	if !a.send(attached) {
		return
	}

	for _, cm := range replay {
		if !a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionMessage,
			Channel:       a.channelName,
			ChannelSerial: cm.ChannelSerial,
			Messages:      cm.Messages,
		}) {
			return
		}
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

// attachedChannelSerial returns the value to put in ATTACHED.channelSerial:
// the client's supplied resume cursor when a resume was attempted (so
// the client knows the server understood their cursor), or the live
// anchor for a fresh attach.
func (a *attachment) attachedChannelSerial(anchor string) string {
	if a.resumeFrom != "" {
		return a.resumeFrom
	}
	return anchor
}

// computeReplay decides what to replay and whether the resume succeeded
// in full. Returns the cms to replay (oldest-first, ready to forward
// as MESSAGE frames), whether ATTACHED.flags.RESUMED should be set,
// and an optional ErrorInfo for ATTACHED.error.
func (a *attachment) computeReplay(anchor string) ([]*protocol.ChannelMessage, bool, *protocol.ErrorInfo) {
	if a.resumeFrom == "" {
		// Fresh attach: no replay, RESUMED clear.
		return nil, false, nil
	}
	if a.resumeFrom == anchor {
		// Caught up: nothing to replay, but the resume is "complete".
		return nil, true, nil
	}

	// Backwards from the anchor inclusive; cap+1 lets us distinguish
	// a perfect-fit gap (== cap) from a cap-exceeded gap (> cap).
	page, err := a.channel.History(a.ctx, storage.HistoryQuery{
		Direction:        storage.DirectionBackwards,
		EndChannelSerial: anchor,
		Limit:            a.replayCap + 1,
	})
	if err != nil {
		a.logger.Warn("resume history failed; falling back to fresh attach", "err", err)
		return nil, false, &protocol.ErrorInfo{
			Message:    "history lookup failed; resume replay was skipped",
			Code:       40000,
			StatusCode: 500,
		}
	}

	// page is newest-first. Each ChannelMessage's Messages slice has
	// been reversed by the backwards direction (TASK-6) — we un-reverse
	// before delivery.
	cms := page.ChannelMessages

	// Find the index where channel_serial <= resumeFrom. Everything
	// before that index is strictly > resumeFrom (i.e. part of the
	// gap to replay). Everything at or after has already been seen by
	// the client.
	cut := len(cms)
	for i, cm := range cms {
		if cm.ChannelSerial <= a.resumeFrom {
			cut = i
			break
		}
	}

	if cut < len(cms) {
		// We reached back to (or past) the client's cursor — the gap
		// fits within the cap. Trim and replay.
		return reverseAndNormalise(cms[:cut]), true, nil
	}

	// We never saw the client's cursor: cap was exceeded (or the
	// cursor was older than retained history, which today only
	// happens via a fabricated client cursor — once TASK-26 lands,
	// the same code path covers aged-out cursors).
	//
	// If we got cap+1 cms, drop the oldest to keep newest cap.
	if len(cms) > a.replayCap {
		cms = cms[:a.replayCap]
	}
	return reverseAndNormalise(cms), false, &protocol.ErrorInfo{
		Message:    fmt.Sprintf("replay was truncated to the most recent %d messages", a.replayCap),
		Code:       40012,
		StatusCode: 200,
	}
}

// reverseAndNormalise flips the ChannelMessage slice from newest-first
// to oldest-first, and un-reverses each cm's Messages slice (which
// the backwards-direction storage scan emits in reverse idx order).
// The cms are copies — safe to mutate.
func reverseAndNormalise(cms []*protocol.ChannelMessage) []*protocol.ChannelMessage {
	out := make([]*protocol.ChannelMessage, len(cms))
	for i, cm := range cms {
		out[len(cms)-1-i] = cm
		for j, k := 0, len(cm.Messages)-1; j < k; j, k = j+1, k-1 {
			cm.Messages[j], cm.Messages[k] = cm.Messages[k], cm.Messages[j]
		}
	}
	return out
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
