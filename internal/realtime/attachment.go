package realtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// defaultReplayCap caps the number of Messages replayed per ATTACH
// (resume or rewind). A client that has missed more than this many
// messages still receives the most recent defaultReplayCap, with
// ATTACHED.Error set and FlagResumed cleared so the SDK can surface a
// discontinuity.
const defaultReplayCap = 1000

// attachment is the (connection, channel) pair on this node. It owns
// one goroutine that walks the channel's Stream and pushes frames
// (ATTACHED, then MESSAGE per published message) onto the connection's
// outbound chan. The goroutine exits when the attachment's context is
// cancelled — either because the connection is closing, or because the
// client sent a DETACH.
type attachment struct {
	channelName string
	channel     *core.Channel
	stream      *core.Stream
	// resumeFrom: client-supplied channelSerial, "" for a fresh attach.
	// Takes precedence over rewindParam (per DESIGN §4.3).
	resumeFrom string
	// rewindParam: raw value of params["rewind"], parsed by ParseRewind.
	// Ignored when resumeFrom is non-empty.
	rewindParam string
	// params: full ATTACH params, echoed in ATTACHED.params.
	params    map[string]string
	replayCap int
	out       chan<- *protocol.ProtocolMessage
	logger    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// newAttachment derives a cancellable context from parent and returns
// an attachment ready to be run. resumeFrom may be empty for a fresh
// attach; if non-empty, run() will replay the gap before entering the
// live Stream loop. rewindParam takes effect only when resumeFrom is
// empty — channelSerial wins (DESIGN §4.3).
func newAttachment(parent context.Context, name string, channel *core.Channel, stream *core.Stream, resumeFrom string, params map[string]string, out chan<- *protocol.ProtocolMessage, logger *slog.Logger) *attachment {
	ctx, cancel := context.WithCancel(parent)
	rewind := ""
	if resumeFrom == "" {
		rewind = params["rewind"]
	}
	return &attachment{
		channelName: name,
		channel:     channel,
		stream:      stream,
		resumeFrom:  resumeFrom,
		rewindParam: rewind,
		params:      params,
		replayCap:   defaultReplayCap,
		out:         out,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
	}
}

// run sends ATTACHED, optionally replays history (resume or rewind),
// then forwards stream ChannelMessages to the connection until the
// attachment's context is cancelled. done is closed on exit.
func (a *attachment) run() {
	defer close(a.done)

	anchor := a.stream.ChannelSerial()
	replay, attachPoint, resumed, errInfo := a.computeReplay(anchor)

	flags := int64(0)
	if resumed {
		flags = protocol.FlagResumed
	}

	attached := &protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       a.channelName,
		ChannelSerial: attachPoint,
		Flags:         flags,
		Error:         errInfo,
		Params:        a.params,
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

// computeReplay decides what to replay, what attach point to advertise
// in ATTACHED.channelSerial, whether RESUMED should be set, and any
// ErrorInfo to surface.
//
// Three modes:
//
//   - Fresh attach (no resumeFrom, no rewindParam): no replay,
//     attach point = anchor (the live tail), RESUMED clear.
//   - Resume (resumeFrom != ""): replay the gap from the client's
//     cursor up to anchor; attach point = client's cursor.
//   - Rewind (rewindParam != "" and resumeFrom == ""): replay
//     historical context preceding the live tail; attach point =
//     the predecessor of the rewind window (or the channel's
//     immutable initial serial when the window covers everything).
//     RESUMED always clear for rewind.
func (a *attachment) computeReplay(anchor string) (replay []*protocol.ChannelMessage, attachPoint string, resumed bool, errInfo *protocol.ErrorInfo) {
	switch {
	case a.resumeFrom != "":
		return a.computeResumeReplay(anchor)
	case a.rewindParam != "":
		return a.computeRewindReplay(anchor)
	default:
		return nil, anchor, false, nil
	}
}

func (a *attachment) computeResumeReplay(anchor string) ([]*protocol.ChannelMessage, string, bool, *protocol.ErrorInfo) {
	if a.resumeFrom == anchor {
		// Caught up: nothing to replay, but the resume is "complete".
		return nil, a.resumeFrom, true, nil
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
		return nil, a.resumeFrom, false, &protocol.ErrorInfo{
			Message:    "history lookup failed; resume replay was skipped",
			Code:       40000,
			StatusCode: 500,
		}
	}

	cms := page.ChannelMessages

	// Find the oldest cm whose serial <= resumeFrom — everything
	// before that index is part of the gap to replay.
	cut := len(cms)
	for i, cm := range cms {
		if cm.ChannelSerial <= a.resumeFrom {
			cut = i
			break
		}
	}

	if cut < len(cms) {
		return reverseAndNormalise(cms[:cut]), a.resumeFrom, true, nil
	}

	// Client cursor not matched. Two sub-cases:
	//   - returned cap+1 cms → cap exceeded
	//   - returned <= cap cms → exhausted the backwards walk; storage
	//     has nothing older. Without retention (TASK-26 absent),
	//     that means we delivered the full history, RESUMED set.
	//     The client's cursor is effectively older than everything
	//     we have — common after a rewind that used the channel's
	//     initial serial as its attach point.
	if len(cms) <= a.replayCap {
		return reverseAndNormalise(cms), a.resumeFrom, true, nil
	}
	return reverseAndNormalise(cms[:a.replayCap]), a.resumeFrom, false, &protocol.ErrorInfo{
		Message:    fmt.Sprintf("replay was truncated to the most recent %d messages", a.replayCap),
		Code:       40012,
		StatusCode: 200,
	}
}

func (a *attachment) computeRewindReplay(anchor string) ([]*protocol.ChannelMessage, string, bool, *protocol.ErrorInfo) {
	mode, count, dur, err := ParseRewind(a.rewindParam)
	if err != nil {
		return nil, anchor, false, &protocol.ErrorInfo{
			Message:    fmt.Sprintf("invalid rewind value %q: %v", a.rewindParam, err),
			Code:       40000,
			StatusCode: 400,
		}
	}

	// Common: backwards from anchor with a Limit one greater than the
	// deliverable maximum. The "+1" returned (if any) is the cm just
	// before the rewind window — the natural attach point. If the
	// query returns <= the deliverable maximum (rewind window covers
	// the entire channel), the attach point falls back to the
	// channel's immutable initial serial.
	var (
		query    storage.HistoryQuery
		capLimit int // deliverable maximum (not Limit)
	)
	switch mode {
	case rewindCount:
		capLimit = min(count, a.replayCap)
		query = storage.HistoryQuery{
			Direction:        storage.DirectionBackwards,
			EndChannelSerial: anchor,
			Limit:            capLimit + 1,
		}
	case rewindDuration:
		capLimit = a.replayCap
		nowMs := time.Now().UnixMilli()
		startMs := max(nowMs-dur.Milliseconds(), 1)
		query = storage.HistoryQuery{
			Direction:        storage.DirectionBackwards,
			EndChannelSerial: anchor,
			Start:            startMs,
			Limit:            capLimit + 1,
		}
	default:
		return nil, anchor, false, nil
	}

	page, err := a.channel.History(a.ctx, query)
	if err != nil {
		a.logger.Warn("rewind history failed", "err", err)
		return nil, anchor, false, &protocol.ErrorInfo{
			Message:    "history lookup failed; rewind replay was skipped",
			Code:       40000,
			StatusCode: 500,
		}
	}

	cms := page.ChannelMessages
	// page is newest-first. The oldest in cms (last element) is the
	// "+1" entry when present — the predecessor of the rewind window.
	var (
		attachPoint string
		windowCms   []*protocol.ChannelMessage
		capExceeded bool
	)
	if len(cms) > capLimit {
		// The (capLimit+1)-th oldest is the predecessor; drop it,
		// deliver the rest.
		attachPoint = cms[capLimit].ChannelSerial
		windowCms = cms[:capLimit]
		if mode == rewindCount && count > a.replayCap {
			capExceeded = true
		} else if mode == rewindDuration {
			capExceeded = true
		}
	} else {
		// Rewind window covers everything we have; no predecessor in
		// storage. Use the channel's immutable initial serial.
		attachPoint = a.channel.InitialChannelSerial()
		windowCms = cms
	}

	var info *protocol.ErrorInfo
	if capExceeded {
		info = &protocol.ErrorInfo{
			Message:    fmt.Sprintf("rewind was truncated to the most recent %d messages", a.replayCap),
			Code:       40012,
			StatusCode: 200,
		}
	}
	return reverseAndNormalise(windowCms), attachPoint, false, info
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
