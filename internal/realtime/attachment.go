package realtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// ParamAppendMode is the ATTACH channel param that selects how a
// subscriber receives streamed appends (DESIGN.md §13.3); AppendModeFull
// is its only value, opting the subscriber into full rolled-up versions
// instead of incremental append deltas. Names match Ably's reference.
const (
	ParamAppendMode = "appendMode"
	AppendModeFull  = "full"
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
	params map[string]string
	// mu guards the fields a repeat ATTACH mutates in place:
	// modes, params, appendModeFull, and curSerial. handleAttach writes
	// them from the connection read loop while the run goroutine reads
	// them on the forward path (and the read loop itself consults modes
	// on the publish/presence/annotation paths), so all access is
	// serialised here.
	mu sync.Mutex
	// modes is the effective channel-mode set for this attachment: the
	// requested modes intersected with the capability-permitted set,
	// resolved by the connection before the attachment is created
	// (DESIGN.md §3.1, §4.2). Gates frame flow — SUBSCRIBE for MESSAGE,
	// PRESENCE_SUBSCRIBE for PRESENCE/SYNC. Guarded by mu.
	modes int64
	// requestedModes is the channel-mode set originally requested at
	// ATTACH, before capability intersection. recheckCapability
	// re-derives modes from this (not from the already-narrowed modes) on
	// a live capability change, so a later widening reauth can restore a
	// mode an earlier narrowing dropped — matching the reference server's
	// permittedModeAndParams, which always recomputes from the request.
	// Guarded by mu.
	requestedModes int64
	// curSerial is the channelSerial the run goroutine has advanced the
	// live cursor to (the attach point until the first live cm). A repeat
	// ATTACH replies ATTACHED at this position (DESIGN.md §4.1). Guarded
	// by mu.
	curSerial string
	replayCap int
	out       chan<- *protocol.ProtocolMessage
	// connID is the owning connection's id, and echo its `echo` setting.
	// When echo is false the fan-out skips message cms this connection
	// published itself (DESIGN.md §2.1).
	connID  string
	echo    bool
	metrics *metrics.Metrics
	logger  *logging.Logger

	// appendModeFull is set when the subscriber requested
	// appendMode=full: it always receives full rolled-up versions rather
	// than incremental append deltas (DESIGN.md §13.3). Guarded by mu.
	appendModeFull bool
	// seen tracks the message identity serials this attachment has
	// delivered since attach, so the first append for a not-yet-seen
	// message is a full aggregated update and later appends arrive as
	// deltas (DESIGN.md §13.3). Touched only by the run goroutine.
	seen map[string]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// newAttachment derives a cancellable context from parent and returns
// an attachment ready to be run. requestedModes is the channel modes the
// ATTACH asked for, before capability intersection; modes is the
// already-resolved effective set (requestedModes ∩ capability-permitted).
// resumeFrom may be empty for a fresh attach; if non-empty, run() will
// replay the gap before entering the live Stream loop. rewindParam takes
// effect only when resumeFrom is empty — channelSerial wins (DESIGN §4.3).
func newAttachment(parent context.Context, name string, channel *core.Channel, stream *core.Stream, resumeFrom string, attachResume bool, requestedModes, modes int64, params map[string]string, out chan<- *protocol.ProtocolMessage, connID string, echo bool, m *metrics.Metrics, logger *logging.Logger) *attachment {
	ctx, cancel := context.WithCancel(parent)
	// rewind is suppressed when the attach is a resume — either a supplied
	// channelSerial cursor or the ATTACH_RESUME flag (RTL4j) — so a
	// continuation does not replay history the client has already seen
	// (DESIGN.md §4.3, matching the reference's isResume gate).
	rewind := ""
	if resumeFrom == "" && !attachResume {
		rewind = params["rewind"]
	}
	return &attachment{
		channelName:    name,
		channel:        channel,
		stream:         stream,
		resumeFrom:     resumeFrom,
		rewindParam:    rewind,
		params:         params,
		modes:          modes,
		requestedModes: requestedModes,
		replayCap:      defaultReplayCap,
		out:            out,
		connID:         connID,
		echo:           echo,
		metrics:        m,
		logger:         logger,
		appendModeFull: params[ParamAppendMode] == AppendModeFull,
		seen:           make(map[string]struct{}),
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
}

// defaultModes is the ATTACH default when a client requests no specific
// modes (DESIGN.md §4.2): the three message/presence modes, presence
// subscribe, and ANNOTATION_PUBLISH — matching the reference server's
// MODE_DEFAULT and the SDK's expected default channel.modes. Only
// ANNOTATION_SUBSCRIBE is opt-in (§14.3): raw annotation delivery must be
// requested explicitly.
const defaultModes = protocol.FlagPresence | protocol.FlagPublish | protocol.FlagSubscribe | protocol.FlagPresenceSubscribe | protocol.FlagAnnotationPublish

// modeMask is every recognised channel-mode bit — the default set plus the
// opt-in ANNOTATION_SUBSCRIBE (DESIGN.md §4.2, §14.3). Used to extract the
// requested mode bits from an ATTACH flags word.
const modeMask = defaultModes | protocol.FlagAnnotationSubscribe

// resolveModes extracts the channel-mode bits from an ATTACH flags word.
// A request with no mode bits is treated as the default set (DESIGN.md
// §4.2; ANNOTATION_SUBSCRIBE must be opted into explicitly, §14.3).
func resolveModes(flags int64) int64 {
	if m := flags & modeMask; m != 0 {
		return m
	}
	return defaultModes
}

// resolveRequestedModes resolves the channel modes an ATTACH requests,
// honouring the reference precedence (DESIGN.md §4.2): a comma-separated
// `modes` channel param wins over the flags mode bits, which win over the
// default set. The `modes` param is how SDKs send ChannelOptions.params
// = {modes: 'subscribe,presence'} (RTL4k), and the SDK expects the resolved
// set reflected both in ATTACHED.flags (from which it reads channel.modes)
// and echoed in ATTACHED.params.modes. A param that names no valid mode is
// ignored and the flags/default path is used instead.
func resolveRequestedModes(flags int64, params map[string]string) int64 {
	if s, ok := params["modes"]; ok {
		if m := parseModesParam(s); m != 0 {
			return m
		}
	}
	return resolveModes(flags)
}

// modeParamNames maps each channel-mode bit to its wire token, in the
// order the reference emits them in ATTACHED.params.modes (Mode.ParamsString).
var modeParamNames = []struct {
	bit  int64
	name string
}{
	{protocol.FlagPresence, "presence"},
	{protocol.FlagPublish, "publish"},
	{protocol.FlagSubscribe, "subscribe"},
	{protocol.FlagPresenceSubscribe, "presence_subscribe"},
	{protocol.FlagAnnotationSubscribe, "annotation_subscribe"},
	{protocol.FlagAnnotationPublish, "annotation_publish"},
}

// parseModesParam parses a comma-separated channel-modes param value into
// its mode-bit set, ignoring unrecognised tokens (matching the reference's
// ParseModes, which drops invalid tokens rather than erroring).
func parseModesParam(s string) int64 {
	var m int64
	for _, tok := range strings.Split(s, ",") {
		tok = strings.ToLower(strings.TrimSpace(tok))
		for _, mp := range modeParamNames {
			if tok == mp.name {
				m |= mp.bit
			}
		}
	}
	return m
}

// modesParamString renders a mode-bit set as the comma-separated token
// list carried in ATTACHED.params.modes (reference Mode.ParamsString order).
func modesParamString(modes int64) string {
	names := make([]string, 0, len(modeParamNames))
	for _, mp := range modeParamNames {
		if modes&mp.bit != 0 {
			names = append(names, mp.name)
		}
	}
	return strings.Join(names, ",")
}

// hasMode reports whether this attachment holds the given channel mode.
// Read under mu since a repeat ATTACH can mutate the mode set in place.
func (a *attachment) hasMode(mode int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.modes&mode != 0
}

// modeSet returns the current effective mode set under mu.
func (a *attachment) modeSet() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.modes
}

// applyReattach mutates the attachment in place for a repeat ATTACH:
// it swaps in the freshly-requested mode set, the freshly-resolved
// effective mode set, and the new params (re-deriving appendMode) and
// returns the current live serial to advertise on the re-ATTACHED. The
// stream and its goroutine are left untouched, so delivery continuity is
// preserved by construction.
func (a *attachment) applyReattach(requestedModes, modes int64, params map[string]string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requestedModes = requestedModes
	a.modes = modes
	a.params = params
	a.appendModeFull = params[ParamAppendMode] == AppendModeFull
	return a.curSerial
}

// recheckCapability re-derives the effective mode set from the
// originally-requested modes intersected with newPermitted (DESIGN.md §3,
// RTC8a1) — used after an inband reauth changes the connection's
// capability, so a widening reauth can restore a mode a previous
// narrowing dropped, and a narrowing one only fails the channel when the
// intersection empties out completely (matching the reference server's
// permittedModeAndParams recheck). Returns the new effective set (0 if no
// longer permitted at all).
func (a *attachment) recheckCapability(newPermitted int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.modes = a.requestedModes & newPermitted
	return a.modes
}

// setCurSerial records the channelSerial the live cursor has advanced to,
// so a concurrent repeat ATTACH can advertise the current position.
func (a *attachment) setCurSerial(serial string) {
	a.mu.Lock()
	a.curSerial = serial
	a.mu.Unlock()
}

// recognisedParams is the set of ATTACH channel params this server
// understands (DESIGN.md §4.2). ATTACHED.params echoes only these: an
// unrecognised param is dropped rather than reflected back, matching the
// reference, so channel.params (RTL4k1) carries only params that actually
// took effect. A client that requests a bogus param sees channel.params
// without it.
var recognisedParams = map[string]struct{}{
	"rewind": {},
	"modes":  {},
	// delta is echoed for SDK compatibility (the client asked for it) even
	// though delta encoding itself is unsupported: this server never emits
	// deltas, so the echo is inert.
	"delta":         {},
	ParamAppendMode: {},
}

// echoParams builds the params map echoed on ATTACHED (DESIGN.md §4.1),
// from which the SDK populates channel.params (RTL4k1). It reflects only
// the recognised params the client requested (recognisedParams), with the
// `modes` entry rewritten to the effective (capability-intersected) mode
// set so channel.params.modes agrees with the modes the SDK decodes from
// ATTACHED.flags. Returns nil when no recognised param was requested.
func (a *attachment) echoParams() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	echo := make(map[string]string, len(a.params))
	for k, v := range a.params {
		if _, ok := recognisedParams[k]; !ok {
			continue
		}
		echo[k] = v
	}
	if _, ok := echo["modes"]; ok {
		echo["modes"] = modesParamString(a.modes)
	}
	if len(echo) == 0 {
		return nil
	}
	return echo
}

// run sends ATTACHED, optionally replays history (resume or rewind),
// then forwards stream ChannelMessages to the connection until the
// attachment's context is cancelled. done is closed on exit.
func (a *attachment) run() {
	defer close(a.done)

	anchor := a.stream.ChannelSerial()
	replay, attachPoint, resumed, errInfo := a.computeReplay(anchor)
	// Seed the position a concurrent repeat ATTACH advertises:
	// the advertised attach point until the live loop advances it.
	a.setCurSerial(attachPoint)

	// Presence sync snapshot (DESIGN.md §12.4): only PRESENCE_SUBSCRIBE
	// attachments get the current set. Captured before ATTACHED so the
	// HAS_PRESENCE flag can be set. The snapshot is taken at-or-after the
	// live anchor; any member that enters or leaves past the anchor also
	// arrives on the live cursor, and the client converges by serial.
	var (
		syncMembers []*protocol.PresenceMessage
		syncAsOf    string
	)
	if a.hasMode(protocol.FlagPresenceSubscribe) {
		members, asOf, err := a.channel.Members(a.ctx)
		if err != nil {
			a.logger.Warn("presence sync: Members failed; skipping sync", "err", err)
		} else if len(members) > 0 {
			syncMembers = presentSnapshot(members)
			syncAsOf = asOf
		}
	}

	// ATTACHED.flags carries the effective channel-mode set (DESIGN.md
	// §4.2), plus the status flags below.
	flags := a.modeSet()
	if resumed {
		flags |= protocol.FlagResumed
	}
	// Any replay (rewind or resume gap-fill) means backlog was delivered
	// ahead of live messages; the SDK surfaces this as hasBacklog (RTL2i).
	if len(replay) > 0 {
		flags |= protocol.FlagHasBacklog
	}
	if len(syncMembers) > 0 {
		flags |= protocol.FlagHasPresence
	}

	attached := &protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       new(a.channelName),
		ChannelSerial: attachPoint,
		Flags:         flags,
		Error:         errInfo,
		Params:        a.echoParams(),
	}
	if !a.send(attached) {
		return
	}

	// Deliver the presence set as a SYNC frame before live delivery. A
	// single frame suffices at our scale; the channelSerial carries the
	// sync cursor — "<serial>:" with an empty cursor part marks the set
	// complete (paging is a later phase, DESIGN.md §12.4).
	if len(syncMembers) > 0 {
		if !a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionSync,
			Channel:       new(a.channelName),
			ChannelSerial: syncAsOf + ":",
			Presence:      syncMembers,
		}) {
			return
		}
	}

	// Replay (resume/rewind) is backlog delivery: appends arrive as full
	// rolled-up updates, never deltas (DESIGN.md §13.3).
	for _, cm := range replay {
		if !a.forward(cm, true) {
			return
		}
	}

	for {
		cm, err := a.stream.Next(a.ctx)
		if err != nil {
			return
		}
		// Advance the position a concurrent repeat ATTACH advertises to
		// this cm's serial before delivery, so the re-ATTACHED reflects
		// where the (never-interrupted) stream has reached.
		a.setCurSerial(cm.ChannelSerial)
		if !a.forward(cm, false) {
			return
		}
	}
}

// forward delivers one ChannelMessage to the connection as the wire
// frame appropriate to its kind, gated by the attachment's modes: a
// message cm becomes a MESSAGE frame (SUBSCRIBE), a presence cm becomes
// a PRESENCE frame (PRESENCE_SUBSCRIBE). A cm the attachment is not
// subscribed to is skipped. backlog is true during resume/rewind replay,
// which forces appends to be delivered as full versions. Returns false
// if the send is cancelled.
func (a *attachment) forward(cm *protocol.ChannelMessage, backlog bool) bool {
	if len(cm.Messages) > 0 {
		if !a.hasMode(protocol.FlagSubscribe) {
			return true
		}
		// echo=false suppresses delivery of this connection's own published
		// messages back to it (DESIGN.md §2.1). All messages in one cm share
		// the publishing connection, so the first message's stamped
		// connectionId identifies the origin. Presence is never suppressed —
		// it is handled by the branch below.
		selfEcho := a.connID != "" && cm.Messages[0].ConnectionID == a.connID
		if !a.echo && selfEcho {
			a.logger.Trace("message delivery skipped (echo=false)",
				"channelSerial", cm.ChannelSerial, "name", cm.Messages[0].Name)
			return true
		}
		if !a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionMessage,
			Channel:       new(a.channelName),
			ChannelSerial: cm.ChannelSerial,
			Messages:      a.resolveAppends(cm.Messages, backlog),
		}) {
			return false
		}
		a.logger.Trace("message delivered",
			"channelSerial", cm.ChannelSerial, "name", cm.Messages[0].Name, "selfEcho", selfEcho)
		a.metrics.MessageDelivered()
		return true
	}
	if len(cm.Presence) > 0 {
		if !a.hasMode(protocol.FlagPresenceSubscribe) {
			return true
		}
		return a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionPresence,
			Channel:       new(a.channelName),
			ChannelSerial: cm.ChannelSerial,
			Presence:      cm.Presence,
		})
	}
	if len(cm.Annotations) > 0 {
		// An annotation cm fans out two ways (DESIGN.md §14.3), both derived
		// from this one cm so cluster nodes deliver identically:
		//   - SUBSCRIBE: a MESSAGE with action summary (4) per annotation,
		//     carrying the target's unchanged serial and the post-fold
		//     snapshot the backend stamped on the annotation — this is how a
		//     subscriber that knows nothing about annotations sees reactions
		//     accumulate;
		//   - ANNOTATION_SUBSCRIBE: the raw ANNOTATION frame, as today.
		// A dual-mode attachment gets both.
		if a.hasMode(protocol.FlagSubscribe) {
			for _, an := range cm.Annotations {
				if !a.send(&protocol.ProtocolMessage{
					Action:        protocol.ActionMessage,
					Channel:       new(a.channelName),
					ChannelSerial: cm.ChannelSerial,
					Messages: []*protocol.Message{{
						Action:  protocol.MessageSummary,
						Serial:  an.MessageSerial,
						Summary: an.Summary,
					}},
				}) {
					return false
				}
			}
		}
		if !a.hasMode(protocol.FlagAnnotationSubscribe) {
			return true
		}
		return a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionAnnotation,
			Channel:       new(a.channelName),
			ChannelSerial: cm.ChannelSerial,
			Annotations:   cm.Annotations,
		})
	}
	return true
}

// resolveAppends adapts a message cm's payload to this subscriber's
// append delivery mode (DESIGN.md §13.3). An append is stored as a full
// action=update carrying the rolled-up aggregate plus the incremental
// delta in Alt[DeltaAppend]. A caught-up subscriber receives the delta
// (action=append); otherwise the full aggregate is delivered.
//
// Deltas are used only when NONE of the following forces a full version:
// the subscriber opted into full versions (appendMode=full); delivery is
// backlog replay (resume/rewind); or the cm carries an append for a
// message this attachment has not yet seen — the first delivery for a
// not-yet-seen message is always the full aggregate so the subscriber
// has complete state before later deltas apply. The decision is
// all-or-nothing across the cm's messages, matching the atomic frame.
// Every message's identity is then recorded as seen. The internal Alt
// carrier is stripped from any message delivered as a full version.
func (a *attachment) resolveAppends(msgs []*protocol.Message, backlog bool) []*protocol.Message {
	a.mu.Lock()
	appendModeFull := a.appendModeFull
	a.mu.Unlock()
	asDeltas := !backlog && !appendModeFull
	if asDeltas {
		for _, m := range msgs {
			if m.HasAppendDelta() && m.Serial != "" {
				if _, ok := a.seen[m.Serial]; !ok {
					asDeltas = false
					break
				}
			}
		}
	}
	for _, m := range msgs {
		if m.Serial != "" {
			a.seen[m.Serial] = struct{}{}
		}
	}

	// Fast path: nothing carries an append delta, so there is nothing to
	// strip or swap.
	transform := false
	for _, m := range msgs {
		if m.HasAppendDelta() {
			transform = true
			break
		}
	}
	if !transform {
		return msgs
	}

	out := make([]*protocol.Message, len(msgs))
	for i, m := range msgs {
		switch {
		case !m.HasAppendDelta():
			out[i] = m
		case asDeltas:
			out[i] = m.Alt[protocol.DeltaAppend]
		default:
			// Full version: hand over the aggregate without the internal
			// Alt carrier.
			clone := *m
			clone.Alt = nil
			out[i] = &clone
		}
	}
	return out
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
	//     has nothing older. Without retention (not yet implemented),
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

// resync answers a client-initiated SYNC frame (ably-js RealtimeChannel.sync,
// RTP19) by re-delivering the channel's current presence set as a SYNC frame,
// exactly as the ATTACH-time snapshot does (DESIGN.md §12.4). The client
// reconciles its local presence map against this authoritative set,
// synthesising LEAVEs for any member it holds that is absent here. Only a
// PRESENCE_SUBSCRIBE attachment receives presence, so a channel without that
// mode gets no reply. Called on the connection read goroutine; the send goes
// through the same outbound channel as the live loop, so ordering holds.
func (a *attachment) resync(ctx context.Context) {
	if !a.hasMode(protocol.FlagPresenceSubscribe) {
		return
	}
	members, asOf, err := a.channel.Members(ctx)
	if err != nil {
		a.logger.Warn("presence resync: Members failed; skipping", "err", err)
		return
	}
	// A single frame carries the whole set at our scale; the "<serial>:"
	// channelSerial (empty cursor part) marks the sync complete so the
	// client ends its sync and applies the reconciliation in one step.
	a.send(&protocol.ProtocolMessage{
		Action:        protocol.ActionSync,
		Channel:       new(a.channelName),
		ChannelSerial: asOf + ":",
		Presence:      presentSnapshot(members),
	})
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
