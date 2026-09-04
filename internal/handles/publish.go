package handles

import (
	"context"
	stderrors "errors"
	"strconv"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/wire"
)

// Publish commits a publish to this server's storage and reports the outcome
// on ackQueue, which is where the client's ACK or NACK comes from.
//
// The shared code has already decided the publish is allowed and well-formed:
// the capability, the channel mode, the message size, the clientId and the
// namespace are all checked before this is reached. What is left is storing it
// and saying what serials it was given.
func (c *liveChannel) Publish(ctx context.Context, msg *wire.ChannelMessage, ackQueue *live.Queue[channel.Response]) *errors.ErrorInfo {
	return c.publish(ctx, msg, ackQueue, nil)
}

// PublishWithOwnership is Publish for a message that changes another: it may
// only be applied if the message it targets was published by the same client.
func (c *liveChannel) PublishWithOwnership(ctx context.Context, msg *wire.ChannelMessage, ackQueue *live.Queue[channel.Response], onlyIfClientIDEquals *string) *errors.ErrorInfo {
	return c.publish(ctx, msg, ackQueue, onlyIfClientIDEquals)
}

func (c *liveChannel) publish(ctx context.Context, msg *wire.ChannelMessage, ackQueue *live.Queue[channel.Response], onlyIfClientIDEquals *string) *errors.ErrorInfo {
	stored, errInfo := c.store(ctx, msg, onlyIfClientIDEquals)
	if errInfo != nil {
		// A synchronous refusal queues nothing: the caller answers the client
		// from the returned error, so a Response as well would answer it twice.
		return errInfo
	}

	c.respond(ackQueue, channel.Response{
		Request:     msg,
		Channel:     c.ch.Name(),
		Count:       publishedCount(msg),
		Timeserials: assignedSerials(msg, stored),
	})
	return nil
}

// store routes the publish to the part of storage that takes it. Which kind it
// is comes from the message itself, the same way this server's own path
// decides it.
func (c *liveChannel) store(ctx context.Context, msg *wire.ChannelMessage, onlyIfClientIDEquals *string) (*protocol.ChannelMessage, *errors.ErrorInfo) {
	connID := msg.GetConnectionId()
	switch {
	case len(msg.GetAnnotations()) > 0:
		// An annotation names the message it is about, and storage checks that
		// message exists. Without this the publish fell through to the message
		// path carrying no messages, and was refused for having none.
		annotations := msg.GetAnnotations()
		for _, a := range annotations {
			if a.ConnectionId == "" {
				a.ConnectionId = connID
			}
		}
		stored, _, err := c.ch.PublishAnnotation(ctx, annotations, c.fold())
		if err != nil {
			return nil, storeError(err)
		}
		c.publishSummaries(ctx, annotations)
		return stored, nil

	case len(msg.GetState()) > 0:
		// A LiveObjects publish, whose operations storage applies to the
		// channel's materialised object set as it stores them (DESIGN.md
		// §15.2). The module has already validated each operation and derived
		// its object id; what is left is the serial that orders it, which is
		// storage's to mint.
		state := msg.GetState()
		for i, sm := range state {
			if sm.ConnectionId == "" {
				sm.ConnectionId = connID
			}
			// The id the publisher's ACK names the operation by, which the
			// module derives from the publish and the operation's place in it
			// when the client sent none (wire.ChannelMessage.GetStateIDs). It
			// is stamped here because storage keys idempotency off it, and an
			// unstamped operation would dedupe against nothing.
			if sm.Id == nil && msg.GetId() != "" {
				sm.Id = new(msg.GetId() + ":" + strconv.Itoa(i))
			}
		}
		stored, _, err := c.ch.PublishState(ctx, state, c.apply())
		return stored, storeError(err)

	case len(msg.GetPresence()) > 0:
		// A leave published because a connection went away outlives the
		// attachment that went with it. The shared code publishes it on that
		// attachment's context, which is already cancelled by the time this is
		// reached — so storing it on that context stored nothing, and the
		// member stayed present for good.
		if isConnectionLeave(msg.GetPresence()) {
			ctx = context.WithoutCancel(ctx)
		}
		presence, errInfo := c.expandConnectionLeaves(ctx, msg.GetPresence())
		if errInfo != nil {
			return nil, errInfo
		}
		if len(presence) == 0 {
			return nil, nil
		}
		stored, _, err := c.ch.PublishPresence(ctx, presence)
		return stored, storeError(err)

	case isMutation(msg):
		// A mutation carries exactly one message: the change to apply to the
		// one it names.
		mutation := msg.GetMessages()[0]
		if mutation.ConnectionId == "" {
			mutation.ConnectionId = connID
		}
		if onlyIfClientIDEquals != nil {
			target, err := c.ch.LatestVersion(ctx, mutation.Serial)
			if err != nil {
				return nil, storeError(err)
			}
			if target.GetClientId() != *onlyIfClientIDEquals {
				return nil, errors.New(40160, 401, "a message may only be changed by the client that published it")
			}
		}
		stored, _, err := c.ch.Mutate(ctx, mutation, c.merge())
		return stored, storeError(err)

	default:
		messages := msg.GetMessages()
		for _, m := range messages {
			if m.ConnectionId == "" {
				m.ConnectionId = connID
			}
		}
		stored, _, err := c.ch.Publish(ctx, messages)
		return stored, storeError(err)
	}
}

// publishSummaries tells subscribers what the annotated messages now say about
// their annotations (DESIGN.md §14.2).
//
// The fold has already run, so each target's summary is current where it lives:
// on the message. What is left is to say so on the channel, which is how a
// subscriber hears about an annotation it is not itself subscribed to — a
// summary is a message, delivered as one.
//
// A failure here is not the publisher's problem: its annotation is stored and
// acknowledged, and the summary is a consequence of it rather than part of it.
// So it is logged and the publish still succeeds, and the next annotation on
// the same message carries the fold forward anyway.
func (c *liveChannel) publishSummaries(ctx context.Context, annotations []*wire.Annotation) {
	targets := make([]string, 0, len(annotations))
	seen := make(map[string]bool, len(annotations))
	for _, a := range annotations {
		if t := a.MessageSerial; t != "" && !seen[t] {
			seen[t] = true
			targets = append(targets, t)
		}
	}

	summaries := make([]*wire.Message, 0, len(targets))
	for _, target := range targets {
		latest, err := c.ch.LatestVersion(ctx, target)
		if err != nil {
			c.log.ClientErr("unable to read the annotated message to summarise it", "serial", target, "error", err)
			continue
		}
		// A message that is currently deleted is not published about: its
		// annotations are still folded and kept, because it may be undeleted.
		if latest.Action == wire.MessageAction_MESSAGE_DELETE {
			continue
		}
		summary := latest.Clone()
		summary.Action = wire.MessageAction_MESSAGE_SUMMARY
		// The summary is not a publish of its own, so it carries no publish id
		// for a client to key idempotency off.
		summary.Id = nil
		summaries = append(summaries, summary)
	}
	if len(summaries) == 0 {
		return
	}
	if _, err := c.ch.PublishSummaries(ctx, summaries); err != nil {
		c.log.Error("unable to publish message summaries", "error", err)
	}
}

// merge and fold are what an edit and an annotation mean, which the shared
// module decides and storage runs (see core.MergeVersion).
//
// The size limit handed to the merge is nothing, because this server limits
// nothing: it reports no maximum message size (app.NopLimits), so an append's
// aggregate is bounded no more tightly than an ordinary publish is.
func (c *liveChannel) merge() storage.MergeFunc { return core.MergeVersion(0, c.log) }

func (c *liveChannel) fold() storage.FoldFunc { return core.FoldSummary(c.log) }

// apply is what an object operation means, which is the same arrangement:
// decided by the shared module, run by storage with the objects loaded and the
// write not yet made (see core.ApplyOperations).
func (c *liveChannel) apply() storage.ApplyFunc { return core.ApplyOperations(c.log) }

// isConnectionLeave reports whether a presence publish is a connection saying
// it has gone: a LEAVE naming the connection and no client (DESIGN.md §12.5).
func isConnectionLeave(presence []*wire.PresenceMessage) bool {
	for _, p := range presence {
		if p.Action == wire.PresenceMessage_LEAVE && p.GetClientId() == "" {
			return true
		}
	}
	return false
}

// expandConnectionLeaves rewrites a leave for a whole connection into one per
// member that connection has present.
//
// The protocol says a member leaves by connection when its connection goes:
// one LEAVE naming the connection and no client. This server keys its presence
// set by connection and client together, so a leave naming no client matches
// nobody and would silently leave every one of that connection's members
// present forever. Which members those are is only knowable here, so the
// widening belongs at this boundary rather than in each storage backend.
func (c *liveChannel) expandConnectionLeaves(ctx context.Context, presence []*wire.PresenceMessage) ([]*wire.PresenceMessage, *errors.ErrorInfo) {
	var expanded []*wire.PresenceMessage
	for _, p := range presence {
		if p.Action != wire.PresenceMessage_LEAVE || p.GetClientId() != "" {
			expanded = append(expanded, p)
			continue
		}

		// The whole set, because a connection's members are scattered through
		// it and this is expanding a leave for all of them.
		page, err := c.ch.Members(ctx, storage.MembersQuery{})
		if err != nil {
			return nil, errors.New(50000, 500, "unable to read presence members: %s", err)
		}
		for _, m := range page.Members {
			if m.ConnectionId != p.ConnectionId {
				continue
			}
			leave := p.Clone()
			leave.ClientId = m.ClientId
			expanded = append(expanded, leave)
		}
	}
	return expanded, nil
}

// isMutation reports whether this publish changes an existing message rather
// than creating one.
func isMutation(msg *wire.ChannelMessage) bool {
	messages := msg.GetMessages()
	if len(messages) != 1 {
		return false
	}
	switch messages[0].Action {
	case wire.MessageAction_MESSAGE_UPDATE, wire.MessageAction_MESSAGE_DELETE, wire.MessageAction_MESSAGE_APPEND:
		return true
	default:
		return false
	}
}

// assignedSerials pairs each message with the serial storage gave it, keyed
// the way the shared code looks them up: by the id the client sent, or by the
// publish's own id and the message's place in it where the client sent none.
//
// The serial a publish is acknowledged with is the version's, not the
// message's. For a create the two are the same, so the distinction only shows
// on an edit — where the message keeps the identity it already had and what is
// new, and what the client is waiting to be told, is the version. Returning
// the identity told an editing client the serial it had just sent.
func assignedSerials(msg *wire.ChannelMessage, stored *protocol.ChannelMessage) map[string]*wire.Timeserial {
	if stored == nil {
		return nil
	}
	if len(stored.State) > 0 {
		return assignedStateSerials(msg, stored)
	}
	if len(stored.Annotations) > 0 {
		return assignedAnnotationSerials(msg, stored)
	}
	ids := msg.GetIDs()
	serials := make(map[string]*wire.Timeserial, len(ids))
	for i, id := range ids {
		if i >= len(stored.Messages) {
			break
		}
		if ts, err := wire.TimeserialFromString(stored.Messages[i].VersionOrSerial()); err == nil {
			serials[id] = ts
		}
	}
	return serials
}

// publishedCount is how many things the publish acknowledged, which is what
// the connection records against it. A LiveObjects publish counts its
// operations, a data publish its messages.
func publishedCount(msg *wire.ChannelMessage) int {
	if n := len(msg.GetState()); n > 0 {
		return n
	}
	return len(msg.GetMessages())
}

// assignedStateSerials is assignedSerials for a LiveObjects publish: the serial
// each operation was given, keyed the way the shared code looks them up.
//
// A state message's serial is a timeserial in its own right rather than the
// `<channelSerial>:<idx>` string a message carries, so it is read straight off
// the stored operation rather than parsed back out of one.
func assignedStateSerials(msg *wire.ChannelMessage, stored *protocol.ChannelMessage) map[string]*wire.Timeserial {
	ids := msg.GetStateIDs()
	serials := make(map[string]*wire.Timeserial, len(ids))
	for i, id := range ids {
		if i >= len(stored.State) {
			break
		}
		if ts := stored.State[i].GetSerial(); ts != nil {
			serials[id] = ts
		}
	}
	return serials
}

// assignedAnnotationSerials is assignedSerials for an annotation publish: the
// serial each annotation was given, keyed the way the shared code looks them
// up.
//
// An annotation's serial is a timeserial in its own right, the same as a state
// operation's, so it is read straight off the stored annotation.
func assignedAnnotationSerials(msg *wire.ChannelMessage, stored *protocol.ChannelMessage) map[string]*wire.Timeserial {
	ids := msg.GetAnnotationIDs()
	serials := make(map[string]*wire.Timeserial, len(ids))
	for i, id := range ids {
		if i >= len(stored.Annotations) {
			break
		}
		if ts := stored.Annotations[i].GetSerial(); ts != nil {
			serials[id] = ts
		}
	}
	return serials
}

// respond queues the outcome, if anyone is waiting for one. A publish made on
// the server's own behalf has no client to ACK.
func (c *liveChannel) respond(ackQueue *live.Queue[channel.Response], resp channel.Response) {
	if ackQueue == nil {
		return
	}
	ackQueue.Push(resp)
}

// storeError is a storage failure as the client is told about it. A publish
// naming a message that is not there is the client's mistake; anything else is
// this server's.
func storeError(err error) *errors.ErrorInfo {
	switch {
	case err == nil:
		return nil
	case isTargetNotFound(err):
		return errors.New(40142, 404, "the message this publish refers to does not exist")
	default:
		return errors.New(50000, 500, "unable to store the publish: %s", err)
	}
}

// isTargetNotFound reports whether a store refused because the message being
// changed is not there.
func isTargetNotFound(err error) bool {
	return stderrors.Is(err, storage.ErrTargetNotFound)
}
