package handles

import (
	"context"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/optional"
	"github.com/ably/server-protocol/go/pagination"
	"github.com/ably/server-protocol/go/wire"
)

// The reads a message store answers, out of this server's storage.
//
// What the client asked for and what it is told are the shared code's: it
// parses the query, decides what the client may see, and renders the paging
// links from what comes back. What this decides is only how to find it.

func (c *messageStore) History(ctx context.Context, query channel.HistoryQuery) (wire.Messages, *pagination.RelLinks[channel.HistoryQuery], *errors.ErrorInfo) {
	ch, errInfo := c.channel(ctx)
	if errInfo != nil {
		return nil, nil, errInfo
	}

	// A message history read shows each message once, as it now stands: an
	// edited message keeps its place in the timeline but reads with its
	// current content, and a deleted one reads as a tombstone (DESIGN.md
	// §13.4). The uncollapsed stream — every version in the order it was
	// published — is what a resuming or rewinding attachment reads, and that
	// goes through the cache rather than here.
	q := storageQuery(query, storage.KindMessage)
	q.Collapse = true
	page, err := ch.History(ctx, q)
	if err != nil {
		return nil, nil, errors.New(50000, 500, "unable to read history: %s", err)
	}

	var messages wire.Messages
	for _, cm := range page.ChannelMessages {
		messages = append(messages, cm.Messages...)
	}
	return messages, historyLinks(query, page, len(messages)), nil
}

func (c *messageStore) PresenceHistory(ctx context.Context, query channel.HistoryQuery) ([]*wire.PresenceMessage, *pagination.RelLinks[channel.HistoryQuery], *errors.ErrorInfo) {
	ch, errInfo := c.channel(ctx)
	if errInfo != nil {
		return nil, nil, errInfo
	}

	page, err := ch.History(ctx, storageQuery(query, storage.KindPresence))
	if err != nil {
		return nil, nil, errors.New(50000, 500, "unable to read presence history: %s", err)
	}

	var members []*wire.PresenceMessage
	for _, cm := range page.ChannelMessages {
		members = append(members, cm.Presence...)
	}
	return members, historyLinks(query, page, len(members)), nil
}

// LoadMessageLatest is the current version of one message, which this server
// keeps as a projection so there is nothing to fold here.
func (c *messageStore) LoadMessageLatest(ctx context.Context, serial *wire.Timeserial) (*wire.Message, *errors.ErrorInfo) {
	ch, errInfo := c.channel(ctx)
	if errInfo != nil {
		return nil, errInfo
	}

	latest, err := ch.LatestVersion(ctx, serial.ToTimeserialString())
	if err != nil {
		return nil, readError(err, "message")
	}
	return latest, nil
}

func (c *messageStore) LoadMessageVersions(ctx context.Context, serial *wire.Timeserial) (wire.Messages, *pagination.RelLinks[channel.HistoryQuery], *errors.ErrorInfo) {
	ch, errInfo := c.channel(ctx)
	if errInfo != nil {
		return nil, nil, errInfo
	}

	// Oldest-first: a version chain reads create-then-edits, which is the
	// order the reference returns it in and the order the SDKs present. The
	// zero query would read backwards, message history's default.
	page, err := ch.Versions(ctx, serial.ToTimeserialString(), storage.HistoryQuery{
		Direction: storage.DirectionForwards,
	})
	if err != nil {
		return nil, nil, readError(err, "message")
	}

	var versions wire.Messages
	for _, cm := range page.ChannelMessages {
		versions = append(versions, cm.Messages...)
	}
	return versions, historyLinks(channel.HistoryQuery{}, page, len(versions)), nil
}

func (c *messageStore) AnnotationHistory(ctx context.Context, query channel.HistoryQuery, serial string) ([]*wire.Annotation, *pagination.RelLinks[channel.HistoryQuery], *errors.ErrorInfo) {
	ch, errInfo := c.channel(ctx)
	if errInfo != nil {
		return nil, nil, errInfo
	}

	// Oldest-first, like a version chain and unlike message history: a
	// message's annotations are read as the sequence they arrived in, and the
	// SDKs offer no way to ask for the other order — so the newest-first
	// default a history read carries is not one a client here can have meant.
	q := storageQuery(query, storage.KindMessage)
	q.Direction = storage.DirectionForwards
	page, err := ch.Annotations(ctx, serial, q)
	if err != nil {
		return nil, nil, readError(err, "message")
	}

	var annotations []*wire.Annotation
	for _, cm := range page.ChannelMessages {
		annotations = append(annotations, cm.Annotations...)
	}
	return annotations, historyLinks(query, page, len(annotations)), nil
}

// channel is the stored channel this store reads. Reaching it costs nothing
// beyond a map lookup, and brings no channel live: what a channel is holding
// now is the message cache's, which a read of the past never touches.
func (c *messageStore) channel(ctx context.Context) (*core.Channel, *errors.ErrorInfo) {
	ch, err := c.app.manager.channels.GetChannel(ctx, c.spec.ChannelID())
	if err != nil {
		return nil, errors.New(50000, 500, "channel unavailable: %s", err)
	}
	return ch, nil
}

// storageQuery is a client's history query as this server's storage takes it.
// The bounds are on publish time either way; what differs is that the protocol
// says which direction by a flag and this server by an enum.
//
// Reversed means newest first, which is what a client gets by default and what
// direction=backwards asks for. Reading the flag the other way round returns
// every history page in the opposite order to the one asked for.
func storageQuery(query channel.HistoryQuery, kind storage.Kind) storage.HistoryQuery {
	q := storage.HistoryQuery{
		Kind:      kind,
		Direction: storage.DirectionForwards,
		Limit:     query.Limit,
	}
	if query.Reversed {
		q.Direction = storage.DirectionBackwards
	}
	if start, ok := query.Start.Get(); ok {
		q.Start = int64(start)
	}
	if end, ok := query.End.Get(); ok {
		q.End = int64(end)
	}
	if cursor, ok := query.Cursor.Get(); ok {
		// A cursor is normally a message serial — `<channelSerial>:<idx>` — and
		// storage compares it exclusively, which is what paging past a message
		// means. But the shared code also pages from an attach point, which
		// names a whole publish and carries no idx; compared as a message
		// serial that sorts before every message in the publish, so a backwards
		// page would drop the publish the client asked to see up to. A cursor
		// with no idx is therefore a bound on the publish, which storage
		// expresses at the granularity it is meant in.
		if _, _, err := serial.ParseMessageSerial(cursor); err != nil {
			if q.Direction == storage.DirectionBackwards {
				q.EndChannelSerial = cursor
			} else {
				q.AfterChannelSerial = cursor
			}
		} else {
			q.Cursor = cursor
		}
	}
	if from, ok := query.FromSerial.Get(); ok && from != nil {
		q.EndChannelSerial = from.ToTimeserialString()
	}
	return q
}

// historyLinks is where the client goes for the next page, which it is only
// given when there is one. The shared code renders these as Link headers.
func historyLinks(query channel.HistoryQuery, page storage.HistoryPage, returned int) *pagination.RelLinks[channel.HistoryQuery] {
	first := query
	first.Cursor = optional.Empty[string]()

	next := optional.Empty[channel.HistoryQuery]()
	if page.HasMore && returned > 0 {
		after := query
		after.Cursor = optional.Of(page.LastSerial)
		next = optional.Of(after)
	}
	return pagination.NewRelLinks(query, first, next)
}

// readError is a storage failure as the client is told about it: something it
// asked for that is not there is its own mistake.
func readError(err error, what string) *errors.ErrorInfo {
	if isTargetNotFound(err) {
		return errors.New(40400, 404, "the %s does not exist", what)
	}
	return errors.New(50000, 500, "unable to read the %s: %s", what, err)
}
