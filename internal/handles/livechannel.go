package handles

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/wire"
)

// trimInterval is how often a channel's cache is brought back within its
// bound.
const trimInterval = 30 * time.Second

// liveChannel is one of this server's channels as the protocol code holds it.
//
// This is the seam an attachment publishes through and reads from. Reading is
// the module's: the cache it made for this channel holds the recent messages
// and decides where a stream attaches within them, the presence map holds the
// set a sync is served from, and both fill themselves through the reads
// declared at the bottom of this file. What is left here is answerable
// straight off core.Channel, apart from a handful of methods that presume the
// channel is somewhere else. A realtime channel is a remote handle over an RPC
// connection to a coordinator, so it can be unready, error, be persistently
// disconnected, and be subscribed-to or not. A channel that is local storage
// is none of those things, and answers them by saying so.
type liveChannel struct {
	ch        *core.Channel
	cache     *channel.Cache
	namespace *live.Value[*wire.Namespace]
	leaves    *delayedLeaves
	log       logging.Logger

	// objectsUpdated is fired each time a LiveObjects publish reaches the
	// channel, carrying whether it changed anything. An attachment watching
	// objects inband reads the set again when it does.
	objectsUpdated *live.Value[bool]

	// occupancy counts who is attached here and reports what the whole
	// server makes of it (occupancy.go).
	occupancy *occupancyReporter

	// ctx bounds this channel's own work — tailing itself into its cache, and
	// keeping that cache within its bound — and is cancelled when the manager
	// collects the channel or the server stops.
	ctx  context.Context
	stop context.CancelFunc

	// startOnce keeps Start to the one call the manager makes, and startErr
	// holds what it answered: a channel started twice would tail itself twice,
	// and one asked twice must not be told it started when it did not.
	startOnce sync.Once
	startErr  error
}

var _ channel.Channel = (*liveChannel)(nil)

func newLiveChannel(parent context.Context, ch *core.Channel, cache *channel.Cache, namespace *live.Value[*wire.Namespace], log logging.Logger) *liveChannel {
	ctx, stop := context.WithCancel(parent)
	return &liveChannel{
		ch:             ch,
		cache:          cache,
		namespace:      namespace,
		log:            log,
		objectsUpdated: live.NewValue(false),
		occupancy:      newOccupancyReporter(ch, log),
		ctx:            ctx,
		stop:           stop,
	}
}

// Start puts the channel behind its cache: it attaches to the stored channel,
// seeds the cache at the position that attach saw, and leaves the channel
// tailing itself for as long as the manager keeps it.
//
// Everything published after the seed arrives over the stream; everything
// before it comes from storage when an attachment reaches back for it. The
// manager calls this after it has initialised the cache, so both halves are
// whole before anything reads either.
func (c *liveChannel) Start(ctx context.Context) error {
	c.startOnce.Do(func() {
		var stream *core.Stream
		if stream, c.startErr = c.ch.Attach(ctx); c.startErr != nil {
			return
		}
		serial, _ := wire.TimeserialFromString(stream.ChannelSerial())
		c.cache.MessageCache().SetEpoch(serial, serial.GetSeriesId(), channel.SubscribingInSync)

		go c.tail(stream)
		go c.trim()
		go c.occupancy.run(c.ctx)
	})
	return c.startErr
}

// Stop ends the channel's own work, which is what the manager collecting it
// means: nothing has referenced it for long enough that keeping it tailing
// itself is waste. Its messages, its presence set and its objects are in
// storage, so a later attach builds it again from there.
//
// Its occupancy is not, and must not be left behind: a contribution this node
// no longer serves would keep the channel looking occupied until the lease
// lapsed (DESIGN.md §16.2).
func (c *liveChannel) Stop(context.Context) error {
	c.occupancy.withdraw()
	c.stop()
	return nil
}

// IsStopped reports a channel that can no longer be served, which the manager
// replaces rather than hands out.
func (c *liveChannel) IsStopped() bool { return c.ctx.Err() != nil }

// tail feeds the channel's cache everything published to it, for as long as
// the channel is held.
func (c *liveChannel) tail(stream *core.Stream) {
	for {
		cm, err := stream.Next(c.ctx)
		if err != nil {
			// The only error is the context ending, which is the channel being
			// collected or the server shutting down.
			return
		}
		wireCM := &channel.CachedEncodingChannelMessage{ChannelMessage: channelMessage(cm)}
		if len(cm.State) > 0 {
			// Before the message cache, because the state cache rewrites the
			// operations a pre-v6 client cannot read as ones it can, and the
			// message cache is what hands them on to be delivered.
			updated, _ := c.cache.StateCache().Add(wireCM)
			c.objectsUpdated.Set(updated)
		}
		c.cache.MessageCache().Add(wireCM)
		if len(cm.Presence) > 0 {
			c.cache.PresenceMap().PutChannelMessage(wireCM)
		}
	}
}

// trim keeps the channel's cache within its bound, so that a channel published
// to forever does not hold every message it ever carried.
//
// The bound is how far back a client may attach: past that the cache is
// holding messages no attachment can ask it for. Nothing is dropped for age,
// which is what the zero time means here — how long this server keeps a
// message is the retention question DESIGN.md §6 leaves open, and the cache
// must not answer it by accident. A message trimmed out is still in storage
// and still reachable; the cache fetches it back if an attachment reaches
// that far.
func (c *liveChannel) trim() {
	ticker := time.NewTicker(trimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.cache.MessageCache().Trim(0)
		}
	}
}

// ChannelSerial is the channel's current position, which a client is given so
// it can resume from it. The cache tracks it as it tails the channel.
func (c *liveChannel) ChannelSerial() *wire.Timeserial {
	return c.cache.MessageCache().ChannelSerial()
}

// Namespace is watched rather than read once, because on a server whose apps
// are configured live a namespace change has to reach an attached channel.
// This server reads its config at startup, so the value never changes.
func (c *liveChannel) Namespace(context.Context) (*live.Value[*wire.Namespace], *errors.ErrorInfo) {
	return c.namespace, nil
}

// Publishing is in publish.go.

// GetRESTPresence is one page of the channel's presence set, for a REST read.
// It is served from the presence map, which is where the live set is: this
// server has no cheaper authoritative store to ask instead, since the map is
// kept in sync with the same storage a re-read would go to.
func (c *liveChannel) GetRESTPresence(ctx context.Context, params channel.PresenceParams) (*channel.RESTPresenceResult, *errors.ErrorInfo) {
	return c.cache.PresenceMap().GetRESTPresence(ctx, params)
}

// MayHavePresence is always true here, because this server has no cheaper
// answer than the sync itself: its presence set is a map lookup away, so
// deciding whether to look costs what looking costs.
//
// Saying true costs a client attaching to an empty channel one empty sync.
// Saying false while someone is present would tell that client the channel is
// empty and never correct it — which is what happened while the protocol code
// read this server's absent occupancy as absent presence.
func (c *liveChannel) MayHavePresence() bool { return true }

// Occupancy. Each attachment reports its contribution as it arrives, changes
// and goes; what this server does with them is in occupancy.go — count them,
// store this node's share, and report what the whole server makes of it
// (DESIGN.md §16).
//
// AddOccupancy returns a real handle, which is what its holder calls Update
// and Remove on as its mode changes and ends; both land back here.
func (c *liveChannel) AddOccupancy(mode channel.Mode) *channel.Occupancy {
	c.occupancy.add(mode)
	return channel.NewOccupancy(c, mode)
}

func (c *liveChannel) RemoveOccupancy(mode channel.Mode) { c.occupancy.remove(mode) }

func (c *liveChannel) UpdateOccupancy(oldMode, newMode channel.Mode) {
	c.occupancy.update(oldMode, newMode)
}

// GetAggregateOccupancy is the whole server's occupancy of this channel, or
// nil while it has yet to settle. It is read on every publish, to price what
// an app-wide limit refused, so it answers from the last value read rather
// than reading through to storage.
func (c *liveChannel) GetAggregateOccupancy() *wire.ChannelOccupancy {
	return c.occupancy.aggregateOccupancy()
}

// InbandOccupancy is the live value an attachment watching one occupancy
// category is served from. The channel updates it as the aggregate moves; the
// attachment writes whatever it sees to its client.
func (c *liveChannel) InbandOccupancy(param channel.OccupancyParam) *live.Value[*channel.CachedEncodingChannelMessage] {
	return c.occupancy.inbandValue(param)
}

// InbandObjectsUpdates fires when the channel's LiveObjects change, so that an
// attachment subscribed to them inband is told to re-read the set.
//
// It carries whether the change altered anything, which is what its holder
// acts on: an operation the objects had already seen is stored and delivered
// like any other, but there is nothing new for a reader to fetch.
func (c *liveChannel) InbandObjectsUpdates() *live.Value[bool] { return c.objectsUpdated }

// A presence leave held back so that a client reconnecting inside its
// remainPresentFor window never appears to have left. The ref is held for as
// long as the leave is, because a channel collected out from under an
// outstanding leave has nothing left to publish it through.
func (c *liveChannel) RegisterDelayedLeave(leave *wire.ChannelMessage, connID string, delay time.Duration, ref channel.Ref) {
	c.leaves.hold(leave, connID, delay, ref)
}

func (c *liveChannel) RemoveDelayedLeave(connID string) {
	c.leaves.cancel(connID)
}

// Whether the channel can be served at all. A realtime channel is a remote
// handle that can lag, retry and give up; this one is a map entry, so it is
// ready as soon as it exists and cannot fail afterwards.
func (c *liveChannel) IsReady() bool                                          { return true }
func (c *liveChannel) WaitReady(context.Context, time.Time) *errors.ErrorInfo { return nil }
func (c *liveChannel) Error() *live.Value[*errors.ErrorInfo]                  { return neverErrored }
func (c *liveChannel) PersistentlyDisconnectedError() *live.Value[*errors.ErrorInfo] {
	return neverErrored
}

// IsSubscribing asks whether this server receives the channel's messages at
// all, as opposed to only publishing to it. It always does.
func (c *liveChannel) IsSubscribing() bool { return true }

// ChannelKey is how this server names the channel to itself.
func (c *liveChannel) ChannelKey() string { return c.ch.Name() }

// InspectOccupancy renders occupancy for an operator, in whatever shape that
// operator's tooling reads.
func (c *liveChannel) InspectOccupancy(values url.Values) any {
	return c.occupancy.inspect(values)
}

// The reads the channel's cache fills itself from. HistorySlices, the third of
// them, is in historyslices.go.

// PresencePage is where the presence map gets the channel's set: whoever
// storage says is present.
//
// It is one page, because that is what storage answers with. Paging the set
// for a client, deciding which members it may see, and reconciling what
// arrives live against what was read are the map's — which matters most where
// the two can disagree, on a cluster whose live presence arrives over a
// different connection from the read.
func (c *liveChannel) PresencePage(ctx context.Context, params channel.PresenceParams) ([]*wire.PresenceMessage, string, *errors.ErrorInfo) {
	page, err := c.ch.Members(ctx, storage.MembersQuery{After: params.Cursor, Limit: params.Limit})
	if err != nil {
		return nil, "", errors.New(50000, 500, "unable to read presence members: %s", err)
	}
	return page.Members, page.NextCursor, nil
}

// StatePage is where the state cache gets the channel's LiveObjects: the
// objects storage materialised as it stored the operations that made them
// (DESIGN.md §15.3).
//
// It is one page, because that is what storage answers with, ordered by object
// id — the order the cache's own paging walks the set in, so the cursor it
// hands back is one it can hand on to a client resuming a sync.
//
// afterTimeserial is the attach point the sync has to be consistent with, and
// is not consulted, because on this server it cannot fail to be. The set is
// updated in the same write as the operation that changed it, and the serial a
// caller can be holding is one that write has already committed — in
// single-process modes because the cm reaches the live list only after the
// store returns, and in cluster mode because the NOTIFY that carries it is
// sent inside the same transaction. So the set is never behind the serial;
// at worst the caller's serial is behind the set, which is what a sync
// consistent with a later point means anyway.
func (c *liveChannel) StatePage(ctx context.Context, params channel.StateParams, _ *wire.Timeserial) ([]*wire.StateMessage, string, *errors.ErrorInfo) {
	page, err := c.ch.Objects(ctx, storage.ObjectsQuery{After: params.Cursor, Limit: params.Limit})
	if err != nil {
		return nil, "", errors.New(50000, 500, "unable to read the channel's objects: %s", err)
	}
	msgs := make([]*wire.StateMessage, 0, len(page.Objects))
	for _, object := range page.Objects {
		// The cache reads nothing off a synced message but its Object, so
		// nothing else is put on one.
		msgs = append(msgs, &wire.StateMessage{Object: object})
	}
	return msgs, page.NextCursor, nil
}

var neverErrored = live.NewValue[*errors.ErrorInfo](nil)
