package handles

import (
	"context"
	"maps"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/wire"
	"google.golang.org/protobuf/proto"
)

// reportInterval is how often a channel's occupancy is stored and reported
// when nothing about it changed shape — DESIGN.md §16.3.
//
// It is the roll-up: a busy channel's occupancy changes with every attach and
// detach, and a subscriber watching it wants to know roughly how many are
// there, not to be woken by each arrival. Five seconds is what Ably reports
// at, and it is short enough that a client watching a channel fill up sees it
// fill up.
//
// A package var rather than a const only so tests can shrink it; in production
// it is effectively constant.
var reportInterval = 5 * time.Second

// occupancyReporter is one channel's occupancy: what this node contributes to
// it, what the whole server makes of it, and when anyone is told.
//
// The division of labour is the module's. It reports each attachment's
// contribution as it arrives, changes and goes, and trusts this server with
// everything after: how contributions are aggregated across nodes, and when
// subscribers hear about it. What is here is that trust discharged.
//
// Counting is the module's own OccupancyCounts, which is not merely a counter
// — it reports which modes went from unoccupied to occupied, or the reverse.
// That distinction is the whole of the reporting policy (§16.3): a change of
// the channel's aggregate mode is reported at once, and anything else waits
// for the next tick.
type occupancyReporter struct {
	ch  *core.Channel
	log logging.Logger

	// counts is this node's contribution: the holders it serves and their
	// modes. It is the module's type, so what counts as a publisher here is
	// what counts as one everywhere the protocol is spoken.
	counts channel.OccupancyCounts

	// wake carries a demand for an immediate store, from a change that
	// altered the channel's aggregate mode. It is buffered by one and sent to
	// without blocking: two mode changes arriving before the reporter wakes
	// need one store between them, not two, because a store sends the whole
	// contribution rather than a delta.
	wake chan struct{}

	// pending is set by a change that did not alter the aggregate mode, and
	// cleared by the store that carries it. It is what the tick looks at, so
	// that a channel nothing is happening on does no work at all.
	pending atomic.Bool

	mu sync.Mutex
	// aggregate is the whole server's occupancy as last read from storage,
	// nil until the first read. GetAggregateOccupancy answers from it rather
	// than reading through, because it is consulted on every publish.
	aggregate *wire.ChannelOccupancy
	// stored is the contribution this node last wrote, so Stop can tell
	// whether it has anything to withdraw.
	stored *wire.ChannelOccupancy
	// inband holds one live value per occupancy category anyone has asked
	// for. They are made on demand because there are nine and an attachment
	// watches one: a channel nobody watches occupancy on holds none.
	inband map[channel.OccupancyParam]*live.Value[*channel.CachedEncodingChannelMessage]
}

func newOccupancyReporter(ch *core.Channel, log logging.Logger) *occupancyReporter {
	return &occupancyReporter{
		ch:     ch,
		log:    log,
		wake:   make(chan struct{}, 1),
		inband: make(map[channel.OccupancyParam]*live.Value[*channel.CachedEncodingChannelMessage]),
	}
}

// add, remove and update are the three reports the module makes. Each returns
// whether the channel's aggregate mode changed, which is what decides between
// storing now and storing on the next tick.
func (o *occupancyReporter) add(mode channel.Mode) {
	o.changed(o.counts.Add(mode) != 0)
}

func (o *occupancyReporter) remove(mode channel.Mode) {
	o.changed(o.counts.Remove(mode) != 0)
}

func (o *occupancyReporter) update(oldMode, newMode channel.Mode) {
	added, removed := o.counts.Update(oldMode, newMode)
	o.changed(added|removed != 0)
}

// changed records that the contribution moved, and demands an immediate store
// if the move altered which modes the channel is occupied in.
//
// It never blocks and never stores inline. A store is a write to storage —
// across the network in cluster mode — and the caller is an attachment
// attaching, detaching or being re-authorised. Making a client wait on the
// bookkeeping about it would be the wrong trade in every case.
func (o *occupancyReporter) changed(modeChanged bool) {
	if !modeChanged {
		o.pending.Store(true)
		return
	}
	select {
	case o.wake <- struct{}{}:
	default:
		// A store is already demanded and has not happened yet. It will carry
		// this change too: what is stored is the contribution as it stands
		// when the store runs, not as it stood when the demand was made.
	}
}

// run stores this node's contribution and reports the aggregate for as long as
// the channel is held.
//
// The two halves are separate on purpose. Storing is this node saying what it
// serves; reporting is every node reacting to what storage now says the whole
// server serves — including this one, which hears its own store back rather
// than short-cutting to its own numbers. That is the same single delivery path
// a publish takes (§7.2), and it is what makes the cluster case work without a
// second mechanism: a node learns about another node's attachments exactly the
// way it learns about its own.
//
// The roll-up bounds this node's stores, and so its share of the reports —
// not the total. A channel held by several nodes has several tickers, and a
// watcher can be woken once per node per interval (DESIGN.md §16.3). What the
// interval does bound everywhere is staleness: a change is visible within one
// interval however many nodes there are.
func (o *occupancyReporter) run(ctx context.Context) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	updates := o.ch.OccupancyUpdates().State()

	for {
		select {
		case <-ctx.Done():
			return

		case <-o.wake:
			// A mode change: the channel became occupied in a way it was not,
			// or stopped being. Store it now.
			o.store(ctx)

		case <-ticker.C:
			// Everything else, rolled up: a store only if something moved
			// since the last one.
			if o.pending.Swap(false) {
				o.store(ctx)
			}

		case <-updates.Notify:
			updates = updates.Next
			o.refresh(ctx)
		}
	}
}

// store writes this node's contribution and clears the pending flag, so that a
// change arriving during the write is not lost — it sets the flag again and
// the next tick carries it.
func (o *occupancyReporter) store(ctx context.Context) {
	o.pending.Store(false)

	counts := o.counts.Aggregate()
	if err := o.ch.StoreOccupancy(ctx, counts); err != nil {
		if ctx.Err() == nil {
			o.log.Warn("unable to store the channel's occupancy", "error", err)
		}
		// Nothing is retried and nothing is lost: the next store sends the
		// whole contribution, so one that failed is repaired by the one after
		// it rather than replayed.
		o.pending.Store(true)
		return
	}

	o.mu.Lock()
	o.stored = counts
	o.mu.Unlock()
}

// refresh reads what the whole server now makes of the channel and tells
// whoever is watching, if it is news.
//
// An unchanged aggregate is not reported. Storage signals whenever a
// contribution is written, without diffing it, so a node re-storing what it
// already stored — or bumping a lease — would otherwise wake every attachment
// watching occupancy to tell it nothing.
func (o *occupancyReporter) refresh(ctx context.Context) {
	agg, err := o.ch.Occupancy(ctx)
	if err != nil {
		if ctx.Err() == nil {
			o.log.Warn("unable to read the channel's occupancy", "error", err)
		}
		return
	}

	o.mu.Lock()
	if proto.Equal(o.aggregate, agg) {
		o.mu.Unlock()
		return
	}
	o.aggregate = agg
	// Copied under the lock and published outside it: rendering nine
	// categories is work that does not need the lock, and a watcher woken by
	// one of them may come straight back in for the aggregate.
	watched := maps.Clone(o.inband)
	o.mu.Unlock()

	for param, value := range watched {
		value.Set(channel.MakeInbandOccupancyMessage(param, agg))
	}
}

// withdraw takes this node's contribution out of the aggregate, for a channel
// being collected.
//
// It is nearly always a no-op: the last holder leaving takes every mode it had
// with it, which is a mode change, which stores an empty contribution at once
// — and a channel is only collected once nothing holds it. What it covers is
// the case where that store failed, where the lease would otherwise leave this
// node counted until it lapsed.
func (o *occupancyReporter) withdraw() {
	o.mu.Lock()
	stored := o.stored
	o.mu.Unlock()
	if storage.OccupancyIsEmpty(stored) {
		return
	}

	// On a context of its own: the channel's is already cancelled by the time
	// this runs, and a withdrawal made on it would store nothing — the same
	// mistake a connection's last presence leave made (§12.5).
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), reportInterval)
	defer cancel()
	if err := o.ch.StoreOccupancy(ctx, nil); err != nil {
		o.log.Warn("unable to withdraw the channel's occupancy", "error", err)
	}
}

// aggregateOccupancy is the whole server's occupancy as last read, or nil
// before the first read has landed. Nil means "not settled yet" rather than
// "empty", which is what a caller pricing a refused publish against it needs
// to be able to tell.
func (o *occupancyReporter) aggregateOccupancy() *wire.ChannelOccupancy {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.aggregate
}

// inbandValue is the live value for one occupancy category, made on first ask
// and kept for the channel's life.
//
// A fresh value is seeded with the occupancy as it stands, so an attachment
// that subscribes is told it immediately rather than waiting for the next
// change — which on a quiet channel could be never.
//
// The first watcher on a channel is a case of its own. It is subscribing
// during its own attach, so its contribution has been counted but the store
// that would publish it has not round-tripped yet, and the aggregate is still
// unsettled. Seeding from that would tell a client the channel it is attached
// to holds nobody, and correct it a moment later. So an unsettled aggregate
// falls back to this node's own counts, which is the best answer available:
// exactly right where this is the only node, and a lower bound elsewhere that
// the first refresh raises.
func (o *occupancyReporter) inbandValue(param channel.OccupancyParam) *live.Value[*channel.CachedEncodingChannelMessage] {
	o.mu.Lock()
	defer o.mu.Unlock()

	seed := o.aggregate
	if seed == nil {
		seed = o.counts.Aggregate()
	}
	value := o.inband[param]
	if value == nil {
		value = channel.NewInbandOccupancyLiveValue(param, seed)
		o.inband[param] = value
	}
	return value
}

// inspect renders this node's contribution for an operator. It is the
// contribution rather than the aggregate because what an operator asks a
// particular server is what that server is doing; the aggregate is the same
// answer from every node.
func (o *occupancyReporter) inspect(params url.Values) any {
	return o.counts.Inspect(params)
}
