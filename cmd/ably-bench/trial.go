package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-go/ably"
	"golang.org/x/time/rate"
)

const msgName = "m"

type endpoint struct {
	host string
	port int
}

type trialConfig struct {
	endpoints   []endpoint
	key         string
	binary      bool
	channels    int
	publishers  int
	subscribers int
	msgSize     int

	rate     float64       // offered load, messages/sec across all publishers
	warmup   time.Duration // discarded from latency/throughput stats
	duration time.Duration // measurement window
}

type correctness struct {
	expected     int64 // messages every subscriber should have received
	received     int64
	missing      int64 // expected-received (loss), if positive
	extra        int64 // received-expected (duplication), if positive
	outOfOrder   int64 // per-publisher seqs at or below expected (reorder/dup)
	gaps         int64 // per-publisher forward skips (loss or transient)
	publishNACKs int64
}

func (c correctness) ok() bool {
	return c.missing == 0 && c.extra == 0 && c.outOfOrder == 0 && c.gaps == 0 && c.publishNACKs == 0
}

type trialResult struct {
	offered  float64
	achieved float64 // messages/sec actually published within the window
	p50      time.Duration
	p99      time.Duration
	max      time.Duration
	corr     correctness
}

// pass is the verdict the throughput search keys on: delivery was
// correct, the offered load was actually sustained (publishers kept up),
// and both latency targets were met.
func (r trialResult) pass(p50Target, p99Target time.Duration) bool {
	sustained := r.achieved >= 0.95*r.offered
	return r.corr.ok() && sustained && r.p50 <= p50Target && r.p99 <= p99Target
}

// subscriber owns one channel subscription. Its handler runs on a single
// ably-go goroutine, so its histogram and per-publisher state need no
// locking; totalRecv is atomic only so the drain loop can poll it.
type subscriber struct {
	channel   string
	hist      *histogram
	pubStates map[int]*pubSeqState
}

type publisher struct {
	client  *ably.Realtime
	ch      *ably.RealtimeChannel
	id      int
	channel string
}

// runTrial drives one fixed-rate trial end to end and returns its
// latency distribution and a correctness verdict.
func runTrial(ctx context.Context, cfg trialConfig) (trialResult, error) {
	budget := cfg.warmup + cfg.duration + 10*time.Second
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	// --- Subscribers: connect, attach, and start receiving. ---
	subs := make([]*subscriber, cfg.subscribers)
	subClients := make([]*ably.Realtime, cfg.subscribers)
	subsPerChannel := make([]int, cfg.channels)

	startNano := time.Now().UnixNano()
	measureStartNano := startNano + cfg.warmup.Nanoseconds()
	measureEndNano := measureStartNano + cfg.duration.Nanoseconds()

	var totalRecv int64
	for i := range subs {
		chName := channelName(i % cfg.channels)
		subsPerChannel[i%cfg.channels]++
		s := &subscriber{
			channel:   chName,
			hist:      newHistogram(),
			pubStates: map[int]*pubSeqState{},
		}
		subs[i] = s

		client, err := newClient(cfg, i)
		if err != nil {
			return trialResult{}, err
		}
		subClients[i] = client
		if err := connect(ctx, client); err != nil {
			return trialResult{}, fmt.Errorf("subscriber %d connect: %w", i, err)
		}
		ch := client.Channels.Get(chName)
		if err := ch.Attach(ctx); err != nil {
			return trialResult{}, fmt.Errorf("subscriber %d attach %s: %w", i, chName, err)
		}
		if _, err := ch.SubscribeAll(ctx, func(m *ably.Message) {
			data, ok := m.Data.(string)
			if !ok {
				return
			}
			pubID, seq, tNano, ok := decodePayload(data)
			if !ok {
				return
			}
			atomic.AddInt64(&totalRecv, 1)

			st := s.pubStates[pubID]
			if st == nil {
				st = &pubSeqState{}
				s.pubStates[pubID] = st
			}
			st.observe(seq)

			if tNano >= measureStartNano && tNano <= measureEndNano {
				s.hist.record(time.Duration(time.Now().UnixNano() - tNano))
			}
		}); err != nil {
			return trialResult{}, fmt.Errorf("subscriber %d subscribe %s: %w", i, chName, err)
		}
	}

	// --- Publishers: connect. ---
	pubs := make([]*publisher, cfg.publishers)
	for i := range pubs {
		chName := channelName(i % cfg.channels)
		client, err := newClient(cfg, cfg.subscribers+i)
		if err != nil {
			return trialResult{}, err
		}
		if err := connect(ctx, client); err != nil {
			return trialResult{}, fmt.Errorf("publisher %d connect: %w", i, err)
		}
		pubs[i] = &publisher{
			client:  client,
			ch:      client.Channels.Get(chName),
			id:      i,
			channel: chName,
		}
	}

	// --- Offer load. Each publisher is paced to its share of the total
	// rate; a bounded in-flight semaphore caps memory and turns a server
	// that can't keep up into observable backpressure (achieved < offered)
	// rather than unbounded growth. ---
	const maxInflight = 50_000
	inflight := make(chan struct{}, maxInflight)
	perPub := cfg.rate / float64(cfg.publishers)

	var sentInWindow int64
	var nacks int64
	publishStop := time.Unix(0, measureEndNano)

	var wg sync.WaitGroup
	for _, p := range pubs {
		wg.Add(1)
		go func(p *publisher) {
			defer wg.Done()
			lim := rate.NewLimiter(rate.Limit(perPub), 1)
			var seq int64
			for {
				if err := lim.WaitN(ctx, 1); err != nil {
					return
				}
				now := time.Now()
				if !now.Before(publishStop) {
					return
				}
				select {
				case inflight <- struct{}{}:
				case <-ctx.Done():
					return
				}
				tNano := now.UnixNano()
				if tNano >= measureStartNano {
					atomic.AddInt64(&sentInWindow, 1)
				}
				data := encodePayload(p.id, seq, tNano, cfg.msgSize)
				seq++
				if err := p.ch.PublishAsync(msgName, data, func(err error) {
					if err != nil {
						atomic.AddInt64(&nacks, 1)
					}
					<-inflight
				}); err != nil {
					atomic.AddInt64(&nacks, 1)
					<-inflight
				}
			}
		}(p)
	}
	wg.Wait()

	// Wait for outstanding acks, then for delivery to settle.
	drainInflight(ctx, inflight)
	drainDelivery(ctx, &totalRecv)

	// Quiesce subscribers before reading their state, so no handler races
	// the aggregation below.
	for _, c := range subClients {
		c.Close()
	}
	for _, p := range pubs {
		p.client.Close()
	}

	return aggregate(cfg, subs, subsPerChannel, sentInWindow, nacks, measureStartNano, measureEndNano), nil
}

// drainInflight waits until every PublishAsync has been acked (semaphore
// fully released) or the context fires.
func drainInflight(ctx context.Context, inflight chan struct{}) {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if len(inflight) == 0 {
			return
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-deadline.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// drainDelivery waits until the received count stops growing, i.e. the
// last published messages have landed.
func drainDelivery(ctx context.Context, totalRecv *int64) {
	const stableFor = 750 * time.Millisecond
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	last := atomic.LoadInt64(totalRecv)
	stableSince := time.Now()
	for {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-deadline.C:
			return
		case <-ctx.Done():
			return
		}
		now := atomic.LoadInt64(totalRecv)
		if now != last {
			last = now
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) >= stableFor {
			return
		}
	}
}

// aggregate merges the per-subscriber histograms and per-publisher
// sequence state into the trial result. Expected delivery is computed
// from each publisher's observed sent count times the number of
// subscribers on its channel.
func aggregate(cfg trialConfig, subs []*subscriber, subsPerChannel []int, sentInWindow, nacks, measureStartNano, measureEndNano int64) trialResult {
	merged := newHistogram()
	corr := correctness{publishNACKs: nacks}

	// Highest seq+1 observed for each publisher is its sent total (per
	// publisher the stream is 0..n-1); take the max across subscribers.
	sentByPub := map[int]int64{}
	for _, s := range subs {
		merged.merge(s.hist)
		for pubID, st := range s.pubStates {
			corr.received += st.recv
			corr.outOfOrder += st.backwards
			corr.gaps += st.forwardGaps
			if st.expectNext > sentByPub[pubID] {
				sentByPub[pubID] = st.expectNext
			}
		}
	}
	for pubID, sent := range sentByPub {
		corr.expected += sent * int64(subsPerChannel[pubID%cfg.channels])
	}
	if d := corr.expected - corr.received; d > 0 {
		corr.missing = d
	} else if d < 0 {
		corr.extra = -d
	}

	windowSecs := float64(measureEndNano-measureStartNano) / float64(time.Second)
	achieved := 0.0
	if windowSecs > 0 {
		achieved = float64(sentInWindow) / windowSecs
	}

	return trialResult{
		offered:  cfg.rate,
		achieved: achieved,
		p50:      merged.percentile(50),
		p99:      merged.percentile(99),
		max:      merged.max(),
		corr:     corr,
	}
}

func channelName(i int) string {
	return fmt.Sprintf("bench-%d", i)
}

// newClient builds an ably-go realtime client wired at one of the
// configured endpoints (round-robin by index), matching the basic-auth,
// no-TLS setup the integration tests use against a local server.
func newClient(cfg trialConfig, idx int) (*ably.Realtime, error) {
	ep := cfg.endpoints[idx%len(cfg.endpoints)]
	opts := []ably.ClientOption{
		ably.WithKey(cfg.key),
		ably.WithEndpoint(ep.host),
		ably.WithPort(ep.port),
		ably.WithTLS(false),
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseTokenAuth(false),
		ably.WithAutoConnect(false),
		ably.WithUseBinaryProtocol(cfg.binary),
		ably.WithLogLevel(ably.LogNone),
	}
	return ably.NewRealtime(opts...)
}

func connect(ctx context.Context, client *ably.Realtime) error {
	connected := make(chan struct{}, 1)
	off := client.Connection.Once(ably.ConnectionEventConnected, func(ably.ConnectionStateChange) {
		connected <- struct{}{}
	})
	defer off()
	client.Connect()
	select {
	case <-connected:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for CONNECTED (state %v)", client.Connection.State())
	}
}
