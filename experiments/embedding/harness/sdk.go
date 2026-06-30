package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ably/ably-go/ably"
)

// runNonce makes channel names unique per harness invocation so that
// repeated runs against a long-lived server (e.g. the Go in-process
// mount) never observe a previous run's history.
var runNonce = strconv.FormatInt(time.Now().UnixNano(), 36)

func chanName(suffix string) string {
	return fmt.Sprintf("poc-%s-%s", runNonce, suffix)
}

// newRealtime builds an unmodified ably-go realtime client pointed at the
// endpoint by host+port — exactly the wiring a host app would use
// (EMBEDDING-POC.md §6), with TLS off for the local PoC.
func newRealtime(c config) (*ably.Realtime, error) {
	return ably.NewRealtime(
		ably.WithKey(c.key),
		ably.WithEndpoint(c.host),
		ably.WithPort(c.port),
		ably.WithTLS(false),
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseTokenAuth(false),
		ably.WithAutoConnect(false),
		ably.WithUseBinaryProtocol(c.binary),
		ably.WithLogLevel(ably.LogNone),
	)
}

// newREST builds an unmodified ably-go REST client at the same endpoint.
func newREST(c config) (*ably.REST, error) {
	return ably.NewREST(
		ably.WithKey(c.key),
		ably.WithEndpoint(c.host),
		ably.WithPort(c.port),
		ably.WithTLS(false),
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseTokenAuth(false),
		ably.WithUseBinaryProtocol(c.binary),
	)
}

// connectRealtime drives the client to CONNECTED, failing on FAILED or
// timeout.
func connectRealtime(ctx context.Context, client *ably.Realtime) error {
	connected := make(chan struct{}, 1)
	failed := make(chan error, 1)
	client.Connection.Once(ably.ConnectionEventConnected, func(ably.ConnectionStateChange) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	client.Connection.Once(ably.ConnectionEventFailed, func(change ably.ConnectionStateChange) {
		select {
		case failed <- fmt.Errorf("connection FAILED: %v", change.Reason):
		default:
		}
	})
	client.Connect()
	select {
	case <-connected:
		return nil
	case err := <-failed:
		return err
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for CONNECTED (state=%v): %w", client.Connection.State(), ctx.Err())
	}
}

// scenarioConnect proves an unmodified SDK reaches CONNECTED through the
// host app, and records how long that took.
func scenarioConnect(ctx context.Context, c config) (map[string]any, error) {
	client, err := newRealtime(c)
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}
	defer client.Close()

	t0 := time.Now()
	if err := connectRealtime(ctx, client); err != nil {
		return nil, err
	}
	return map[string]any{"connectMs": round2(msFloat(time.Since(t0)))}, nil
}

// scenarioPubSub measures the realtime publish→subscribe round-trip: the
// publisher's own message must come back over the WS, sampled repeatedly
// for a latency distribution.
func scenarioPubSub(ctx context.Context, c config) (map[string]any, error) {
	client, err := newRealtime(c)
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}
	defer client.Close()
	if err := connectRealtime(ctx, client); err != nil {
		return nil, err
	}

	ch := client.Channels.Get(chanName("pubsub"))
	if err := ch.Attach(ctx); err != nil {
		return nil, fmt.Errorf("attach: %w", err)
	}

	recv := make(chan *ably.Message, 64)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) { recv <- m })
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	defer unsub()

	samples := make([]float64, 0, c.latencySamples)
	for i := 0; i < c.latencySamples; i++ {
		data := fmt.Sprintf("rt-%d", i)
		t0 := time.Now()
		if err := ch.Publish(ctx, "rt", data); err != nil {
			return nil, fmt.Errorf("publish %d: %w", i, err)
		}
		got, err := awaitData(ctx, recv, data, t0, c.opTimeout)
		if err != nil {
			return nil, fmt.Errorf("sample %d: %w", i, err)
		}
		samples = append(samples, got)
	}
	return latencyStats(samples), nil
}

// scenarioRESTPubSub proves a REST publish is delivered to a realtime WS
// subscriber (the cross-protocol path), and measures REST→WS latency.
func scenarioRESTPubSub(ctx context.Context, c config) (map[string]any, error) {
	client, err := newRealtime(c)
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}
	defer client.Close()
	if err := connectRealtime(ctx, client); err != nil {
		return nil, err
	}

	name := chanName("restpubsub")
	ch := client.Channels.Get(name)
	if err := ch.Attach(ctx); err != nil {
		return nil, fmt.Errorf("attach: %w", err)
	}
	recv := make(chan *ably.Message, 64)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) { recv <- m })
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	defer unsub()

	const samples = 10
	lat := make([]float64, 0, samples)
	for i := 0; i < samples; i++ {
		data := fmt.Sprintf("rp-%d", i)
		t0 := time.Now()
		if err := restPublish(ctx, c, name, "rp", data); err != nil {
			return nil, fmt.Errorf("rest publish %d: %w", i, err)
		}
		got, err := awaitData(ctx, recv, data, t0, c.opTimeout)
		if err != nil {
			return nil, fmt.Errorf("sample %d: %w", i, err)
		}
		lat = append(lat, got)
	}
	return latencyStats(lat), nil
}

// scenarioHistory publishes N messages via the SDK's REST client, then
// reads them back via the SDK's history API, asserting count and order.
func scenarioHistory(ctx context.Context, c config) (map[string]any, error) {
	rest, err := newREST(c)
	if err != nil {
		return nil, fmt.Errorf("build REST client: %w", err)
	}
	name := chanName("history")
	ch := rest.Channels.Get(name)

	for i := 0; i < c.historyCount; i++ {
		if err := ch.Publish(ctx, "h", fmt.Sprintf("h-%d", i)); err != nil {
			return nil, fmt.Errorf("rest publish %d: %w", i, err)
		}
	}

	// Read forwards (oldest→newest) so the sequence matches publish order.
	items, err := ch.History(
		ably.HistoryWithDirection(ably.Forwards),
		ably.HistoryWithLimit(c.historyCount+10),
	).Items(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: %w", err)
	}
	got := make([]string, 0, c.historyCount)
	for items.Next(ctx) {
		if d, ok := items.Item().Data.(string); ok {
			got = append(got, d)
		}
	}
	if len(got) != c.historyCount {
		return nil, fmt.Errorf("history returned %d messages, want %d (%v)", len(got), c.historyCount, got)
	}
	for i := 0; i < c.historyCount; i++ {
		want := fmt.Sprintf("h-%d", i)
		if got[i] != want {
			return nil, fmt.Errorf("history order[%d] = %q, want %q (full=%v)", i, got[i], want, got)
		}
	}
	return map[string]any{"count": len(got), "ordered": true}, nil
}

// scenarioSoak publishes M messages via REST to a WS subscriber and
// asserts every one arrives exactly once (zero loss), recording
// throughput. This is the steady-state reliability signal.
func scenarioSoak(ctx context.Context, c config) (map[string]any, error) {
	client, err := newRealtime(c)
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}
	defer client.Close()
	if err := connectRealtime(ctx, client); err != nil {
		return nil, err
	}
	name := chanName("soak")
	ch := client.Channels.Get(name)
	if err := ch.Attach(ctx); err != nil {
		return nil, fmt.Errorf("attach: %w", err)
	}
	recv := make(chan *ably.Message, c.soakMessages+16)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) { recv <- m })
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	defer unsub()

	t0 := time.Now()
	for i := 0; i < c.soakMessages; i++ {
		if err := restPublish(ctx, c, name, "soak", strconv.Itoa(i)); err != nil {
			return nil, fmt.Errorf("publish %d: %w", i, err)
		}
	}
	seen := make(map[int]int, c.soakMessages)
	deadline := time.After(30 * time.Second)
	for len(seen) < c.soakMessages {
		select {
		case m := <-recv:
			if d, ok := m.Data.(string); ok {
				if n, err := strconv.Atoi(d); err == nil {
					seen[n]++
				}
			}
		case <-deadline:
			return nil, fmt.Errorf("soak: received %d/%d before deadline", len(seen), c.soakMessages)
		case <-ctx.Done():
			return nil, fmt.Errorf("soak: %w (received %d/%d)", ctx.Err(), len(seen), c.soakMessages)
		}
	}
	dur := time.Since(t0)
	dupes := 0
	for _, n := range seen {
		if n > 1 {
			dupes += n - 1
		}
	}
	if dupes > 0 {
		return nil, fmt.Errorf("soak: %d duplicate deliveries", dupes)
	}
	return map[string]any{
		"messages":         c.soakMessages,
		"lost":             0,
		"duplicates":       dupes,
		"durationMs":       round2(msFloat(dur)),
		"throughputPerSec": round2(float64(c.soakMessages) / dur.Seconds()),
	}, nil
}

// awaitData waits for a message whose string Data equals want and
// returns the round-trip latency measured from since (the pre-publish
// instant the caller captured). It skips non-matching stragglers.
func awaitData(ctx context.Context, recv <-chan *ably.Message, want string, since time.Time, timeout time.Duration) (float64, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case m := <-recv:
			if d, ok := m.Data.(string); ok && d == want {
				return msFloat(time.Since(since)), nil
			}
			// non-matching straggler; keep waiting
		case <-deadline.C:
			return 0, fmt.Errorf("timeout waiting for %q", want)
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// restPublish issues one REST publish to the endpoint exactly as the SDK
// would (POST /channels/{name}/messages, basic auth, JSON body), used by
// the cross-protocol and resume scenarios.
func restPublish(ctx context.Context, c config, channel, name, data string) error {
	u := url.URL{
		Scheme: "http",
		Host:   c.addr(),
		Path:   c.pathPrefix() + "/channels/" + url.PathEscape(channel) + "/messages",
	}
	body := fmt.Sprintf(`{"name":%q,"data":%q}`, name, data)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader([]byte(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.keyName(), c.keySecret())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("REST publish status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}
