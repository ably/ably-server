// Command harness is the language-agnostic conformance runner for the
// embedding PoC (EMBEDDING-POC.md §8). It drives an Ably-compatible
// endpoint over host+port and asserts the four core behaviours the PoC
// must prove through whatever sits in front of the embedded server
// (nothing, a Go in-process mount, or a Node/.NET reverse proxy):
//
//   - pubsub    realtime publish→subscribe round-trip via the ably-go SDK
//   - restpubsub REST publish observed by a realtime WS subscriber
//   - history   REST publish of N messages, read back in order via the SDK
//   - resume    resume-after-drop: reconnect with a channelSerial cursor
//     and receive exactly the gap, RESUMED flag set (raw protocol, so the
//     drop is deterministic and proxy-independent)
//
// The same binary is run against every track, so its numbers are
// apples-to-apples. It prints human-readable progress to stderr and, with
// --json, a single JSON report line to stdout. Exit code is 0 iff every
// selected scenario passed.
//
// This lives inside the ably-server module so the resume scenario can
// speak the wire protocol directly via internal/protocol. It is a
// runnable binary (no _test.go), so `go test ./...` only compiles it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// config holds the parsed CLI flags shared by every scenario.
type config struct {
	host           string
	port           int
	key            string // appId.keyId:keySecret
	label          string
	scenarios      []string
	binary         bool   // SDK protocol: msgpack when true, JSON when false
	basePath       string // e.g. "/ably" when the server is mounted under a subpath
	latencySamples int
	historyCount   int
	soakMessages   int
	opTimeout      time.Duration
}

// pathPrefix returns the normalised base path ("" or "/ably"), used to
// build REST and WebSocket URLs when the server is mounted under a subpath.
func (c config) pathPrefix() string {
	p := strings.TrimRight(c.basePath, "/")
	if p != "" && !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// keyName returns the basic-auth username (appId.keyId) for REST calls.
func (c config) keyName() string {
	if i := strings.IndexByte(c.key, ':'); i >= 0 {
		return c.key[:i]
	}
	return c.key
}

// keySecret returns the basic-auth password for REST calls.
func (c config) keySecret() string {
	if i := strings.IndexByte(c.key, ':'); i >= 0 {
		return c.key[i+1:]
	}
	return ""
}

func (c config) addr() string { return fmt.Sprintf("%s:%d", c.host, c.port) }

// scenarioResult is one scenario's outcome plus its measured metrics.
type scenarioResult struct {
	Name       string         `json:"name"`
	Pass       bool           `json:"pass"`
	DurationMs float64        `json:"durationMs"`
	Detail     string         `json:"detail,omitempty"`
	Metrics    map[string]any `json:"metrics,omitempty"`
}

// report is the full run, emitted as one JSON line with --json.
type report struct {
	Label     string           `json:"label"`
	Endpoint  string           `json:"endpoint"`
	Pass      bool             `json:"pass"`
	StartedAt string           `json:"startedAt"`
	WallMs    float64          `json:"wallMs"`
	Scenarios []scenarioResult `json:"scenarios"`
}

// scenarioFunc runs one scenario and returns its measured metrics. A
// non-nil error means the scenario failed (its assertions did not hold).
type scenarioFunc func(ctx context.Context, c config) (map[string]any, error)

func main() {
	c := config{}
	var scenarios string
	flag.StringVar(&c.host, "host", "127.0.0.1", "endpoint host")
	flag.IntVar(&c.port, "port", 0, "endpoint port (required)")
	defaultKey := os.Getenv("ABLY_SERVER_API_KEY")
	if defaultKey == "" {
		defaultKey = "app.key:secret"
	}
	flag.StringVar(&c.key, "key", defaultKey, "API key appId.keyId:keySecret (env: ABLY_SERVER_API_KEY)")
	flag.StringVar(&c.label, "label", "unlabelled", "label for this run in the report")
	flag.StringVar(&scenarios, "scenarios", "all", "comma list: connect,pubsub,restpubsub,history,resume,soak,rawpubsub (or all)")
	flag.BoolVar(&c.binary, "binary", false, "use the msgpack SDK protocol instead of JSON")
	flag.StringVar(&c.basePath, "base-path", "", "subpath the server is mounted under, e.g. /ably (runs raw-protocol scenarios only; stock SDKs have no basePath option yet)")
	flag.IntVar(&c.latencySamples, "latency-samples", 20, "round-trips to sample for pubsub latency")
	flag.IntVar(&c.historyCount, "history-count", 10, "messages to publish+read back in the history scenario")
	flag.IntVar(&c.soakMessages, "soak-messages", 200, "messages for the soak scenario (0 disables)")
	flag.DurationVar(&c.opTimeout, "op-timeout", 10*time.Second, "per-operation timeout")
	asJSON := flag.Bool("json", false, "emit the JSON report to stdout")
	flag.Parse()

	if c.port == 0 {
		fmt.Fprintln(os.Stderr, "harness: --port is required")
		os.Exit(2)
	}

	// Scenario registry, run in this fixed order. rawpubsub uses only the
	// wire protocol (no ably-go SDK), so it is the one pub/sub scenario that
	// can target a subpath mount.
	registry := []struct {
		name string
		fn   scenarioFunc
		raw  bool // uses only raw protocol — safe under --base-path
	}{
		{"connect", scenarioConnect, false},
		{"pubsub", scenarioPubSub, false},
		{"restpubsub", scenarioRESTPubSub, false},
		{"history", scenarioHistory, false},
		{"rawpubsub", scenarioRawPubSub, true},
		{"resume", scenarioResume, true},
		{"soak", scenarioSoak, false},
	}

	// Stock SDKs build root-rooted URLs (no basePath option — EMBEDDING-POC
	// §6), so under --base-path only the raw-protocol scenarios can run.
	// "all" then means the raw-capable set.
	all := scenarios == "all"
	want := map[string]bool{}
	for _, s := range strings.Split(scenarios, ",") {
		want[strings.TrimSpace(s)] = true
	}
	if c.pathPrefix() != "" {
		fmt.Fprintf(os.Stderr, "  (base-path %q: running raw-protocol scenarios only; stock SDKs cannot target a subpath yet)\n", c.pathPrefix())
	}

	started := time.Now()
	rep := report{
		Label:     c.label,
		Endpoint:  c.addr(),
		StartedAt: started.Format(time.RFC3339),
		Pass:      true,
	}

	fmt.Fprintf(os.Stderr, "== harness [%s] against %s (protocol=%s) ==\n",
		c.label, c.addr(), protocolName(c.binary))

	for _, s := range registry {
		if !all && !want[s.name] {
			continue
		}
		// rawpubsub is part of the raw subpath suite, not the default SDK
		// "all" run (pubsub already covers SDK pub/sub at root).
		if all && s.name == "rawpubsub" && c.pathPrefix() == "" {
			continue
		}
		if c.pathPrefix() != "" && !s.raw {
			continue // SDK-based scenario cannot target a subpath
		}
		if s.name == "soak" && c.soakMessages <= 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		t0 := time.Now()
		metrics, err := s.fn(ctx, c)
		dur := time.Since(t0)
		cancel()

		res := scenarioResult{
			Name:       s.name,
			Pass:       err == nil,
			DurationMs: msFloat(dur),
			Metrics:    metrics,
		}
		if err != nil {
			res.Detail = err.Error()
			rep.Pass = false
			fmt.Fprintf(os.Stderr, "  FAIL %-11s %6.0fms  %v\n", s.name, res.DurationMs, err)
		} else {
			fmt.Fprintf(os.Stderr, "  ok   %-11s %6.0fms  %s\n", s.name, res.DurationMs, summarise(metrics))
		}
		rep.Scenarios = append(rep.Scenarios, res)
	}

	rep.WallMs = msFloat(time.Since(started))

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "harness: encode report: %v\n", err)
			os.Exit(3)
		}
	}

	if rep.Pass {
		fmt.Fprintf(os.Stderr, "== PASS [%s] (%.0fms wall) ==\n", c.label, rep.WallMs)
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "== FAIL [%s] ==\n", c.label)
	os.Exit(1)
}

func protocolName(binary bool) string {
	if binary {
		return "msgpack"
	}
	return "json"
}

// summarise renders a compact one-line view of a scenario's metrics for
// the stderr progress log.
func summarise(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, m[k]))
	}
	return strings.Join(parts, " ")
}

func msFloat(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// latencyStats reduces a sample slice (in ms) to the summary the report
// records. Returns a map ready to embed under a metrics key.
func latencyStats(samplesMs []float64) map[string]any {
	if len(samplesMs) == 0 {
		return map[string]any{"samples": 0}
	}
	sorted := append([]float64(nil), samplesMs...)
	sort.Float64s(sorted)
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	return map[string]any{
		"samples": len(sorted),
		"minMs":   round2(sorted[0]),
		"meanMs":  round2(sum / float64(len(sorted))),
		"p50Ms":   round2(percentile(sorted, 50)),
		"p99Ms":   round2(percentile(sorted, 99)),
		"maxMs":   round2(sorted[len(sorted)-1]),
	}
}

// percentile returns the p-th percentile (nearest-rank) of an
// already-sorted slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int((p/100.0)*float64(len(sorted)-1) + 0.5)
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100.0
}
