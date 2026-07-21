// Command ably-bench drives pub/sub load against a running ably-server
// (a single node or the docker-compose cluster), verifies delivery
// correctness, measures end-to-end latency, and — in search mode — finds
// the highest sustained throughput that stays within a p50/p99 budget.
//
//	# Fixed-rate run against the compose cluster (default endpoints):
//	ably-bench --rate 5000 --duration 10s
//
//	# Find max throughput within p50<=20ms, p99<=100ms:
//	ably-bench --search --p50 20ms --p99 100ms --max-rate 100000
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}

type options struct {
	endpoints   string
	key         string
	binary      bool
	channels    int
	publishers  int
	subscribers int
	msgSize     int
	duration    time.Duration
	warmup      time.Duration

	search    bool
	rate      float64
	maxRate   float64
	p50Target time.Duration
	p99Target time.Duration
}

func run(args []string, out *os.File) int {
	fs := flag.NewFlagSet("ably-bench", flag.ContinueOnError)
	fs.SetOutput(out)
	var o options
	fs.StringVar(&o.endpoints, "endpoints", "localhost:8081,localhost:8082,localhost:8083", "comma-separated host:port list; load is spread round-robin across them")
	fs.StringVar(&o.key, "key", envOr("ABLY_SERVER_KEYS", "app.key:secret"), "API key in appId.keyId:keySecret format (env: ABLY_SERVER_KEYS)")
	fs.BoolVar(&o.binary, "binary", false, "use the msgpack protocol instead of JSON")
	fs.IntVar(&o.channels, "channels", 4, "number of channels")
	fs.IntVar(&o.publishers, "publishers", 4, "number of publisher connections (assigned round-robin to channels)")
	fs.IntVar(&o.subscribers, "subscribers", 4, "number of subscriber connections (assigned round-robin to channels)")
	fs.IntVar(&o.msgSize, "msg-size", 128, "approximate message payload size in bytes")
	fs.DurationVar(&o.duration, "duration", 10*time.Second, "measurement window per trial")
	fs.DurationVar(&o.warmup, "warmup", 2*time.Second, "warmup period excluded from stats")

	fs.BoolVar(&o.search, "search", false, "search for the max throughput within the p50/p99 budget instead of a single fixed-rate run")
	fs.Float64Var(&o.rate, "rate", 1000, "offered load in messages/sec (fixed-run rate, or the starting rate in --search)")
	fs.Float64Var(&o.maxRate, "max-rate", 200000, "upper bound on offered load in --search mode")
	fs.DurationVar(&o.p50Target, "p50", 20*time.Millisecond, "target p50 latency for --search")
	fs.DurationVar(&o.p99Target, "p99", 100*time.Millisecond, "target p99 latency for --search")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	endpoints, err := parseEndpoints(o.endpoints)
	if err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 2
	}
	if o.channels < 1 || o.publishers < 1 || o.subscribers < 1 {
		fmt.Fprintln(out, "error: --channels, --publishers and --subscribers must each be >= 1")
		return 2
	}
	if o.subscribers < o.channels {
		fmt.Fprintf(out, "warning: %d subscribers across %d channels — channels without a subscriber are not verified\n", o.subscribers, o.channels)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg := trialConfig{
		endpoints:   endpoints,
		key:         o.key,
		binary:      o.binary,
		channels:    o.channels,
		publishers:  o.publishers,
		subscribers: o.subscribers,
		msgSize:     o.msgSize,
		warmup:      o.warmup,
		duration:    o.duration,
	}

	fmt.Fprintf(out, "ably-bench: %d endpoint(s), %d channels, %d publishers, %d subscribers, %dB messages, %s protocol\n",
		len(endpoints), o.channels, o.publishers, o.subscribers, o.msgSize, protocolName(o.binary))

	if o.search {
		return searchThroughput(ctx, out, cfg, o)
	}
	return fixedRun(ctx, out, cfg, o)
}

func fixedRun(ctx context.Context, out *os.File, cfg trialConfig, o options) int {
	cfg.rate = o.rate
	fmt.Fprintf(out, "running fixed-rate trial at %.0f msg/s for %s (warmup %s)\n\n", o.rate, o.duration, o.warmup)
	res, err := runTrial(ctx, cfg)
	if err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	printResult(out, res)
	if !res.corr.ok() {
		return 1
	}
	return 0
}

// searchThroughput ramps the offered rate by doubling until a trial fails
// the budget, then binary-searches between the last pass and first fail
// for the highest passing rate.
func searchThroughput(ctx context.Context, out *os.File, cfg trialConfig, o options) int {
	fmt.Fprintf(out, "searching for max throughput within p50<=%s p99<=%s (trial=%s, warmup=%s)\n\n",
		msStr(o.p50Target), msStr(o.p99Target), o.duration, o.warmup)
	fmt.Fprintf(out, "%-12s %-12s %-9s %-9s %-9s %s\n", "offered", "achieved", "p50", "p99", "max", "correct")

	trial := func(rate float64) (trialResult, bool) {
		if ctx.Err() != nil {
			return trialResult{}, false
		}
		cfg.rate = rate
		res, err := runTrial(ctx, cfg)
		if err != nil {
			fmt.Fprintf(out, "  trial at %.0f msg/s failed: %v\n", rate, err)
			return trialResult{}, false
		}
		pass := res.pass(o.p50Target, o.p99Target)
		printTrialLine(out, res, pass)
		return res, pass
	}

	var best *trialResult
	lo := o.rate    // highest known-good rate
	hi := o.maxRate // lowest known-bad rate (or the cap)
	haveFail := false

	// Ramp: double until a failure or the cap.
	rate := o.rate
	for rate <= o.maxRate {
		res, pass := trial(rate)
		if ctx.Err() != nil {
			break
		}
		if pass {
			r := res
			best = &r
			lo = rate
			rate *= 2
			continue
		}
		hi = rate
		haveFail = true
		break
	}

	// Binary search between the last pass (lo) and first fail (hi).
	if best != nil && haveFail {
		for i := 0; i < 6 && ctx.Err() == nil; i++ {
			mid := (lo + hi) / 2
			if mid-lo < 0.02*lo { // within ~2%, stop refining
				break
			}
			res, pass := trial(mid)
			if pass {
				r := res
				best = &r
				lo = mid
			} else {
				hi = mid
			}
		}
	}

	fmt.Fprintln(out)
	if best == nil {
		fmt.Fprintf(out, "FAIL: even the starting rate of %.0f msg/s did not meet the budget\n", o.rate)
		return 1
	}
	fmt.Fprintf(out, "MAX SUSTAINED THROUGHPUT: %.0f msg/s within p50<=%s p99<=%s\n",
		best.achieved, msStr(o.p50Target), msStr(o.p99Target))
	fmt.Fprintf(out, "  at that load: p50=%s p99=%s max=%s\n", msStr(best.p50), msStr(best.p99), msStr(best.max))
	if !haveFail {
		fmt.Fprintf(out, "  note: budget never broke up to the --max-rate cap of %.0f msg/s; raise it to find the true ceiling\n", o.maxRate)
	}
	return 0
}

func printResult(out *os.File, res trialResult) {
	fmt.Fprintf(out, "offered:   %.0f msg/s\n", res.offered)
	fmt.Fprintf(out, "achieved:  %.0f msg/s\n", res.achieved)
	fmt.Fprintf(out, "latency:   p50=%s p99=%s max=%s\n", msStr(res.p50), msStr(res.p99), msStr(res.max))
	c := res.corr
	if c.ok() {
		fmt.Fprintf(out, "correct:   PASS (%d delivered, no loss/dup/reorder)\n", c.received)
		return
	}
	fmt.Fprintf(out, "correct:   FAIL — expected=%d received=%d missing=%d extra=%d out-of-order=%d gaps=%d nacks=%d\n",
		c.expected, c.received, c.missing, c.extra, c.outOfOrder, c.gaps, c.publishNACKs)
}

func printTrialLine(out *os.File, res trialResult, pass bool) {
	verdict := "ok"
	if !res.corr.ok() {
		verdict = "FAIL"
	}
	mark := "✗"
	if pass {
		mark = "✓"
	}
	fmt.Fprintf(out, "%-12.0f %-12.0f %-9s %-9s %-9s %s %s\n",
		res.offered, res.achieved, msStr(res.p50), msStr(res.p99), msStr(res.max), verdict, mark)
}

func parseEndpoints(s string) ([]endpoint, error) {
	var eps []endpoint
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		host, portStr, err := net.SplitHostPort(part)
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", part, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: bad port: %w", part, err)
		}
		eps = append(eps, endpoint{host: host, port: port})
	}
	if len(eps) == 0 {
		return nil, fmt.Errorf("no endpoints given")
	}
	return eps, nil
}

func msStr(d time.Duration) string {
	return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64) + "ms"
}

func protocolName(binary bool) string {
	if binary {
		return "msgpack"
	}
	return "json"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
