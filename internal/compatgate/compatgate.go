// Package compatgate implements the known-failures gate for the SDK
// compatibility harnesses (ably-go, ably-js, ably-ai-transport-js):
// diffing a harness's per-test JSON results against a checked-in ignore
// list of tests currently expected to fail, so CI can fail on a genuine
// regression while staying green on already-tracked gaps.
//
// The harnesses themselves are policy-free — they just run their suite
// and report a verdict per test (see each SDK's compat script). This
// package, and the ignore list it reads, own the policy: which failures
// are currently acceptable, and why.
package compatgate

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/BurntSushi/toml"
)

// Result is one test's outcome within a harness report's results array.
type Result struct {
	Name     string  `json:"name"`
	Verdict  string  `json:"verdict"`
	Duration float64 `json:"duration"`
}

// Report is the shared JSON shape every SDK compat runner emits: run
// metadata plus a flat per-test results array. Only the results are
// consulted here; the metadata (sdk, sdkRev, tally, ...) is for humans
// reading the CI artifact. Both the ably-go and ably-js runners emit this
// same shape so this gate is their single consumer.
type Report struct {
	Results []Result `json:"results"`
}

// Failing reports whether the result counts as a failure the ignore list
// must account for. A "skip" is the test's own deliberate decision (e.g.
// an environment precondition it checks itself) — neither a failure to
// track nor grounds for pruning an ignore-list entry.
func (r Result) Failing() bool {
	switch r.Verdict {
	case "fail", "panic", "timeout":
		return true
	default:
		return false
	}
}

// IgnoreEntry is one checked-in known-failure: a test name and why it's
// expected to fail. Every entry must carry a reason — an ignore list is
// meant to be read, not just machine-diffed.
type IgnoreEntry struct {
	Test   string `toml:"test"`
	Reason string `toml:"reason"`
}

// IgnoreList is the checked-in TOML shape: a flat array of entries under
// the [[failure]] table array.
type IgnoreList struct {
	Failure []IgnoreEntry `toml:"failure"`
}

// LoadResults reads a harness report and returns its per-test results.
func LoadResults(path string) ([]Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read results %q: %w", path, err)
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, fmt.Errorf("parse results %q: %w", path, err)
	}
	return report.Results, nil
}

// LoadIgnoreList reads a checked-in known-failures TOML file, rejecting a
// malformed one outright: a duplicate test name (ambiguous — which reason
// applies?) or an entry with no reason (defeats the point of a
// human-readable list) is a load error, not something the diff silently
// tolerates.
func LoadIgnoreList(path string) (IgnoreList, error) {
	var list IgnoreList
	if _, err := toml.DecodeFile(path, &list); err != nil {
		return IgnoreList{}, fmt.Errorf("parse ignore list %q: %w", path, err)
	}
	seen := make(map[string]bool, len(list.Failure))
	for _, e := range list.Failure {
		if e.Test == "" {
			return IgnoreList{}, fmt.Errorf("ignore list %q: entry with empty test name", path)
		}
		if e.Reason == "" {
			return IgnoreList{}, fmt.Errorf("ignore list %q: entry %q has no reason", path, e.Test)
		}
		if seen[e.Test] {
			return IgnoreList{}, fmt.Errorf("ignore list %q: duplicate entry for %q", path, e.Test)
		}
		seen[e.Test] = true
	}
	return list, nil
}

// Diff compares a harness run against the ignore list it should be judged
// against. regressions are failing tests not on the list — new breakage
// (or a not-yet-tracked gap) a PR must not merge with. stale are
// ignore-list entries that are NOT currently failing — either the test
// now passes, or it didn't run at all (renamed/removed upstream) — so the
// entry no longer describes reality and must be pruned. Both directions
// are reported so the list can't silently rot in either direction: it
// can't hide a new failure, and it can't accumulate dead entries once
// whatever they described is no longer true.
func Diff(results []Result, ignore IgnoreList) (regressions, stale []string) {
	failing := make(map[string]bool, len(results))
	for _, r := range results {
		if r.Failing() {
			failing[r.Name] = true
		}
	}
	ignored := make(map[string]bool, len(ignore.Failure))
	for _, e := range ignore.Failure {
		ignored[e.Test] = true
	}

	for name := range failing {
		if !ignored[name] {
			regressions = append(regressions, name)
		}
	}
	for _, e := range ignore.Failure {
		if !failing[e.Test] {
			stale = append(stale, e.Test)
		}
	}
	sort.Strings(regressions)
	sort.Strings(stale)
	return regressions, stale
}
