package compatgate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiffCleanRun(t *testing.T) {
	results := []Result{
		{Name: "TestA", Verdict: "pass"},
		{Name: "TestB", Verdict: "fail"},
		{Name: "TestC", Verdict: "skip"},
	}
	ignore := IgnoreList{Failure: []IgnoreEntry{
		{Test: "TestB", Reason: "known gap"},
	}}
	regressions, stale := Diff(results, ignore)
	if len(regressions) != 0 {
		t.Errorf("regressions = %v, want none", regressions)
	}
	if len(stale) != 0 {
		t.Errorf("stale = %v, want none", stale)
	}
}

func TestDiffDetectsRegression(t *testing.T) {
	results := []Result{
		{Name: "TestA", Verdict: "pass"},
		{Name: "TestB", Verdict: "fail"},
		{Name: "TestC", Verdict: "panic"},
	}
	ignore := IgnoreList{Failure: []IgnoreEntry{
		{Test: "TestB", Reason: "known gap"},
	}}
	regressions, stale := Diff(results, ignore)
	if len(regressions) != 1 || regressions[0] != "TestC" {
		t.Errorf("regressions = %v, want [TestC]", regressions)
	}
	if len(stale) != 0 {
		t.Errorf("stale = %v, want none", stale)
	}
}

func TestDiffDetectsStaleEntryThatNowPasses(t *testing.T) {
	results := []Result{
		{Name: "TestA", Verdict: "pass"},
		{Name: "TestB", Verdict: "pass"}, // was failing, now fixed
	}
	ignore := IgnoreList{Failure: []IgnoreEntry{
		{Test: "TestB", Reason: "known gap"},
	}}
	regressions, stale := Diff(results, ignore)
	if len(regressions) != 0 {
		t.Errorf("regressions = %v, want none", regressions)
	}
	if len(stale) != 1 || stale[0] != "TestB" {
		t.Errorf("stale = %v, want [TestB]", stale)
	}
}

func TestDiffDetectsStaleEntryForRemovedTest(t *testing.T) {
	// TestB isn't in the results at all — e.g. renamed or deleted upstream.
	results := []Result{
		{Name: "TestA", Verdict: "pass"},
	}
	ignore := IgnoreList{Failure: []IgnoreEntry{
		{Test: "TestB", Reason: "known gap"},
	}}
	_, stale := Diff(results, ignore)
	if len(stale) != 1 || stale[0] != "TestB" {
		t.Errorf("stale = %v, want [TestB]", stale)
	}
}

func TestDiffTimeoutAndPanicCountAsFailing(t *testing.T) {
	results := []Result{
		{Name: "TestA", Verdict: "timeout"},
		{Name: "TestB", Verdict: "panic"},
	}
	regressions, _ := Diff(results, IgnoreList{})
	if len(regressions) != 2 {
		t.Errorf("regressions = %v, want both TestA and TestB flagged", regressions)
	}
}

func TestDiffSkipIsNeitherFailingNorGroundsForStale(t *testing.T) {
	results := []Result{
		{Name: "TestA", Verdict: "skip"},
	}
	ignore := IgnoreList{Failure: []IgnoreEntry{
		{Test: "TestA", Reason: "shouldn't be here, but just in case"},
	}}
	regressions, stale := Diff(results, ignore)
	if len(regressions) != 0 {
		t.Errorf("regressions = %v, want none (skip isn't a failure)", regressions)
	}
	// A skipped test isn't "failing", so an ignore-list entry naming it is
	// stale by this package's rule (not failing -> stale). That's correct:
	// there's no reason to carry an ignore-list entry for a test the SDK
	// itself skips.
	if len(stale) != 1 {
		t.Errorf("stale = %v, want [TestA]", stale)
	}
}

func TestLoadResults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.json")
	if err := os.WriteFile(path, []byte(`{"sdk": "ably-go", "results": [
		{"name": "TestA", "verdict": "pass", "duration": 0.1},
		{"name": "TestB", "verdict": "fail", "duration": 0.2}
	]}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	results, err := LoadResults(path)
	if err != nil {
		t.Fatalf("LoadResults: %v", err)
	}
	if len(results) != 2 || results[1].Name != "TestB" || results[1].Verdict != "fail" {
		t.Errorf("results = %+v, want 2 entries ending with TestB/fail", results)
	}
}

func TestLoadIgnoreList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known-failures.toml")
	if err := os.WriteFile(path, []byte(`
[[failure]]
test = "TestA"
reason = "documented non-goal"

[[failure]]
test = "TestB"
reason = "environment-incompatible"
`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	list, err := LoadIgnoreList(path)
	if err != nil {
		t.Fatalf("LoadIgnoreList: %v", err)
	}
	if len(list.Failure) != 2 {
		t.Fatalf("entries = %d, want 2", len(list.Failure))
	}
}

func TestLoadIgnoreListRejectsMissingReason(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known-failures.toml")
	if err := os.WriteFile(path, []byte(`
[[failure]]
test = "TestA"
`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadIgnoreList(path); err == nil {
		t.Error("LoadIgnoreList accepted an entry with no reason, want error")
	}
}

func TestLoadIgnoreListRejectsDuplicateTest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known-failures.toml")
	if err := os.WriteFile(path, []byte(`
[[failure]]
test = "TestA"
reason = "first"

[[failure]]
test = "TestA"
reason = "second"
`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadIgnoreList(path); err == nil {
		t.Error("LoadIgnoreList accepted a duplicate test name, want error")
	}
}
