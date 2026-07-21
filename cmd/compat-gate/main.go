// Command compat-gate diffs an SDK compatibility harness's per-test JSON
// results against a checked-in known-failures list (internal/compatgate),
// so CI can fail a PR on a genuine regression while staying green on
// already-tracked gaps. It fails on either a failing test not on the list
// (a regression, or an untracked gap) or a list entry that isn't currently
// failing (stale — the list must be pruned).
//
// Usage:
//
//	compat-gate --results ably-server-compat-results.json --ignore compat/known-failures/ably-go.toml
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ably/ably-server/internal/compatgate"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("compat-gate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	resultsPath := fs.String("results", "", "path to the harness's --json results file")
	ignorePath := fs.String("ignore", "", "path to the known-failures TOML file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultsPath == "" || *ignorePath == "" {
		fmt.Fprintln(stderr, "usage: compat-gate --results PATH --ignore PATH")
		return 2
	}

	results, err := compatgate.LoadResults(*resultsPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	ignore, err := compatgate.LoadIgnoreList(*ignorePath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	regressions, stale := compatgate.Diff(results, ignore)
	if len(regressions) == 0 && len(stale) == 0 {
		fmt.Fprintf(stdout, "compat-gate: clean — %d tests, %d known failures, all accounted for\n", len(results), len(ignore.Failure))
		return 0
	}

	if len(regressions) > 0 {
		fmt.Fprintf(stdout, "compat-gate: %d failing test(s) not in %s (new regression, or an untracked gap — fix it, or add a reason and file a task):\n", len(regressions), *ignorePath)
		for _, name := range regressions {
			fmt.Fprintf(stdout, "  FAIL  %s\n", name)
		}
	}
	if len(stale) > 0 {
		fmt.Fprintf(stdout, "compat-gate: %d entries in %s no longer failing (prune them):\n", len(stale), *ignorePath)
		for _, name := range stale {
			fmt.Fprintf(stdout, "  STALE %s\n", name)
		}
	}
	return 1
}
