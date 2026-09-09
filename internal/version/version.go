// Package version reports which build of ably-server is running, so a
// deployed binary can be traced back to the source it was built from.
//
// A release build stamps the values below with -ldflags -X; see the
// Dockerfile and .github/workflows/release.yml. An unstamped build —
// `go build`, `go run`, `go install` — falls back to the VCS
// information the Go toolchain embeds automatically, so a developer
// binary still identifies itself.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

// Stamped by the linker. Unexported so the fallbacks in the
// accessors below are the only way to read them.
var (
	version = ""
	commit  = ""
	date    = ""
)

// Version is the release this binary was built from: a semver tag for
// a release build, "dev" for anything else.
func Version() string {
	if version != "" {
		return version
	}
	// A binary produced by `go install module@version` carries its
	// version here even though nothing stamped it. A binary built
	// from a checkout also carries one, but a synthesised
	// pseudo-version that only restates the commit — so it is used
	// only when there is no VCS stamp, which is what distinguishes
	// the two cases.
	if buildSetting("vcs.revision") == "" {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
	}
	return "dev"
}

// Commit is the full git SHA the binary was built from, or "" if the
// build recorded none — which is the case for a build whose context
// excludes the .git directory and which was not stamped.
func Commit() string {
	if commit != "" {
		return commit
	}
	return buildSetting("vcs.revision")
}

// Date is the build timestamp in RFC 3339, or "" if unrecorded.
func Date() string {
	if date != "" {
		return date
	}
	return buildSetting("vcs.time")
}

// Modified reports whether the working tree had uncommitted changes
// at build time. Only the toolchain records this, and it records it
// for the tree the build ran in — which for a release build is the
// build container, not the checkout the code came from. So a stamped
// build reports false and leaves the question to its commit.
func Modified() bool {
	return commit == "" && buildSetting("vcs.modified") == "true"
}

// String is the one-line identification printed by --version and
// logged at startup.
func String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ably-server %s", Version())
	if c := Commit(); c != "" {
		fmt.Fprintf(&b, " (%s", short(c))
		if Modified() {
			b.WriteString("-dirty")
		}
		b.WriteString(")")
	}
	fmt.Fprintf(&b, " %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	return b.String()
}

// short abbreviates a git SHA the way git itself does.
func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

func buildSetting(key string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}
