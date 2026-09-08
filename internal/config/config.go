// Package config supports ably-server's optional TOML config file
// (--config, DESIGN.md §9). Resolution order across every source is
// flag > env > config file > hardcoded default; File and Default
// exist to let cmd/ably-server seed each flag.String/flag.Duration
// call with the env-then-file-then-default value, so flag.Parse's own
// explicit-flag-wins behaviour produces the full precedence chain
// without extra bookkeeping.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// File is the shape of the optional TOML config file. It covers the
// same keys as the CLI flags that exist today (DESIGN.md §9); every
// field is optional — a zero value means "absent from the file" and
// resolution falls through to the flag's environment variable or
// hardcoded default. ShutdownGrace is kept as the raw string (e.g.
// "10s") since TOML has no duration type; callers parse it with
// DefaultDuration the same way the --shutdown-grace flag's value is
// parsed.
type File struct {
	Mode   string `toml:"mode"`
	Listen string `toml:"listen"`
	// Keys are structured [[keys]] entries: each a key spec plus an
	// optional per-key capability (DESIGN.md §3.1, §9). This is the only
	// file-tier source of API keys; at least one key must be configured
	// across all sources (flag, env, or this).
	Keys          []KeyEntry `toml:"keys"`
	DataDir       string     `toml:"data-dir"`
	PostgresDSN   string     `toml:"postgres-dsn"`
	ShutdownGrace string     `toml:"shutdown-grace"`
	LogLevel      string     `toml:"log-level"`
	LogFormat     string     `toml:"log-format"`
	DebugListen   string     `toml:"debug-listen"`
	// EnableStatsStub registers the GET/POST /stats compatibility stub
	// (DESIGN.md §1); absent/false — the zero value — keeps it
	// unregistered, matching the fallback default, so the usual
	// "zero value means absent" convention costs nothing here.
	EnableStatsStub bool `toml:"enable-stats-stub"`
	// Namespaces are [[namespaces]] entries mirroring the test-app-setup
	// post_apps shape (DESIGN.md §9, §12.5). A channel takes the flags of
	// the namespace selecting it, so what is configured here is what a
	// channel is then allowed to do.
	Namespaces []Namespace `toml:"namespaces"`
	// Channels are [[channels]] entries whose nested presence members are
	// seeded at startup as static fixtures (DESIGN.md §9, §12.5),
	// replacing the retired --fixtures JSON path.
	Channels []Channel `toml:"channels"`
	// KeysDir, NamespacesDir and AppStatusFile name the sources that are read
	// again while the server runs, so that a key, a namespace or the app's
	// status can change under it (DESIGN.md §9.1). They are paths in the file
	// tier of the usual precedence chain, like every other option here; what
	// they point at is described on Dynamic.
	KeysDir       string `toml:"keys-dir"`
	NamespacesDir string `toml:"namespaces-dir"`
	AppStatusFile string `toml:"app-status-file"`
}

// KeyEntry is one structured [[keys]] entry (DESIGN.md §3.1, §9): an
// Ably-format key spec plus an optional capability. Capability is an
// `x-ably-capability`-format JSON object string; empty means the key
// grants the full capability, matching a --keys flag or env entry.
type KeyEntry struct {
	Key        string `toml:"key"`
	Capability string `toml:"capability"`
}

// Namespace is one [[namespaces]] entry (DESIGN.md §9, §12.5): a
// namespace id plus feature flags mirroring test-app-setup's post_apps
// shape.
//
// Mode decides how ID selects channels. Empty — every namespace
// predating generalised channel rules — means ID is one channel name
// segment, matching every channel whose first segment is that id.
// "matcher" means ID is a match expression in its own right, with the
// same segment semantics as a capability resource: a non-trailing "*"
// matches one segment, a trailing "*" one or more. Where several
// namespaces match a channel the most specific applies.
type Namespace struct {
	ID              string `toml:"id"`
	Mode            string `toml:"mode"`
	Persisted       bool   `toml:"persisted"`
	MutableMessages bool   `toml:"mutableMessages"`
	PushEnabled     bool   `toml:"pushEnabled"`
}

// Channel is one [[channels]] entry: a channel name plus the presence
// members to seed at startup (DESIGN.md §9, §12.5).
type Channel struct {
	Name     string           `toml:"name"`
	Presence []PresenceMember `toml:"presence"`
}

// PresenceMember is one nested presence entry under a [[channels]] entry.
// Data and Encoding round-trip verbatim — the server treats Encoding as
// opaque and never decodes Data (DESIGN.md §9), so a cipher payload is
// seeded exactly as given.
type PresenceMember struct {
	ClientID string `toml:"clientId"`
	Data     string `toml:"data"`
	Encoding string `toml:"encoding"`
}

// Load parses the TOML file at path into a File.
func Load(path string) (*File, error) {
	var f File
	if _, err := toml.DecodeFile(path, &f); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}
	return &f, nil
}

// PathFromArgs scans args for --config/-config's value, recognising
// both the "=value" and the "next argument" forms the stdlib flag
// package accepts. main needs the config file's path before it can
// define its other flags with config-seeded defaults, and the flag
// package has no way to parse a single flag ahead of the rest, so
// this walks args by hand. It stops at a bare "--", matching flag's
// own terminator convention.
func PathFromArgs(args []string) string {
	for i, a := range args {
		switch {
		case a == "--":
			return ""
		case a == "--config" || a == "-config":
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		case strings.HasPrefix(a, "--config="):
			return strings.TrimPrefix(a, "--config=")
		case strings.HasPrefix(a, "-config="):
			return strings.TrimPrefix(a, "-config=")
		}
	}
	return ""
}

// Default resolves a flag's default value by precedence env > file >
// fallback; whichever of env/file is non-empty and comes first wins.
// The flag itself, if passed explicitly on the command line, is
// applied on top of this by flag.Parse — giving the full flag > env >
// file > default chain.
func Default(env, file, fallback string) string {
	if env != "" {
		return env
	}
	if file != "" {
		return file
	}
	return fallback
}

// DefaultDuration is Default for a time.Duration-valued flag: it
// resolves the env/file/fallback string precedence and then parses
// the winning string. A malformed env or file value is reported as an
// error rather than silently falling back, since that's very likely a
// typo the operator wants to know about at startup.
func DefaultDuration(env, file string, fallback time.Duration) (time.Duration, error) {
	v := Default(env, file, "")
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid duration %q: %w", v, err)
	}
	return d, nil
}

// DefaultBool is Default for a bool-valued flag: env, if set, is parsed
// with strconv.ParseBool (accepting "true"/"false"/"1"/"0"/etc, reported
// as an error on a malformed value — likely a typo the operator wants to
// know about at startup); otherwise file wins when true; otherwise
// fallback. file is a plain bool rather than Default's string precedence
// chain because the File struct's fields already use "zero value means
// absent from the file" as their convention — which only loses
// information when an option's fallback is true and the file wants to
// override it to false, a case no current option needs.
func DefaultBool(env string, file bool, fallback bool) (bool, error) {
	if env != "" {
		b, err := strconv.ParseBool(env)
		if err != nil {
			return false, fmt.Errorf("config: invalid bool %q: %w", env, err)
		}
		return b, nil
	}
	if file {
		return true, nil
	}
	return fallback, nil
}
