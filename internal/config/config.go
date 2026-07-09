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
	// APIKey and APIKeys both configure API keys (DESIGN.md §3, §9): the
	// singular key is retained for backwards compatibility and combined
	// with the api-keys array (both are used when both are present). At
	// least one key must be configured across all sources.
	APIKey        string   `toml:"api-key"`
	APIKeys       []string `toml:"api-keys"`
	DataDir       string   `toml:"data-dir"`
	DBDSN         string   `toml:"db-dsn"`
	ShutdownGrace string   `toml:"shutdown-grace"`
	LogLevel      string   `toml:"log-level"`
	LogFormat     string   `toml:"log-format"`
	DebugListen   string   `toml:"debug-listen"`
	// Fixtures is the path to an Ably test-app-setup-shaped JSON file
	// whose channels' presence members are pre-seeded at startup — for
	// SDK test-suite compatibility only (DESIGN.md §9).
	Fixtures string `toml:"fixtures"`
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
