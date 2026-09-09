package server

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/version"
)

func TestRunVersionPrintsAndExits(t *testing.T) {
	for _, args := range [][]string{
		{"--version"},
		{"-version"},
		{"--version=true"},
	} {
		var out bytes.Buffer
		code := Run(context.Background(), Opts{
			Args:   args,
			Getenv: emptyEnv,
			Out:    &out,
		})
		if code != 0 {
			t.Errorf("%v: exit code = %d, want 0", args, code)
		}
		if got, want := strings.TrimSpace(out.String()), version.String(); got != want {
			t.Errorf("%v: output = %q, want %q", args, got, want)
		}
	}
}

// --version is answered before the configuration is read, so a broken
// or missing config cannot stop a binary identifying itself — which is
// the state it is most likely to be asked in.
func TestRunVersionIgnoresBrokenConfig(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + filepath.Join(t.TempDir(), "absent.toml"), "--version"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if !strings.HasPrefix(out.String(), "ably-server ") {
		t.Errorf("output = %q, want the version line", out.String())
	}
}

// --version=false asks to run the server, so it must fall through to
// the normal startup path rather than print and exit.
func TestRunVersionFalseStartsUp(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--version=false"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (no api key)", code)
	}
	if !strings.Contains(out.String(), "api key is required") {
		t.Errorf("output = %q, want the startup path's error", out.String())
	}
}

func TestHasVersionFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"--version"}, true},
		{[]string{"-version"}, true},
		{[]string{"--version=true"}, true},
		{[]string{"--version=1"}, true},
		{[]string{"--version=false"}, false},
		{[]string{"--version=nonsense"}, false},
		{[]string{"--listen=:0", "--version"}, true},
		{[]string{"--", "--version"}, false},
	} {
		if got := hasVersionFlag(tc.args); got != tc.want {
			t.Errorf("hasVersionFlag(%q) = %v, want %v", tc.args, got, tc.want)
		}
	}
}
