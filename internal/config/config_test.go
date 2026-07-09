package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func writeTOML(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ably-server.toml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func TestLoadParsesAllKeys(t *testing.T) {
	path := writeTOML(t, `
mode = "cluster"
listen = ":9090"
api-key = "app.key:secret"
data-dir = "/var/lib/ably"
db-dsn = "postgres://user:pw@host:5432/db"
shutdown-grace = "30s"
log-level = "debug"
log-format = "json"
debug-listen = "127.0.0.1:6060"
`)

	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := File{
		Mode:          "cluster",
		Listen:        ":9090",
		APIKey:        "app.key:secret",
		DataDir:       "/var/lib/ably",
		DBDSN:         "postgres://user:pw@host:5432/db",
		ShutdownGrace: "30s",
		LogLevel:      "debug",
		LogFormat:     "json",
		DebugListen:   "127.0.0.1:6060",
	}
	if !reflect.DeepEqual(*f, want) {
		t.Errorf("Load() = %+v, want %+v", *f, want)
	}
}

func TestLoadParsesAPIKeysArray(t *testing.T) {
	path := writeTOML(t, `
api-key = "app.key0:secret0"
api-keys = ["app.key1:secret1", "app.key2:secret2"]
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.APIKey != "app.key0:secret0" {
		t.Errorf("APIKey = %q", f.APIKey)
	}
	want := []string{"app.key1:secret1", "app.key2:secret2"}
	if !reflect.DeepEqual(f.APIKeys, want) {
		t.Errorf("APIKeys = %v, want %v", f.APIKeys, want)
	}
}

func TestLoadPartialFileLeavesOtherFieldsZero(t *testing.T) {
	path := writeTOML(t, `log-format = "json"`)

	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want %q", f.LogFormat, "json")
	}
	if f.Mode != "" || f.Listen != "" || f.APIKey != "" {
		t.Errorf("unset fields should be zero, got %+v", *f)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.toml")); err == nil {
		t.Error("Load(missing file) error = nil, want an error")
	}
}

func TestLoadRejectsMalformedTOML(t *testing.T) {
	path := writeTOML(t, `this is not = = toml`)
	if _, err := Load(path); err == nil {
		t.Error("Load(malformed TOML) error = nil, want a parse error")
	}
}

func TestPathFromArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"absent", []string{"--api-key=x"}, ""},
		{"equals form", []string{"--config=/etc/ably-server.toml"}, "/etc/ably-server.toml"},
		{"single dash equals", []string{"-config=/etc/ably.toml"}, "/etc/ably.toml"},
		{"next-arg form", []string{"--config", "/etc/ably.toml", "--api-key=x"}, "/etc/ably.toml"},
		{"single dash next-arg", []string{"-config", "/etc/ably.toml"}, "/etc/ably.toml"},
		{"trailing with no value", []string{"--api-key=x", "--config"}, ""},
		{"stops at terminator", []string{"--", "--config=/etc/ably.toml"}, ""},
		{"mixed with other flags", []string{"--log-level=debug", "--config=/x.toml", "--mode=cluster"}, "/x.toml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PathFromArgs(tc.args); got != tc.want {
				t.Errorf("PathFromArgs(%v) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestDefault(t *testing.T) {
	cases := []struct {
		name            string
		env, file, back string
		want            string
	}{
		{"env wins", "from-env", "from-file", "fallback", "from-env"},
		{"file wins over fallback", "", "from-file", "fallback", "from-file"},
		{"fallback when both empty", "", "", "fallback", "fallback"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Default(tc.env, tc.file, tc.back); got != tc.want {
				t.Errorf("Default(%q, %q, %q) = %q, want %q", tc.env, tc.file, tc.back, got, tc.want)
			}
		})
	}
}

func TestDefaultDuration(t *testing.T) {
	d, err := DefaultDuration("", "", 10*time.Second)
	if err != nil || d != 10*time.Second {
		t.Errorf("DefaultDuration(empty, empty, 10s) = %v, %v; want 10s, nil", d, err)
	}

	d, err = DefaultDuration("", "30s", 10*time.Second)
	if err != nil || d != 30*time.Second {
		t.Errorf("DefaultDuration(empty, 30s, 10s) = %v, %v; want 30s, nil", d, err)
	}

	d, err = DefaultDuration("5s", "30s", 10*time.Second)
	if err != nil || d != 5*time.Second {
		t.Errorf("DefaultDuration(5s, 30s, 10s) = %v, %v; want 5s, nil (env wins)", d, err)
	}

	if _, err := DefaultDuration("", "not-a-duration", 10*time.Second); err == nil {
		t.Error("DefaultDuration with malformed value error = nil, want an error")
	}
}
