package server

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/handles"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/rest"
)

// watchedDirs makes a keys directory, a namespaces directory and a path for
// the app-status file, and returns the sources naming them.
func watchedDirs(t *testing.T) config.Dynamic {
	t.Helper()
	dir := t.TempDir()
	sources := config.Dynamic{
		KeysDir:       filepath.Join(dir, "keys"),
		NamespacesDir: filepath.Join(dir, "namespaces"),
		AppStatusFile: filepath.Join(dir, "app-status"),
	}
	for _, d := range []string{sources.KeysDir, sources.NamespacesDir} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return sources
}

// write puts contents at path, through a rename so a reader never sees half
// of it.
func write(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path+".tmp", []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

// newTestReloader builds an app holding one static key and a reloader over
// the sources given, ready to load.
func newTestReloader(t *testing.T, sources config.Dynamic, static ...config.Namespace) (*reloader, *handles.App, *syncBuffer) {
	t.Helper()

	key, err := auth.ParseAPIKey("app.static:s3cr3t")
	if err != nil {
		t.Fatal(err)
	}
	app, err := handles.NewApp("app", []auth.APIKey{key}, static, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out syncBuffer
	return &reloader{
		sources:          sources,
		appID:            "app",
		staticKeys:       []keySpec{{key: "app.static:s3cr3t"}},
		staticNamespaces: static,
		app:              app,
		rest:             rest.NewServer([]auth.APIKey{key}, logging.New(slog.DiscardHandler), nil),
		log:              logging.New(slog.NewTextHandler(&out, nil)),
	}, app, &out
}

// The watched sources layer over the static configuration rather than
// replacing it: a key configured on the command line survives whatever the
// keys directory says, and an entry naming the same key or namespace wins.
func TestReloadLayersOverStaticConfig(t *testing.T) {
	sources := watchedDirs(t)
	write(t, filepath.Join(sources.KeysDir, "extra.toml"), `key = "app.extra:another"`)
	write(t, filepath.Join(sources.KeysDir, "static.toml"),
		"key = \"app.static:s3cr3t\"\ncapability = '{\"chat:*\":[\"subscribe\"]}'\n")
	write(t, filepath.Join(sources.NamespacesDir, "chat.toml"), "id = \"chat\"\nmutableMessages = true\n")

	r, app, _ := newTestReloader(t, sources, config.Namespace{ID: "chat"}, config.Namespace{ID: "other"})
	if err := r.load(); err != nil {
		t.Fatalf("load: %s", err)
	}

	// The static key is still there, narrowed by the file naming it.
	ref, errInfo := app.WatchKey(t.Context(), "static")
	if errInfo != nil {
		t.Fatalf("the static key stopped resolving: %s", errInfo)
	}
	if got := ref.Get().Capability; got != `{"chat:*":["subscribe"]}` {
		t.Errorf("capability = %q, want the watched entry's", got)
	}
	// And the key that only the directory declares.
	if _, errInfo := app.WatchKey(t.Context(), "extra"); errInfo != nil {
		t.Errorf("the watched key does not resolve: %s", errInfo)
	}

	// The namespace declared in both places is the directory's; the one only
	// the static config declares is still there.
	if got := app.Namespaces().MostSpecific("chat:room"); !got.GetMutableMessages() {
		t.Error("the watched namespace did not win over the static one")
	}
	if _, ok := app.Namespaces().Get("other"); !ok {
		t.Error("the static-only namespace was dropped")
	}
}

// A poll picks up an edit and applies it; the app's status comes from the
// file, and removing the file enables the app again.
func TestReloadRunAppliesEdits(t *testing.T) {
	sources := watchedDirs(t)
	r, app, _ := newTestReloader(t, sources)
	if err := r.load(); err != nil {
		t.Fatalf("load: %s", err)
	}
	go r.run(t.Context(), time.Millisecond)

	write(t, sources.AppStatusFile, "disabled\n")
	waitFor(t, "the app to be disabled", func() bool {
		return !app.Enabled()
	})
	if app.FatalError().Get() == nil {
		t.Error("a disabled app carries no fatal error, so its connections would be served forever")
	}

	if err := os.Remove(sources.AppStatusFile); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the app to be enabled again", func() bool {
		return app.Enabled() && app.FatalError().Get() == nil
	})

	write(t, filepath.Join(sources.NamespacesDir, "chat.toml"), "id = \"chat\"\npersisted = true\n")
	waitFor(t, "the namespace to arrive", func() bool {
		return app.Namespaces().MostSpecific("chat:room").GetPersisted()
	})
}

// A file that cannot be read or applied leaves the server serving what it
// already was, and it recovers when the file is fixed: it is already serving,
// and a half-written file is a reason to keep going rather than to stop.
func TestReloadKeepsTheLastGoodConfig(t *testing.T) {
	sources := watchedDirs(t)
	write(t, filepath.Join(sources.NamespacesDir, "chat.toml"), "id = \"chat\"\npersisted = true\n")

	r, app, out := newTestReloader(t, sources)
	if err := r.load(); err != nil {
		t.Fatalf("load: %s", err)
	}
	go r.run(t.Context(), time.Millisecond)

	for name, contents := range map[string]string{
		"unparseable":  "id = ",
		"unknown mode": "id = \"chat\"\nmode = \"matchers\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			write(t, filepath.Join(sources.NamespacesDir, "chat.toml"), contents)
			waitFor(t, "the failure to be reported", func() bool {
				return strings.Contains(out.String(), "watched config")
			})
			if !app.Namespaces().MostSpecific("chat:room").GetPersisted() {
				t.Error("a refused config was applied anyway")
			}
			out.Reset()

			// Fixing the file recovers, rather than leaving the server stuck
			// on the last good config forever.
			write(t, filepath.Join(sources.NamespacesDir, "chat.toml"), "id = \"chat\"\nmutableMessages = true\n")
			waitFor(t, "the fixed config to be applied", func() bool {
				return app.Namespaces().MostSpecific("chat:room").GetMutableMessages()
			})
			write(t, filepath.Join(sources.NamespacesDir, "chat.toml"), "id = \"chat\"\npersisted = true\n")
			waitFor(t, "the original config to be restored", func() bool {
				ns := app.Namespaces().MostSpecific("chat:room")
				return ns.GetPersisted() && !ns.GetMutableMessages()
			})
		})
	}
}

// A key naming another app is refused: this server is its app, so a second one
// appearing in the keys directory is a misconfiguration rather than a second
// app to serve.
func TestReloadRejectsAnotherAppsKey(t *testing.T) {
	sources := watchedDirs(t)
	write(t, filepath.Join(sources.KeysDir, "other.toml"), `key = "other.one:s3cr3t"`)

	r, _, _ := newTestReloader(t, sources)
	if err := r.load(); err == nil {
		t.Fatal("load = nil error, want a refusal")
	}
}

func TestMergeNamespaces(t *testing.T) {
	got := mergeNamespaces(
		[]config.Namespace{{ID: "a", Persisted: true}, {ID: "b"}},
		[]config.Namespace{{ID: "b", MutableMessages: true}, {ID: "c"}},
	)
	want := []config.Namespace{
		{ID: "a", Persisted: true},
		{ID: "b", MutableMessages: true},
		{ID: "c"},
	}
	if len(got) != len(want) {
		t.Fatalf("mergeNamespaces = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mergeNamespaces[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// syncBuffer is a log sink the reloader's goroutine writes to and the test
// reads, which is two goroutines and therefore a lock.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// waitFor polls until check passes, failing the test with what it was waiting
// for if it never does.
func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
