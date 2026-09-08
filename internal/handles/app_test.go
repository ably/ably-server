package handles

import (
	"testing"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"

	protoapp "github.com/ably/server-protocol/go/app"
	"github.com/ably/server-protocol/go/errors"
)

// parseKey parses an API key spec, failing the test if it will not.
func parseKey(t *testing.T, spec, capability string) auth.APIKey {
	t.Helper()
	key, err := auth.ParseAPIKeyWithCapability(spec, capability)
	if err != nil {
		t.Fatalf("parsing %q: %s", spec, err)
	}
	return key
}

// newTestApp builds an app holding one full-capability key.
func newTestApp(t *testing.T, namespaces ...config.Namespace) (*App, auth.APIKey) {
	t.Helper()
	key := parseKey(t, "app.one:s3cr3t", "")
	app, err := NewApp("app", []auth.APIKey{key}, namespaces, nil)
	if err != nil {
		t.Fatalf("building the app: %s", err)
	}
	return app, key
}

// A key's capability can be narrowed under whoever is already holding it: the
// reference they resolved keeps resolving, which is what lets a long-lived
// connection see the change without authenticating again.
func TestSetKeysReachesAHeldReference(t *testing.T) {
	app, _ := newTestApp(t)

	ref, errInfo := app.WatchKey(t.Context(), "one")
	if errInfo != nil {
		t.Fatalf("watching the key: %s", errInfo)
	}
	before := ref.Get()

	narrowed := parseKey(t, "app.one:s3cr3t", `{"chat:*":["subscribe"]}`)
	if err := app.SetKeys([]auth.APIKey{narrowed}); err != nil {
		t.Fatalf("setting the keys: %s", err)
	}

	after := ref.Get()
	if after.Capability != `{"chat:*":["subscribe"]}` {
		t.Errorf("capability = %q, want the narrowed one", after.Capability)
	}
	if after.Modified == before.Modified {
		t.Error("the key was changed but not reported as modified")
	}
	if after.Created != before.Created {
		t.Error("a changed key was reported as newly created")
	}
}

// Setting the same keys again changes nothing, so a re-read of an unedited
// directory does not look to a watcher like a key rotation.
func TestSetKeysIsQuietWhenNothingChanged(t *testing.T) {
	app, key := newTestApp(t)

	ref, _ := app.WatchKey(t.Context(), "one")
	before := ref.Get()

	if err := app.SetKeys([]auth.APIKey{key}); err != nil {
		t.Fatalf("setting the keys: %s", err)
	}
	if ref.Get() != before {
		t.Error("re-applying the same key replaced it")
	}
}

// A key that is no longer configured is marked gone before it is dropped, so a
// holder of it finds out rather than going on using a key that is not there.
func TestSetKeysMarksARemovedKeyGone(t *testing.T) {
	app, _ := newTestApp(t)

	ref, _ := app.WatchKey(t.Context(), "one")

	other := parseKey(t, "app.two:another", "")
	if err := app.SetKeys([]auth.APIKey{other}); err != nil {
		t.Fatalf("setting the keys: %s", err)
	}

	if got := ref.Get().Status; got != protoapp.KeyStatusGone {
		t.Errorf("status = %v, want %v", got, protoapp.KeyStatusGone)
	}
	if _, errInfo := app.WatchKey(t.Context(), "one"); errInfo == nil {
		t.Error("a removed key still resolves")
	}
	if _, errInfo := app.WatchKey(t.Context(), "two"); errInfo != nil {
		t.Errorf("the new key does not resolve: %s", errInfo)
	}
}

// A key set this server will not serve leaves the app exactly as it was, so a
// typo in one file does not take the other keys with it.
func TestSetKeysIsAllOrNothing(t *testing.T) {
	app, _ := newTestApp(t)

	for name, keys := range map[string][]auth.APIKey{
		"another app's key":    {parseKey(t, "other.one:s3cr3t", "")},
		"malformed capability": {parseKey(t, "app.one:s3cr3t", `{"chat:*":["fly"]}`)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := app.SetKeys(keys); err == nil {
				t.Fatal("SetKeys = nil error, want error")
			}
			ref, errInfo := app.WatchKey(t.Context(), "one")
			if errInfo != nil {
				t.Fatalf("the original key stopped resolving: %s", errInfo)
			}
			if ref.Get().Status != protoapp.KeyStatusEnabled {
				t.Error("the original key was disturbed by a refused set")
			}
		})
	}
}

// A fresh app is serviceable, and disabling it refuses everything and tells
// whoever is already connected.
func TestDisablingAnAppRefusesEverything(t *testing.T) {
	app, _ := newTestApp(t)

	if !app.Enabled() {
		t.Error("a fresh app is not enabled")
	}
	if err := app.CheckStatus(); err != nil {
		t.Errorf("a fresh app failed its status check: %s", err)
	}
	if err := app.FatalError().Get(); err != nil {
		t.Errorf("a fresh app already carries a fatal error: %s", err)
	}

	app.SetEnabled(false)

	for name, err := range map[string]*errors.ErrorInfo{
		"CheckStatus":               app.CheckStatus(),
		"CheckStatusForAPIRequests": app.CheckStatusForAPIRequests(),
		"CheckStatusForConnections": app.CheckStatusForConnections(),
		"FatalError":                app.FatalError().Get(),
	} {
		if err == nil {
			t.Errorf("%s = nil, want a refusal for a disabled app", name)
			continue
		}
		if err.StatusCode != 403 {
			t.Errorf("%s status = %d, want 403", name, err.StatusCode)
		}
	}

	app.SetEnabled(true)
	if err := app.CheckStatus(); err != nil {
		t.Errorf("a re-enabled app is still refused: %s", err)
	}
	if err := app.FatalError().Get(); err != nil {
		t.Errorf("a re-enabled app still carries a fatal error: %s", err)
	}
}

// The app-status file is a boolean wearing a word: only "enabled" — and an
// absent file, which reads as the empty string — leaves the app served.
func TestEnabledByStatus(t *testing.T) {
	for status, want := range map[string]bool{
		"":         true,
		"enabled":  true,
		"disabled": false,
		// A word this server does not know is not a status it interprets: it
		// serves the app or it does not, and anything but "enabled" is does
		// not. A typo disabling the app is the safe way round, and the
		// reloader logs the word so it is not a mystery.
		"restricted": false,
		"enbaled":    false,
	} {
		if got := EnabledByStatus(status); got != want {
			t.Errorf("EnabledByStatus(%q) = %v, want %v", status, got, want)
		}
	}
}
