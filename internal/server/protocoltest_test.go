package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/integration"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"

	protocol "github.com/ably/server-protocol/go"
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/protocoltest"
	"github.com/ably/server-protocol/go/random"
	"github.com/ably/server-protocol/go/scope"
	"github.com/ably/server-protocol/go/wire"
)

// TestProtocol runs the shared module's suite against this server: the module
// states what it requires of a server embedding it, and this is where this
// server is held to it.
//
// It runs once per storage backend the server can be configured with. What a
// client observes is meant not to depend on where the server keeps its state,
// so the suite is the same for all three and only the storage differs.
func TestProtocol(t *testing.T) {
	for _, backend := range []struct {
		name string
		// integration marks a backend needing something outside the
		// process, so the suite only runs against it when asked for.
		integration bool
		storage     func(t *testing.T) storage.Storage
	}{
		{
			name:    "Memory",
			storage: func(*testing.T) storage.Storage { return memory.New(memory.Options{}) },
		},
		{
			name: "Bbolt",
			storage: func(t *testing.T) storage.Storage {
				s, err := bbolt.Open(bbolt.Options{Path: filepath.Join(t.TempDir(), "ably.db")})
				if err != nil {
					t.Fatalf("opening bbolt storage: %s", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
		{
			name:        "Postgres",
			integration: true,
			storage: func(t *testing.T) storage.Storage {
				dsn := pgtest.Start(t).FreshSchemaDSN(t)
				s, err := postgres.Open(t.Context(), postgres.Options{DSN: dsn})
				if err != nil {
					t.Fatalf("opening postgres storage: %s", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.integration {
				integration.Require(t)
			}
			protocoltest.Run(t, newProtocolTarget(backend.storage))
		})
	}
}

// newProtocolTarget returns the suite's way of bringing up a whole server for
// the app it asked for, keeping its state in the storage given:
// this server serves one app, so an app is a server rather than something
// provisioned on one.
//
// Everything about the app that can change while it runs — its keys, its
// namespaces, its status — is written to a watched config directory rather
// than passed in (DESIGN.md §9.1), and the server is left watching it. That is
// what lets this target implement the suite's updaters: a test asking for a
// key's capability to change gets the same code path an operator editing that
// file would, rather than a back door into the app.
//
// Its storage is in memory and nothing else shares it, so each target is an
// empty server with the suite's app on it.
func newProtocolTarget(newStorage func(t *testing.T) storage.Storage) protocoltest.NewTargetFunc {
	return func(ctx context.Context, t *testing.T, spec protocoltest.AppSpec) protocoltest.Target {
		t.Helper()

		appID := "app" + random.String(8)

		dir := t.TempDir()
		sources := config.Dynamic{
			KeysDir:       filepath.Join(dir, "keys"),
			NamespacesDir: filepath.Join(dir, "namespaces"),
			AppStatusFile: filepath.Join(dir, "app-status"),
		}
		for _, d := range []string{sources.KeysDir, sources.NamespacesDir} {
			if err := os.Mkdir(d, 0o755); err != nil {
				t.Fatalf("making the watched config directory: %s", err)
			}
		}

		target := &protocolTarget{
			sources: sources,
			app:     protocoltest.App{ID: appID},
		}

		for _, spec := range spec.Keys {
			keyID := "key" + random.String(8)
			secret := "secret" + random.String(8)
			target.keys = append(target.keys, protocoltest.Key{Name: appID + "." + keyID, Secret: secret})
			target.writeKey(t, keyID, config.KeyEntry{
				Key:        appID + "." + keyID + ":" + secret,
				Capability: spec.Capability,
			})
		}
		for _, ns := range spec.Namespaces {
			target.UpdateNamespace(ctx, t, ns)
		}

		watched, err := sources.Read()
		if err != nil {
			t.Fatalf("reading the watched config: %s", err)
		}
		specs := make([]keySpec, 0, len(watched.Keys))
		for _, entry := range watched.Keys {
			specs = append(specs, keySpec{key: entry.Key, capability: entry.Capability})
		}
		keys, _, err := parseAPIKeys(specs)
		if err != nil {
			t.Fatalf("parsing the app's keys: %s", err)
		}

		logger := logging.New(slog.DiscardHandler)
		store := core.NewManager(newStorage(t))

		shared, err := newSharedProtocol(ctx, keys, watched.Namespaces, store, nil, logger, sharedOptions{})
		if err != nil {
			t.Fatalf("wiring the shared protocol code: %s", err)
		}
		t.Cleanup(shared.Close)

		rs := rest.NewServer(keys, logger, nil)
		reload := &reloader{
			sources: sources,
			appID:   appID,
			app:     shared.App(),
			rest:    rs,
			log:     logger,
		}
		if err := reload.load(); err != nil {
			t.Fatalf("applying the watched config: %s", err)
		}
		// Faster than the production cadence, so a test waiting on a change
		// waits on the change rather than on the poll.
		go reload.run(t.Context(), 20*time.Millisecond)

		srv := httptest.NewServer(newMux(shared, rs, nil, false))
		t.Cleanup(srv.Close)

		target.protocol = shared.Protocol
		target.url = srv.URL
		return target
	}
}

// protocolTarget is one server, serving one app, for the suite to test.
//
// It changes the app the way this server means one to be changed: by writing
// the watched config sources it was started against, and letting the server
// pick the change up on its own.
type protocolTarget struct {
	protocol *protocol.Protocol
	url      string
	app      protocoltest.App
	keys     []protocoltest.Key
	sources  config.Dynamic
}

var (
	_ protocoltest.KeyUpdater       = (*protocolTarget)(nil)
	_ protocoltest.NamespaceUpdater = (*protocolTarget)(nil)
	_ protocoltest.AppDisabler      = (*protocolTarget)(nil)
)

// UpdateKey rewrites the key's file with the capability given. The key keeps
// the secret it already has: what the suite is changing is what the key
// grants, not who holds it.
func (s *protocolTarget) UpdateKey(_ context.Context, t *testing.T, keyID, capability string) {
	t.Helper()

	for _, key := range s.keys {
		if _, id, _ := strings.Cut(key.Name, "."); id == keyID {
			s.writeKey(t, keyID, config.KeyEntry{Key: key.Name + ":" + key.Secret, Capability: capability})
			return
		}
	}
	t.Fatalf("the app has no key %q to update", keyID)
}

// UpdateNamespace rewrites the namespace's file with the settings given. Only
// the settings this server configures are written; the rest of a wire
// namespace is not something it has anywhere to put.
func (s *protocolTarget) UpdateNamespace(_ context.Context, t *testing.T, ns *wire.Namespace) {
	t.Helper()

	writeConfigEntry(t, s.namespacePath(ns.GetId()), config.Namespace{
		ID:              ns.GetId(),
		Mode:            ns.GetMode(),
		Persisted:       ns.GetPersisted(),
		MutableMessages: ns.GetMutableMessages(),
		PushEnabled:     ns.GetPushEnabled(),
	})
}

// DisableApp writes the app's status file, which is how this server is told
// its app can no longer be served.
func (s *protocolTarget) DisableApp(_ context.Context, t *testing.T) {
	t.Helper()

	writeFileAtomically(t, s.sources.AppStatusFile, []byte("disabled\n"))
}

// writeKey writes one key's file, named for the key id so that updating a key
// rewrites its own file rather than adding a second one for the same key.
func (s *protocolTarget) writeKey(t *testing.T, keyID string, entry config.KeyEntry) {
	t.Helper()

	writeConfigEntry(t, filepath.Join(s.sources.KeysDir, keyID+".toml"), entry)
}

// namespacePath is the file a namespace is written to. The id is hex-encoded
// because a namespace id is a channel-name expression, which may hold anything
// including the one byte a filename may not.
func (s *protocolTarget) namespacePath(id string) string {
	return filepath.Join(s.sources.NamespacesDir, hex.EncodeToString([]byte(id))+".toml")
}

// writeConfigEntry writes one TOML config entry.
func writeConfigEntry(t *testing.T, path string, entry any) {
	t.Helper()

	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(entry); err != nil {
		t.Fatalf("encoding %s: %s", path, err)
	}
	writeFileAtomically(t, path, buf.Bytes())
}

// writeFileAtomically writes the file through a rename, so the watching server
// never reads a half-written one and complains about a file that is about to
// be fine.
func writeFileAtomically(t *testing.T, path string, content []byte) {
	t.Helper()

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		t.Fatalf("writing %s: %s", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("renaming %s into place: %s", path, err)
	}
}

func (*protocolTarget) Name() string                   { return "ably-server" }
func (s *protocolTarget) Protocol() *protocol.Protocol { return s.protocol }
func (s *protocolTarget) URL() string                  { return s.url }
func (s *protocolTarget) App() protocoltest.App        { return s.app }
func (s *protocolTarget) Keys() []protocoltest.Key     { return s.keys }

// Publish stores each message through the channel this server gave the module,
// which is the path a client's publish takes: it routes a message, a mutation,
// an annotation or a state operation to the part of storage that takes it.
func (s *protocolTarget) Publish(ctx context.Context, t *testing.T, channelName string, msgs []*wire.ChannelMessage) []*wire.Timeserial {
	t.Helper()

	ch := s.channel(ctx, t, channelName)

	serials := make([]*wire.Timeserial, 0, len(msgs))
	for _, msg := range msgs {
		resp := s.publish(ctx, t, ch, msg)

		// One thing published, so one serial, whatever the map keys it under: a
		// message, a state operation and an annotation are each keyed by an id
		// of their own.
		if len(resp.Timeserials) != 1 {
			t.Fatalf("the publish of %q reported %d serials, and this arranges one at a time", msg.GetId(), len(resp.Timeserials))
		}
		for _, serial := range resp.Timeserials {
			serials = append(serials, serial)
		}
	}
	return serials
}

// PublishPresence enters, updates or leaves each member the same way, as one
// presence publish per message so that each is stored in the order given.
func (s *protocolTarget) PublishPresence(ctx context.Context, t *testing.T, channelName string, msgs []*wire.PresenceMessage) {
	t.Helper()

	ch := s.channel(ctx, t, channelName)

	for _, msg := range msgs {
		s.publish(ctx, t, ch, &wire.ChannelMessage{
			Id:       random.String(8),
			Action:   wire.ProtocolMessageAction_ACTION_PRESENCE,
			Presence: []*wire.PresenceMessage{msg},
		})
	}
}

// publish sends one message and returns the outcome, which arrives on the
// queue once storage has taken it.
func (s *protocolTarget) publish(ctx context.Context, t *testing.T, ch channel.Channel, msg *wire.ChannelMessage) channel.Response {
	t.Helper()

	queue := live.NewQueue[channel.Response]()
	if errInfo := ch.Publish(ctx, msg, queue); errInfo != nil {
		t.Fatalf("the publish of %q was refused: %s", msg.GetId(), errInfo)
	}

	resp, err := queue.Pop(ctx)
	if err != nil {
		t.Fatalf("no outcome arrived for the publish of %q: %s", msg.GetId(), err)
	}
	if resp.Error != nil {
		t.Fatalf("the publish of %q was accepted and then failed: %s", msg.GetId(), resp.Error)
	}
	return resp
}

// channel borrows the named channel from the module's manager, which is where
// every channel this server serves comes from, so what is published here
// reaches the same storage a served attachment reads.
func (s *protocolTarget) channel(ctx context.Context, t *testing.T, name string) channel.Channel {
	t.Helper()

	spec, errInfo := channel.ParseSpec(name, scope.New(scope.App, s.app.ID))
	if errInfo != nil {
		t.Fatalf("the channel name %q did not parse: %s", name, errInfo)
	}

	ref, errInfo := s.protocol.Config().Channels.GetChannel(ctx, spec)
	if errInfo != nil {
		t.Fatalf("the channel manager would not lend %q: %s", name, errInfo)
	}

	// The reference is what keeps the channel alive, and a Channel value is not
	// something the collector can see it through.
	t.Cleanup(func() { runtime.KeepAlive(ref) })

	return ref.Get()
}
