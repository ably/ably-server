package server

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ably/ably-server/internal/auth"
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
// this server serves one app, whose keys and namespaces it reads before it
// starts, so an app is a server rather than something provisioned on one.
//
// Its storage is in memory and nothing else shares it, so each target is an
// empty server with the suite's app on it.
func newProtocolTarget(newStorage func(t *testing.T) storage.Storage) protocoltest.NewTargetFunc {
	return func(ctx context.Context, t *testing.T, spec protocoltest.AppSpec) protocoltest.Target {
		t.Helper()

		appID := "app" + random.String(8)

		keys := make([]auth.APIKey, 0, len(spec.Keys))
		names := make([]protocoltest.Key, 0, len(spec.Keys))
		for i, keySpec := range spec.Keys {
			name := appID + ".key" + random.String(8)
			secret := "secret" + random.String(8)

			key, err := auth.ParseAPIKeyWithCapability(name+":"+secret, keySpec.Capability)
			if err != nil {
				t.Fatalf("parsing key #%d: %s", i, err)
			}
			keys = append(keys, key)
			names = append(names, protocoltest.Key{Name: name, Secret: secret})
		}

		namespaces := make([]config.Namespace, 0, len(spec.Namespaces))
		for _, ns := range spec.Namespaces {
			namespaces = append(namespaces, config.Namespace{
				ID:              ns.GetId(),
				Persisted:       ns.GetPersisted(),
				MutableMessages: ns.GetMutableMessages(),
				PushEnabled:     ns.GetPushEnabled(),
			})
		}

		logger := logging.New(slog.DiscardHandler)
		store := core.NewManager(newStorage(t))

		shared, err := newSharedProtocol(ctx, keys, config.File{Namespaces: namespaces}, store, nil, logger, sharedOptions{})
		if err != nil {
			t.Fatalf("wiring the shared protocol code: %s", err)
		}
		t.Cleanup(shared.Close)

		srv := httptest.NewServer(newMux(shared, rest.NewServer(keys, logger, nil), nil, false))
		t.Cleanup(srv.Close)

		return &protocolTarget{
			protocol: shared.Protocol,
			url:      srv.URL,
			app:      protocoltest.App{ID: appID},
			keys:     names,
		}
	}
}

// protocolTarget is one server, serving one app, for the suite to test.
//
// It implements none of the suite's optional interfaces: this server's keys,
// apps and namespaces are read once before it starts, so there is no change
// for those tests to watch for and the suite skips them.
type protocolTarget struct {
	protocol *protocol.Protocol
	url      string
	app      protocoltest.App
	keys     []protocoltest.Key
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
