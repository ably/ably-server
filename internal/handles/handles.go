// Package handles is this server's side of the shared protocol module's
// boundary: what it hands that code in order to service a request.
//
// The module declares the handles as interfaces and never names anything of
// ours; everything here is the answer to one of its questions, given from this
// server's config, keys and storage. Where an answer is "nothing to say" —
// this server has no limits and no accounting — the method says so plainly
// rather than being left to a default, so that reading this file tells you
// what this server does not do.
package handles

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"

	"github.com/ably/server-protocol/go/analytics"
	protoapp "github.com/ably/server-protocol/go/app"
	"github.com/ably/server-protocol/go/app/appid"
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/handles"
	"github.com/ably/server-protocol/go/live"
	protocollog "github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/scope"
	"github.com/ably/server-protocol/go/unixtime"
	"github.com/ably/server-protocol/go/wire"
)

// Manager builds a handle for each request. It holds what every handle needs:
// the one app this server serves, and the logger to scope.
type Manager struct {
	app      *App
	channels *core.Manager
	metrics  *metrics.Metrics
	log      *logging.Logger
}

var _ handles.Manager = (*Manager)(nil)

// NewManager returns a handle manager serving app out of channels, logging to
// log and counting to m. m may be nil, in which case nothing is counted.
func NewManager(app *App, channels *core.Manager, m *metrics.Metrics, log *logging.Logger) *Manager {
	return &Manager{app: app, channels: channels, metrics: m, log: log}
}

func (b *Manager) NewRequest(_ context.Context, req *http.Request, logFields ...any) handles.Request {
	return &request{
		manager: b,
		appID:   appid.FromRequest(req),
		log:     b.log.Protocol().With(logFields...),
	}
}

// request is a request before its app has been resolved.
//
// appID is the app the request asked for, which this server reads the same way
// the protocol code does. It is not necessarily the app it gets: a request
// naming another app is refused when it resolves.
type request struct {
	manager *Manager
	appID   string
	log     protocollog.Logger
}

func (r *request) Log() protocollog.Logger { return r.log }
func (r *request) AppID() string           { return r.appID }

func (r *request) App(context.Context) (handles.App, *errors.ErrorInfo) {
	if r.appID != "" && r.appID != r.manager.app.id {
		return nil, errors.New(40400, 404, "Application not found")
	}
	return &appHandle{request: r, servedApp: r.manager.app}, nil
}

// AppForConnection is App: this server holds an app for as long as it is
// running, so resolving one for a connection is no different from resolving
// one for a single call.
func (r *request) AppForConnection(ctx context.Context) (handles.App, *errors.ErrorInfo) {
	return r.App(ctx)
}

// Nothing is counted, here or on the app handle: this server keeps no
// per-app accounting, and what happened to a request is in its access log.
func (r *request) RecordRESTResponse(*errors.ErrorInfo)                 {}
func (r *request) RecordStateResponse(*errors.ErrorInfo)                {}
func (r *request) RecordConnectionResponse(*errors.ErrorInfo, string)   {}
func (r *request) RecordTransportRejected(string, *errors.ErrorInfo)    {}
func (a *appHandle) RecordRESTResponse(*errors.ErrorInfo)               {}
func (a *appHandle) RecordStateResponse(*errors.ErrorInfo)              {}
func (a *appHandle) RecordConnectionResponse(*errors.ErrorInfo, string) {}

// appHandle is a request whose app is known: a request and an app at once,
// which is what the protocol code asks for.
//
// The app is embedded under an alias, because embedding *App itself would give
// the handle a field named App, colliding with the App method it gets from the
// request.
type appHandle struct {
	*request
	servedApp
}

type servedApp = *App

var _ handles.App = (*appHandle)(nil)

// MessageStore is where a read of this channel's persisted history is
// answered from. Building one reads nothing and brings no channel live: it
// holds the app and the channel asked for, and goes to storage only when a
// read is actually made.
func (a *appHandle) MessageStore(spec *channel.Spec) handles.MessageStore {
	return &messageStore{app: a, spec: spec}
}

func (a *appHandle) Connection(connectionID, transport string) handles.Connection {
	return &connectionHandle{
		log:     a.log.With("connId", connectionID, "transport", transport),
		metrics: a.manager.metrics,
		opened:  time.Now(),
	}
}

// ConnectionRefusal is a connection that was never established, so nothing
// about its lifetime is counted.
func (a *appHandle) ConnectionRefusal() handles.Connection {
	return &connectionHandle{log: a.log}
}

// ClientIDLimiter is nil: this server does not limit how many connections one
// clientId may hold.
func (a *appHandle) ClientIDLimiter() handles.ClientIDLimiter { return nil }

// App is the one app this server serves. Its keys, its namespaces and whether
// it is served at all are what a watched config source can change while the
// process runs (DESIGN.md §9.1); its identity is not, since this server is its
// app.
type App struct {
	protoapp.NopLimits
	handles.NopReporter

	// metrics counts this app's traffic, and is nil when nothing is counted.
	metrics *metrics.Metrics

	id       string
	appScope scope.ID

	// namespaces is live, so that a namespace edited while the server runs
	// reaches the channels already resolved against it (DESIGN.md §9.1). It is
	// fed whole rather than a change at a time: this server re-reads its whole
	// configuration and knows what is absent as well as what is present.
	namespaces *protoapp.NamespaceMap

	// keysMu guards keys. A key is replaced by setting the value its
	// reference already points at, so a connection holding one sees the change
	// without re-resolving; the map itself is only rebuilt under the lock.
	keysMu sync.Mutex
	keys   map[string]*live.Reference[*protoapp.Key]

	// enabled is whether the app is served, and fatal is what an established
	// connection is closed with once it is not. They are set together and
	// always agree: fatal is the same fact in the terms the module's
	// connections read it in.
	enabled atomic.Bool
	fatal   *live.Value[*errors.ErrorInfo]
}

// NewApp builds the app from the configured keys and namespaces, enabled. The
// app id is the one every key shares, which config loading has already
// checked.
func NewApp(appID string, keys []auth.APIKey, namespaces []config.Namespace, m *metrics.Metrics) (*App, error) {
	app := &App{
		metrics:    m,
		id:         appID,
		appScope:   scope.New(scope.App, appID),
		namespaces: protoapp.NewNamespaceMap(),
		keys:       map[string]*live.Reference[*protoapp.Key]{},
		fatal:      live.NewValue[*errors.ErrorInfo](nil),
	}
	app.enabled.Store(true)
	app.SetNamespaces(namespaces)

	// The namespaces are read before the listener opens, so the map is loaded
	// as soon as it is built: nothing ever waits to find out whether a
	// namespace it cannot find is absent or merely not read yet.
	app.namespaces.SetLoaded()

	if err := app.SetKeys(keys); err != nil {
		return nil, err
	}
	return app, nil
}

func (a *App) ID() string                         { return a.id }
func (a *App) Scope() scope.ID                    { return a.appScope }
func (a *App) Namespaces() *protoapp.NamespaceMap { return a.namespaces }

// SetNamespaces replaces the app's namespaces, which reaches the channels
// already attached: a channel watches the namespace it resolved to, so one
// whose settings change here starts behaving differently without being
// reattached.
func (a *App) SetNamespaces(configured []config.Namespace) {
	a.namespaces.Replace(wireNamespaces(configured))
}

// SetKeys replaces the app's API keys.
//
// A key that is still configured keeps the reference it already had, its value
// replaced only where something about it actually changed — so a connection
// that resolved the key sees the new capability without re-authenticating, and
// one whose key was not touched sees nothing at all. A key that is no longer
// configured is marked gone before it is dropped, which is how a holder of it
// finds out rather than going on using a key that is no longer there.
//
// Every key is built before any is installed, so a malformed capability leaves
// the app exactly as it was rather than half reconfigured.
func (a *App) SetKeys(keys []auth.APIKey) error {
	built := make([]*protoapp.Key, 0, len(keys))
	for _, k := range keys {
		if k.AppID != a.id {
			return fmt.Errorf("key %s belongs to app %s, not %s", k.Name(), k.AppID, a.id)
		}
		capabilities, err := resourceCapabilities(k, a.appScope)
		if err != nil {
			return err
		}
		built = append(built, &protoapp.Key{
			ID:           k.KeyID,
			AppID:        k.AppID,
			Value:        []byte(k.KeySecret),
			ValuePadded:  protoapp.PadKeyValue(k.KeySecret),
			Capability:   k.CapabilityString(),
			Capabilities: capabilities,
			Status:       protoapp.KeyStatusEnabled,
		})
	}

	a.keysMu.Lock()
	defer a.keysMu.Unlock()

	now := unixtime.Now()
	next := make(map[string]*live.Reference[*protoapp.Key], len(built))
	for _, key := range built {
		ref, ok := a.keys[key.ID]
		if !ok {
			key.Created, key.Modified = now, now
			next[key.ID] = live.NewReference(live.NewRefCounter(key, nil))
			continue
		}
		next[key.ID] = ref
		prev := ref.Get()
		key.Created = prev.Created
		if sameKey(prev, key) {
			continue
		}
		// Modified has to move, since it is how the protocol code orders two
		// versions of a key. Two edits within the same millisecond would
		// otherwise land on the same timestamp and the second look like no
		// change at all.
		key.Modified = max(now, prev.Modified+1)
		ref.Set(key)
	}
	for id, ref := range a.keys {
		if _, ok := next[id]; ok {
			continue
		}
		gone := *ref.Get()
		gone.Status = protoapp.KeyStatusGone
		gone.Modified = max(now, gone.Modified+1)
		ref.Set(&gone)
	}
	a.keys = next
	return nil
}

// sameKey reports whether two versions of a key differ in anything this
// server configures. Created and Modified are excluded: they are how a change
// is reported, not part of what changed.
func sameKey(a, b *protoapp.Key) bool {
	return a.Status == b.Status &&
		a.Capability == b.Capability &&
		bytes.Equal(a.Value, b.Value)
}

// SetEnabled sets whether the app is served. Disabling it refuses every new
// request and connection, and closes every established one; enabling it again
// restores both.
func (a *App) SetEnabled(enabled bool) {
	if a.enabled.Swap(enabled) == enabled {
		return
	}
	if enabled {
		a.fatal.Set(nil)
		return
	}
	a.fatal.Set(errAppDisabled)
}

// Enabled reports whether the app is currently served.
func (a *App) Enabled() bool { return a.enabled.Load() }

// Whether the app is served, in the three questions the module asks about it.
// This server draws no distinction between them: it has one app, served or
// not, so an app that will not take a connection will not answer a REST
// request either.
//
// All three read the fatal error rather than deriving one, so that what a
// request is refused with and what an established connection is closed with
// cannot come apart, and so that the common case — an enabled app, checked on
// every request — is one atomic load and a nil.
func (a *App) CheckStatus() *errors.ErrorInfo               { return a.fatal.Get() }
func (a *App) CheckStatusForAPIRequests() *errors.ErrorInfo { return a.fatal.Get() }
func (a *App) CheckStatusForConnections() *errors.ErrorInfo { return a.fatal.Get() }

// FatalError is what an established connection must be closed with once the
// app stops being serviceable, and holds nil while it is. It is a watched
// value rather than a check, because a connection that was admitted has
// nowhere left to be refused: it has to be told.
func (a *App) FatalError() *live.Value[*errors.ErrorInfo] { return a.fatal }

// This server has no accounts, so the app stands in for its own: one app, one
// billing relationship, nothing above it to aggregate to.
func (a *App) AccountID() string      { return a.id }
func (a *App) AccountScope() scope.ID { return a.appScope }

// WatchKey resolves one of the configured keys, and keeps resolving it: the
// reference tracks whatever SetKeys does to that key afterwards, so a
// long-lived connection sees its capability narrowed, or the key removed,
// without having to authenticate again.
func (a *App) WatchKey(_ context.Context, keyID string) (*live.Reference[*protoapp.Key], *errors.ErrorInfo) {
	a.keysMu.Lock()
	defer a.keysMu.Unlock()

	key, ok := a.keys[keyID]
	if !ok {
		return nil, errors.New(40400, 404, "Key not found")
	}
	return key, nil
}

// The app settings the protocol code asks about. This server accepts what a
// client sends, propagates no traces, identifies no clients by force, and
// bills nothing — so a meta channel is never billable.
// A message published counts whichever way it arrived: over a connection,
// which is RecordConnectionInboundSuccess, or over REST, which is one kind of
// API request. The publish latency this server used to time is the storage
// commit, which happens inside the publish the protocol code makes rather than
// being something it reports.
func (a *App) RecordConnectionInboundSuccess(wire.ChannelMessageLike) {
	if a.metrics != nil {
		a.metrics.MessagePublished(0)
	}
}

func (a *App) RecordAPIRequestInbound(count, _ int64, action wire.ProtocolMessageAction, err *errors.ErrorInfo) {
	if a.metrics == nil || err != nil || action != wire.ProtocolMessageAction_ACTION_MESSAGE {
		return
	}
	for range count {
		a.metrics.MessagePublished(0)
	}
}

func (a *App) RecordOutboundRealtime(_, _ wire.ChannelMessageLike, _ int64) {
	if a.metrics != nil {
		a.metrics.MessageDelivered()
	}
}

// MaxTokenTTL is zero: this server has no per-app settings, so the cap on a
// token's lifetime is the configured one, which the module applies itself.
func (a *App) MaxTokenTTL() time.Duration { return 0 }

func (a *App) AllowInvalidExtrasInRestMessages() bool { return false }
func (a *App) TracePropagationEnabled() bool          { return false }
func (a *App) ClientIDEnforcementEnabled() bool       { return false }
func (a *App) ConcurrentTokenLimitExempt() bool       { return true }
func (a *App) LegacyCapabilityWildcardExempt() bool   { return false }
func (a *App) PusherCompatibilityEnabled() bool       { return false }
func (a *App) IsMetaChannelBillable(string) bool      { return false }

// connectionHandle is what the protocol code reports one connection through.
// Nothing is recorded: this server keeps no analytics, and what it wants in
// the log it logs itself.
type connectionHandle struct {
	log     protocollog.Logger
	metrics *metrics.Metrics
	opened  time.Time
}

var _ handles.Connection = (*connectionHandle)(nil)

func (c *connectionHandle) Log() protocollog.Logger { return c.log }

// MessageLogs are the connection's log: this server has no separate sink for
// decoded messages or raw frames, and its trace level already carries them.
func (c *connectionHandle) MessageLogs() (messages, raw protocollog.Logger) {
	return c.log, c.log
}

// No connection is rate limited, so every counter admits everything.
func (c *connectionHandle) InboundRateCounter() protoapp.RateCounter  { return protoapp.NopRateCounter }
func (c *connectionHandle) OutboundRateCounter() protoapp.RateCounter { return protoapp.NopRateCounter }
func (c *connectionHandle) BacklogRateCounter() protoapp.RateCounter  { return protoapp.NopRateCounter }

func (c *connectionHandle) ProtocolMessageRateCounter() protoapp.RateCounter {
	return protoapp.NopRateCounter
}

// Sampled keeps every connection's analytics, which costs nothing when
// nothing is collected.
func (c *connectionHandle) Sampled() bool { return true }

// The connection's lifecycle, its traffic and what its client was told. None
// of it is recorded: this server has no analytics pipeline, and the protocol
// code already logs through the handle's logger. They are spelled out rather
// than embedded from a no-op, so that adding one later means filling in a
// method here rather than discovering the list.
func (c *connectionHandle) RecordOpened(bool, *errors.ErrorInfo, analytics.ConnectionEvent) {
	if c.metrics != nil {
		c.metrics.ConnectionOpened()
	}
}

func (c *connectionHandle) RecordClosed(analytics.ConnectionEvent) {
	if c.metrics != nil {
		c.metrics.ConnectionClosed(time.Since(c.opened).Seconds())
	}
}

func (c *connectionHandle) RecordAttached(*errors.ErrorInfo, analytics.ConnectionEvent) {
	if c.metrics != nil {
		c.metrics.AttachmentOpened()
	}
}

func (c *connectionHandle) RecordRefused(bool, *errors.ErrorInfo, analytics.ConnectionEvent) {}
func (c *connectionHandle) RecordError(analytics.ConnectionEvent)                            {}
func (c *connectionHandle) RecordDetached(*errors.ErrorInfo, analytics.ConnectionEvent)      {}
func (c *connectionHandle) RecordResponse(*errors.ErrorInfo)                                 {}
func (c *connectionHandle) RecordChannelResponse(*errors.ErrorInfo)                          {}
func (c *connectionHandle) RecordMessageResponse(uint32, *errors.ErrorInfo)                  {}
func (c *connectionHandle) RecordLifecycle(string, any)                                      {}

func (c *connectionHandle) RecordInboundError(wire.ChannelMessageLike, *errors.ErrorInfo, analytics.ConnectionEvent) {
}
