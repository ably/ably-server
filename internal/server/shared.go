package server

import (
	"context"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/handles"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"

	"github.com/ably/server-protocol/go/conf"
)

// connectionMacKey signs the connection keys this server hands out, which a
// client presents to resume. It is a constant because this server is one
// process with nothing to share a key with: a key it signed is only ever
// verified by itself, and only while it is running.
var connectionMacKey = []byte("ably-server-connection-key")

// sharedOptions are the settings this server's flags still configure. They are
// the protocol code's to apply; what this server does is name them on its
// command line, as it did when it served the protocol itself.
type sharedOptions struct {
	heartbeatInterval time.Duration
	remainPresentFor  time.Duration
}

// newSharedProtocol wires github.com/ably/server-protocol/go to this server.
//
// Everything a client can observe comes from there — the connection, the
// attachment, the channel semantics, the wire framing, the REST surface — and
// what this supplies is the storage underneath it, the keys, the namespaces
// and somewhere to log.
func newSharedProtocol(ctx context.Context, keys []auth.APIKey, file config.File, channels *core.Manager, m *metrics.Metrics, logger *logging.Logger, opts sharedOptions) (*handles.Protocol, error) {
	appID := "ably-server"
	if len(keys) > 0 {
		appID = keys[0].AppID
	}

	app, err := handles.NewApp(appID, keys, file.Namespaces, m)
	if err != nil {
		return nil, err
	}

	c := conf.Default()
	c.Auth.ConnectionMacKey.Store(connectionMacKey)

	// An hour is as long as a token this server issues may live. The module
	// defaults to a day; an app may tighten it further, but this server has no
	// per-app settings, so its own ceiling is the only one.
	c.Auth.MaxTokenTTL.Store(time.Hour)

	// A key may be presented over an unencrypted connection here. This server
	// serves plaintext in development with no terminator in front of it, and
	// a client connecting to it with a key is the point rather than a mistake.
	c.Auth.AllowBasicAuthWithoutTls = true

	// A client may publish MAP_CLEAR, which the module otherwise refuses as an
	// operation only a server issues — it is what clears a channel's objects
	// when they reach their time-to-live. The module defaults it off "for a
	// server serving real traffic"; this one is the other case, the same way it
	// is for basic auth above. An SDK exposes clear() as an ordinary method, so
	// a server a client cannot call it against is a server that SDK cannot be
	// developed or tested against.
	//
	// Two flags because there are two surfaces: the realtime publish path reads
	// the connection's, the REST objects endpoints read the state one.
	c.Connection.AllowMapClear.Store(true)
	c.State.AllowMapClear.Store(true)
	if opts.heartbeatInterval > 0 {
		c.WebSocket.HeartbeatInterval.Store(opts.heartbeatInterval)
	}
	if opts.remainPresentFor > 0 {
		c.Connection.DefaultRemainPresentFor.Store(opts.remainPresentFor)
	}

	return handles.New(
		ctx,
		c,
		handles.NewManager(app, channels, m, logger),
		handles.NewChannelManager(channels, c, app.Namespaces(), logger),
		handles.NewAuthManager(c.Auth, logger),
		logger,
	)
}
