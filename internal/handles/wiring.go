package handles

import (
	"context"
	"net/http"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/serial"

	protocol "github.com/ably/server-protocol/go"
	"github.com/ably/server-protocol/go/analytics"
	protoauth "github.com/ably/server-protocol/go/auth"
	"github.com/ably/server-protocol/go/conf"
	"github.com/ably/server-protocol/go/connection"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/wire"
)

// This file wires the shared protocol code to this server: everything the
// module's Config asks for that is not a channel, a handle or a key.
//
// Everything below is a collaborator that Config names. They are implemented
// rather than stubbed wherever this server has an answer, because an answer
// that is wrong in an interesting way is what this is for.

// Instance identifies the server a connection is served from. Realtime asks
// its netmap; a client is told the site so it can be routed back to the same
// one, and the site code goes in the connection key.
//
// This server is one process with no cluster and no routing, so all four are
// constants — but they cannot be empty, because the site code is part of every
// connection key it mints.
type Instance struct{}

var _ connection.Instance = Instance{}

func (Instance) ID() string     { return "ably-server" }
func (Instance) NodeID() string { return "ably-server" }
func (Instance) Site() string   { return "ably-local" }

// SiteCode is the serial package's, not a constant of its own. A client
// applying a LiveObjects operation optimistically on its ACK keys it by the
// site code it was given here, and the echo that follows keys it by the one
// read off the serial — so the two have to be the same string, and the way to
// guarantee that is to have one of them (DESIGN.md §15.1).
func (Instance) SiteCode() string { return serial.SiteCode }

// Sites is which sites' connection keys are accepted. There is one.
type Sites struct{}

var _ protoauth.Sites = Sites{}

func (Sites) MySite() string     { return "ably-local" }
func (Sites) GetSites() []string { return []string{"ably-local"} }

// TokenGetter resolves a token this server stored rather than handed to the
// client whole. Realtime persists a token whose claims are too large to carry;
// this server has nowhere to persist one, so it never issues one and is never
// asked for one back.
type TokenGetter struct{}

var _ protoauth.TokenGetter = TokenGetter{}

func (TokenGetter) GetToken(context.Context, string, string) (*wire.Token, *errors.ErrorInfo) {
	return nil, errors.New(40143, 401, "Token unrecognised")
}

// NewAuthManager builds the shared auth manager against this server.
func NewAuthManager(conf *conf.Auth, log *logging.Logger) *protoauth.Manager {
	return protoauth.NewManager(
		conf,
		TokenGetter{},
		protoauth.NoopRevocationManager,
		Sites{},
		log.Protocol(),
	)
}

// Analytics is where the module reports what happened to an API request and
// what cluster to attribute a connection to. This server keeps no analytics:
// what it wants written down it writes to its access log, and it serves one
// unnamed cluster.
type Analytics struct{}

var _ protocol.Analytics = Analytics{}

func (Analytics) ClusterInfo() analytics.ClusterInfo { return analytics.ClusterInfo{} }

func (Analytics) LogRequest(*http.Request, string, *protoauth.Params, *errors.ErrorInfo, []string) {
}

func (Analytics) LogRequestWithStats(*http.Request, string, *protoauth.Params, *errors.ErrorInfo, *analytics.AnalyticsStats, []string) {
}

// LogChannelStats and RecordRequestCounters are the split a request touching
// several channels needs: one analytics event per channel, one request-level
// count for the request. Both fold into the same nothing here.
func (Analytics) LogChannelStats(*http.Request, string, *protoauth.Params, *errors.ErrorInfo, *analytics.AnalyticsStats, string) {
}

func (Analytics) RecordRequestCounters(*http.Request, string, *protoauth.Params, *errors.ErrorInfo) {
}

// Protocol is the shared protocol module assembled against this server, held
// with the channel manager whose lifecycle this server owns.
//
// The module's own Protocol is embedded, so its Routes and its REST and
// connection surfaces are reached straight through this.
type Protocol struct {
	*protocol.Protocol

	app      *App
	channels *ChannelManager
}

// App is the one app this Protocol serves, which is where a reconfiguration
// of its keys, its namespaces or its status is applied.
func (p *Protocol) App() *App { return p.app }

// Close stops the work the channels do on their own behalf, and the collection
// of idle ones. Connections are shed separately, by cancelling the requests
// serving them.
func (p *Protocol) Close() { p.channels.Close() }

// New assembles the shared protocol code against this server: the channel
// manager makes the channels, the handle manager makes the per-request
// handles, and the module does everything a client can observe.
//
// The channel manager is started here rather than by its caller, because a
// Protocol that is serving and a manager that is not collecting is not a state
// worth being able to reach.
//
// The module's two process-wide seams are deliberately left as they come. Its
// panic hook is unset, so a panic it recovers is re-raised and takes the
// process down rather than being swallowed by a server with nowhere to report
// it; its HTML hook is unset, so ?format=html is refused, this server having
// no renderer. Its Prometheus collectors are likewise left unregistered: what
// this server exposes is the low-cardinality set DESIGN.md §10 enumerates, and
// the module's are named for the server they were written in.
func New(ctx context.Context, c *conf.Conf, app *App, handles *Manager, channels *ChannelManager, authMgr *protoauth.Manager, log *logging.Logger) (*Protocol, error) {
	if err := channels.Start(ctx); err != nil {
		return nil, err
	}
	return &Protocol{
		app: app,
		Protocol: protocol.New(protocol.Config{
			Conf:      c,
			Log:       log.Protocol(),
			Handles:   handles,
			Channels:  channels.Public(),
			Auth:      authMgr,
			Analytics: Analytics{},
			Instance:  Instance{},
		}),
		channels: channels,
	}, nil
}
