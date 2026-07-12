// Package realtime implements the WebSocket realtime endpoint.
package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/trace"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
)

// DefaultHeartbeatInterval is the cadence of server-driven HEARTBEAT
// frames when no other outbound frame has been sent.
const DefaultHeartbeatInterval = 15 * time.Second

// Server holds the realtime endpoint's state. Its HTTP handlers are
// exported methods; callers register them on their own ServeMux.
type Server struct {
	authn             *auth.Authenticator
	manager           *core.Manager
	heartbeatInterval time.Duration
	logger            *slog.Logger
	metrics           *metrics.Metrics
	tracer            trace.Tracer
	upgrader          websocket.Upgrader

	// mu guards conns, the registry of live connections used by Shutdown
	// to disconnect them gracefully on SIGTERM (DESIGN.md §11).
	mu    sync.Mutex
	conns map[*connection]struct{}
}

// NewServer constructs a Server. The Manager pairs each Channel with
// its storage facet — publishes go through Channel.Publish, which
// delegates to the storage backend.
func NewServer(keys []auth.APIKey, manager *core.Manager, heartbeatInterval time.Duration, logger *slog.Logger, m *metrics.Metrics, tracer trace.Tracer) *Server {
	return &Server{
		authn:             auth.NewAuthenticator(keys...),
		manager:           manager,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
		metrics:           m,
		tracer:            tracer,
		conns:             make(map[*connection]struct{}),
		upgrader: websocket.Upgrader{
			// Tests use httptest.Server which sets up a same-origin
			// connection; production deployments terminate TLS at a
			// reverse proxy, so cross-origin is the norm. Auth is the
			// real boundary, not Origin.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// HandleWebSocket authenticates the request, upgrades to a WebSocket,
// and runs the connection loop. A fatal auth failure does NOT reject the
// upgrade with HTTP 401 — ably-js (and Ably) treat a failed upgrade as a
// transport error and retry (DISCONNECTED), never reaching FAILED. Instead
// the upgrade completes and the server sends an in-band ERROR frame
// carrying the Ably auth error (40101 invalid credentials, 40142 token
// expired, etc.), which the SDK maps to the FAILED state, then closes
// (DESIGN.md §2.1, §3; mirrors the reference frontdoor's closeWithError).
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Resolve the wire format before upgrading; a bad format is a genuine
	// bad request, not an auth failure, so it stays an HTTP-level rejection.
	format, err := protocol.FormatFromQuery(r.URL.Query().Get("format"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Authenticate and resolve the clientId, but defer surfacing any failure
	// until after the upgrade so it can be sent as an in-band ERROR.
	principal, authErr := s.authn.Authenticate(r)
	var clientID string
	if authErr == nil {
		clientID, authErr = auth.ResolveClientID(principal, r.URL.Query().Get("clientId"))
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written the error response.
		s.logger.Debug("websocket upgrade failed", "err", err)
		return
	}

	if authErr != nil {
		s.rejectWithError(ws, format, authErr)
		return
	}

	connID := id.NewConnectionID()

	// A resume/recover attempt with a malformed connection key is declined
	// per protocol (DESIGN.md §4.3): the connection still succeeds with a
	// fresh connectionId, but the CONNECTED carries error 80018 so the SDK
	// treats the resume/recover as failed (RTN15c7, RTN16e). A well-formed
	// key is left un-errored — connection-state resume is a non-goal, so the
	// SDK recovers message flow via per-channel re-attach instead.
	resumeKey := r.URL.Query().Get("resume")
	if resumeKey == "" {
		resumeKey = r.URL.Query().Get("recover")
	}
	var resumeError *protocol.ErrorInfo
	if resumeKey != "" && !id.ValidConnectionID(resumeKey) {
		resumeError = &protocol.ErrorInfo{
			Code:       80018,
			StatusCode: 400,
			Message:    "invalid connection key; resume/recover could not be satisfied",
		}
	}

	conn := &connection{
		ws:                ws,
		format:            format,
		id:                connID,
		clientID:          clientID,
		principal:         principal,
		authn:             s.authn,
		cap:               principal.Capabilities(),
		tokenExpiry:       principal.ExpiresAt,
		heartbeatInterval: s.heartbeatInterval,
		echo:              echoFromQuery(r.URL.Query().Get("echo")),
		logger:            s.logger.With("connId", connID),
		manager:           s.manager,
		metrics:           s.metrics,
		tracer:            s.tracer,
		outbound:          make(chan *protocol.ProtocolMessage, 16),
		attachments:       make(map[string]*attachment),
		entered:           make(map[string]map[string]struct{}),
		publishQ:          make(chan func(), 16),
		reauth:            make(chan time.Time, 1),
		resumeError:       resumeError,
		lastMsgSerial:     -1,
	}

	// The upgrade succeeded: count the connection and time its lifetime,
	// bracketing conn.run so the gauge and lifetime histogram stay
	// balanced whichever way the loop exits (DESIGN.md §10).
	opened := time.Now()
	s.metrics.ConnectionOpened()
	defer func() { s.metrics.ConnectionClosed(time.Since(opened).Seconds()) }()

	s.register(conn)
	defer s.deregister(conn)
	conn.run(r.Context())
}

// register adds a live connection to the shutdown registry.
func (s *Server) register(c *connection) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

// deregister removes a connection from the shutdown registry once its
// run loop has returned.
func (s *Server) deregister(c *connection) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Shutdown gracefully disconnects every live WebSocket connection,
// pacing the closures evenly across the window implied by ctx's deadline
// to avoid a thundering-herd reconnect against the next node (DESIGN.md
// §11). Each connection is sent a DISCONNECTED frame and then closed,
// which drives its normal teardown (including synthesised presence
// LEAVEs). When ctx's deadline is reached, any remaining stragglers are
// force-closed at once. Shutdown returns once every connection has been
// disconnected (or the deadline forced them closed).
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	conns := make([]*connection, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if len(conns) == 0 {
		return
	}

	interval := pacingInterval(ctx, len(conns))
	for i, c := range conns {
		c.disconnect()
		if i == len(conns)-1 {
			return
		}
		select {
		case <-ctx.Done():
			// Deadline hit: force-close the remaining stragglers now
			// rather than continuing to pace past the window.
			for _, straggler := range conns[i+1:] {
				straggler.forceClose()
			}
			return
		case <-time.After(interval):
		}
	}
}

// pacingInterval spreads n closures evenly across the window implied by
// ctx's deadline: closures fire at 0, interval, 2*interval, …, leaving a
// 1/n slice of headroom before the deadline. With no deadline (or none
// left) it returns 0, closing everything promptly.
func pacingInterval(ctx context.Context, n int) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok || n <= 0 {
		return 0
	}
	window := time.Until(dl)
	if window <= 0 {
		return 0
	}
	return window / time.Duration(n)
}

// echoFromQuery resolves the `echo` upgrade param. Ably defaults echo to
// true; only an explicit, valid boolean flips it (a malformed value is
// ignored, keeping the safe default of echoing).
func echoFromQuery(v string) bool {
	if v == "" {
		return true
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return true
	}
	return b
}

// rejectWithError sends a connection-level ERROR frame carrying the Ably
// auth error (code/statusCode from auth.AuthErrorInfo) on the freshly
// upgraded socket, then closes it. No CONNECTED precedes it, so the SDK
// treats the connection as never established and moves it to FAILED for a
// non-renewable error (40101/40102), or attempts token renewal for a
// renewable one (40142) — never a plain transport retry (DESIGN.md §3).
// The close code mirrors the reference: a normal closure for renewable
// token errors, a policy violation otherwise.
func (s *Server) rejectWithError(ws *websocket.Conn, format protocol.Format, err error) {
	code, statusCode, msg := auth.AuthErrorInfo(err)
	frame := &protocol.ProtocolMessage{
		Action: protocol.ActionError,
		Error: &protocol.ErrorInfo{
			Message:    msg,
			Code:       code,
			StatusCode: statusCode,
		},
	}
	if data, mErr := protocol.Marshal(frame, format); mErr == nil {
		wsType := websocket.TextMessage
		if format == protocol.FormatMsgpack {
			wsType = websocket.BinaryMessage
		}
		_ = ws.WriteMessage(wsType, data)
	}
	closeCode := websocket.ClosePolicyViolation
	if code >= 40140 && code < 40150 {
		closeCode = websocket.CloseNormalClosure
	}
	_ = ws.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(closeCode, msg),
		time.Now().Add(time.Second),
	)
	_ = ws.Close()
}
