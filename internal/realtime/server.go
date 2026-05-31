// Package realtime implements the WebSocket realtime endpoint.
package realtime

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/id"
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
	upgrader          websocket.Upgrader
}

// NewServer constructs a Server. The Manager pairs each Channel with
// its storage facet — publishes go through Channel.Publish, which
// delegates to the storage backend.
func NewServer(key auth.APIKey, manager *core.Manager, heartbeatInterval time.Duration, logger *slog.Logger) *Server {
	return &Server{
		authn:             auth.NewAuthenticator(key),
		manager:           manager,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
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
// and runs the connection loop. Auth failures are returned as HTTP 401
// before the upgrade.
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	if err := s.authn.Authenticate(r); err != nil {
		s.writeAuthError(w, err)
		return
	}

	format, err := protocol.FormatFromQuery(r.URL.Query().Get("format"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written the error response.
		s.logger.Debug("websocket upgrade failed", "err", err)
		return
	}

	connID := id.NewConnectionID()
	conn := &connection{
		ws:                ws,
		format:            format,
		id:                connID,
		heartbeatInterval: s.heartbeatInterval,
		logger:            s.logger.With("connId", connID),
		manager:           s.manager,
		outbound:          make(chan *protocol.ProtocolMessage, 16),
		attachments:       make(map[string]*attachment),
	}
	conn.run(r.Context())
}

func (s *Server) writeAuthError(w http.ResponseWriter, err error) {
	w.Header().Set("WWW-Authenticate", `Basic realm="ably-server"`)
	switch {
	case errors.Is(err, auth.ErrNoCredentials):
		http.Error(w, "no credentials presented", http.StatusUnauthorized)
	default:
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	}
}
