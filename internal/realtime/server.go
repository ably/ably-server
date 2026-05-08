// Package realtime implements the WebSocket realtime endpoint.
package realtime

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/protocol"
)

// DefaultHeartbeatInterval is the cadence of server-driven HEARTBEAT
// frames when no other outbound frame has been sent.
const DefaultHeartbeatInterval = 15 * time.Second

// Config holds runtime knobs for the realtime endpoint.
type Config struct {
	// HeartbeatInterval is the cadence of HEARTBEAT frames. Zero
	// resolves to DefaultHeartbeatInterval.
	HeartbeatInterval time.Duration

	// Logger receives connection-level log records. nil resolves to
	// slog.Default().
	Logger *slog.Logger
}

// Server is the WebSocket handler. It implements http.Handler.
type Server struct {
	heartbeatInterval time.Duration
	logger            *slog.Logger
	upgrader          websocket.Upgrader
}

// NewServer constructs a Server.
func NewServer(cfg Config) *Server {
	hb := cfg.HeartbeatInterval
	if hb == 0 {
		hb = DefaultHeartbeatInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		heartbeatInterval: hb,
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

// ServeHTTP upgrades to a WebSocket and runs the connection loop.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		outbound:          make(chan *protocol.ProtocolMessage, 16),
	}
	conn.run(r.Context())
}
