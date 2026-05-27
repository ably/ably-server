// Package rest implements the REST endpoints exposed by ably-server.
//
// At this stage the endpoints are: POST /channels/{name}/messages
// (publish), GET /time, GET /healthz, GET /readyz. History is not yet
// implemented — it'll land alongside the storage backend.
package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
)

// Server holds the REST endpoint's state. Its HTTP handlers are
// exported methods; callers register them on their own ServeMux.
type Server struct {
	authn   *auth.Authenticator
	manager *core.Manager
	logger  *slog.Logger
}

// NewServer constructs a Server. All arguments are required; callers
// supply an explicit Manager and logger.
func NewServer(key auth.APIKey, manager *core.Manager, logger *slog.Logger) *Server {
	return &Server{
		authn:   auth.NewAuthenticator(key),
		manager: manager,
		logger:  logger,
	}
}

// HandlePublish authenticates the request, parses the body (a single
// Message or an array of Messages, JSON or msgpack), and appends each
// to the named channel.
func (s *Server) HandlePublish(w http.ResponseWriter, r *http.Request) {
	if !s.authenticate(w, r) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "channel name required", http.StatusBadRequest)
		return
	}

	format, err := contentTypeFormat(r.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msgs, err := parseMessages(body, format)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(msgs) == 0 {
		http.Error(w, "no messages", http.StatusBadRequest)
		return
	}

	ch := s.manager.GetChannel(name)
	for _, m := range msgs {
		ch.Append(m)
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleTime returns the server's current time in Ably's wire form: a
// JSON array containing one element, milliseconds since the Unix epoch.
func (s *Server) HandleTime(w http.ResponseWriter, r *http.Request) {
	out, err := json.Marshal([]int64{time.Now().UnixMilli()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// HandleHealthz returns a 200 OK response with body "ok". No auth.
func (s *Server) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok")
}

// HandleReadyz returns a 200 OK response with body "ok". No auth.
func (s *Server) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok")
}

// authenticate writes a 401 response on failure and returns false; on
// success it returns true.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) bool {
	err := s.authn.Authenticate(r)
	if err == nil {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="ably-server"`)
	switch {
	case errors.Is(err, auth.ErrNoCredentials):
		http.Error(w, "no credentials presented", http.StatusUnauthorized)
	default:
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	}
	return false
}

// contentTypeFormat resolves a Content-Type header to a protocol
// format. Missing/empty defaults to JSON.
func contentTypeFormat(ct string) (protocol.Format, error) {
	// Strip parameters (e.g. "; charset=utf-8").
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "", "application/json":
		return protocol.FormatJSON, nil
	case "application/x-msgpack", "application/msgpack":
		return protocol.FormatMsgpack, nil
	default:
		return 0, fmt.Errorf("unsupported Content-Type %q", ct)
	}
}

// parseMessages decodes a publish body. The body may be a single
// Message object or an array of Messages.
func parseMessages(body []byte, format protocol.Format) ([]*protocol.Message, error) {
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	if looksLikeArray(body, format) {
		var arr []*protocol.Message
		if err := unmarshal(body, format, &arr); err != nil {
			return nil, fmt.Errorf("decode array: %w", err)
		}
		return arr, nil
	}
	var single protocol.Message
	if err := unmarshal(body, format, &single); err != nil {
		return nil, fmt.Errorf("decode message: %w", err)
	}
	return []*protocol.Message{&single}, nil
}

// looksLikeArray reports whether the encoded body's outermost value is
// an array.
func looksLikeArray(body []byte, format protocol.Format) bool {
	switch format {
	case protocol.FormatJSON:
		for _, b := range body {
			switch b {
			case ' ', '\t', '\n', '\r':
				continue
			}
			return b == '['
		}
		return false
	case protocol.FormatMsgpack:
		first := body[0]
		// fixarray (0x90–0x9f), array16 (0xdc), array32 (0xdd)
		return (first&0xf0) == 0x90 || first == 0xdc || first == 0xdd
	}
	return false
}

func unmarshal(body []byte, format protocol.Format, v any) error {
	switch format {
	case protocol.FormatJSON:
		return json.Unmarshal(body, v)
	case protocol.FormatMsgpack:
		return msgpack.Unmarshal(body, v)
	}
	return fmt.Errorf("unsupported format")
}
