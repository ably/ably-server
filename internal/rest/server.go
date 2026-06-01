// Package rest implements the REST endpoints exposed by ably-server.
//
// Endpoints: POST /channels/{name}/messages (publish), GET
// /channels/{name}/messages (history), GET /time, GET /healthz, GET
// /readyz.
package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Server holds the REST endpoint's state. Its HTTP handlers are
// exported methods; callers register them on their own ServeMux.
type Server struct {
	authn   *auth.Authenticator
	manager *core.Manager
	logger  *slog.Logger
}

// NewServer constructs a Server. The Manager pairs each Channel with
// its storage facet — publishes go through Channel.Publish, which
// delegates to the storage backend.
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

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		http.Error(w, "channel unavailable", http.StatusInternalServerError)
		return
	}
	if _, _, err := ch.Publish(r.Context(), msgs); err != nil {
		s.logger.Warn("publish failed", "channel", name, "err", err)
		http.Error(w, "publish failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleHistory authenticates the request, parses the Ably-SDK query
// shape (direction/start/end/limit + an internal opaque cursor in the
// page links), and returns a flat array of Messages from the channel's
// stored history.
//
// The response format follows the Accept header (application/json by
// default; application/x-msgpack supported). Pagination uses RFC 5988
// Link headers with rel="current", rel="first", and (when more results
// exist) rel="next" — clients are required to treat the URLs opaquely.
//
// Capability enforcement (the `history` op) is deferred to TASK-12;
// today the endpoint requires only the API key.
func (s *Server) HandleHistory(w http.ResponseWriter, r *http.Request) {
	if !s.authenticate(w, r) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "channel name required", http.StatusBadRequest)
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		http.Error(w, "channel unavailable", http.StatusInternalServerError)
		return
	}
	page, err := ch.History(r.Context(), q)
	if err != nil {
		s.logger.Warn("history failed", "channel", name, "err", err)
		http.Error(w, "history failed", http.StatusInternalServerError)
		return
	}

	out := flattenHistory(page.ChannelMessages)
	body, err := marshalBody(out, format)
	if err != nil {
		s.logger.Warn("history encode failed", "channel", name, "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}

	writeHistoryLinks(w, r, page)
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(body)
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

const (
	defaultHistoryLimit = 100
	maxHistoryLimit     = 1000

	// internalCursorParam is the query-string key used in the rel=next
	// link to carry the opaque pagination cursor. Clients are required
	// to treat link URLs as opaque, so the parameter name is internal —
	// it just needs to be stable for the server-to-server round-trip.
	internalCursorParam = "from"
)

// parseHistoryQuery maps the Ably-SDK query shape (direction, start,
// end, limit) plus the internal opaque cursor onto a storage.HistoryQuery.
// All params are optional; the defaults match Ably (direction=backwards,
// limit=100, no time bounds).
func parseHistoryQuery(values url.Values) (storage.HistoryQuery, error) {
	q := storage.HistoryQuery{
		Direction: storage.DirectionBackwards,
		Limit:     defaultHistoryLimit,
	}

	if v := values.Get("direction"); v != "" {
		switch v {
		case "backwards":
			q.Direction = storage.DirectionBackwards
		case "forwards":
			q.Direction = storage.DirectionForwards
		default:
			return q, fmt.Errorf("direction: must be 'backwards' or 'forwards'")
		}
	}

	if v := values.Get("start"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return q, fmt.Errorf("start: must be a non-negative integer (ms since epoch)")
		}
		q.Start = n
	}

	if v := values.Get("end"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return q, fmt.Errorf("end: must be a non-negative integer (ms since epoch)")
		}
		q.End = n
	}

	if v := values.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxHistoryLimit {
			return q, fmt.Errorf("limit: must be an integer between 1 and %d", maxHistoryLimit)
		}
		q.Limit = n
	}

	q.Cursor = values.Get(internalCursorParam)
	return q, nil
}

// acceptFormat resolves an Accept header to a protocol format. Missing
// or "*/*" defaults to JSON. Only the SDK-relevant formats are
// honoured; q-values are ignored — the first parsable type wins.
func acceptFormat(accept string) (protocol.Format, error) {
	if accept == "" {
		return protocol.FormatJSON, nil
	}
	for part := range strings.SplitSeq(accept, ",") {
		mediaType := strings.TrimSpace(part)
		if i := strings.Index(mediaType, ";"); i >= 0 {
			mediaType = strings.TrimSpace(mediaType[:i])
		}
		switch mediaType {
		case "*/*", "application/*", "application/json":
			return protocol.FormatJSON, nil
		case "application/x-msgpack", "application/msgpack":
			return protocol.FormatMsgpack, nil
		}
	}
	return 0, fmt.Errorf("no acceptable response format in Accept %q", accept)
}

// contentTypeFor returns the canonical Content-Type string for a
// protocol format.
func contentTypeFor(format protocol.Format) string {
	switch format {
	case protocol.FormatMsgpack:
		return "application/x-msgpack"
	default:
		return "application/json"
	}
}

// flattenHistory turns a page of ChannelMessages into the flat
// []Message wire shape Ably's SDKs expect from a history call.
// Backends are responsible for direction-aware reordering (including
// reversing Messages within each ChannelMessage when backwards), so
// this just concatenates in storage order.
func flattenHistory(cms []*protocol.ChannelMessage) []*protocol.Message {
	total := 0
	for _, cm := range cms {
		total += len(cm.Messages)
	}
	out := make([]*protocol.Message, 0, total)
	for _, cm := range cms {
		out = append(out, cm.Messages...)
	}
	return out
}

// lastMessageSerial returns the Serial of the trailing Message in the
// page (the boundary against which a `next` cursor is built), or "" if
// the page is empty.
func lastMessageSerial(page storage.HistoryPage) string {
	if len(page.ChannelMessages) == 0 {
		return ""
	}
	cm := page.ChannelMessages[len(page.ChannelMessages)-1]
	if len(cm.Messages) == 0 {
		return ""
	}
	return cm.Messages[len(cm.Messages)-1].Serial
}

// marshalBody encodes v using the requested format. JSON encodes nil
// slices as "null" by default — we normalise to "[]" so an empty
// history page is a well-formed empty array, matching Ably and most
// REST clients' expectations.
func marshalBody(v []*protocol.Message, format protocol.Format) ([]byte, error) {
	if v == nil {
		v = []*protocol.Message{}
	}
	switch format {
	case protocol.FormatJSON:
		return json.Marshal(v)
	case protocol.FormatMsgpack:
		return msgpack.Marshal(v)
	}
	return nil, fmt.Errorf("unsupported format")
}

// writeHistoryLinks emits RFC 5988 Link headers: rel="current"
// (request URL verbatim), rel="first" (request URL minus the opaque
// cursor), and — when more results exist past the page — rel="next"
// carrying the cursor that should bound the next request.
//
// Clients are required to treat the link URLs opaquely; the cursor's
// parameter name and value are internal-only.
func writeHistoryLinks(w http.ResponseWriter, r *http.Request, page storage.HistoryPage) {
	current := *r.URL
	current.Host, current.Scheme = "", ""

	first := current
	firstQ := first.Query()
	firstQ.Del(internalCursorParam)
	first.RawQuery = firstQ.Encode()

	links := []string{
		fmt.Sprintf(`<%s>; rel="current"`, current.RequestURI()),
		fmt.Sprintf(`<%s>; rel="first"`, first.RequestURI()),
	}

	if boundary := lastMessageSerial(page); boundary != "" && page.HasMore {
		next := current
		nextQ := next.Query()
		// The cursor is a Message.Serial (`<channelSerial>:<idx>`),
		// matching Ably's REST: pagination strictly excludes the
		// cursor in the requested direction.
		nextQ.Set(internalCursorParam, boundary)
		next.RawQuery = nextQ.Encode()
		links = append(links, fmt.Sprintf(`<%s>; rel="next"`, next.RequestURI()))
	}

	w.Header().Set("Link", strings.Join(links, ", "))
}
