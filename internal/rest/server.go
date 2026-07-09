// Package rest implements the REST endpoints exposed by ably-server.
//
// Endpoints: POST /channels/{name}/messages (publish), GET
// /channels/{name}/messages (history), GET /time, GET /healthz, GET
// /readyz.
package rest

import (
	"context"
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
	ready   storage.Pinger
}

// NewServer constructs a Server. The Manager pairs each Channel with
// its storage facet — publishes go through Channel.Publish, which
// delegates to the storage backend. ready, if non-nil, is consulted by
// HandleReadyz on every request (see DESIGN.md §2.2); callers pass nil
// for backends with no external dependency to check (memory, bbolt).
func NewServer(keys []auth.APIKey, manager *core.Manager, logger *slog.Logger, ready storage.Pinger) *Server {
	return &Server{
		authn:   auth.NewAuthenticator(keys...),
		manager: manager,
		logger:  logger,
		ready:   ready,
	}
}

// HandlePublish authenticates the request, parses the body (a single
// Message or an array of Messages, JSON or msgpack), and appends each
// to the named channel.
func (s *Server) HandlePublish(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	clientID, ok := s.resolveRequestClientID(w, r, principal)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "channel name required", http.StatusBadRequest)
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpPublish) {
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
	// POST creates messages only. A mutation (update/delete/append)
	// carries a non-create action and a target serial; it is published
	// via PATCH .../messages/{serial} (DESIGN.md §13.6), never here.
	for _, m := range msgs {
		if m.Action != protocol.MessageCreate {
			http.Error(w, fmt.Sprintf("action %s not permitted on POST; use PATCH for mutations", m.Action), http.StatusBadRequest)
			return
		}
		// Resolve and stamp each message's clientId against the request's
		// identity (DESIGN.md §3.2), as for the realtime publish path.
		cid, ok := auth.MessageClientID(clientID, m.ClientID)
		if !ok {
			http.Error(w, "message clientId not permitted", http.StatusBadRequest)
			return
		}
		m.ClientID = cid
	}

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
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
	cm, _, err := ch.Publish(r.Context(), msgs)
	if errors.Is(err, storage.ErrInvalidMessageID) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		s.logger.Warn("publish failed", "channel", name, "err", err)
		http.Error(w, "publish failed", http.StatusInternalServerError)
		return
	}

	// Ably's publish response (RSL1): 201 with {channel, messageId}. The
	// messageId is the server-stamped id of the publish's first message —
	// "<batchID>:0" (DESIGN.md §8) — which is exactly the id carried on the
	// delivered MESSAGE frame for this publish, and matches Ably's observed
	// shape (e.g. "TojWzTkLiH:0").
	respBody, err := marshalValue(publishResponse{Channel: name, MessageID: publishMessageID(cm)}, respFormat)
	if err != nil {
		s.logger.Warn("publish encode failed", "channel", name, "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(respFormat))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respBody)
}

// publishResponse is the REST POST /messages response body (Ably RSL1):
// the channel name and the publish's messageId.
type publishResponse struct {
	Channel   string `json:"channel"   msgpack:"channel"`
	MessageID string `json:"messageId" msgpack:"messageId"`
}

// publishMessageID returns the messageId for a publish response: the
// stamped id of the first message in the batch ("<batchID>:0"), which is
// the value delivered on the wire for that message (DESIGN.md §8).
func publishMessageID(cm *protocol.ChannelMessage) string {
	if len(cm.Messages) == 0 {
		return cm.ID
	}
	return cm.Messages[0].ID
}

// updateDeleteResponse is the REST PATCH /messages/{serial} response
// body: the new version's serial.
type updateDeleteResponse struct {
	VersionSerial string `json:"versionSerial,omitempty" msgpack:"versionSerial,omitempty"`
}

// marshalValue encodes an arbitrary value in the requested wire format.
func marshalValue(v any, format protocol.Format) ([]byte, error) {
	switch format {
	case protocol.FormatJSON:
		return json.Marshal(v)
	case protocol.FormatMsgpack:
		return msgpack.Marshal(v)
	}
	return nil, fmt.Errorf("unsupported format")
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
// The `history` capability op is required (DESIGN.md §3.1).
func (s *Server) HandleHistory(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "channel name required", http.StatusBadRequest)
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The default message history collapses to the latest version of each
	// message positioned at its create serial (DESIGN.md §13.4): an edited
	// message keeps its place but shows current content, a deleted one
	// shows as a tombstone. (Raw stream order is only for live/resume.)
	q.Collapse = true

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

// HandleMutate applies a mutation (update/delete/append) to an existing
// message: PATCH /channels/{name}/messages/{serial} (DESIGN.md §13.2,
// §13.6). The target serial is in the path; the body is a single Message
// carrying the action and the fields to mix in (JSON or msgpack via
// Content-Type). On success it returns the resulting merged version in
// the Accept format. A missing/aged-out target is a 404.
//
// No capability/ownership gating is applied: the capability framework
// (TASK-12) is unbuilt, so the endpoint requires only the API key, like
// publish. TASK-51 adds the message-* ownership check once TASK-12 lands.
func (s *Server) HandleMutate(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	name := r.PathValue("name")
	target := r.PathValue("serial")
	if name == "" || target == "" {
		http.Error(w, "channel name and message serial required", http.StatusBadRequest)
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
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	var mut protocol.Message
	if err := unmarshal(body, format, &mut); err != nil {
		http.Error(w, fmt.Sprintf("decode mutation: %v", err), http.StatusBadRequest)
		return
	}
	if !mut.Action.IsMutation() {
		http.Error(w, fmt.Sprintf("action %s is not a mutation; PATCH requires update/delete/append", mut.Action), http.StatusBadRequest)
		return
	}
	// The target serial comes from the path — it is authoritative.
	mut.Serial = target

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
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
	cm, _, err := ch.Mutate(r.Context(), &mut)
	if errors.Is(err, storage.ErrTargetNotFound) {
		http.Error(w, "target message not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("mutate failed", "channel", name, "target", target, "err", err)
		http.Error(w, "mutate failed", http.StatusInternalServerError)
		return
	}

	// Respond with the new version serial (the SDK's UpdateDeleteResult
	// wire shape, RSL15e); the full merged version is available via
	// GET .../messages/{serial} and .../versions.
	out, err := marshalValue(updateDeleteResponse{VersionSerial: storage.VersionSerial(cm.Messages[0])}, respFormat)
	if err != nil {
		s.logger.Warn("mutate encode failed", "channel", name, "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(respFormat))
	_, _ = w.Write(out)
}

// HandleMessage returns the latest version of a single message —
// GET /channels/{name}/messages/{serial} (DESIGN.md §13.4) — or its
// tombstone if deleted. A message that never existed (or aged out) is a
// 404. Gated by the `history` capability op (DESIGN.md §3.1).
func (s *Server) HandleMessage(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	serial := r.PathValue("serial")
	if name == "" || serial == "" {
		http.Error(w, "channel name and message serial required", http.StatusBadRequest)
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
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
	m, err := ch.LatestVersion(r.Context(), serial)
	if errors.Is(err, storage.ErrTargetNotFound) {
		http.Error(w, "message not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("message read failed", "channel", name, "serial", serial, "err", err)
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}

	out, err := marshalMessage(m, format)
	if err != nil {
		s.logger.Warn("message encode failed", "channel", name, "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(out)
}

// HandleMessageVersions returns every version of a message ordered by
// version — GET /channels/{name}/messages/{serial}/versions (DESIGN.md
// §13.4) — paginated with the same Link convention as message history,
// except the cursor is a version serial. A message with no versions is a
// 404. Gated by the `history` capability op (DESIGN.md §3.1).
func (s *Server) HandleMessageVersions(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	serial := r.PathValue("serial")
	if name == "" || serial == "" {
		http.Error(w, "channel name and message serial required", http.StatusBadRequest)
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
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
	page, err := ch.Versions(r.Context(), serial, q)
	if errors.Is(err, storage.ErrTargetNotFound) {
		http.Error(w, "message not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("versions failed", "channel", name, "serial", serial, "err", err)
		http.Error(w, "versions failed", http.StatusInternalServerError)
		return
	}

	out := flattenHistory(page.ChannelMessages)
	resp, err := marshalBody(out, format)
	if err != nil {
		s.logger.Warn("versions encode failed", "channel", name, "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}

	writeLinkHeaders(w, r, lastVersionSerial(page), page.HasMore)
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(resp)
}

// HandlePresence returns the channel's current presence set as a flat
// array of PresenceMessages, each stamped action=PRESENT (DESIGN.md
// §12.6). Format follows the Accept header.
//
// Gated by the `subscribe` capability op (DESIGN.md §3.1).
func (s *Server) HandlePresence(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "channel name required", http.StatusBadRequest)
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpSubscribe) {
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
	members, _, err := ch.Members(r.Context())
	if err != nil {
		s.logger.Warn("members failed", "channel", name, "err", err)
		http.Error(w, "presence failed", http.StatusInternalServerError)
		return
	}

	body, err := marshalPresence(presentMembers(members), format)
	if err != nil {
		s.logger.Warn("presence encode failed", "channel", name, "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(body)
}

// HandlePresenceHistory returns the channel's presence history — a flat
// array of PresenceMessages from the presence stream (DESIGN.md §12.6).
// It reuses the message-history query shape and Link-header pagination,
// scanning the presence kind. Gated by the `history` capability op
// (DESIGN.md §3.1).
func (s *Server) HandlePresenceHistory(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "channel name required", http.StatusBadRequest)
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q.Kind = storage.KindPresence

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
		s.logger.Warn("presence history failed", "channel", name, "err", err)
		http.Error(w, "history failed", http.StatusInternalServerError)
		return
	}

	body, err := marshalPresence(flattenPresence(page.ChannelMessages), format)
	if err != nil {
		s.logger.Warn("presence history encode failed", "channel", name, "err", err)
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

// readyzTimeout bounds the dependency check HandleReadyz performs on
// every request, so a wedged database can't hang the probe.
const readyzTimeout = 2 * time.Second

// HandleHealthz is the liveness probe: it reports 200 "ok" as soon as
// the process is serving HTTP, with no dependency checks. No auth.
func (s *Server) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok")
}

// HandleReadyz is the readiness probe: it reports whether the server
// is ready to take traffic. In memory/disk mode (s.ready is nil)
// that's always true. In cluster mode it pings Postgres and returns
// 503 when the database is unreachable, so orchestrators stop routing
// to a node that can't serve (DESIGN.md §2.2). No auth.
func (s *Server) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
		defer cancel()
		if err := s.ready.Ping(ctx); err != nil {
			s.logger.Warn("readyz: dependency unreachable", "err", err)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready")
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok")
}

// authenticate writes a 401 response on failure and returns false; on
// success it returns true.
// HandleRequestToken mints an Ably-compatible JWT for a signed (or
// Basic-authenticated) TokenRequest and returns it as application/jwt
// (DESIGN.md §3). The token is signed with the key's secret (HS256), so a
// client can present it as an access_token that this server then verifies
// via the normal token path.
func (s *Server) HandleRequestToken(w http.ResponseWriter, r *http.Request) {
	keyName := r.PathValue("keyName")

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
	var tr auth.TokenRequest
	if err := unmarshal(body, format, &tr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if tr.KeyName == "" {
		tr.KeyName = keyName
	}
	if tr.KeyName != keyName {
		http.Error(w, "keyName mismatch between path and body", http.StatusBadRequest)
		return
	}

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	if err := s.authn.ValidateTokenRequest(&tr, r); err != nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="ably-server"`)
		http.Error(w, "invalid token request", http.StatusUnauthorized)
		return
	}

	token, issued, expires, err := s.authn.MintToken(&tr)
	if err != nil {
		s.logger.Warn("mint token failed", "err", err)
		http.Error(w, "token minting failed", http.StatusInternalServerError)
		return
	}

	// The SDK decodes the requestToken response as a TokenDetails; the
	// minted JWT rides in its Token field (DESIGN.md §3). issued/expires
	// are milliseconds since epoch.
	respBody, err := marshalValue(tokenDetailsResponse{
		Token:      token,
		KeyName:    tr.KeyName,
		Issued:     issued.UnixMilli(),
		Expires:    expires.UnixMilli(),
		ClientID:   tr.ClientID,
		Capability: tr.Capability,
	}, respFormat)
	if err != nil {
		s.logger.Warn("token encode failed", "err", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(respFormat))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBody)
}

// tokenDetailsResponse is the requestToken response (Ably TokenDetails).
// issued/expires are milliseconds since epoch; Token carries the JWT.
type tokenDetailsResponse struct {
	Token      string `json:"token" msgpack:"token"`
	KeyName    string `json:"keyName,omitempty" msgpack:"keyName,omitempty"`
	Issued     int64  `json:"issued" msgpack:"issued"`
	Expires    int64  `json:"expires" msgpack:"expires"`
	ClientID   string `json:"clientId,omitempty" msgpack:"clientId,omitempty"`
	Capability string `json:"capability,omitempty" msgpack:"capability,omitempty"`
}

// authenticate verifies the request's credentials. On success it returns
// the verified principal and true; on failure it writes a 401 and returns
// false. Handlers pass the returned principal to authorize for the
// per-endpoint capability check (DESIGN.md §3.1).
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (*auth.Principal, bool) {
	principal, err := s.authn.Authenticate(r)
	if err == nil {
		return principal, true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="ably-server"`)
	switch {
	case errors.Is(err, auth.ErrNoCredentials):
		http.Error(w, "no credentials presented", http.StatusUnauthorized)
	default:
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	}
	return nil, false
}

// errorResponse is the Ably REST error wire shape (`{"error":{...}}`)
// used for capability rejections (DESIGN.md §3.1).
type errorResponse struct {
	Error *protocol.ErrorInfo `json:"error" msgpack:"error"`
}

// authorize reports whether the principal's capability grants op on
// channel (DESIGN.md §3.1). On failure it writes a 401 carrying the Ably
// error shape (code 40160) and returns false.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request, p *auth.Principal, channel string, op auth.Op) bool {
	if p.Capabilities().Permits(channel, op) {
		return true
	}
	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		format = protocol.FormatJSON
	}
	body, _ := marshalValue(errorResponse{Error: &protocol.ErrorInfo{
		Message:    fmt.Sprintf("insufficient capability: %q required for channel %q", op, channel),
		Code:       40160,
		StatusCode: http.StatusUnauthorized,
	}}, format)
	w.Header().Set("Content-Type", contentTypeFor(format))
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write(body)
	return false
}

// resolveRequestClientID applies the §3.2 resolution for a REST request:
// the verified principal plus the clientId query param yield the request's
// clientId. On a disallowed param it writes a 401 and returns ok=false.
func (s *Server) resolveRequestClientID(w http.ResponseWriter, r *http.Request, p *auth.Principal) (string, bool) {
	clientID, err := auth.ResolveClientID(p, r.URL.Query().Get("clientId"))
	if err != nil {
		http.Error(w, "clientId not permitted by credential", http.StatusUnauthorized)
		return "", false
	}
	return clientID, true
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

// lastMessageSerial returns the Serial of the trailing item in the page
// (the boundary against which a `next` cursor is built), or "" if the
// page is empty. Handles both kinds: a message page's last Message, or a
// presence page's last PresenceMessage.
func lastMessageSerial(page storage.HistoryPage) string {
	if len(page.ChannelMessages) == 0 {
		return ""
	}
	cm := page.ChannelMessages[len(page.ChannelMessages)-1]
	if n := len(cm.Messages); n > 0 {
		return cm.Messages[n-1].Serial
	}
	if n := len(cm.Presence); n > 0 {
		return cm.Presence[n-1].Serial
	}
	return ""
}

// flattenPresence turns a page of presence ChannelMessages into the flat
// []PresenceMessage wire shape, concatenating in storage order (the
// backend has already applied direction-aware reordering).
func flattenPresence(cms []*protocol.ChannelMessage) []*protocol.PresenceMessage {
	total := 0
	for _, cm := range cms {
		total += len(cm.Presence)
	}
	out := make([]*protocol.PresenceMessage, 0, total)
	for _, cm := range cms {
		out = append(out, cm.Presence...)
	}
	return out
}

// presentMembers copies members for a presence-set response, stamping
// each with action PRESENT (DESIGN.md §12.6). Members may return
// pointers into live backend state, so we copy rather than mutate.
func presentMembers(members []*protocol.PresenceMessage) []*protocol.PresenceMessage {
	out := make([]*protocol.PresenceMessage, len(members))
	for i, m := range members {
		cp := *m
		cp.Action = protocol.PresencePresent
		out[i] = &cp
	}
	return out
}

// marshalPresence encodes a presence slice using the requested format,
// normalising nil to an empty array (matching marshalBody for messages).
func marshalPresence(v []*protocol.PresenceMessage, format protocol.Format) ([]byte, error) {
	if v == nil {
		v = []*protocol.PresenceMessage{}
	}
	switch format {
	case protocol.FormatJSON:
		return json.Marshal(v)
	case protocol.FormatMsgpack:
		return msgpack.Marshal(v)
	}
	return nil, fmt.Errorf("unsupported format")
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

// writeHistoryLinks emits the pagination Link headers for a message or
// presence history page; the next-cursor boundary is the page's last
// item serial.
func writeHistoryLinks(w http.ResponseWriter, r *http.Request, page storage.HistoryPage) {
	writeLinkHeaders(w, r, lastMessageSerial(page), page.HasMore)
}

// writeLinkHeaders emits RFC 5988 Link headers: rel="current" (request
// URL verbatim), rel="first" (request URL minus the opaque cursor), and
// — when hasMore and boundary is non-empty — rel="next" carrying the
// cursor that should bound the next request. Clients are required to
// treat the link URLs opaquely; the cursor's parameter name and value
// are internal-only.
func writeLinkHeaders(w http.ResponseWriter, r *http.Request, boundary string, hasMore bool) {
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

	if boundary != "" && hasMore {
		next := current
		nextQ := next.Query()
		// Pagination strictly excludes the cursor in the requested
		// direction (matching Ably's REST).
		nextQ.Set(internalCursorParam, boundary)
		next.RawQuery = nextQ.Encode()
		links = append(links, fmt.Sprintf(`<%s>; rel="next"`, next.RequestURI()))
	}

	w.Header().Set("Link", strings.Join(links, ", "))
}

// lastVersionSerial returns the version serial of the trailing message in
// a versions page — the cursor boundary for version-history pagination
// (each version's own serial, not the shared message identity).
func lastVersionSerial(page storage.HistoryPage) string {
	if len(page.ChannelMessages) == 0 {
		return ""
	}
	cm := page.ChannelMessages[len(page.ChannelMessages)-1]
	if n := len(cm.Messages); n > 0 {
		return storage.VersionSerial(cm.Messages[n-1])
	}
	return ""
}

// marshalMessage encodes a single Message using the requested format —
// used by the single-message read and the mutation result.
func marshalMessage(m *protocol.Message, format protocol.Format) ([]byte, error) {
	switch format {
	case protocol.FormatJSON:
		return json.Marshal(m)
	case protocol.FormatMsgpack:
		return msgpack.Marshal(m)
	}
	return nil, fmt.Errorf("unsupported format")
}
