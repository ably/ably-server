// Package rest implements the REST API handlers exposed by ably-server:
// channel message publish/history/mutation/versions, presence and presence
// history, annotations, token requests, health/readiness checks, and gated
// stats stubs. For the authoritative, up-to-date list of routes and methods,
// see newMux in internal/server/server.go, which registers every handler in
// this package.
package rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// ConnectionResolver resolves a REST publish's connectionKey to the live
// realtime connection it names (DESIGN.md §13). The realtime Server
// implements it against its in-process connection registry; ok is false for
// a key no live connection on this node holds (which the publish path maps
// to Ably error 40006). It is an interface so the REST package does not
// depend on the realtime package and tests can supply a fake.
type ConnectionResolver interface {
	ResolveConnectionKey(key string) (connID string, ok bool)
}

// Server holds the REST endpoint's state. Its HTTP handlers are
// exported methods; callers register them on their own ServeMux.
type Server struct {
	authn   *auth.Authenticator
	manager *core.Manager
	logger  *logging.Logger
	ready   storage.Pinger
	metrics *metrics.Metrics
	// conns resolves a publish's connectionKey to a live connection for
	// publish-on-behalf (DESIGN.md §13); nil in deployments/tests without a
	// realtime endpoint, in which case any connectionKey is unresolvable.
	conns ConnectionResolver
	// tracer is nil unless OTEL tracing is enabled; guarded on every use.
	tracer trace.Tracer
}

// NewServer constructs a Server. The Manager pairs each Channel with
// its storage facet — publishes go through Channel.Publish, which
// delegates to the storage backend. ready, if non-nil, is consulted by
// HandleReadyz on every request (see DESIGN.md §2.2); callers pass nil
// for backends with no external dependency to check (memory, bbolt).
// conns resolves a publish's connectionKey for publish-on-behalf
// (DESIGN.md §13); callers pass the realtime Server, or nil when no
// realtime endpoint is mounted.
func NewServer(keys []auth.APIKey, manager *core.Manager, logger *logging.Logger, ready storage.Pinger, m *metrics.Metrics, tracer trace.Tracer, conns ConnectionResolver) *Server {
	return &Server{
		authn:   auth.NewAuthenticator(keys...),
		manager: manager,
		logger:  logger,
		ready:   ready,
		metrics: m,
		conns:   conns,
		tracer:  tracer,
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
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40010, "channel name required")
		return
	}
	// Reject an invalid channel name with Ably 40010 (DESIGN.md §4), the
	// same predicate the realtime ATTACH/publish paths apply.
	if !core.ValidChannelName(name) {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40010, "invalid channel name")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpPublish) {
		return
	}

	format, err := contentTypeFormat(r.Header.Get("Content-Type"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusUnsupportedMediaType, 40004, err.Error())
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, err.Error())
		return
	}
	msgs, err := parseMessages(body, format)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, err.Error())
		return
	}
	if len(msgs) == 0 {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, "no messages")
		return
	}
	// POST creates messages only. A mutation (update/delete/append)
	// carries a non-create action and a target serial; it is published
	// via PATCH .../messages/{serial} (DESIGN.md §13.6), never here.
	for _, m := range msgs {
		if m.Action != protocol.MessageCreate {
			s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, fmt.Sprintf("action %s not permitted on POST; use PATCH for mutations", m.Action))
			return
		}
		// Resolve and stamp each message's clientId against the request's
		// identity (DESIGN.md §3.2), as for the realtime publish path. A
		// concrete auth clientId is stamped onto a message that omits one
		// (RSL1m1 — the SDK deliberately elides the implicit clientId); a
		// wildcard/anonymous identity stamps no clientId ("*" is never a
		// concrete identity).
		cid, ok := auth.MessageClientID(clientID, m.ClientID)
		if !ok {
			s.writeErrorInfo(w, r, http.StatusBadRequest, 40012, "message clientId not permitted")
			return
		}
		m.ClientID = cid
		// Publish-on-behalf (DESIGN.md §13): a connectionKey names a live
		// realtime connection whose connectionId the stored/delivered message
		// inherits. Resolve it against this node's registry and strip the key
		// so it is never persisted or delivered (mirrors the reference, which
		// derives only connectionId from the key — clientId always comes from
		// the REST request's own identity above, never from the target
		// connection). An unresolvable key — malformed, or a connection on
		// another node (per-node registry, §1) — is Ably 40006.
		if m.ConnectionKey != "" {
			connID, ok := s.resolveConnectionKey(m.ConnectionKey)
			if !ok {
				s.writeErrorInfo(w, r, http.StatusBadRequest, 40006, "invalid connectionKey")
				return
			}
			m.ConnectionID = connID
			m.ConnectionKey = ""
		}
	}

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ctx := r.Context()
	if s.tracer != nil {
		var span trace.Span
		ctx, span = s.tracer.Start(ctx, "channel.publish",
			trace.WithAttributes(attribute.String("ably.channel", name)))
		defer span.End()
	}

	ch, err := s.manager.GetChannel(ctx, name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	accepted := time.Now()
	cm, _, err := ch.Publish(ctx, msgs)
	if errors.Is(err, storage.ErrInvalidMessageID) {
		// Ably 40031 — "invalid publish request (invalid client-specified
		// id)": a multi-message publish whose client-supplied ids don't
		// follow the required "<batchID>:<idx>" shape (RSL1k3, §8). Emit the
		// Ably error envelope so the SDK surfaces the 40031 code, not a bare
		// 400 the SDK defaults to 40000.
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40031, err.Error())
		return
	}
	if err != nil {
		s.logger.Warn("publish failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "publish failed")
		return
	}
	s.metrics.MessagePublished(time.Since(accepted).Seconds())

	// Ably's publish response (RSL1): 201 with {channel, messageId} plus a
	// serials array (RSL1n / PBR2a) the SDK's PublishWithResult reads. The
	// messageId is the server-stamped id of the publish's first message —
	// "<batchID>:0" (DESIGN.md §8) — which is exactly the id carried on the
	// delivered MESSAGE frame for this publish, and matches Ably's observed
	// shape (e.g. "TojWzTkLiH:0"). serials carries the stable identity Serial
	// of each published message in batch order, which the client then uses to
	// address the message via PATCH/GET .../messages/{serial} (§8, §13).
	respBody, err := marshalValue(publishResponse{
		Channel:   name,
		MessageID: publishMessageID(cm),
		Serials:   publishSerials(cm),
	}, respFormat)
	if err != nil {
		s.logger.Warn("publish encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(respFormat))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respBody)
}

// publishResponse is the REST POST /messages response body (Ably RSL1):
// the channel name, the publish's messageId, and the per-message serials
// (RSL1n) the SDK's PublishWithResult decodes to address each message.
type publishResponse struct {
	Channel   string   `json:"channel"           msgpack:"channel"`
	MessageID string   `json:"messageId"         msgpack:"messageId"`
	Serials   []string `json:"serials,omitempty" msgpack:"serials,omitempty"`
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

// publishSerials returns the stable identity Serial of each message in the
// publish, in batch order — the serials the client uses to address a
// message via PATCH/GET .../messages/{serial} (DESIGN.md §8, §13).
func publishSerials(cm *protocol.ChannelMessage) []string {
	serials := make([]string, len(cm.Messages))
	for i, m := range cm.Messages {
		serials[i] = m.Serial
	}
	return serials
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
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40010, "channel name required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40003, err.Error())
		return
	}
	// The default message history collapses to the latest version of each
	// message positioned at its create serial (DESIGN.md §13.4): an edited
	// message keeps its place but shows current content, a deleted one
	// shows as a tombstone. (Raw stream order is only for live/resume.)
	q.Collapse = true

	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	page, err := ch.History(r.Context(), q)
	if err != nil {
		s.logger.Warn("history failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "history failed")
		return
	}

	out := flattenHistory(page.ChannelMessages)
	body, err := marshalBody(out, format)
	if err != nil {
		s.logger.Warn("history encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
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
// The mutation is authorised against message-{update,delete}-{own,any}
// (DESIGN.md §13.5): -any waives ownership, -own requires the request's
// resolved clientId to equal the target's creator. Insufficient capability
// is a 401 with the Ably error shape.
func (s *Server) HandleMutate(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	clientID, ok := s.resolveRequestClientID(w, r, principal)
	if !ok {
		return
	}
	name := r.PathValue("name")
	target := r.PathValue("serial")
	if name == "" || target == "" {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, "channel name and message serial required")
		return
	}

	format, err := contentTypeFormat(r.Header.Get("Content-Type"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusUnsupportedMediaType, 40004, err.Error())
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, err.Error())
		return
	}
	if len(body) == 0 {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, "empty body")
		return
	}
	var mut protocol.Message
	if err := unmarshal(body, format, &mut); err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, fmt.Sprintf("decode mutation: %v", err))
		return
	}
	if !mut.Action.IsMutation() {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, fmt.Sprintf("action %s is not a mutation; PATCH requires update/delete/append", mut.Action))
		return
	}
	// The target serial comes from the path — it is authoritative.
	mut.Serial = target
	// Resolve the operating clientId for the new version (DESIGN.md §13.1).
	// The SDK serialises the operation object (clientId/description/metadata)
	// into the inbound message's version, so the operator's clientId arrives
	// as version.clientId; validate it against the request principal exactly
	// like a publish clientId, then stamp it as the operator (MergeVersion
	// reads it back from Message.ClientID). The message's creator clientId is
	// carried forward from the target by the merge, so this never overwrites
	// it.
	opClientID := ""
	if mut.Version != nil {
		opClientID = mut.Version.ClientID
	}
	stamped, ok := auth.MessageClientID(clientID, opClientID)
	if !ok {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40012, "operation clientId not permitted")
		return
	}
	mut.ClientID = stamped

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	if !s.authorizeMutation(w, r, principal, clientID, ch, name, &mut) {
		return
	}
	cm, _, err := ch.Mutate(r.Context(), &mut)
	if errors.Is(err, storage.ErrTargetNotFound) {
		s.writeErrorInfo(w, r, http.StatusNotFound, 40400, "target message not found")
		return
	}
	if errors.Is(err, storage.ErrIncompatibleAppend) {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40013, "append data type is incompatible with the target's current data")
		return
	}
	if err != nil {
		s.logger.Warn("mutate failed", "channel", name, "target", target, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "mutate failed")
		return
	}

	// Respond with the new version serial (the SDK's UpdateDeleteResult
	// wire shape, RSL15e); the full merged version is available via
	// GET .../messages/{serial} and .../versions.
	out, err := marshalValue(updateDeleteResponse{VersionSerial: storage.VersionSerial(cm.Messages[0])}, respFormat)
	if err != nil {
		s.logger.Warn("mutate encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
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
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, "channel name and message serial required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	m, err := ch.LatestVersion(r.Context(), serial)
	if errors.Is(err, storage.ErrTargetNotFound) {
		s.writeErrorInfo(w, r, http.StatusNotFound, 40400, "message not found")
		return
	}
	if err != nil {
		s.logger.Warn("message read failed", "channel", name, "serial", serial, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "read failed")
		return
	}

	out, err := marshalMessage(m, format)
	if err != nil {
		s.logger.Warn("message encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
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
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, "channel name and message serial required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40003, err.Error())
		return
	}
	// Versions read oldest-first (ascending version) by default: a version
	// chain reads naturally create-then-edits, and the SDK's
	// GetMessageVersions sends no direction yet expects the create first
	// (DESIGN.md §13.4). Message history defaults backwards; versions
	// default forwards, and an explicit direction param still wins.
	if r.URL.Query().Get("direction") == "" {
		q.Direction = storage.DirectionForwards
	}
	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	page, err := ch.Versions(r.Context(), serial, q)
	if errors.Is(err, storage.ErrTargetNotFound) {
		s.writeErrorInfo(w, r, http.StatusNotFound, 40400, "message not found")
		return
	}
	if err != nil {
		s.logger.Warn("versions failed", "channel", name, "serial", serial, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "versions failed")
		return
	}

	out := flattenHistory(page.ChannelMessages)
	resp, err := marshalBody(out, format)
	if err != nil {
		s.logger.Warn("versions encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}

	writeLinkHeaders(w, r, lastVersionSerial(page), page.HasMore)
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(resp)
}

// HandlePublishAnnotation publishes one or more annotations on a message —
// POST /channels/{name}/messages/{serial}/annotations (DESIGN.md §14.4).
// The target serial comes from the path (authoritative, overriding any in
// the body). The body is a single Annotation or an array (JSON or msgpack
// via Content-Type). It returns 201 with the assigned serial(s); a
// missing/aged-out target is a 404. Gated by the annotation-publish
// capability op (DESIGN.md §3.1, §14.5).
func (s *Server) HandlePublishAnnotation(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	clientID, ok := s.resolveRequestClientID(w, r, principal)
	if !ok {
		return
	}
	name := r.PathValue("name")
	target := r.PathValue("serial")
	if name == "" || target == "" {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, "channel name and message serial required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpAnnotationPublish) {
		return
	}

	format, err := contentTypeFormat(r.Header.Get("Content-Type"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusUnsupportedMediaType, 40004, err.Error())
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, err.Error())
		return
	}
	annotations, err := parseAnnotations(body, format)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, err.Error())
		return
	}
	if len(annotations) == 0 {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, "no annotations")
		return
	}

	now := time.Now().UnixMilli()
	for _, an := range annotations {
		// The path serial is authoritative: it names the target message,
		// overriding any messageSerial in the body (matches the reference).
		an.MessageSerial = target
		cid, ok := auth.MessageClientID(clientID, an.ClientID)
		if !ok {
			s.writeErrorInfo(w, r, http.StatusBadRequest, 40012, "annotation clientId not permitted")
			return
		}
		an.ClientID = cid
		// REST publishes carry no connection, so connectionId stays empty
		// (matching the reference's NewAnnotationChannelMessage).
		if an.Timestamp == 0 {
			an.Timestamp = now
		}
		if verr := an.Validate(); verr != nil {
			s.writeErrorInfo(w, r, verr.StatusCode, verr.Code, verr.Message)
			return
		}
	}

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	cm, _, err := ch.PublishAnnotation(r.Context(), annotations)
	if errors.Is(err, storage.ErrTargetNotFound) {
		s.writeErrorInfo(w, r, http.StatusNotFound, 40400, "target message not found")
		return
	}
	if err != nil {
		s.logger.Warn("annotation publish failed", "channel", name, "target", target, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "annotation publish failed")
		return
	}

	serials := annotationSerials(cm.Annotations)
	var first string
	if len(serials) > 0 {
		first = serials[0]
	}
	respBody, err := marshalValue(annotationResponse{Channel: name, Serial: first, Serials: serials}, respFormat)
	if err != nil {
		s.logger.Warn("annotation publish encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(respFormat))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respBody)
}

// HandleListAnnotations returns the annotations attached to a message in
// stream order — GET /channels/{name}/messages/{serial}/annotations
// (DESIGN.md §14.4) — paginated with the shared Link convention (§2.2).
// An unknown target yields an empty array (not a 404), mirroring Ably.
// Gated by the `history` capability op (DESIGN.md §3.1, §14.5).
func (s *Server) HandleListAnnotations(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	serial := r.PathValue("serial")
	if name == "" || serial == "" {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, "channel name and message serial required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40003, err.Error())
		return
	}
	// Annotations read in stream order (ascending serial) by default —
	// "in stream order" (§14.4), the same forwards default as versions. An
	// explicit direction param still wins.
	if r.URL.Query().Get("direction") == "" {
		q.Direction = storage.DirectionForwards
	}

	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	page, err := ch.Annotations(r.Context(), serial, q)
	if err != nil {
		s.logger.Warn("annotations read failed", "channel", name, "serial", serial, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "annotations read failed")
		return
	}

	out := flattenAnnotations(page.ChannelMessages)
	resp, err := marshalAnnotations(out, format)
	if err != nil {
		s.logger.Warn("annotations encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}

	writeLinkHeaders(w, r, lastAnnotationSerial(page), page.HasMore)
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(resp)
}

// annotationResponse is the REST POST annotation response body: the
// channel, the first assigned annotation serial (Serial), and every
// assigned serial in batch order (Serials).
type annotationResponse struct {
	Channel string   `json:"channel"           msgpack:"channel"`
	Serial  string   `json:"serial,omitempty"  msgpack:"serial,omitempty"`
	Serials []string `json:"serials,omitempty" msgpack:"serials,omitempty"`
}

// annotationSerials returns the server-assigned Serial of each annotation
// in batch order.
func annotationSerials(annotations []*protocol.Annotation) []string {
	out := make([]string, len(annotations))
	for i, a := range annotations {
		out[i] = a.Serial
	}
	return out
}

// parseAnnotations decodes an annotation publish body. The body may be a
// single Annotation object or an array of Annotations.
func parseAnnotations(body []byte, format protocol.Format) ([]*protocol.Annotation, error) {
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	if looksLikeArray(body, format) {
		var arr []*protocol.Annotation
		if err := unmarshal(body, format, &arr); err != nil {
			return nil, fmt.Errorf("decode array: %w", err)
		}
		return arr, nil
	}
	var single protocol.Annotation
	if err := unmarshal(body, format, &single); err != nil {
		return nil, fmt.Errorf("decode annotation: %w", err)
	}
	return []*protocol.Annotation{&single}, nil
}

// flattenAnnotations turns a page of annotation ChannelMessages into the
// flat []Annotation wire shape, concatenating in storage order.
func flattenAnnotations(cms []*protocol.ChannelMessage) []*protocol.Annotation {
	total := 0
	for _, cm := range cms {
		total += len(cm.Annotations)
	}
	out := make([]*protocol.Annotation, 0, total)
	for _, cm := range cms {
		out = append(out, cm.Annotations...)
	}
	return out
}

// marshalAnnotations encodes an annotation slice, normalising nil to an
// empty array (as marshalBody does for messages).
func marshalAnnotations(v []*protocol.Annotation, format protocol.Format) ([]byte, error) {
	if v == nil {
		v = []*protocol.Annotation{}
	}
	switch format {
	case protocol.FormatJSON:
		return json.Marshal(v)
	case protocol.FormatMsgpack:
		return msgpack.Marshal(v)
	}
	return nil, fmt.Errorf("unsupported format")
}

// lastAnnotationSerial returns the Serial of the trailing annotation in a
// page — the boundary for the next-page cursor — or "" if the page is empty.
func lastAnnotationSerial(page storage.HistoryPage) string {
	if len(page.ChannelMessages) == 0 {
		return ""
	}
	cm := page.ChannelMessages[len(page.ChannelMessages)-1]
	if n := len(cm.Annotations); n > 0 {
		return cm.Annotations[n-1].Serial
	}
	return ""
}

// HandlePresence returns the channel's current presence set as a flat
// array of PresenceMessages, each stamped action=PRESENT (DESIGN.md
// §12.6). It honours the `clientId` and `connectionId` filter query
// params (RSP3a2/RSP3a3) and paginates with `limit` and the shared Link
// convention (RSP3a1). Format follows the Accept header.
//
// Gated by the `subscribe` capability op (DESIGN.md §3.1).
func (s *Server) HandlePresence(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40010, "channel name required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpSubscribe) {
		return
	}

	pq, err := parsePresenceQuery(r.URL.Query())
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40003, err.Error())
		return
	}

	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	members, _, err := ch.Members(r.Context())
	if err != nil {
		s.logger.Warn("members failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "presence failed")
		return
	}

	page, boundary, hasMore := paginateMembers(members, pq)

	body, err := marshalPresence(presentMembers(page), format)
	if err != nil {
		s.logger.Warn("presence encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}
	writeLinkHeaders(w, r, boundary, hasMore)
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(body)
}

// presenceQuery bounds a GET .../presence read: an optional clientId /
// connectionId equality filter (RSP3a2/RSP3a3), a limit, and an opaque
// pagination cursor (the trailing member's Serial from the prior page).
type presenceQuery struct {
	clientID     string
	connectionID string
	limit        int
	cursor       string
}

// parsePresenceQuery reads the GET .../presence query params. limit
// defaults to defaultHistoryLimit and is bounded like history's; clientId
// and connectionId are exact-match filters; the cursor is the internal
// pagination param.
func parsePresenceQuery(values url.Values) (presenceQuery, error) {
	q := presenceQuery{
		clientID:     values.Get("clientId"),
		connectionID: values.Get("connectionId"),
		limit:        defaultHistoryLimit,
		cursor:       values.Get(internalCursorParam),
	}
	if v := values.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxHistoryLimit {
			return q, fmt.Errorf("limit: must be an integer between 1 and %d", maxHistoryLimit)
		}
		q.limit = n
	}
	return q, nil
}

// paginateMembers applies the clientId/connectionId filter, orders the
// surviving members by Serial for a stable page sequence, then slices one
// limit-bounded page after the query's cursor. It returns the page, the
// boundary serial for the next cursor, and whether more members remain.
// The membership set is small (§12.4), so an in-memory sort-and-slice is
// adequate — the presence set is a snapshot, not a large history scan.
func paginateMembers(members []*protocol.PresenceMessage, q presenceQuery) (page []*protocol.PresenceMessage, boundary string, hasMore bool) {
	filtered := make([]*protocol.PresenceMessage, 0, len(members))
	for _, m := range members {
		if q.clientID != "" && m.ClientID != q.clientID {
			continue
		}
		if q.connectionID != "" && m.ConnectionID != q.connectionID {
			continue
		}
		if q.cursor != "" && m.Serial <= q.cursor {
			continue
		}
		filtered = append(filtered, m)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Serial < filtered[j].Serial })

	if q.limit > 0 && len(filtered) > q.limit {
		page = filtered[:q.limit]
		hasMore = true
	} else {
		page = filtered
	}
	if n := len(page); n > 0 {
		boundary = page[n-1].Serial
	}
	return page, boundary, hasMore
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
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40010, "channel name required")
		return
	}
	if !s.authorize(w, r, principal, name, auth.OpHistory) {
		return
	}

	q, err := parseHistoryQuery(r.URL.Query())
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40003, err.Error())
		return
	}
	q.Kind = storage.KindPresence
	// fromSerial (untilAttached) is a message-history concept only;
	// the reference server doesn't bound presence history by
	// it either (there's no live presence cache to anchor against),
	// so undo whatever parseHistoryQuery's shared parsing set.
	q.EndChannelSerial = ""

	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	ch, err := s.manager.GetChannel(r.Context(), name)
	if err != nil {
		s.logger.Warn("GetChannel failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "channel unavailable")
		return
	}
	page, err := ch.History(r.Context(), q)
	if err != nil {
		s.logger.Warn("presence history failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "history failed")
		return
	}

	body, err := marshalPresence(flattenPresence(page.ChannelMessages), format)
	if err != nil {
		s.logger.Warn("presence history encode failed", "channel", name, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}

	writeHistoryLinks(w, r, page)
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(body)
}

// HandleTime returns the server's current time in Ably's wire form: an
// array containing one element, milliseconds since the Unix epoch,
// encoded in the format requested by the Accept header (JSON by
// default, msgpack on request).
func (s *Server) HandleTime(w http.ResponseWriter, r *http.Request) {
	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}
	body, err := marshalValue([]int64{time.Now().UnixMilli()}, format)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(body)
}

// HandleStats serves GET /stats. Statistics collection is a non-goal
// (DESIGN.md §1): the endpoint exists so SDK flows that call Stats()
// against this server succeed, and it always returns an empty page.
// The request is authenticated like any other REST read and requires
// the app-wide `stats` op — granted on the `*` resource (DESIGN.md
// §3.1).
func (s *Server) HandleStats(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !principal.Capabilities().Permits("*", auth.OpStats) {
		s.writeCapabilityError(w, r, `insufficient capability: "stats" required`)
		return
	}
	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}
	body, err := marshalValue([]struct{}{}, format)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(format))
	_, _ = w.Write(body)
}

// HandlePostStats serves POST /stats. Statistics collection is a non-goal
// (DESIGN.md §1), but Ably SDKs' test flows POST stats before reading them
// back (e.g. ably-go's TestRestClient), and the SDK's REST write path
// treats a non-2xx as an error whose body it then reads — a 404 here leaves
// the SDK blocked reading the error body of a request whose body the server
// never consumed. So the endpoint is accepted as a no-op: it authenticates
// like the GET (app-wide `stats` op), fully drains the request body, and
// returns an empty 201 so the SDK's write succeeds and returns promptly. No
// statistics are stored.
func (s *Server) HandlePostStats(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !principal.Capabilities().Permits("*", auth.OpStats) {
		s.writeCapabilityError(w, r, `insufficient capability: "stats" required`)
		return
	}
	// Drain and discard the posted stats: reading the request body to
	// completion keeps the connection clean for the SDK's follow-up reads.
	_, _ = io.Copy(io.Discard, r.Body)
	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}
	body, err := marshalValue([]struct{}{}, format)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
		return
	}
	w.Header().Set("Content-Type", contentTypeFor(format))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(body)
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

// HandleRequestToken mints an Ably-compatible JWT for a signed (or
// Basic-authenticated) TokenRequest and returns it as application/jwt
// (DESIGN.md §3). The token is signed with the key's secret (HS256), so a
// client can present it as an access_token that this server then verifies
// via the normal token path.
func (s *Server) HandleRequestToken(w http.ResponseWriter, r *http.Request) {
	keyName := r.PathValue("keyName")

	format, err := contentTypeFormat(r.Header.Get("Content-Type"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusUnsupportedMediaType, 40004, err.Error())
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, err.Error())
		return
	}
	var tr auth.TokenRequest
	if err := unmarshal(body, format, &tr); err != nil {
		// A malformed body (e.g. a non-numeric ttl) is a client error; emit
		// the Ably envelope with a code so the SDK reads statusCode 400
		// rather than normalising a code-less error to 401 (RSA4e).
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40001, "invalid token request body")
		return
	}
	if tr.KeyName == "" {
		tr.KeyName = keyName
	}
	if tr.KeyName != keyName {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40000, "keyName mismatch between path and body")
		return
	}

	respFormat, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusNotAcceptable, 40004, err.Error())
		return
	}

	// Validate the request shape (400s) before authenticating: a requested
	// capability must be well-formed, and the ttl in range (DESIGN.md §3.3).
	if tr.Capability != "" {
		if err := auth.ValidateCapability(tr.Capability); err != nil {
			s.writeTokenRequestError(w, r, err)
			return
		}
	}
	if err := auth.ValidateTTL(tr.TTL); err != nil {
		s.writeTokenRequestError(w, r, err)
		return
	}

	if err := s.authn.ValidateTokenRequest(&tr, r); err != nil {
		s.writeTokenRequestError(w, r, err)
		return
	}

	token, issued, expires, capability, err := s.authn.MintToken(&tr)
	if err != nil {
		// A requested capability the key cannot grant is an authorisation
		// failure (401), not a server error; everything else is a 500.
		if errors.Is(err, auth.ErrCapabilityDenied) {
			s.writeTokenRequestError(w, r, err)
			return
		}
		s.logger.Warn("mint token failed", "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "token minting failed")
		return
	}

	// The SDK decodes the requestToken response as a TokenDetails; the
	// minted JWT rides in its Token field (DESIGN.md §3). issued/expires
	// are milliseconds since epoch. capability is the granted (narrowed)
	// capability — always present so the SDK can JSON.parse it (§3.3).
	respBody, err := marshalValue(tokenDetailsResponse{
		Token:      token,
		KeyName:    tr.KeyName,
		Issued:     issued.UnixMilli(),
		Expires:    expires.UnixMilli(),
		ClientID:   tr.ClientID,
		Capability: capability,
	}, respFormat)
	if err != nil {
		s.logger.Warn("token encode failed", "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "encode failed")
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
	code, status, msg := auth.AuthErrorInfo(err)
	s.writeErrorInfo(w, r, status, code, msg)
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
	s.writeCapabilityError(w, r, fmt.Sprintf("insufficient capability: %q required for channel %q", op, channel))
	return false
}

// writeTokenRequestError writes an Ably-shaped error for a failed token
// request, mapping the auth failure to its code/status via
// auth.AuthErrorInfo (DESIGN.md §3.3). A 401 also carries the
// WWW-Authenticate challenge.
func (s *Server) writeTokenRequestError(w http.ResponseWriter, r *http.Request, err error) {
	code, status, msg := auth.AuthErrorInfo(err)
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Basic realm="ably-server"`)
	}
	s.writeErrorInfo(w, r, status, code, msg)
}

// writeCapabilityError writes a 401 carrying the Ably error shape with the
// insufficient-capability code 40160 (DESIGN.md §3.1).
func (s *Server) writeCapabilityError(w http.ResponseWriter, r *http.Request, msg string) {
	s.writeErrorInfo(w, r, http.StatusUnauthorized, 40160, msg)
}

// HandleNotFound writes an Ably-shaped 404 for an unknown REST resource —
// an unrecognised path, or a request whose method Ably would treat as a
// missing resource rather than a method error (DESIGN.md §2.2). It is the
// router's catch-all, so it also converts Go's ServeMux 405 for a known
// path under a non-registered method into the 404 the SDK expects.
func (s *Server) HandleNotFound(w http.ResponseWriter, r *http.Request) {
	s.writeErrorInfo(w, r, http.StatusNotFound, 40400, "requested resource not found")
}

// errHref returns the FAQ error page link Ably SDKs surface for a code,
// mirroring the reference implementation's httpapi.errHref.
func errHref(code int) string {
	return fmt.Sprintf("https://help.ably.io/error/%d", code)
}

// writeErrorInfo writes an Ably error response: the `{"error":{...}}`
// envelope in the Accept format, plus the X-Ably-Errorcode /
// X-Ably-Errormessage headers Ably SDKs read for the error code and
// message (HP6/HP7) — without them an HTTPPaginatedResponse reports no
// code even when the body carries one. It mirrors the reference's
// httpapi.Error: a missing code defaults to 50000, a missing status to
// 500, and the href defaults to https://help.ably.io/error/<code>.
func (s *Server) writeErrorInfo(w http.ResponseWriter, r *http.Request, statusCode, code int, msg string) {
	if code == 0 {
		code = 50000
	}
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}
	format, err := acceptFormat(r.Header.Get("Accept"))
	if err != nil {
		format = protocol.FormatJSON
	}
	body, _ := marshalValue(errorResponse{Error: &protocol.ErrorInfo{
		Message:    msg,
		Code:       code,
		StatusCode: statusCode,
		HRef:       errHref(code),
	}}, format)
	w.Header().Set("Content-Type", contentTypeFor(format))
	w.Header().Set("X-Ably-Errorcode", strconv.Itoa(code))
	w.Header().Set("X-Ably-Errormessage", msg)
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
}

// authorizeMutation applies the §13.5 capability + ownership check for a
// REST mutation. It returns true when permitted; on rejection it writes
// the response (401 for insufficient capability, 404 for a missing target
// whose ownership had to be checked) and returns false. The creator lookup
// runs only when the caller holds just the -own op.
func (s *Server) authorizeMutation(w http.ResponseWriter, r *http.Request, p *auth.Principal, clientID string, ch *core.Channel, channel string, mut *protocol.Message) bool {
	ownOp, anyOp := mutationOps(mut.Action)
	switch p.Capabilities().MutationGrant(channel, ownOp, anyOp) {
	case auth.MutationAllowed:
		return true
	case auth.MutationDeniedCapability:
		s.writeCapabilityError(w, r, "insufficient capability for message mutation")
		return false
	}
	// MutationNeedsOwnership: the caller must own the target message.
	latest, err := ch.LatestVersion(r.Context(), mut.Serial)
	if errors.Is(err, storage.ErrTargetNotFound) {
		s.writeErrorInfo(w, r, http.StatusNotFound, 40400, "target message not found")
		return false
	}
	if err != nil {
		s.logger.Warn("mutation authorization failed", "channel", channel, "target", mut.Serial, "err", err)
		s.writeErrorInfo(w, r, http.StatusInternalServerError, 50000, "mutate failed")
		return false
	}
	if ownsMessage(clientID, latest.ClientID) {
		return true
	}
	s.writeCapabilityError(w, r, "insufficient capability: caller does not own the target message")
	return false
}

// mutationOps maps a mutation action to its ownership-scoped capability op
// pair (DESIGN.md §13.5): update and append are gated by message-update-*,
// delete by message-delete-*.
func mutationOps(a protocol.MessageAction) (own, any auth.Op) {
	if a == protocol.MessageDelete {
		return auth.OpMessageDeleteOwn, auth.OpMessageDeleteAny
	}
	return auth.OpMessageUpdateOwn, auth.OpMessageUpdateAny
}

// ownsMessage reports whether a caller with the given resolved clientId
// owns a message whose creator clientId is creator (DESIGN.md §13.5): a
// concrete identity matching the creator. A wildcard/anonymous caller owns
// nothing.
func ownsMessage(callerClientID, creator string) bool {
	return callerClientID != "" && callerClientID != auth.WildcardClientID && callerClientID == creator
}

// resolveRequestClientID applies the §3.2 resolution for a REST request:
// the verified principal plus the request's asserted clientId yield the
// request's clientId. On a disallowed value it writes a 401 and returns
// ok=false.
func (s *Server) resolveRequestClientID(w http.ResponseWriter, r *http.Request, p *auth.Principal) (string, bool) {
	param, err := requestClientIDParam(r)
	if err != nil {
		s.writeErrorInfo(w, r, http.StatusBadRequest, 40012, "invalid clientId; must be base64-encoded if passed by header")
		return "", false
	}
	clientID, err := auth.ResolveClientID(p, param)
	if err != nil {
		code, status, msg := auth.AuthErrorInfo(err)
		s.writeErrorInfo(w, r, status, code, msg)
		return "", false
	}
	return clientID, true
}

// requestClientIDParam returns the clientId a REST request asserts out of
// band, applied through §3.2 resolution against the credential. It is the
// `clientId` query param when present, else the base64-decoded
// X-Ably-ClientId header — the SDKs' way of asserting the client-configured
// clientId on a request whose credential (a raw key, or a token string the
// SDK cannot introspect) does not itself carry it (RSC17; mirrors the
// reference's clientIDFromRequest). A concrete value here is what lets a
// clientId-configured client stamp its implicit clientId on a publish
// (RSL1m1). Query wins over header when both are present, as in the
// reference.
func requestClientIDParam(r *http.Request) (string, error) {
	if v := r.URL.Query().Get("clientId"); v != "" {
		return v, nil
	}
	if h := r.Header.Get("X-Ably-ClientId"); h != "" {
		decoded, err := base64.StdEncoding.DecodeString(h)
		if err != nil {
			return "", err
		}
		return string(decoded), nil
	}
	return "", nil
}

// resolveConnectionKey resolves a publish's connectionKey to the target
// connection's connectionId (DESIGN.md §13). It returns ok=false — surfaced
// by the caller as Ably 40006 — when no resolver is configured or no live
// connection on this node holds the key.
func (s *Server) resolveConnectionKey(key string) (connID string, ok bool) {
	if s.conns == nil {
		return "", false
	}
	return s.conns.ResolveConnectionKey(key)
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
// end, limit, fromSerial) plus the internal opaque cursor onto a
// storage.HistoryQuery. All params are optional; the defaults match
// Ably (direction=backwards, limit=100, no time bounds).
//
// fromSerial carries the channel's attachSerial for an untilAttached
// history read: ably-js's RealtimeChannel.history({untilAttach: true})
// sets it to the ATTACHED ChannelSerial and sends it on the wire as
// from_serial (realtimechannel.ts sets params.from_serial, not
// params.fromSerial — camelCase is accepted too since the reference
// server takes both spellings).
// It maps onto q.EndChannelSerial, the same inclusive channel-serial
// upper bound already used by this server's resume/rewind gap-fetch
// (internal/realtime/attachment.go), since the semantics are
// identical: only HandleHistory (the default message history) wires
// it through — HandlePresenceHistory deliberately clears it again,
// matching the reference (messagedb/presence.go:102-104: "FromSerial
// is not supported" for presence).
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

	if v := values.Get("fromSerial"); v != "" {
		q.EndChannelSerial = v
	} else if v := values.Get("from_serial"); v != "" {
		q.EndChannelSerial = v
	}

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
//
// The link URL is the "./"-prefixed final path segment plus query (e.g.
// "./messages?limit=2&from=..."), not the absolute path, matching the
// form Ably emits: ably-js's parseRelLinks only accepts a `^\./(\w+)\?`
// URL and extracts the query from it (it ignores the path segment and
// reissues against the original request path), so a bare "messages?..."
// yields no next link and pagination stalls (DESIGN.md §2.2).
func writeLinkHeaders(w http.ResponseWriter, r *http.Request, boundary string, hasMore bool) {
	base := "./" + path.Base(r.URL.Path)
	rel := func(q url.Values) string {
		if enc := q.Encode(); enc != "" {
			return base + "?" + enc
		}
		return base
	}

	first := r.URL.Query()
	first.Del(internalCursorParam)

	// Each rel is emitted as its own Link header line, not one comma-joined
	// value: Ably SDKs parse each Header["Link"] element with a single-match
	// regexp, so multiple rels folded into one line would leave all but the
	// first (here rel="next") unseen and pagination would stall.
	h := w.Header()
	h.Add("Link", fmt.Sprintf(`<%s>; rel="current"`, rel(r.URL.Query())))
	h.Add("Link", fmt.Sprintf(`<%s>; rel="first"`, rel(first)))

	if boundary != "" && hasMore {
		next := r.URL.Query()
		// Pagination strictly excludes the cursor in the requested
		// direction (matching Ably's REST).
		next.Set(internalCursorParam, boundary)
		h.Add("Link", fmt.Sprintf(`<%s>; rel="next"`, rel(next)))
	}
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
