// Package rest implements the REST API handlers exposed by ably-server:
// channel message publish/history/mutation/versions, presence and presence
// history, annotations, token requests, health/readiness checks, and gated
// stats stubs. For the authoritative, up-to-date list of routes and methods,
// see newMux in internal/server/server.go, which registers every handler in
// this package.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/vmihailenco/msgpack/v5"
)

// Server holds the state of the endpoints this server serves itself. The
// channel surface — publishing, history, presence, annotations — is the shared
// module's, so what is left here is minting a token, the stats stub, the
// readiness probes and the Ably-shaped 404 for anything unrouted.
//
// Its HTTP handlers are exported methods; callers register them on their own
// ServeMux.
type Server struct {
	authn  *auth.Authenticator
	logger *logging.Logger
	ready  storage.Pinger
}

// NewServer constructs a Server. ready, if non-nil, is consulted by
// HandleReadyz on every request (see DESIGN.md §2.2); callers pass nil for
// backends with no external dependency to check (memory, bbolt).
func NewServer(keys []auth.APIKey, logger *logging.Logger, ready storage.Pinger) *Server {
	return &Server{
		authn:  auth.NewAuthenticator(keys...),
		logger: logger,
		ready:  ready,
	}
}

// SetKeys replaces the keys the token endpoint mints from and authenticates
// against, so that a key changed under the running server (DESIGN.md §9.1) is
// changed here too rather than only on the protocol module's side.
func (s *Server) SetKeys(keys []auth.APIKey) { s.authn.SetKeys(keys...) }

// publishResponse is the REST POST /messages response body (Ably RSL1):
// the channel name, the publish's messageId, and the per-message serials
// (RSL1n) the SDK's PublishWithResult decodes to address each message.
type publishResponse struct {
	Channel   string   `json:"channel"           msgpack:"channel"`
	MessageID string   `json:"messageId"         msgpack:"messageId"`
	Serials   []string `json:"serials,omitempty" msgpack:"serials,omitempty"`
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
		return packMsgpack(v)
	}
	return nil, fmt.Errorf("unsupported format")
}

// annotationResponse is the REST POST annotation response body: the
// channel, the first assigned annotation serial (Serial), and every
// assigned serial in batch order (Serials).
type annotationResponse struct {
	Channel string   `json:"channel"           msgpack:"channel"`
	Serial  string   `json:"serial,omitempty"  msgpack:"serial,omitempty"`
	Serials []string `json:"serials,omitempty" msgpack:"serials,omitempty"`
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

func unmarshal(body []byte, format protocol.Format, v any) error {
	switch format {
	case protocol.FormatJSON:
		return json.Unmarshal(body, v)
	case protocol.FormatMsgpack:
		return unpackMsgpack(body, v)
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

// packMsgpack and unpackMsgpack are wire.Pack and wire.FromPacked for a value
// whose type is not known here.
//
// The wire types are encoded by their json tags, which is what those two set
// up; they take a typed pointer, and the REST handlers hold an `any`, so the
// same encoder is configured directly rather than through them — decoding
// through them would decode into the interface rather than into what it holds.
func packMsgpack(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.SetCustomStructTag("json")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unpackMsgpack(body []byte, v any) error {
	dec := msgpack.NewDecoder(bytes.NewReader(body))
	dec.SetCustomStructTag("json")
	return dec.Decode(v)
}
