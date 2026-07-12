// Package auth implements credential parsing and verification for
// realtime and REST requests.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Errors returned by Authenticator.Authenticate.
var (
	ErrNoCredentials = errors.New("no credentials presented")
	ErrInvalidKey    = errors.New("invalid api key")
	ErrInvalidToken  = errors.New("invalid token")
)

// ErrTokenExpired is returned by verification when a token's exp has
// passed. It wraps ErrInvalidToken (so callers matching ErrInvalidToken
// still match) but is distinguished so the WS/REST layers can surface the
// renewable token-expired code 40142 rather than a generic auth failure
// (DESIGN.md §3).
var ErrTokenExpired = fmt.Errorf("%w: token expired", ErrInvalidToken)

// ErrClientIDMismatch is returned by ResolveClientID when the requested
// clientId is not permitted by the credential.
var ErrClientIDMismatch = errors.New("clientId not permitted by credential")

// Errors returned by token-request validation and minting (DESIGN.md §3.3).
var (
	// ErrTimestampNotCurrent is a token request whose timestamp is outside
	// the accepted window (Ably 40104).
	ErrTimestampNotCurrent = errors.New("token request timestamp not current")
	// ErrNonceReplayed is a token request reusing a nonce already seen
	// within the timestamp window (Ably 40105).
	ErrNonceReplayed = errors.New("token request nonce replayed")
	// ErrInvalidCapability is a malformed or structurally invalid requested
	// capability (bad op name, "*" mixed with other ops, empty op list, or
	// unparseable JSON) — a client error (Ably 40000, status 400).
	ErrInvalidCapability = errors.New("invalid capability")
	// ErrInvalidTTL is a negative, excessive, or otherwise invalid requested
	// ttl — a client error (status 400).
	ErrInvalidTTL = errors.New("invalid ttl")
	// ErrCapabilityDenied is a requested capability the signing key cannot
	// grant at all (empty intersection): insufficient capability (Ably 40160).
	ErrCapabilityDenied = fmt.Errorf("%w: requested capability is not permitted by the key", ErrInvalidToken)
)

// WildcardClientID is the clientId marker (DESIGN.md §3.2) meaning the
// credential's bearer may assume any identity, choosing it per operation.
// It is never itself stamped as a message or member identity.
const WildcardClientID = "*"

// jwt validates and (de)serialises time claims at TimePrecision
// granularity, truncating anything finer. The default is one second, which
// would collapse a sub-second token exp back to a whole second (a 1 ms
// token would appear valid for up to a second). Set millisecond precision
// so a short-lived token expires when it should (DESIGN.md §3.3, TASK-100).
func init() {
	jwt.TimePrecision = time.Millisecond
}

// clockSkewLeeway is the tolerance applied to a token's iat (issued-at)
// claim to absorb a small forward clock difference between the token
// issuer and this server. It is NOT applied to exp: an expired token is
// rejected outright (no grace), matching Ably (DESIGN.md §3) so a
// short-lived token is unusable the instant it lapses.
const clockSkewLeeway = 60 * time.Second

// maxTokenTTL bounds the lifetime a token request may ask for; a larger
// ttl is rejected as excessive (DESIGN.md §3.3), matching Ably's 24h
// default cap.
const maxTokenTTL = 24 * time.Hour

// tokenRequestTimestampWindow bounds how far a token request's timestamp
// may sit from server time before it is rejected as not-current (DESIGN.md
// §3.3), guarding against stale/replayed requests.
const tokenRequestTimestampWindow = 2 * time.Minute

// APIKey is a parsed Ably-format API key in the form
// `appId.keyId:keySecret`, carrying the capability it grants (DESIGN.md
// §3.1). A key configured without an explicit capability grants the full
// {"*":["*"]} set; a structured config entry may narrow it.
type APIKey struct {
	AppID     string
	KeyID     string
	KeySecret string

	cap    Capability // the capability this key grants; bounds any token minted from it
	capRaw string     // the key's capability as its original JSON string (the granted capability echoed on requestToken)
	raw    string     // cached `appId.keyId:keySecret` for constant-time compare
}

// Name returns the key's `appId.keyId` portion — the value carried as a
// JWT `kid` header and as the key name in token-request paths.
func (k APIKey) Name() string {
	return k.AppID + "." + k.KeyID
}

// Capability returns the key's capability — the ceiling a token minted
// from it can be narrowed to, and the set a Basic-auth holder of it
// resolves to (DESIGN.md §3.1, §3.3).
func (k APIKey) Capability() Capability {
	return k.cap
}

// CapabilityString returns the key's capability as its original JSON
// string — the granted capability a token minted from this key with no
// narrowing carries, echoed verbatim on requestToken so it matches the
// capability the app was provisioned with (DESIGN.md §3.3).
func (k APIKey) CapabilityString() string {
	return k.capRaw
}

// ParseAPIKey validates and decomposes an Ably-format API key, granting
// it the full capability. This is the flag/env path, where keys are not
// individually scoped (DESIGN.md §9). All three components must be
// non-empty.
func ParseAPIKey(s string) (APIKey, error) {
	return ParseAPIKeyWithCapability(s, "")
}

// ParseAPIKeyWithCapability parses an Ably-format API key and attaches
// the capability described by capJSON, an `x-ably-capability`-format JSON
// object (DESIGN.md §3.1). An empty capJSON grants the full {"*":["*"]}
// capability, so the flag/env path (ParseAPIKey) stays full-capability; a
// malformed capJSON is an error.
func ParseAPIKeyWithCapability(s, capJSON string) (APIKey, error) {
	name, secret, ok := strings.Cut(s, ":")
	if !ok {
		return APIKey{}, fmt.Errorf("api key missing ':' between name and secret")
	}
	if secret == "" {
		return APIKey{}, fmt.Errorf("api key has empty secret")
	}

	appID, keyID, ok := strings.Cut(name, ".")
	if !ok {
		return APIKey{}, fmt.Errorf("api key name missing '.' between appId and keyId")
	}
	if appID == "" {
		return APIKey{}, fmt.Errorf("api key has empty appId")
	}
	if keyID == "" {
		return APIKey{}, fmt.Errorf("api key has empty keyId")
	}

	cap := AllowAllCapability()
	capRaw := AllowAllCapability().String()
	if capJSON != "" {
		c, err := ParseCapability(capJSON)
		if err != nil {
			return APIKey{}, fmt.Errorf("api key capability: %w", err)
		}
		cap = c
		capRaw = capJSON
	}

	return APIKey{
		AppID:     appID,
		KeyID:     keyID,
		KeySecret: secret,
		cap:       cap,
		capRaw:    capRaw,
		raw:       s,
	}, nil
}

// Method identifies how a request authenticated.
type Method int

const (
	// MethodBasic is API-key auth (Basic header or `key` query param).
	MethodBasic Method = iota
	// MethodToken is JWT bearer-token auth.
	MethodToken
)

// Principal is the result of authenticating a request: how it
// authenticated, plus any authorisation/identity claims a token carried
// for downstream resolution. Capability enforcement (TASK-12) and
// clientId resolution (TASK-11) consume these; this package only
// surfaces them.
type Principal struct {
	Method Method

	// Capability is the raw `x-ably-capability` claim (a JSON string), or
	// "" if absent. Empty for Basic auth, where the key's full capability
	// is implied.
	Capability string

	// ClientID is the `x-ably-clientId` claim; HasClientID distinguishes
	// an absent claim from a present one (including the "*" wildcard,
	// preserved verbatim). Always empty/false for Basic auth.
	ClientID    string
	HasClientID bool

	// cap is the resolved capability set enforced for this principal
	// (DESIGN.md §3.1): the authenticating key's capability for Basic auth
	// or a token with no capability claim, otherwise the claim intersected
	// with the signing key's capability.
	cap Capability

	// ExpiresAt is the token's expiry (from the `exp` claim), used to
	// drive inband re-auth (DESIGN.md §3, TASK-17). Zero for Basic auth,
	// which never expires.
	ExpiresAt time.Time
}

// Capabilities returns the principal's resolved capability set (§3.1).
func (p *Principal) Capabilities() Capability {
	return p.cap
}

// Authenticator verifies presented credentials against one or more
// configured API keys. All keys share a single appId (enforced at
// startup, DESIGN.md §3): the server owns one channel namespace, so the
// keys differ only in keyId/secret and capability. A request
// authenticates against ANY configured key.
type Authenticator struct {
	keys   []APIKey
	byName map[string]APIKey // appId.keyId -> key, for kid / keyName lookup
	parser *jwt.Parser

	// nonceMu guards seenNonces, the set of token-request nonces accepted
	// within the timestamp window, used to reject replays (DESIGN.md §3.3).
	// The map value is the entry's expiry, after which it is evicted.
	nonceMu     sync.Mutex
	seenNonces  map[string]time.Time
	nonceReaped time.Time
}

// NewAuthenticator constructs an Authenticator accepting any of the
// given keys. At least one key is required; the caller (cmd/ably-server)
// enforces that and the shared-appId invariant at startup.
func NewAuthenticator(keys ...APIKey) *Authenticator {
	byName := make(map[string]APIKey, len(keys))
	for _, k := range keys {
		byName[k.Name()] = k
	}
	return &Authenticator{
		keys:       keys,
		byName:     byName,
		seenNonces: make(map[string]time.Time),
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"HS256"}),
			// No leeway: exp is checked with zero grace so an expired token
			// is rejected the instant it lapses (Ably behaviour, DESIGN.md §3).
			// iat is validated separately (verifyToken) with a small forward
			// leeway; iat-future rejection is not left to the library.
			jwt.WithExpirationRequired(), // exp must be present
		),
	}
}

// matchKey returns the configured key equal to presented, comparing in
// constant time. Every key is compared (no early return) so the timing
// does not reveal which key, if any, matched; the matched key is needed
// so a Basic-auth principal resolves to that key's capability (§3.1).
func (a *Authenticator) matchKey(presented string) (APIKey, bool) {
	pb := []byte(presented)
	var matched APIKey
	found := 0
	for _, k := range a.keys {
		eq := subtle.ConstantTimeCompare(pb, []byte(k.raw))
		if eq == 1 {
			matched = k
		}
		found |= eq
	}
	return matched, found == 1
}

// Authenticate extracts and verifies the request's credentials. A bearer
// token (Authorization: Bearer, or the access_token / accessToken query
// param) is tried first, then an API key (Basic auth, or the `key` query
// param). Returns ErrNoCredentials if none are presented, ErrInvalidKey
// or ErrInvalidToken if verification fails.
func (a *Authenticator) Authenticate(r *http.Request) (*Principal, error) {
	if tok, ok := extractToken(r); ok {
		return a.verifyToken(tok)
	}
	if k, ok := extractKey(r); ok {
		key, ok := a.matchKey(k)
		if !ok {
			return nil, ErrInvalidKey
		}
		return &Principal{Method: MethodBasic, cap: key.Capability()}, nil
	}
	return nil, ErrNoCredentials
}

// verifyToken verifies an HS256 JWT against the configured keys' secrets
// and extracts the Ably claims. The JWT `kid` header selects the signing
// key (§3): a token minted by this server carries its key's name as kid.
// When kid is absent or names no configured key, verification falls back
// to trying every configured key's secret (jwt tries each in the
// VerificationKeySet), so a token minted elsewhere with the same secret
// still verifies.
func (a *Authenticator) verifyToken(tokenString string) (*Principal, error) {
	claims := jwt.MapClaims{}
	// signingKey records which configured key verified the token, so its
	// capability can bound the token's (§3): claim ∩ key, with an absent
	// claim inheriting the key's capability outright.
	var signingKey APIKey
	_, err := a.parser.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		if kid, ok := t.Header["kid"].(string); ok {
			if k, ok := a.byName[kid]; ok {
				signingKey = k
				return []byte(k.KeySecret), nil
			}
		}
		// No usable kid: find the key whose secret verifies the signature
		// (a token minted elsewhere with a shared secret), so its
		// capability still bounds the token. text is the signed portion —
		// everything before the trailing `.signature` segment.
		text := t.Raw[:strings.LastIndexByte(t.Raw, '.')]
		for _, k := range a.keys {
			if t.Method.Verify(text, t.Signature, []byte(k.KeySecret)) == nil {
				signingKey = k
				return []byte(k.KeySecret), nil
			}
		}
		// Nothing verified: return the full set so the library reports a
		// signature-invalid error the same way it always has.
		set := jwt.VerificationKeySet{Keys: make([]jwt.VerificationKey, len(a.keys))}
		for i, k := range a.keys {
			set.Keys[i] = []byte(k.KeySecret)
		}
		return set, nil
	})
	if err != nil {
		// An expired token is renewable (40142); surface it distinctly from a
		// generic invalid-token failure (DESIGN.md §3).
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, fmt.Errorf("%w: %v", ErrTokenExpired, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if _, ok := claims["iat"]; !ok {
		return nil, fmt.Errorf("%w: missing iat claim", ErrInvalidToken)
	}
	// Reject a token issued in the future beyond the clock-skew leeway. The
	// parser no longer validates iat (so exp can be checked without grace),
	// so this is enforced here.
	if iat, ierr := claims.GetIssuedAt(); ierr == nil && iat != nil {
		if iat.Time.After(time.Now().Add(clockSkewLeeway)) {
			return nil, fmt.Errorf("%w: iat in the future", ErrInvalidToken)
		}
	}

	// An absent capability claim inherits the signing key's capability;
	// a present claim narrows it via intersection (DESIGN.md §3).
	p := &Principal{Method: MethodToken, cap: signingKey.Capability()}
	if c, ok := claims["x-ably-capability"].(string); ok {
		p.Capability = c
		// A present capability claim narrows access (§3.1); a malformed
		// one makes the token unusable.
		cap, err := ParseCapability(c)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		p.cap = cap.Intersect(signingKey.Capability())
	}
	if cid, ok := claims["x-ably-clientId"].(string); ok {
		p.ClientID = cid
		p.HasClientID = true
	}
	if exp, err := claims.GetExpirationTime(); err == nil && exp != nil {
		p.ExpiresAt = exp.Time
	}
	return p, nil
}

// VerifyToken verifies a raw JWT string and returns its principal, used
// for inband re-authentication on an established connection (DESIGN.md
// §3, TASK-17) where the token arrives in an AUTH frame rather than an
// HTTP request. It is the token half of Authenticate.
func (a *Authenticator) VerifyToken(tokenString string) (*Principal, error) {
	return a.verifyToken(tokenString)
}

// AuthErrorInfo maps an authentication / token-request failure to the
// Ably error code, HTTP status code, and message the SDK expects
// (DESIGN.md §3). Order matters: the more specific wrapped sentinels
// (ErrTokenExpired, ErrCapabilityDenied) are tested before the
// ErrInvalidToken they wrap.
func AuthErrorInfo(err error) (code, statusCode int, message string) {
	switch {
	case errors.Is(err, ErrTokenExpired):
		return 40142, 401, "token expired"
	case errors.Is(err, ErrCapabilityDenied):
		return 40160, 401, "requested capability not permitted by the key"
	case errors.Is(err, ErrTimestampNotCurrent):
		return 40104, 401, "token request timestamp not current"
	case errors.Is(err, ErrNonceReplayed):
		return 40105, 401, "token request nonce replayed"
	case errors.Is(err, ErrInvalidCapability):
		return 40000, 400, "invalid capability"
	case errors.Is(err, ErrInvalidTTL):
		return 40003, 400, "invalid ttl"
	case errors.Is(err, ErrClientIDMismatch):
		return 40102, 401, "clientId not permitted by credential"
	case errors.Is(err, ErrInvalidToken):
		return 40101, 401, "invalid credentials"
	case errors.Is(err, ErrInvalidKey):
		return 40101, 401, "invalid credentials"
	case errors.Is(err, ErrNoCredentials):
		return 40101, 401, "no credentials provided"
	default:
		return 40101, 401, "invalid credentials"
	}
}

// ResolveClientID derives a connection's (or REST request's) clientId
// from the verified principal and the clientId supplied out-of-band (the
// `clientId` query param), per the DESIGN.md §3.2 table. It returns ""
// (anonymous — may assert no identity), WildcardClientID (may assume any
// identity per operation), or a concrete clientId. ErrClientIDMismatch is
// returned when the supplied param is not permitted by the credential.
func ResolveClientID(p *Principal, param string) (string, error) {
	if p.Method == MethodBasic {
		// A key holder is fully trusted and may assume any identity. With
		// no clientId param the connection is wildcard (identity chosen per
		// operation, like an Ably key); with one it is pinned to that value.
		if param == "" {
			return WildcardClientID, nil
		}
		return param, nil
	}
	// Token auth.
	if !p.HasClientID {
		// No x-ably-clientId claim: the bearer may not assert an identity.
		if param != "" {
			return "", ErrClientIDMismatch
		}
		return "", nil
	}
	if p.ClientID == WildcardClientID {
		switch param {
		case "":
			return WildcardClientID, nil // retain wildcard; identity chosen per op
		case WildcardClientID:
			return "", ErrClientIDMismatch // "*" is never a concrete identity
		default:
			return param, nil // narrow the wildcard to one identity
		}
	}
	// Concrete claim: the param must match it or be omitted.
	if param == "" || param == p.ClientID {
		return p.ClientID, nil
	}
	return "", ErrClientIDMismatch
}

// MessageClientID applies the §3.2 per-operation rule for a message or
// presence-message clientId. connClientID is the connection's resolved
// clientId (from ResolveClientID); opClientID is the clientId the
// operation carries. It returns the clientId to stamp (possibly "") and
// ok=false if the operation asserts an identity the connection may not
// use. An anonymous connection may carry no identity; a wildcard
// connection may assume any concrete identity (or none); a concrete
// connection may omit (stamped with its own) or match it.
func MessageClientID(connClientID, opClientID string) (stamped string, ok bool) {
	switch connClientID {
	case "":
		return "", opClientID == ""
	case WildcardClientID:
		if opClientID == WildcardClientID {
			return "", false
		}
		return opClientID, true
	default:
		if opClientID == "" || opClientID == connClientID {
			return connClientID, true
		}
		return "", false
	}
}

// defaultTokenTTL is the token lifetime used when a TokenRequest does not
// specify one, matching Ably's 60-minute default.
const defaultTokenTTL = 60 * time.Minute

// TokenRequest is the body of POST /keys/{keyName}/requestToken (Ably
// RSA9): a request to mint a token, signed by the key holder. Field names
// match what ably SDKs send. TTL and Timestamp are milliseconds.
type TokenRequest struct {
	KeyName    string `json:"keyName"    msgpack:"keyName"`
	TTL        int64  `json:"ttl"        msgpack:"ttl"`
	Capability string `json:"capability" msgpack:"capability"`
	ClientID   string `json:"clientId"   msgpack:"clientId"`
	Timestamp  int64  `json:"timestamp"  msgpack:"timestamp"`
	Nonce      string `json:"nonce"      msgpack:"nonce"`
	MAC        string `json:"mac"        msgpack:"mac"`
}

// UnmarshalJSON decodes a TokenRequest, accepting ttl and timestamp as
// either a JSON number or a quoted numeric string. Ably's authUrl exchange
// (e.g. echo's qs_to_body) reassembles a signed TokenRequest from query
// params, so those numeric fields arrive as strings; a plain int64 field
// would fail to decode and the whole exchange would 400. A non-numeric ttl
// (the SDK's "invalid ttl" case) still errors, surfacing as a 400.
func (tr *TokenRequest) UnmarshalJSON(data []byte) error {
	var aux struct {
		KeyName    string          `json:"keyName"`
		TTL        json.RawMessage `json:"ttl"`
		Capability string          `json:"capability"`
		ClientID   string          `json:"clientId"`
		Timestamp  json.RawMessage `json:"timestamp"`
		Nonce      string          `json:"nonce"`
		MAC        string          `json:"mac"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	ttl, err := flexibleInt64(aux.TTL)
	if err != nil {
		return fmt.Errorf("ttl: %w", err)
	}
	ts, err := flexibleInt64(aux.Timestamp)
	if err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	tr.KeyName, tr.Capability, tr.ClientID = aux.KeyName, aux.Capability, aux.ClientID
	tr.Nonce, tr.MAC, tr.TTL, tr.Timestamp = aux.Nonce, aux.MAC, ttl, ts
	return nil
}

// flexibleInt64 decodes a JSON value that may be a number, a quoted
// numeric string, null, or absent into an int64 (0 when empty/absent).
func flexibleInt64(raw json.RawMessage) (int64, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return 0, nil
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return 0, err
		}
		if str == "" {
			return 0, nil
		}
		return strconv.ParseInt(str, 10, 64)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, err
	}
	return n, nil
}

// tokenRequestText builds the canonical string a TokenRequest's mac is
// computed over (Ably RSA9): each field followed by a newline, in order.
// ttl is empty when unset; the other fields are echoed verbatim so the
// text matches the SDK's regardless of their content.
func (tr *TokenRequest) tokenRequestText() string {
	ttl := ""
	if tr.TTL != 0 {
		ttl = strconv.FormatInt(tr.TTL, 10)
	}
	return tr.KeyName + "\n" +
		ttl + "\n" +
		tr.Capability + "\n" +
		tr.ClientID + "\n" +
		strconv.FormatInt(tr.Timestamp, 10) + "\n" +
		tr.Nonce + "\n"
}

// ValidateTokenRequest authenticates a token request against the
// configured key. A request carrying a mac is verified by recomputing the
// HMAC-SHA256 over the canonical text and comparing in constant time. A
// request without a mac is accepted only when r also carries Basic auth
// for the same key (the key holder is explicitly authenticated). Returns
// ErrInvalidToken on any failure.
func (a *Authenticator) ValidateTokenRequest(tr *TokenRequest, r *http.Request) error {
	key, ok := a.byName[tr.KeyName]
	if !ok {
		return fmt.Errorf("%w: unknown key %q", ErrInvalidToken, tr.KeyName)
	}
	// Authenticate the request: a matching mac, or Basic auth for the same
	// key (the holder is explicitly authenticated, so a mac is unnecessary).
	if tr.MAC != "" {
		expected := base64.StdEncoding.EncodeToString(hmacOf(key, tr.tokenRequestText()))
		if subtle.ConstantTimeCompare([]byte(tr.MAC), []byte(expected)) != 1 {
			return fmt.Errorf("%w: request mac does not match", ErrInvalidToken)
		}
	} else if k, ok := extractKey(r); !ok || subtle.ConstantTimeCompare([]byte(k), []byte(key.raw)) != 1 {
		return fmt.Errorf("%w: request mac not provided", ErrInvalidToken)
	}

	// The request is authenticated. Enforce timestamp recency and nonce
	// uniqueness (DESIGN.md §3.3): a stale/future timestamp or a replayed
	// nonce is rejected so a captured request cannot be re-minted later.
	if tr.Timestamp != 0 {
		ts := time.UnixMilli(tr.Timestamp)
		if d := time.Since(ts); d > tokenRequestTimestampWindow || d < -tokenRequestTimestampWindow {
			return fmt.Errorf("%w: %v", ErrTimestampNotCurrent, ts)
		}
	}
	if tr.Nonce != "" && !a.recordNonce(tr.Nonce) {
		return fmt.Errorf("%w: %q", ErrNonceReplayed, tr.Nonce)
	}
	return nil
}

// recordNonce records a token-request nonce and reports whether it was
// previously unseen within the timestamp window. A repeat (still within
// the window) returns false so the caller rejects the replay. Expired
// entries are reaped opportunistically to keep the set bounded — a nonce
// older than the window would fail the timestamp check regardless.
func (a *Authenticator) recordNonce(nonce string) bool {
	now := time.Now()
	a.nonceMu.Lock()
	defer a.nonceMu.Unlock()
	if now.Sub(a.nonceReaped) > tokenRequestTimestampWindow {
		for n, exp := range a.seenNonces {
			if now.After(exp) {
				delete(a.seenNonces, n)
			}
		}
		a.nonceReaped = now
	}
	if exp, seen := a.seenNonces[nonce]; seen && now.Before(exp) {
		return false
	}
	a.seenNonces[nonce] = now.Add(tokenRequestTimestampWindow)
	return true
}

// MintToken issues an HS256 JWT for a validated TokenRequest, signed with
// the key's secret and carrying its name as the kid header. The token's
// clientId claim comes from the request; its capability is the requested
// capability narrowed against (intersected with) the signing key's
// capability (DESIGN.md §3.3), so the token grants only what both the
// request and the key permit — a request the key cannot grant at all is
// rejected. A request with no capability leaves the claim unset, so the
// minted token inherits the key's capability at verification time.
// Returns the signed token and its expiry.
func (a *Authenticator) MintToken(tr *TokenRequest) (token string, issued, expires time.Time, capability string, err error) {
	key, ok := a.byName[tr.KeyName]
	if !ok {
		return "", time.Time{}, time.Time{}, "", fmt.Errorf("%w: unknown key %q", ErrInvalidToken, tr.KeyName)
	}
	issued = time.Now()
	ttl := time.Duration(tr.TTL) * time.Millisecond
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	expires = issued.Add(ttl)

	claims := jwt.MapClaims{
		"iat": issued.Unix(),
		// exp carries sub-second precision so a short (sub-second) ttl yields
		// a token that actually expires when it should, rather than staying
		// valid for up to a whole second (DESIGN.md §3.3).
		"exp": float64(expires.UnixNano()) / float64(time.Second),
	}

	// The granted capability defaults to the key's own (echoed verbatim so it
	// matches how the key was provisioned); a requested capability narrows it
	// via intersection and is stamped on the token (DESIGN.md §3.3).
	capability = key.CapabilityString()
	if tr.Capability != "" {
		requested, perr := ParseCapability(tr.Capability)
		if perr != nil {
			return "", time.Time{}, time.Time{}, "", fmt.Errorf("%w: %v", ErrInvalidCapability, perr)
		}
		narrowed := requested.Intersect(key.Capability())
		if narrowed.IsEmpty() {
			return "", time.Time{}, time.Time{}, "", ErrCapabilityDenied
		}
		capability = narrowed.String()
		claims["x-ably-capability"] = capability
	}
	if tr.ClientID != "" {
		claims["x-ably-clientId"] = tr.ClientID
	}
	if tr.Nonce != "" {
		// Bind the token to the request nonce so distinct requests yield
		// distinct tokens even within the same second.
		claims["jti"] = tr.Nonce
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	t.Header["kid"] = key.Name()
	token, err = t.SignedString([]byte(key.KeySecret))
	return token, issued, expires, capability, err
}

// ValidateTTL checks a requested token ttl (milliseconds) is neither
// negative nor excessive (DESIGN.md §3.3). A zero ttl means "use the
// default" and is accepted; anything above maxTokenTTL is rejected.
// Returns ErrInvalidTTL on violation.
func ValidateTTL(ttlMs int64) error {
	if ttlMs < 0 {
		return fmt.Errorf("%w: negative ttl %d", ErrInvalidTTL, ttlMs)
	}
	if time.Duration(ttlMs)*time.Millisecond > maxTokenTTL {
		return fmt.Errorf("%w: ttl %d exceeds maximum", ErrInvalidTTL, ttlMs)
	}
	return nil
}

// hmacOf returns the HMAC-SHA256 of text keyed with the given key's
// secret.
func hmacOf(key APIKey, text string) []byte {
	h := hmac.New(sha256.New, []byte(key.KeySecret))
	h.Write([]byte(text))
	return h.Sum(nil)
}

// extractToken returns a presented bearer token. The Authorization
// header (Bearer scheme) wins over the query parameters; both
// access_token (the form ably SDKs send) and accessToken are accepted.
func extractToken(r *http.Request) (string, bool) {
	if h := r.Header.Get("Authorization"); h != "" {
		if t, ok := strings.CutPrefix(h, "Bearer "); ok {
			t = strings.TrimSpace(t)
			if t == "" {
				return "", false
			}
			// Ably sends the token base64-encoded in the Authorization
			// header (RSA3a). A raw token (e.g. a JWT, whose '.' separators
			// aren't valid base64) won't decode — fall back to it as-is.
			if dec, err := base64.StdEncoding.DecodeString(t); err == nil {
				return string(dec), true
			}
			return t, true
		}
	}
	q := r.URL.Query()
	for _, name := range []string{"access_token", "accessToken"} {
		if t := q.Get(name); t != "" {
			return t, true
		}
	}
	return "", false
}

// extractKey returns the presented key from a request. Basic auth wins
// over the query parameter when both are present.
func extractKey(r *http.Request) (string, bool) {
	if user, pass, ok := r.BasicAuth(); ok {
		return user + ":" + pass, true
	}
	if k := r.URL.Query().Get("key"); k != "" {
		return k, true
	}
	return "", false
}
