// Package auth implements credential parsing and verification for
// realtime and REST requests.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Errors returned by Authenticator.Authenticate.
var (
	ErrNoCredentials = errors.New("no credentials presented")
	ErrInvalidKey    = errors.New("invalid api key")
	ErrInvalidToken  = errors.New("invalid token")
)

// ErrClientIDMismatch is returned by ResolveClientID when the requested
// clientId is not permitted by the credential.
var ErrClientIDMismatch = errors.New("clientId not permitted by credential")

// WildcardClientID is the clientId marker (DESIGN.md §3.2) meaning the
// credential's bearer may assume any identity, choosing it per operation.
// It is never itself stamped as a message or member identity.
const WildcardClientID = "*"

// clockSkewLeeway is the tolerance applied to time-based JWT claims
// (iat, exp) to absorb small clock differences between token issuer and
// this server.
const clockSkewLeeway = 60 * time.Second

// APIKey is a parsed Ably-format API key in the form
// `appId.keyId:keySecret`.
type APIKey struct {
	AppID     string
	KeyID     string
	KeySecret string

	raw string // cached `appId.keyId:keySecret` for constant-time compare
}

// Name returns the key's `appId.keyId` portion — the value carried as a
// JWT `kid` header and as the key name in token-request paths.
func (k APIKey) Name() string {
	return k.AppID + "." + k.KeyID
}

// Capability returns the key's capability. In this single-key model keys
// are not individually scoped, so every key carries the full capability
// {"*":["*"]} — the ceiling a token minted from it can be narrowed to
// (DESIGN.md §3.1, §3.3).
func (k APIKey) Capability() Capability {
	return AllowAllCapability()
}

// ParseAPIKey validates and decomposes an Ably-format API key. All
// three components must be non-empty.
func ParseAPIKey(s string) (APIKey, error) {
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

	return APIKey{
		AppID:     appID,
		KeyID:     keyID,
		KeySecret: secret,
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
	// (DESIGN.md §3.1): the permissive all-access set for Basic auth or a
	// token with no capability claim, otherwise the parsed claim.
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
		keys:   keys,
		byName: byName,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"HS256"}),
			jwt.WithLeeway(clockSkewLeeway),
			jwt.WithIssuedAt(),           // reject iat in the future (beyond leeway)
			jwt.WithExpirationRequired(), // exp must be present
		),
	}
}

// matchKey reports whether presented equals any configured key, in
// constant time. Every key is compared (no early return) so the timing
// does not reveal which key, if any, matched.
func (a *Authenticator) matchKey(presented string) bool {
	pb := []byte(presented)
	matched := 0
	for _, k := range a.keys {
		matched |= subtle.ConstantTimeCompare(pb, []byte(k.raw))
	}
	return matched == 1
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
		if !a.matchKey(k) {
			return nil, ErrInvalidKey
		}
		return &Principal{Method: MethodBasic, cap: AllowAllCapability()}, nil
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
	_, err := a.parser.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		if kid, ok := t.Header["kid"].(string); ok {
			if k, ok := a.byName[kid]; ok {
				return []byte(k.KeySecret), nil
			}
		}
		set := jwt.VerificationKeySet{Keys: make([]jwt.VerificationKey, len(a.keys))}
		for i, k := range a.keys {
			set.Keys[i] = []byte(k.KeySecret)
		}
		return set, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if _, ok := claims["iat"]; !ok {
		return nil, fmt.Errorf("%w: missing iat claim", ErrInvalidToken)
	}

	p := &Principal{Method: MethodToken, cap: AllowAllCapability()}
	if c, ok := claims["x-ably-capability"].(string); ok {
		p.Capability = c
		// A present capability claim narrows access (§3.1); a malformed
		// one makes the token unusable.
		cap, err := ParseCapability(c)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		p.cap = cap
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
	if tr.MAC != "" {
		expected := base64.StdEncoding.EncodeToString(hmacOf(key, tr.tokenRequestText()))
		if subtle.ConstantTimeCompare([]byte(tr.MAC), []byte(expected)) != 1 {
			return fmt.Errorf("%w: request mac does not match", ErrInvalidToken)
		}
		return nil
	}
	// Unsigned: only a Basic-auth request for the same key is trusted.
	if k, ok := extractKey(r); ok && subtle.ConstantTimeCompare([]byte(k), []byte(key.raw)) == 1 {
		return nil
	}
	return fmt.Errorf("%w: request mac not provided", ErrInvalidToken)
}

// MintToken issues an HS256 JWT for a validated TokenRequest, signed with
// the key's secret and carrying its name as the kid header. The token's
// clientId claim comes from the request; its capability is the requested
// capability narrowed against (intersected with) the signing key's
// capability (DESIGN.md §3.3) — for this single-key model the key carries
// the full `{"*":["*"]}` capability, so a requested capability passes
// through unchanged but a syntactically invalid one is rejected. Returns
// the signed token and its expiry.
func (a *Authenticator) MintToken(tr *TokenRequest) (token string, issued, expires time.Time, err error) {
	key, ok := a.byName[tr.KeyName]
	if !ok {
		return "", time.Time{}, time.Time{}, fmt.Errorf("%w: unknown key %q", ErrInvalidToken, tr.KeyName)
	}
	issued = time.Now()
	ttl := time.Duration(tr.TTL) * time.Millisecond
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	expires = issued.Add(ttl)

	claims := jwt.MapClaims{
		"iat": issued.Unix(),
		"exp": expires.Unix(),
	}
	if tr.Capability != "" {
		requested, perr := ParseCapability(tr.Capability)
		if perr != nil {
			return "", time.Time{}, time.Time{}, fmt.Errorf("%w: %v", ErrInvalidToken, perr)
		}
		narrowed := requested.Intersect(key.Capability())
		if narrowed.IsEmpty() {
			return "", time.Time{}, time.Time{}, fmt.Errorf("%w: requested capability is not permitted by the key", ErrInvalidToken)
		}
		claims["x-ably-capability"] = narrowed.String()
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
	return token, issued, expires, err
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
